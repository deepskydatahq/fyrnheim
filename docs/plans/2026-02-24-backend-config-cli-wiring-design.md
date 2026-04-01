# Backend Config CLI Wiring Design

## Overview
Wire `backend_config` from resolved config through the CLI to `engine_run()` and `engine_run_entity()`. This is a pure routing change — the CLI passes data through without inspection or validation.

## Problem Statement
BigQuery (and future backends) need connection parameters (project_id, dataset_id) to flow from `fyrnheim.yaml` through the CLI to the connection factory. The config layer and runner already support `backend_config` (via prior stories M004-E002-S002 and M004-E002-S003). This story completes the wiring at the CLI layer.

## Expert Perspectives

### Technical
- The CLI is a router, not a validator. No shape or content validation of `backend_config` at the CLI layer.
- Validation belongs in `ResolvedConfig` (construction time) and engine (execution time).
- Test via `assert_called_with` on mocks — minimal, non-brittle assertion that proves the wiring contract.
- Regression test with `backend_config={}` (empty dict) proves DuckDB happy path is unbroken.

### Simplification Review
- Reviewer flagged that `backend_config` doesn't exist on `ResolvedConfig` yet — this is expected since prior stories (M004-E002-S002) add it. The dependency chain is intentional.
- No actual cuts needed. The design is already minimal: 2-line production change + test updates.

## Proposed Solution

### 1. `src/fyrnheim/cli.py` — run command (2-line change)
- Add `backend_config=cfg.backend_config` to the `engine_run()` call
- Add `backend_config=cfg.backend_config` to the `engine_run_entity()` call

### 2. `tests/test_cli_run.py` — test updates
- Update existing mock assertions to expect `backend_config` kwarg
- Add regression test: `test_run_default_duckdb_passes_empty_backend_config`
  - CliRunner invokes `fyr run` with no --backend flag
  - Assert `engine_run` called with `backend_config={}`

### 3. No other files changed
No new modules, no new abstractions, no config validation logic.

## Alternatives Considered
- **Validate backend_config at CLI layer** — Rejected. Creates coupling between CLI and engine. Each layer validates its own inputs.
- **Pass entire config object instead of backend_config** — Rejected. Explicit kwarg is clearer than implicit config bag.

## Success Criteria
- CLI passes `backend_config=cfg.backend_config` to `engine_run()`
- CLI passes `backend_config` to `engine_run_entity()` for single entity mode
- `fyr run` with backend=duckdb works as before (regression test)

---

## Implementation Plan

### Prerequisites

This story depends on:
- **M004-E002-S002** (backend-config-yaml-design): Adds `backend_config: dict` to `ResolvedConfig`
- **M004-E002-S003** (runner-connection-factory-wiring-design): Adds `backend_config` param to `run()` and `run_entity()`
- **M004-E003-S001** (backend-cli-flag-design): Adds `--backend` CLI flag

After those land, `cfg.backend_config` will exist on `ResolvedConfig` and `engine_run()`/`engine_run_entity()` will accept `backend_config` kwarg.

### Step 1: Add `backend_config` to `engine_run_entity()` call in `cli.py`

**File:** `src/fyrnheim/cli.py` line 352

```python
# BEFORE
er = engine_run_entity(info.entity, cfg.data_dir, backend=cfg.backend, generated_dir=cfg.output_dir)

# AFTER
er = engine_run_entity(info.entity, cfg.data_dir, backend=cfg.backend, generated_dir=cfg.output_dir, backend_config=cfg.backend_config)
```

### Step 2: Add `backend_config` to `engine_run()` call in `cli.py`

**File:** `src/fyrnheim/cli.py` line 367

```python
# BEFORE
result = engine_run(cfg.entities_dir, cfg.data_dir, backend=cfg.backend, generated_dir=cfg.output_dir)

# AFTER
result = engine_run(cfg.entities_dir, cfg.data_dir, backend=cfg.backend, generated_dir=cfg.output_dir, backend_config=cfg.backend_config)
```

### Step 3: Update existing test mock assertions in `tests/test_cli_run.py`

The existing tests patch `fyrnheim.engine.runner.run` and `fyrnheim.engine.runner.run_entity` with `return_value` mocks. Since the production code now passes `backend_config` to these functions, the mocks will accept it automatically (they don't assert on call args). **No existing tests need changes** — they use `return_value` mocks that accept any kwargs.

Verify by running:
```bash
uv run pytest tests/test_cli_run.py -v
```

### Step 4: Add regression test for DuckDB default path

Append to `tests/test_cli_run.py`:

```python
class TestRunBackendConfig:
    def test_run_default_duckdb_passes_empty_backend_config(self, tmp_path, monkeypatch):
        """Regression: fyr run without --backend passes backend_config={} to engine_run."""
        _make_project(tmp_path, ["test"])
        monkeypatch.chdir(tmp_path)
        with patch("fyrnheim.engine.runner.run", return_value=_success_result()) as mock_run:
            result = CliRunner().invoke(main, ["run"])
        assert result.exit_code == 0
        mock_run.assert_called_once()
        call_kwargs = mock_run.call_args
        assert call_kwargs.kwargs.get("backend_config") == {}

    def test_run_entity_passes_backend_config(self, tmp_path, monkeypatch):
        """Single entity mode passes backend_config to engine_run_entity."""
        _make_project(tmp_path, ["alpha"])
        monkeypatch.chdir(tmp_path)
        er = EntityRunResult(entity_name="alpha", status="success", row_count=5, duration_seconds=0.2)
        with patch("fyrnheim.engine.runner.run_entity", return_value=er) as mock_run_entity:
            result = CliRunner().invoke(main, ["run", "--entity", "alpha"])
        assert result.exit_code == 0
        mock_run_entity.assert_called_once()
        call_kwargs = mock_run_entity.call_args
        assert call_kwargs.kwargs.get("backend_config") == {}
```

### Step 5: Verify

```bash
# All CLI run tests pass
uv run pytest tests/test_cli_run.py -v

# Full suite still passes
uv run pytest

# Lint
uv run ruff check src/fyrnheim/cli.py tests/test_cli_run.py
uv run mypy src/fyrnheim/cli.py
```

### Summary of changes

| File | Change | Lines |
|------|--------|-------|
| `src/fyrnheim/cli.py` | Add `backend_config=cfg.backend_config` to 2 call sites | ~352, ~367 |
| `tests/test_cli_run.py` | Add `TestRunBackendConfig` class with 2 tests | Append |

**Total: 2 production lines changed, 2 test methods added.**

---
*Plan created via /plan-issue*
