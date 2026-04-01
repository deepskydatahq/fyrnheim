# Implementation Plan: Wire backend_config from config through CLI to runner

**Task:** typedata-8rl — M004-E003-S002
**Mission:** M004 — Production backend: generic Ibis executor with BigQuery support
**Epic:** M004-E003 — CLI --backend flag

## Prerequisites (must land first)

| Task | Story | What it adds | Status |
|------|-------|-------------|--------|
| typedata-y01 | M004-E003-S001 | `--backend` CLI flag + `backend` param in `resolve_config()` | in_progress/plan |
| typedata-jdn | M004-E002-S002 | `backend_config: dict` field on `ResolvedConfig` | open/plan |
| typedata-wrn | M004-E002-S003 | `backend_config` kwarg on `run()` and `run_entity()` | open/ready |

After those land, `cfg.backend_config` will exist on `ResolvedConfig` and the runner functions will accept it.

## Step-by-step Changes

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

### Step 3: Add tests — `TestRunBackendConfig` in `tests/test_cli_run.py`

Append a new test class with 2 test methods:

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
        assert mock_run.call_args.kwargs.get("backend_config") == {}

    def test_run_entity_passes_backend_config(self, tmp_path, monkeypatch):
        """Single entity mode passes backend_config to engine_run_entity."""
        _make_project(tmp_path, ["alpha"])
        monkeypatch.chdir(tmp_path)
        er = EntityRunResult(entity_name="alpha", status="success", row_count=5, duration_seconds=0.2)
        with patch("fyrnheim.engine.runner.run_entity", return_value=er) as mock_run_entity:
            result = CliRunner().invoke(main, ["run", "--entity", "alpha"])
        assert result.exit_code == 0
        mock_run_entity.assert_called_once()
        assert mock_run_entity.call_args.kwargs.get("backend_config") == {}
```

### Step 4: Existing tests — no changes needed

Existing mocks use `return_value` and accept any kwargs — they pass as-is. Verified by inspection:
- All `patch("fyrnheim.engine.runner.run", ...)` calls use `return_value=...` without `assert_called_with` on specific kwargs.
- The `TestRunSingleEntity.test_single_entity_runs` patches `run_entity` the same way.

### Step 5: Verify

```bash
uv run pytest tests/test_cli_run.py -v
uv run pytest -x                          # full suite regression
uv run ruff check src/fyrnheim/cli.py tests/test_cli_run.py
uv run mypy src/fyrnheim/cli.py
```

## Files Modified

| File | Change | Lines |
|------|--------|-------|
| `src/fyrnheim/cli.py` | Add `backend_config=cfg.backend_config` to 2 call sites | ~352, ~367 |
| `tests/test_cli_run.py` | Append `TestRunBackendConfig` class with 2 tests | End of file |

**Total: 2 production lines changed, 2 test methods added.**

## Risk Assessment

- **Very low risk**: Only 2 production lines change, both following the existing kwarg pattern
- **No behavioral change for default path**: When `backend_config={}`, runner behavior is identical
- **No existing tests break**: Mocks accept any kwargs via `return_value`

---
*Plan created via /plan-issue*
