# Implementation Plan: Add --backend CLI option to fyr run

**Task:** typedata-y01 — M004-E003-S001
**Mission:** M004 — Production backend: generic Ibis executor with BigQuery support
**Epic:** M004-E003 — CLI --backend flag

## Prerequisites (must land first)
- **typedata-wrn** (M004-E002-S003): Runner creates IbisExecutor via connection factory — currently `open` with `plan` label. The CLI plumbing can be wired up and tested independently, since the run command already passes `backend=cfg.backend` to the runner. The runner's backend guard (`if backend != "duckdb"`) still exists but that's the dependency's concern, not this story's.

## Step-by-step Changes

### Step 1: Add `backend` param to `resolve_config()` in `src/fyrnheim/config.py`

**Line 88–93** — Add `backend: str | None = None` to the signature:
```python
def resolve_config(
    *,
    entities_dir: str | None = None,
    data_dir: str | None = None,
    output_dir: str | None = None,
    backend: str | None = None,
) -> ResolvedConfig:
```

**Line 105** — Apply the override in the return statement:
```python
backend=backend if backend is not None else (config.backend if config else "duckdb"),
```

This follows the exact same pattern as `entities_dir`, `data_dir`, and `output_dir` overrides: CLI param takes precedence when not None, else config file value, else built-in default.

### Step 2: Add `--backend` Click option to `run` command in `src/fyrnheim/cli.py`

**Line 316–321** — Add the option decorator and parameter:
```python
@main.command()
@click.option("--entity", "entity_name", default=None, help="Run a single entity by name.")
@click.option("--backend", default=None, help="Override backend engine (e.g. duckdb, bigquery).")
@click.option("--entities-dir", type=click.Path(), default=None, help="Override entities directory.")
@click.option("--data-dir", type=click.Path(), default=None, help="Override data directory.")
@click.option("--output-dir", type=click.Path(), default=None, help="Override generated output directory.")
@handle_errors
def run(entity_name: str | None, backend: str | None, entities_dir: str | None, data_dir: str | None, output_dir: str | None) -> None:
```

**Line 327** — Pass `backend` to `resolve_config()`:
```python
cfg = resolve_config(entities_dir=entities_dir, data_dir=data_dir, output_dir=output_dir, backend=backend)
```

No other changes needed in `cli.py` — lines 349, 352, 364, and 367 already use `cfg.backend` for display and engine calls.

### Step 3: Add tests in `tests/test_cli_run.py`

**Test 1: `test_backend_override_passed_to_runner`** — Verify `--backend bigquery` passes through to `engine_run()`:
```python
def test_backend_override_passed_to_runner(self, tmp_path, monkeypatch):
    _make_project(tmp_path, ["test"])
    monkeypatch.chdir(tmp_path)
    with patch("fyrnheim.engine.runner.run", return_value=_success_result(backend="bigquery")) as mock_run:
        result = CliRunner().invoke(main, ["run", "--backend", "bigquery"])
    mock_run.assert_called_once()
    assert mock_run.call_args.kwargs["backend"] == "bigquery"
```

**Test 2: `test_backend_override_shown_in_output`** — Verify output displays the overridden backend:
```python
def test_backend_override_shown_in_output(self, tmp_path, monkeypatch):
    _make_project(tmp_path, ["test"])
    monkeypatch.chdir(tmp_path)
    with patch("fyrnheim.engine.runner.run", return_value=_success_result(backend="bigquery")):
        result = CliRunner().invoke(main, ["run", "--backend", "bigquery"])
    assert "Running on bigquery" in result.output
```

**Test 3: `test_backend_in_help`** — Verify `--backend` appears in help:
```python
def test_backend_in_help(self):
    result = CliRunner().invoke(main, ["run", "--help"])
    assert "--backend" in result.output
```

**Test 4: `test_backend_default_from_config`** — Verify default comes from config when `--backend` not specified:
```python
def test_backend_default_from_config(self, tmp_path, monkeypatch):
    proj = _make_project(tmp_path, ["test"])
    (tmp_path / "fyrnheim.yaml").write_text(
        f"entities_dir: {tmp_path / 'entities'}\ndata_dir: {tmp_path / 'data'}\nbackend: bigquery\n"
    )
    monkeypatch.chdir(tmp_path)
    with patch("fyrnheim.engine.runner.run", return_value=_success_result(backend="bigquery")) as mock_run:
        result = CliRunner().invoke(main, ["run"])
    assert mock_run.call_args.kwargs["backend"] == "bigquery"
```

### Step 4: Add config test in `tests/test_config.py`

**Test: `test_backend_override`** — Verify `resolve_config(backend=...)` overrides config file:
```python
def test_backend_override(self, tmp_path, monkeypatch):
    (tmp_path / "fyrnheim.yaml").write_text("backend: duckdb\n")
    monkeypatch.chdir(tmp_path)
    cfg = resolve_config(backend="bigquery")
    assert cfg.backend == "bigquery"
```

## Files Modified
| File | Changes |
|------|---------|
| `src/fyrnheim/config.py` | Add `backend` param to `resolve_config()` (2 lines) |
| `src/fyrnheim/cli.py` | Add `--backend` option decorator + param + pass to `resolve_config()` (3 lines) |
| `tests/test_cli_run.py` | Add 4 tests in new `TestRunBackendOption` class |
| `tests/test_config.py` | Add 1 test in `TestResolveConfig` class |

## Verification
```bash
uv run pytest tests/test_cli_run.py tests/test_config.py -v
uv run pytest -x  # full suite regression
uv run ruff check src/fyrnheim/config.py src/fyrnheim/cli.py
uv run mypy src/fyrnheim/config.py src/fyrnheim/cli.py
```

## Risk Assessment
- **Very low risk**: Two production files changed, both with small, pattern-following edits
- **No behavioral change for default path**: When `--backend` is not specified, behavior is identical to today
- **Dependency note**: The runner still has a `backend != "duckdb"` guard (typedata-wrn's job to remove). Running `--backend bigquery` will hit that guard and return an error, which is expected until the dependency lands.

---
*Plan created via /plan-issue*
