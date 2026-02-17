# Diagnostic Messages Design (M002-E003-S002)

## Overview

Add specific, user-friendly error messages for every common failure mode in the `fyr` CLI. Each failure should produce an `Error:` line with a precise description plus a `Hint:` line guiding the user to the fix. All 7 scenarios are tested via `CliRunner` with exit code 1.

## Problem Statement

The error handler from S001 catches exceptions generically. This story maps each specific failure condition to a tailored diagnostic message with exact wording, so that new users never have to interpret raw Python exceptions.

## Depends On

- **M002-E003-S001**: Shared error handler with `handle_errors` decorator, `_print_error(message, hint=...)` helper, and `--verbose` flag.
- **M002-E001-S002**: `load_config()` for `fyrnheim.yaml` resolution (config-not-found path).
- **M002-E002-S003**: `fyr run` command (the primary surface for most of these errors).

## Error-to-Hint Mapping

| # | Failure Condition | Exception Type | Source Location | Error Message | Hint Message |
|---|---|---|---|---|---|
| 1 | Missing `fyrnheim.yaml` | `FileNotFoundError` (from `load_config()`) | `cli.py` / `config.py` | `No fyrnheim.yaml found.` | `Run \`fyr init\` to create a project.` |
| 2 | Missing entities dir | `FileNotFoundError` (from `EntityRegistry.discover()` or `run()`) | `engine/registry.py:43`, `engine/runner.py:257` | `Entities directory not found: {path}.` | `Check entities_dir in fyrnheim.yaml.` |
| 3 | No entities found | (no exception -- `len(registry) == 0`) | `engine/runner.py:266-272` | `No entities found in {dir}.` | `Create a .py file with \`entity = Entity(...)\`.` |
| 4 | Missing data file | `SourceNotFoundError` (from `DuckDBExecutor.register_parquet()`) | `engine/executor.py:76` | `Data file not found: {path} (entity: {name}).` | `Check data_dir in fyrnheim.yaml.` |
| 5 | DuckDB not installed | `ImportError` / `ModuleNotFoundError` | `engine/executor.py:53` (ibis.duckdb.connect) | `DuckDB backend not available.` | `Install with: pip install fyrnheim[duckdb]` |
| 6 | Entity syntax error | `SyntaxError` or `Exception` (from `importlib exec_module`) | `engine/registry.py:64` | `Error loading {file}: {error} (line {lineno})` | *(none -- the message is self-contained)* |
| 7 | Duplicate entity name | `ValueError` (from `EntityRegistry.discover()`) | `engine/registry.py:76-79` | `Duplicate entity name "{name}": defined in {path1} and {path2}` | *(none -- the existing message already includes both paths)* |

## Implementation Plan

### 1. Add `_diagnostic_hint()` function to `cli.py`

A single function that inspects an exception and returns the appropriate hint string (or `None`). This keeps the `handle_errors` decorator clean and makes hint logic testable in isolation.

```python
def _diagnostic_hint(exc: Exception) -> str | None:
    """Return a user-facing hint for a caught exception, or None."""
    msg = str(exc)

    if isinstance(exc, FileNotFoundError):
        if "fyrnheim.yaml" in msg or "No fyrnheim.yaml" in msg:
            return "Run `fyr init` to create a project."
        if "Entities directory" in msg or "entities" in msg.lower():
            return "Check entities_dir in fyrnheim.yaml."
        # Generic file-not-found (likely data file)
        return "Check data_dir in fyrnheim.yaml."

    if isinstance(exc, SourceNotFoundError):
        return "Check data_dir in fyrnheim.yaml."

    if isinstance(exc, ImportError) and "duckdb" in msg.lower():
        return "Install with: pip install fyrnheim[duckdb]"

    if isinstance(exc, ValueError) and "Duplicate entity" in msg:
        return None  # message already includes both paths

    if isinstance(exc, (SyntaxError, ImportError)):
        return None  # message includes file + line

    return None
```

### 2. Refine error messages at the raise sites

Some messages need small adjustments to match the required wording exactly:

| Location | Current Message | Required Change |
|---|---|---|
| `registry.py:43` | `"Entities directory not found: {entities_dir}"` | Already matches. No change needed. |
| `registry.py:76-79` | `"Duplicate entity name '{name}': defined in {existing_path} and {entity_file}"` | Already matches. No change needed. |
| `executor.py:76` | `"Parquet file not found: {path}"` | Change to: `"Data file not found: {path}"` and add entity context (see below). |
| `runner.py:266-272` | Returns empty `RunResult` (no error) | Must raise or signal so the CLI can produce the "No entities found" message. |

### 3. Propagate entity name into `SourceNotFoundError`

The current `register_parquet()` does not know the entity name. Two options:

**Option A (preferred)**: Change `_register_entity_source()` in `runner.py` to catch `SourceNotFoundError` and re-raise with entity context:

```python
def _register_entity_source(executor, entity, data_dir):
    source = entity.source
    if source is None:
        return
    duckdb_path = getattr(source, "duckdb_path", None)
    if duckdb_path:
        resolved = data_dir / duckdb_path
        try:
            executor.register_parquet(f"source_{entity.name}", resolved)
        except SourceNotFoundError:
            raise SourceNotFoundError(
                f"Data file not found: {resolved} (entity: {entity.name})"
            )
```

**Option B**: Pass entity name down into `register_parquet()`. This changes the executor API, so Option A is preferred.

### 4. Surface "no entities found" to the CLI

Currently `run()` returns an empty `RunResult` when no entities are discovered. The CLI `fyr run` command should check for this and produce the diagnostic:

```python
@main.command()
@handle_errors
def run(...):
    config = load_config()
    result = fyrnheim.run(config.entities_dir, config.data_dir, ...)
    if not result.entities:
        _print_error(
            f"No entities found in {config.entities_dir}.",
            hint="Create a .py file with `entity = Entity(...)`.",
        )
        raise SystemExit(1)
    ...
```

This avoids changing the library-level `run()` behavior (which reasonably returns an empty result rather than raising).

### 5. Add early DuckDB availability check in `fyr run` and `fyr check`

Before calling into the engine, guard against missing DuckDB:

```python
def _check_duckdb_available() -> None:
    try:
        import duckdb  # noqa: F401
    except ImportError:
        _print_error(
            "DuckDB backend not available.",
            hint="Install with: pip install fyrnheim[duckdb]",
        )
        raise SystemExit(1)
```

This provides a clean message instead of the deep `ibis.duckdb.connect` traceback.

### 6. Handle entity syntax errors in the error handler

When `EntityRegistry.discover()` calls `spec.loader.exec_module(module)`, a `SyntaxError` in the entity file propagates with `filename` and `lineno` attributes. The `handle_errors` decorator should format these:

```python
if isinstance(exc, SyntaxError) and exc.filename:
    msg = f"Error loading {exc.filename}: {exc.msg} (line {exc.lineno})"
    _print_error(msg)
    raise SystemExit(1)
```

For non-SyntaxError exceptions during entity loading (e.g., `NameError`, `TypeError`), `registry.py:64` currently lets them propagate with the module path in the traceback. We can catch and wrap in `handle_errors` by pattern-matching on the exception message containing `_fyrnheim_entity_`.

### 7. Integrate with `handle_errors` decorator (from S001)

Update the `handle_errors` decorator to use `_diagnostic_hint()`:

```python
def handle_errors(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except SystemExit:
            raise  # already handled
        except SyntaxError as e:
            if e.filename:
                _print_error(f"Error loading {e.filename}: {e.msg} (line {e.lineno})")
            else:
                _print_error(str(e), hint=_diagnostic_hint(e))
            raise SystemExit(1)
        except (FileNotFoundError, ValueError, ImportError,
                SourceNotFoundError, FyrnheimEngineError) as e:
            hint = _diagnostic_hint(e)
            _print_error(str(e), hint=hint)
            if ctx.obj.get("verbose"):
                import traceback
                traceback.print_exc()
            raise SystemExit(1)
        except Exception as e:
            _print_error(f"Unexpected error: {e}")
            raise SystemExit(1)
    return wrapper
```

## Files Changed

| File | Change |
|---|---|
| `src/fyrnheim/cli.py` | Add `_diagnostic_hint()`, `_check_duckdb_available()`, "no entities" check in `fyr run`, integrate with `handle_errors` |
| `src/fyrnheim/engine/runner.py` | Wrap `_register_entity_source()` to add entity name to `SourceNotFoundError` |
| `src/fyrnheim/engine/errors.py` | No changes (existing hierarchy is sufficient) |
| `src/fyrnheim/engine/registry.py` | No changes (existing messages already match) |
| `tests/test_cli_diagnostics.py` | New: 7 scenario tests + exit code assertions |

## Test Plan

All tests use `click.testing.CliRunner` to invoke `fyr run` (or the relevant command) against a broken project structure created in `tmp_path`. Each test asserts the exact error substring appears in `result.output` and `result.exit_code == 1`.

### Test Fixtures

A shared `tmp_path`-based helper creates minimal project structures:

```python
def _make_project(tmp_path, *, yaml=True, entities_dir=True, entity_file=None, data_file=True):
    """Create a minimal fyrnheim project structure for testing."""
    if yaml:
        config = {"entities_dir": "entities", "data_dir": "data"}
        (tmp_path / "fyrnheim.yaml").write_text(yaml.dump(config))
    if entities_dir:
        (tmp_path / "entities").mkdir()
    if entity_file:
        (tmp_path / "entities" / entity_file[0]).write_text(entity_file[1])
    if data_file:
        (tmp_path / "data").mkdir(exist_ok=True)
        # create a minimal parquet if needed
```

### Test Cases

| # | Test Function | Setup | Assert Substring | Exit Code |
|---|---|---|---|---|
| 1 | `test_missing_config_file` | No `fyrnheim.yaml` | `"No fyrnheim.yaml found"` + `"fyr init"` | 1 |
| 2 | `test_missing_entities_dir` | Config points to nonexistent `entities/` | `"Entities directory not found"` + `"entities_dir in fyrnheim.yaml"` | 1 |
| 3 | `test_no_entities_found` | Empty `entities/` dir, valid config | `"No entities found"` + `"entity = Entity(...)"` | 1 |
| 4 | `test_missing_data_file` | Valid entity, no matching parquet | `"Data file not found"` + `"data_dir in fyrnheim.yaml"` | 1 |
| 5 | `test_duckdb_not_installed` | Mock `import duckdb` to raise `ImportError` | `"DuckDB backend not available"` + `"pip install fyrnheim[duckdb]"` | 1 |
| 6 | `test_entity_syntax_error` | Entity file with invalid Python | `"Error loading"` + `"line"` | 1 |
| 7 | `test_duplicate_entity_name` | Two entity files defining same name | `"Duplicate entity name"` + both file paths | 1 |

### Test #5 Implementation Detail (DuckDB mock)

Since DuckDB is installed in the dev environment, use `unittest.mock.patch` to simulate its absence:

```python
def test_duckdb_not_installed(tmp_path):
    _make_project(tmp_path, entity_file=("customers.py", VALID_ENTITY))
    runner = CliRunner()
    with mock.patch.dict("sys.modules", {"duckdb": None}):
        result = runner.invoke(main, ["run"], catch_exceptions=False)
    assert result.exit_code == 1
    assert "DuckDB backend not available" in result.output
    assert "pip install fyrnheim[duckdb]" in result.output
```

### Test #6 Implementation Detail (Syntax Error)

```python
def test_entity_syntax_error(tmp_path):
    bad_code = "def broken(\n"  # SyntaxError on line 1
    _make_project(tmp_path, entity_file=("bad.py", bad_code))
    runner = CliRunner()
    result = runner.invoke(main, ["run"], catch_exceptions=False)
    assert result.exit_code == 1
    assert "Error loading" in result.output
    assert "line" in result.output
```

## Design Decisions

1. **Hint logic lives in the CLI layer, not the engine.** The engine raises domain exceptions with technical messages; the CLI translates them into user-facing guidance. This keeps the library usable without Click.

2. **`_diagnostic_hint()` is a pure function.** It takes an exception and returns a string. This makes it easy to unit-test directly without CliRunner if desired.

3. **No new exception types.** The existing hierarchy (`SourceNotFoundError`, `TransformModuleError`, `ExecutionError`, plus stdlib `FileNotFoundError`, `ValueError`, `ImportError`, `SyntaxError`) covers all 7 scenarios. Adding new types would be premature.

4. **"No entities found" is checked at the CLI layer.** The library `run()` function reasonably returns an empty result rather than raising. The CLI interprets this as an error and adds the hint.

5. **Entity name added to `SourceNotFoundError` via re-raise in `_register_entity_source()`.** This is the minimal change that avoids altering the executor API.

## Success Criteria

- All 7 error scenarios produce the exact wording from the acceptance criteria
- Each scenario exits with code 1
- Without `--verbose`, no Python tracebacks appear
- With `--verbose`, full tracebacks are shown
- Existing tests remain passing
