# Shared Error Handler and --verbose Flag Design (M002-E003-S001)

## Overview
Add a shared error-handling decorator and a top-level `--verbose/-v` flag to the `fyr` CLI. Without `--verbose`, errors print a single `Error:` line plus a `Hint:` line to stderr -- no traceback, no debug noise. With `--verbose`, full tracebacks and DEBUG-level logging are enabled.

## Problem Statement
Once all CLI commands are implemented (init, generate, run, check, list), each one can raise `FileNotFoundError`, `ValueError`, `ImportError`, or engine-specific errors (`SourceNotFoundError`, `TransformModuleError`, `ExecutionError`). Without centralized handling, users see raw Python tracebacks that obscure the actionable fix. Duplicating try/except blocks in every command is brittle and inconsistent.

## Expert Perspectives

### Technical
- A decorator is the right pattern -- it wraps each Click command function, catching exceptions in a single place without modifying command logic.
- The `--verbose` flag must live on the `main` group (not individual commands) because it controls global behavior (logging level, traceback display).
- Click's `pass_context` propagates the verbose flag from the group to the decorator via `ctx.obj`.
- Logging should be configured once in the `main` group callback, before any command runs.

### Simplification Review
- Verdict: APPROVED -- this is the minimum viable error UX.
- No abstract error registry or error-code system. Just a mapping of exception type to hint string.
- No structured JSON error output (premature; can be added later if needed).
- The decorator is a single function, not a class hierarchy.

## Proposed Solution

### 1. `--verbose` flag on the main group

Modify the `main` Click group in `src/fyrnheim/cli.py` to accept `--verbose/-v`:

```python
@click.group()
@click.version_option(version=__version__, prog_name="fyr")
@click.option("-v", "--verbose", is_flag=True, help="Show full tracebacks and debug logging.")
@click.pass_context
def main(ctx: click.Context, verbose: bool) -> None:
    """fyrnheim -- typed entity transformations."""
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose

    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(name)s: %(message)s" if verbose else "%(message)s",
        force=True,
    )
```

Key points:
- `force=True` on `basicConfig` ensures reconfiguration works even if logging was already initialized by an import side-effect.
- DEBUG format includes the logger name (`fyrnheim.engine: ...`) for traceability. WARNING format is bare.
- `ctx.obj["verbose"]` is read by the error handler decorator.

### 2. Error handler decorator

New function `handle_errors` in `src/fyrnheim/cli.py` (not a separate module -- the CLI is still a single file per the skeleton design):

```python
import functools
import sys
import traceback

from fyrnheim.engine.errors import FyrnheimEngineError


# Exception type -> hint message
_ERROR_HINTS: dict[type[Exception], str] = {
    FileNotFoundError: "Check that the path exists. Run `fyr init` to create a project.",
    ValueError: "Check your fyrnheim.yaml configuration values.",
    ImportError: (
        "A required dependency may be missing. "
        "Try: pip install fyrnheim[duckdb]"
    ),
}

# Engine error subclasses get more specific hints
_ENGINE_HINTS: dict[type[Exception], str] = {
    SourceNotFoundError: "Check your entity source paths and data directory.",
    TransformModuleError: "Run `fyr generate` to regenerate transform code.",
    ExecutionError: "Check entity definitions and source data. Use --verbose for details.",
}


def handle_errors(f):
    """Decorator that catches known exceptions and prints actionable messages."""

    @functools.wraps(f)
    @click.pass_context
    def wrapper(ctx: click.Context, *args, **kwargs):
        try:
            return ctx.invoke(f, *args, **kwargs)
        except SystemExit:
            raise  # Let explicit exits through
        except Exception as exc:
            verbose = ctx.obj.get("verbose", False) if ctx.obj else False

            if verbose:
                traceback.print_exc(file=sys.stderr)
            else:
                _print_error(exc)

            raise SystemExit(1)

    return wrapper
```

The `_print_error` helper:

```python
def _print_error(exc: Exception) -> None:
    """Print a formatted error + hint to stderr."""
    click.echo(f"Error: {exc}", err=True)

    # Try engine-specific hints first (more specific), then general hints
    hint = None
    for exc_type, exc_hint in _ENGINE_HINTS.items():
        if isinstance(exc, exc_type):
            hint = exc_hint
            break

    if hint is None:
        for exc_type, exc_hint in _ERROR_HINTS.items():
            if isinstance(exc, exc_type):
                hint = exc_hint
                break

    if hint is None:
        hint = "Use --verbose for the full traceback."

    click.echo(f"Hint: {hint}", err=True)
```

### 3. Applying the decorator to all commands

Every command gets the `@handle_errors` decorator stacked under `@main.command()`:

```python
@main.command()
@handle_errors
def generate():
    """Generate transformation code for all entities."""
    ...

@main.command()
@handle_errors
def run():
    """Run the transformation pipeline."""
    ...
```

The decorator must be the innermost decorator (closest to the function) so it wraps the command's body, not Click's command registration.

### 4. Verbose behavior summary

| Condition | stderr output | Logging level |
|-----------|--------------|---------------|
| No `--verbose`, no error | Nothing | WARNING |
| No `--verbose`, error raised | `Error: {msg}` + `Hint: {hint}` | WARNING |
| `--verbose`, no error | Debug log lines from engine | DEBUG |
| `--verbose`, error raised | Full Python traceback | DEBUG |

### 5. Error-to-hint mapping (complete)

| Exception | Hint |
|-----------|------|
| `FileNotFoundError` | "Check that the path exists. Run `fyr init` to create a project." |
| `ValueError` | "Check your fyrnheim.yaml configuration values." |
| `ImportError` | "A required dependency may be missing. Try: pip install fyrnheim[duckdb]" |
| `SourceNotFoundError` | "Check your entity source paths and data directory." |
| `TransformModuleError` | "Run `fyr generate` to regenerate transform code." |
| `ExecutionError` | "Check entity definitions and source data. Use --verbose for details." |
| `CircularDependencyError` | "Check entity source dependencies for cycles." |
| Any other `Exception` | "Use --verbose for the full traceback." |

Note: `CircularDependencyError` (from `fyrnheim.engine.resolution`) should also be included in the engine hints dict.

### 6. File changes

**`src/fyrnheim/cli.py`** (modified, ~30 lines added):
- Add `import functools, sys, traceback, logging`
- Add `from fyrnheim.engine.errors import FyrnheimEngineError, SourceNotFoundError, TransformModuleError, ExecutionError`
- Add `from fyrnheim.engine.resolution import CircularDependencyError`
- Add `_ERROR_HINTS` and `_ENGINE_HINTS` dicts
- Add `_print_error()` helper
- Add `handle_errors` decorator
- Modify `main()` to accept `--verbose` and configure logging
- Add `@handle_errors` to all 5 commands

No new files. No new modules. Everything stays in the single `cli.py` file.

### 7. Tests

**`tests/test_cli_errors.py`** (new, ~80 lines):

Tests use `click.testing.CliRunner` with `mix_stderr=False` to capture stderr separately.

```python
from click.testing import CliRunner
from fyrnheim.cli import main

runner = CliRunner(mix_stderr=False)
```

Test cases:

1. **FileNotFoundError without --verbose**: Mock command to raise `FileNotFoundError("entities/ not found")`. Assert stderr contains `Error: entities/ not found` and `Hint: Check that the path exists`. Assert no traceback lines (no `Traceback (most recent call last)`). Assert exit code 1.

2. **FileNotFoundError with --verbose**: Same mock, invoke with `["--verbose", "run"]`. Assert stderr contains `Traceback (most recent call last)` and `FileNotFoundError`. Assert exit code 1.

3. **SourceNotFoundError without --verbose**: Assert `Error:` line and `Hint: Check your entity source paths`.

4. **TransformModuleError without --verbose**: Assert hint about `fyr generate`.

5. **ExecutionError without --verbose**: Assert hint about checking entity definitions.

6. **ImportError without --verbose**: Assert hint about `pip install fyrnheim[duckdb]`.

7. **ValueError without --verbose**: Assert hint about fyrnheim.yaml.

8. **Unknown exception without --verbose**: Raise `RuntimeError("something broke")`. Assert `Error: something broke` and `Hint: Use --verbose for the full traceback.`

9. **Verbose flag sets DEBUG logging**: Invoke `["--verbose", "list"]` (assuming list succeeds). Check that the root logger or `fyrnheim` logger has DEBUG level effective.

10. **Non-verbose uses WARNING logging**: Invoke `["list"]`. Check WARNING level.

Mocking strategy: Use `unittest.mock.patch` on the underlying function each command calls (e.g., `fyrnheim.engine.runner.run`) to raise the desired exception. Alternatively, register a temporary Click command on the group that raises the exception directly.

Simpler approach -- add a test-only command:

```python
@main.command("_test_error", hidden=True)
@handle_errors
@click.argument("exc_type")
def _test_error_cmd(exc_type):
    """Hidden command for testing error handling."""
    exc_map = {
        "FileNotFoundError": FileNotFoundError("test/path not found"),
        "ValueError": ValueError("bad value"),
        "ImportError": ImportError("No module named 'duckdb'"),
        ...
    }
    raise exc_map[exc_type]
```

Actually, no. Hidden test commands pollute the CLI. Better to patch. The simplest approach: patch the function that each command calls. For unit-level error handler tests, patch at the command level. For example:

```python
def test_file_not_found_no_verbose(monkeypatch):
    """FileNotFoundError prints Error + Hint, no traceback."""
    def raise_fnf(**kwargs):
        raise FileNotFoundError("entities/ not found")

    monkeypatch.setattr("fyrnheim.engine.runner.run", raise_fnf)
    result = runner.invoke(main, ["run"])
    assert result.exit_code == 1
    assert "Error: entities/ not found" in result.output  # stderr
    assert "Hint:" in result.output
    assert "Traceback" not in result.output
```

## Key Decisions

1. **Single decorator, not a context manager** -- a decorator is cleaner for Click commands because each command is a separate function. A context manager (`with handle_errors():`) would require boilerplate in every command body.

2. **Hints are static strings, not dynamic** -- keeps the code simple. The exception message itself provides the dynamic detail (e.g., which file is missing). The hint provides the generic fix action.

3. **Engine errors checked before general errors** -- `SourceNotFoundError` is a `FyrnheimEngineError` but also conceptually overlaps with `FileNotFoundError`. Checking engine hints first ensures the more specific hint wins.

4. **`force=True` on logging.basicConfig** -- necessary because `import ibis` may configure logging before `main()` runs.

5. **No colored output** -- can be added in a later story. Plain text to stderr is universally compatible.

6. **Exit code is always 1 for handled errors** -- the `run` command already has its own exit-code logic (0 for success, 1 for errors, 2 for quality failures) inside the command body. The error handler only catches unexpected exceptions that escape the command; those always get exit code 1.

## Success Criteria
- `fyr run` on a bad path prints `Error:` + `Hint:` to stderr, exit code 1, no traceback
- `fyr --verbose run` on a bad path prints full Python traceback, exit code 1
- `fyr --verbose list` shows DEBUG log lines from the engine
- `fyr list` (no verbose) shows no debug output
- All 5 commands have `@handle_errors`
- Tests cover all 7 exception types in both verbose and non-verbose modes
