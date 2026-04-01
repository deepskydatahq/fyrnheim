# Update All Imports from DuckDBExecutor to IbisExecutor — Design

## Overview

Pure find-and-replace migration of all `DuckDBExecutor` references to `IbisExecutor` across 12 Python files. Relies on the S001-provided `IbisExecutor.duckdb()` classmethod as a drop-in factory. No behavioral changes, no new APIs, no new files.

## Problem Statement

After S001 renames the executor class, all consumers (runner, CLI, tests, public API) still reference the old `DuckDBExecutor` name. This story updates them all so the codebase is consistent and the old name is fully retired.

## Expert Perspectives

### Technical
- `IbisExecutor.duckdb(db_path, generated_dir)` is the convenience factory replacing `DuckDBExecutor()` — same parameters, minimal migration churn.
- `IbisExecutor(conn, backend, generated_dir)` is the composable escape hatch for future backends — not used in this story.
- Tests should use `IbisExecutor.duckdb(...)` to keep the migration a simple name swap.

### Simplification Review
- Removed unnecessary "three categories" framing — the strategy is identical everywhere.
- Removed prescriptive execution order and validation phases — these are implicit in normal development.
- Core truth: "Replace `DuckDBExecutor` with `IbisExecutor` in all 12 Python files, verify tests pass."

## Proposed Solution

Replace `DuckDBExecutor` with `IbisExecutor` across all Python files in `src/` and `tests/`:

**Source files (4):**

| File | Changes |
|------|---------|
| `src/fyrnheim/engine/__init__.py` | Import + `__all__`: `DuckDBExecutor` → `IbisExecutor` |
| `src/fyrnheim/engine/runner.py` | Import, type hints, constructor calls → `IbisExecutor` / `IbisExecutor.duckdb()` |
| `src/fyrnheim/__init__.py` | `_LAZY_IMPORTS` key + `__all__`: `DuckDBExecutor` → `IbisExecutor` |
| `src/fyrnheim/cli.py` | Import + constructor call → `IbisExecutor.duckdb(...)` |

**Test files (7):**

| File | Changes |
|------|---------|
| `tests/test_engine_executor.py` | Import, ~25 constructor calls, class/test names |
| `tests/test_engine_runner.py` | Lazy import assertion |
| `tests/test_e2e_pipeline.py` | Import + constructor calls |
| `tests/test_e2e_analytics.py` | Import + constructor calls |
| `tests/test_e2e_full_pipeline.py` | Import + constructor calls |
| `tests/test_e2e_activity.py` | Import + constructor calls |
| `tests/test_e2e_snapshot.py` | Import + constructor calls |

**Migration pattern:** Every `DuckDBExecutor(...)` call becomes `IbisExecutor.duckdb(...)` with identical parameters.

## Alternatives Considered

- **Use generic constructor `IbisExecutor(conn, backend, ...)` in tests** — Rejected. Forces tests to manually create connections, violating incremental adoption. Unnecessary complexity when the classmethod exists.
- **Keep `DuckDBExecutor` as a re-exported alias** — Rejected. The acceptance criteria explicitly require no remaining references. Clean break is simpler.

## Success Criteria

- No `DuckDBExecutor` references remain in `src/` or `tests/`
- `runner.py` uses `IbisExecutor.duckdb()` to create executor
- Public API exports `IbisExecutor` from `fyrnheim.engine`
- All ~596 existing tests pass (`uv run pytest`)
- Linters pass (`ruff check`, `mypy`)
