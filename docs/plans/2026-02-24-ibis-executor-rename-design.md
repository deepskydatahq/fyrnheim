# IbisExecutor Rename Design

## Overview

Rename `DuckDBExecutor` to `IbisExecutor` with a dependency-injected constructor that accepts any Ibis backend. Move DuckDB-specific connection creation to a `duckdb()` classmethod. Add a module-level backward-compatibility alias so S001 lands independently.

## Problem Statement

`DuckDBExecutor` hardcodes `ibis.duckdb.connect()` in its constructor and hardcodes `backend = "duckdb"` in `_run_transform_pipeline`. This prevents using other backends (BigQuery, Postgres, Snowflake). The executor needs to become backend-agnostic while preserving the DuckDB quick-start path.

## Expert Perspectives

### Technical

- Constructor accepts any Ibis backend and its name via dependency injection. The classmethod `duckdb()` provides the quick-start path for DuckDB.
- Keep `generated_dir` in the constructor — the "defaults at construction, overrides at execution" pattern already exists and shouldn't change.
- Add `DuckDBExecutor = IbisExecutor` alias at module level so S001 lands without breaking files that S002 will update. One line, zero risk, clean removal path.

### Simplification Review

- Core refactoring is sound and necessary.
- Removed noise: `_db_path` removal, log message updates, and docstring changes are implementation consequences, not design decisions.
- Kept the alias despite reviewer preference to expand S001 scope — the story boundaries are explicit and should be respected.

## Proposed Solution

### Constructor Signature

```python
def __init__(
    self,
    conn: ibis.BaseBackend,
    backend: str,
    generated_dir: str | Path | None = None,
) -> None:
```

### DuckDB Classmethod

```python
@classmethod
def duckdb(
    cls,
    db_path: str | Path = ":memory:",
    generated_dir: str | Path | None = None,
) -> IbisExecutor:
    conn = ibis.duckdb.connect(str(db_path))
    return cls(conn=conn, backend="duckdb", generated_dir=generated_dir)
```

### Backend Usage

`_run_transform_pipeline` uses `self._backend` instead of hardcoded `"duckdb"`.

### Backward-Compat Alias

```python
# Backward compatibility — removed in S002
DuckDBExecutor = IbisExecutor
```

## Changes

### `executor.py`

1. Rename class to `IbisExecutor`
2. Rewrite `__init__` to accept `(conn, backend, generated_dir)`
3. Add `duckdb()` classmethod
4. Replace `backend = "duckdb"` with `backend = self._backend`
5. Add `DuckDBExecutor = IbisExecutor` alias at module level

### `test_engine_executor.py`

1. Update import to `IbisExecutor`
2. Replace `DuckDBExecutor(...)` with `IbisExecutor.duckdb(...)`
3. Rename test classes to `TestIbisExecutor*`

### NOT changed (S002 scope)

`runner.py`, `cli.py`, `engine/__init__.py`, top-level `__init__.py`, all other test files.

## Alternatives Considered

1. **No alias, expand S001 to update all imports** — Reviewer preferred this. Rejected because it violates the explicit story scope boundary (S001 = executor.py + its tests).
2. **Keep `db_path` in constructor with backend auto-detection** — Rejected because it continues coupling the constructor to DuckDB specifics.

## Success Criteria

- `IbisExecutor` class in `executor.py` with `(conn, backend)` constructor
- `IbisExecutor.duckdb(db_path)` classmethod works
- `self._backend` used in `_run_transform_pipeline`
- No hardcoded `ibis.duckdb.connect()` in constructor
- All executor tests pass with updated imports
