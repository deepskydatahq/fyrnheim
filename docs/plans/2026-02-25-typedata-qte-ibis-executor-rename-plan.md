# Implementation Plan: typedata-qte — Rename DuckDBExecutor to IbisExecutor

**Task:** M004-E001-S001
**Design:** [ibis-executor-rename-design.md](./2026-02-24-ibis-executor-rename-design.md)

## Pre-flight

```bash
uv run pytest tests/test_engine_executor.py  # confirm all executor tests green
```

## Step 1: Rename class and rewrite constructor (`executor.py:31-55`)

Replace class declaration and `__init__`:
- Rename `DuckDBExecutor` → `IbisExecutor`
- Change constructor signature from `(db_path)` to `(conn: ibis.BaseBackend, backend: str)`
- Store `self._backend = backend`, remove `self._db_path`
- Update module docstring: "DuckDB execution engine" → "Ibis execution engine"
- Update `connection` property docstring (line 58-60)
- Update `close()` docstring and log message (line 215-219) to be backend-agnostic

## Step 2: Add `duckdb()` classmethod (after `__init__`)

```python
@classmethod
def duckdb(cls, db_path=":memory:", generated_dir=None) -> IbisExecutor:
    conn = ibis.duckdb.connect(str(db_path))
    return cls(conn=conn, backend="duckdb", generated_dir=generated_dir)
```

## Step 3: Use `self._backend` in `_run_transform_pipeline` (line 165)

Change `backend = "duckdb"` → `backend = self._backend`

## Step 4: Update `__enter__` return type (line 221)

Change `-> DuckDBExecutor` → `-> IbisExecutor`

## Step 5: Add backward-compat alias at module level (end of file)

```python
# Backward compatibility — removed in S002
DuckDBExecutor = IbisExecutor
```

This ensures `runner.py`, `__init__.py`, and other test files continue working without changes.

## Step 6: Update `test_engine_executor.py`

1. Change import: `DuckDBExecutor` → `IbisExecutor`
2. Replace `DuckDBExecutor()` → `IbisExecutor.duckdb()`
3. Replace `DuckDBExecutor(db_path=...)` → `IbisExecutor.duckdb(db_path=...)`
4. Replace `DuckDBExecutor(generated_dir=...)` → `IbisExecutor.duckdb(generated_dir=...)`
5. Rename test classes:
   - `TestDuckDBExecutorLifecycle` → `TestIbisExecutorLifecycle`
   - `TestDuckDBExecutorRegisterParquet` → `TestIbisExecutorRegisterParquet`
   - `TestDuckDBExecutorExecute` → `TestIbisExecutorExecute`
6. Update test docstrings

## Step 7: Verify

```bash
uv run pytest tests/test_engine_executor.py   # all executor tests pass
uv run pytest                                  # full suite passes (alias keeps everything working)
uv run ruff check src/fyrnheim/engine/executor.py
uv run mypy src/fyrnheim/engine/executor.py
```

## Files changed

| File | Changes |
|------|---------|
| `src/fyrnheim/engine/executor.py` | Rename class, new constructor, classmethod, `self._backend`, alias |
| `tests/test_engine_executor.py` | Update imports, constructor calls, class names |

## Files NOT changed (S002 scope)

- `src/fyrnheim/engine/runner.py`
- `src/fyrnheim/engine/__init__.py`
- `src/fyrnheim/__init__.py`
- All other test files (`test_engine_runner.py`, `test_e2e_*.py`)

## Risk

**Low.** The `DuckDBExecutor = IbisExecutor` alias ensures zero breakage outside the two files being modified.
