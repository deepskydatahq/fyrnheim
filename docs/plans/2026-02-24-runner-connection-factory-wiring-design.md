# Runner Connection Factory Wiring Design

## Overview

Wire `create_connection()` and `IbisExecutor` into `runner.py`, replacing hardcoded DuckDB executor creation and removing backend guards that reject non-duckdb backends.

## Problem Statement

`runner.py` currently hardcodes `DuckDBExecutor` construction and rejects any backend that isn't `"duckdb"`. After the connection factory (M004-E002-S001) and backend config (M004-E002-S002) stories land, the runner needs to use them so that any Ibis-supported backend works.

## Expert Perspectives

### Technical

The architect recommended keeping `run()` as the connection lifecycle owner while preserving `run_entity()`'s standalone capability. Key insight: avoid making `run_entity()` a two-mode function with hidden contracts. The design keeps both paths explicit — when `_executor` is provided, it's used directly; when standalone, `run_entity()` creates its own connection via the factory.

### Simplification Review

Verdict: **APPROVED**. The design removes complexity (backend guards) rather than adding it. No unnecessary abstractions — just a straightforward function call with explicit parameters. The three changes (import swap, guard removal, factory call) are inevitable and minimal.

## Proposed Solution

Replace `DuckDBExecutor` usage in `runner.py` with `create_connection()` + `IbisExecutor`. Remove backend validation guards. Add `backend_config` parameter to both `run()` and `run_entity()`.

## Design Details

### Changes to `runner.py`

#### 1. Import changes

```python
# BEFORE
from fyrnheim.engine.executor import DuckDBExecutor

# AFTER
from fyrnheim.engine.connection import create_connection
from fyrnheim.engine.executor import IbisExecutor
```

#### 2. `_register_entity_source` type annotation

```python
def _register_entity_source(
    executor: IbisExecutor,  # was DuckDBExecutor
    entity: Entity,
    data_dir: Path,
) -> None:
```

#### 3. `run_entity()` changes

Add `backend_config: dict[str, Any] | None = None` parameter. Remove backend guard (lines 144-150). Replace executor creation:

```python
# BEFORE
own_executor = _executor is None
executor = _executor or DuckDBExecutor(generated_dir=gen_dir)

# AFTER
own_executor = _executor is None
if own_executor:
    conn = create_connection(backend, **(backend_config or {}))
    executor = IbisExecutor(conn, backend, generated_dir=gen_dir)
else:
    executor = _executor
```

#### 4. `run()` changes

Add `backend_config: dict[str, Any] | None = None` parameter. Remove backend guard (line 265-266). Replace executor creation:

```python
# BEFORE
with DuckDBExecutor(generated_dir=gen_dir) as executor:

# AFTER
conn = create_connection(backend, **(backend_config or {}))
with IbisExecutor(conn, backend, generated_dir=gen_dir) as executor:
```

#### 5. Add `Any` to typing import

### Changes to `test_engine_runner.py`

- `test_unsupported_backend_raises` → `test_unknown_backend_raises`: test `"unknown_db"` instead of `"bigquery"`
- `test_run_entity_unsupported_backend` → `test_run_entity_unknown_backend`: same change

All other tests remain unchanged — DuckDB is still the default.

## Alternatives Considered

**Make `run_entity()` always require an executor (no standalone mode):** The architect recommended this for cleaner separation. However, the acceptance criteria explicitly require `run_entity()` to accept `backend_config`, implying standalone use. We honor the acceptance criteria while keeping the code path simple.

## Success Criteria

- runner.py no longer raises ValueError for non-duckdb backends
- run() calls create_connection(backend, **backend_config) to get connection
- run_entity() accepts backend_config kwarg and passes to connection factory
- All existing runner tests pass with DuckDB as default backend
