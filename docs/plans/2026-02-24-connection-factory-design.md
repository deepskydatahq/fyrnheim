# Connection Factory Design

## Overview

Add a `create_connection(backend, **kwargs)` factory function in a new `engine/connection.py` module that maps backend name strings to Ibis connections. This is the foundation for multi-backend support in Fyrnheim.

## Problem Statement

Fyrnheim currently hardcodes DuckDB connections inside the executor. To support BigQuery (and future backends), we need a clean separation between "which backend" and "how to execute". The connection factory translates configuration (backend name + kwargs) into infrastructure (Ibis connection objects).

## Expert Perspectives

### Technical

- **Separate module** (`connection.py`) over co-locating in `executor.py`. The factory is a configuration-to-infrastructure adapter — a distinct concern from execution logic. This keeps it independently importable by downstream consumers (config, CLI, runner, tests).
- **Dict dispatch** over class hierarchies or plugin systems. Simple, scannable, easy to extend. No hidden behavior.
- **Lazy BigQuery import** — catch `ImportError` at call time, not module import time, so `connection.py` can always be imported.

### Simplification Review

- Design approved as minimal and inevitable.
- Each component serves a direct purpose; no scaffolding or premature extensibility.
- `SUPPORTED_BACKENDS` public constant is product-aware — enables CLI help text and config validation downstream.

## Proposed Solution

### `src/fyrnheim/engine/connection.py`

```python
"""Connection factory for Ibis backends."""

from __future__ import annotations

from typing import Any

import ibis


def _connect_duckdb(**kwargs: Any) -> ibis.BaseBackend:
    """Create a DuckDB connection via Ibis."""
    db_path = kwargs.pop("db_path", ":memory:")
    return ibis.duckdb.connect(db_path, **kwargs)


def _connect_bigquery(**kwargs: Any) -> ibis.BaseBackend:
    """Create a BigQuery connection via Ibis."""
    try:
        return ibis.bigquery.connect(**kwargs)
    except (ImportError, AttributeError) as exc:
        raise ImportError(
            "BigQuery backend requires extra dependencies. "
            "Install them with: pip install fyrnheim[bigquery]"
        ) from exc


_BACKEND_REGISTRY: dict[str, Any] = {
    "duckdb": _connect_duckdb,
    "bigquery": _connect_bigquery,
}

SUPPORTED_BACKENDS: list[str] = sorted(_BACKEND_REGISTRY.keys())


def create_connection(backend: str, **kwargs: Any) -> ibis.BaseBackend:
    """Create an Ibis connection for the given backend.

    Args:
        backend: Backend name (e.g. "duckdb", "bigquery").
        **kwargs: Backend-specific connection arguments.
            - duckdb: db_path (default ":memory:")
            - bigquery: project_id, dataset_id, etc.

    Returns:
        An Ibis BaseBackend connection.

    Raises:
        ValueError: If backend is not supported.
        ImportError: If required extras are not installed.
    """
    connector = _BACKEND_REGISTRY.get(backend)
    if connector is None:
        raise ValueError(
            f"Unknown backend {backend!r}. "
            f"Supported backends: {', '.join(SUPPORTED_BACKENDS)}"
        )
    return connector(**kwargs)
```

### Changes

| File | Action | Purpose |
|------|--------|---------|
| `src/fyrnheim/engine/connection.py` | Create | Factory function + `SUPPORTED_BACKENDS` |
| `tests/test_engine_connection.py` | Create | Tests for all 4 acceptance criteria |
| `src/fyrnheim/engine/__init__.py` | Edit | Re-export `create_connection` and `SUPPORTED_BACKENDS` |

## Design Details

- **`_connect_duckdb` pops `db_path`** from kwargs to match the Ibis DuckDB API where `db_path` is a positional arg.
- **`_connect_bigquery` catches `AttributeError`** in addition to `ImportError` because accessing `ibis.bigquery` when extras aren't installed may raise either depending on Ibis version.
- **`SUPPORTED_BACKENDS`** is a sorted public constant so downstream code (CLI help, config validation) can reference it.
- **No changes to existing `executor.py`** — the future M004-E001-S002 story refactors it to accept a pre-built connection.

## Alternatives Considered

1. **Add factory inside `executor.py`** — Rejected. Factory is a distinct concern (config → infrastructure), not execution logic. Keeping it separate enables clean imports from downstream consumers.
2. **Class hierarchy / plugin system** — Rejected. YAGNI. Dict dispatch is simpler and covers the known backends.
3. **New error classes** — Rejected. `ValueError` and `ImportError` are standard Python conventions for these error conditions.

## Success Criteria

- `create_connection('duckdb', db_path=':memory:')` returns ibis DuckDB connection
- `create_connection('bigquery', project_id='x', dataset_id='y')` calls `ibis.bigquery.connect` (mocked in tests)
- `create_connection('unknown')` raises `ValueError` listing supported backends
- `create_connection('bigquery', ...)` without extras raises `ImportError` with install hint
