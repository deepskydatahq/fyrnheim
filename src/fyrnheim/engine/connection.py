"""Connection factory for Ibis backends."""

from __future__ import annotations

import importlib
from typing import Any

import ibis

SUPPORTED_BACKENDS = ["duckdb", "bigquery"]


def create_connection(backend: str, **kwargs: Any) -> ibis.BaseBackend:
    """Create an Ibis backend connection by name.

    Args:
        backend: Backend name ("duckdb", "bigquery").
        **kwargs: Backend-specific connection arguments.
            DuckDB: db_path (default ":memory:")
            BigQuery: project_id, dataset_id

    Returns:
        An Ibis backend connection.

    Raises:
        ValueError: If backend is not supported.
        ImportError: If required extras are not installed.
    """
    if backend == "duckdb":
        db_path = kwargs.get("db_path", ":memory:")
        return ibis.duckdb.connect(str(db_path))

    if backend == "bigquery":
        try:
            importlib.import_module("ibis.backends.bigquery")
        except ImportError:
            raise ImportError(
                "BigQuery backend requires extra dependencies. "
                "Install with: pip install 'ibis-framework[bigquery]'"
            ) from None
        project_id = kwargs.get("project_id")
        dataset_id = kwargs.get("dataset_id")
        if not project_id or not dataset_id:
            raise ValueError(
                "BigQuery backend requires 'project_id' and 'dataset_id' in backend_config."
            )
        return ibis.bigquery.connect(project_id=project_id, dataset_id=dataset_id)

    raise ValueError(
        f"Unsupported backend: {backend!r}. Supported backends: {SUPPORTED_BACKENDS}"
    )
