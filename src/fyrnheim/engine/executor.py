"""Ibis execution engine for fyrnheim pipelines."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import ibis

from fyrnheim.engine.errors import SourceNotFoundError

log = logging.getLogger("fyrnheim.engine")


@dataclass(frozen=True)
class ExecutionResult:
    """Result of executing a single entity's transformation."""

    entity_name: str
    target_name: str
    row_count: int
    columns: list[str]
    success: bool
    error: str | None = None


class IbisExecutor:
    """Execute entity transformations on any Ibis-supported backend.

    Usage::

        with IbisExecutor.duckdb() as executor:
            executor.register_parquet("source_customers", Path("data/customers.parquet"))
            # ... use executor.connection for new pipeline
    """

    def __init__(
        self,
        conn: ibis.BaseBackend,
        backend: str,
        generated_dir: str | Path | None = None,
    ) -> None:
        self._conn = conn
        self._backend = backend
        self._generated_dir = Path(generated_dir) if generated_dir else None
        self._registered_sources: dict[str, Path] = {}
        log.info("%s backend connected", self._backend)

    @classmethod
    def duckdb(
        cls,
        db_path: str | Path = ":memory:",
        generated_dir: str | Path | None = None,
    ) -> IbisExecutor:
        """Create an IbisExecutor with a DuckDB backend."""
        conn = ibis.duckdb.connect(str(db_path))
        return cls(conn=conn, backend="duckdb", generated_dir=generated_dir)

    @property
    def connection(self) -> ibis.BaseBackend:
        """The underlying Ibis backend connection."""
        return self._conn

    def register_parquet(self, name: str, path: str | Path) -> None:
        """Register a parquet file (or glob pattern) as a named source table.

        Args:
            name: Table name in DuckDB's catalog.
            path: Path to parquet file or glob pattern.

        Raises:
            SourceNotFoundError: If a non-glob path does not exist.
        """
        path = Path(path)
        path_str = str(path)

        # Validate non-glob paths exist
        if "*" not in path_str and "?" not in path_str and not path.exists():
            raise SourceNotFoundError(f"Parquet file not found: {path}")

        self._conn.read_parquet(path_str, table_name=name)
        self._registered_sources[name] = path
        log.debug("Registered source: %s -> %s", name, path)

    def close(self) -> None:
        """Close the backend connection."""
        if self._conn is not None:
            self._conn.disconnect()
            log.info("%s backend disconnected", self._backend)

    def __enter__(self) -> IbisExecutor:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
