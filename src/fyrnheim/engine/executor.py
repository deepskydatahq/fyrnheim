"""DuckDB execution engine for fyrnheim pipelines."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import ibis

from fyrnheim.engine._loader import load_transform_module
from fyrnheim.engine.errors import ExecutionError, SourceNotFoundError

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


class DuckDBExecutor:
    """Execute entity transformations on a DuckDB backend.

    Usage::

        with DuckDBExecutor() as executor:
            executor.register_parquet("source_customers", Path("data/customers.parquet"))
            result = executor.execute("customers", generated_dir=Path("generated/"))

    Args:
        db_path: DuckDB database path. ":memory:" (default) for in-memory,
                 or a file path for persistent storage.
        generated_dir: Default directory for generated transform modules.
    """

    def __init__(
        self,
        db_path: str | Path = ":memory:",
        generated_dir: str | Path | None = None,
    ) -> None:
        self._db_path = str(db_path)
        self._generated_dir = Path(generated_dir) if generated_dir else None
        self._registered_sources: dict[str, Path] = {}
        self._conn: ibis.BaseBackend = ibis.duckdb.connect(self._db_path)
        log.info("DuckDB connected: %s", self._db_path)

    @property
    def connection(self) -> ibis.BaseBackend:
        """The underlying Ibis DuckDB connection."""
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

    def execute(
        self,
        entity_name: str,
        generated_dir: str | Path | None = None,
        target_name: str | None = None,
    ) -> ExecutionResult:
        """Load and execute a generated transform module for an entity.

        Args:
            entity_name: Entity name (matches generated file name).
            generated_dir: Directory with generated transforms. Falls back to
                          the executor-level default.
            target_name: Target table name. Defaults to the last function's
                        output registered as ``dim_{entity_name}``.

        Returns:
            ExecutionResult with row count and metadata.

        Raises:
            TransformModuleError: If the generated module cannot be loaded.
            ExecutionError: If transform execution fails.
        """
        gen_dir = Path(generated_dir) if generated_dir else self._generated_dir
        if gen_dir is None:
            raise ExecutionError(
                "No generated_dir specified. Pass it to execute() or the constructor."
            )

        module = load_transform_module(entity_name, gen_dir)

        # Resolve target name
        if target_name is None:
            target_name = f"dim_{entity_name}"

        try:
            result_table = self._run_transform_pipeline(entity_name, module)
        except Exception as e:
            raise ExecutionError(
                f"Transform execution failed for {entity_name}: {e}"
            ) from e

        # Persist result
        try:
            self._conn.create_table(target_name, result_table, overwrite=True)
        except Exception as e:
            raise ExecutionError(
                f"Failed to persist {target_name}: {e}"
            ) from e

        # Get result metadata
        persisted = self._conn.table(target_name)
        row_count = persisted.count().execute()
        columns = list(persisted.columns)

        log.info("%s -> %s: %d rows, %d columns", entity_name, target_name, row_count, len(columns))

        return ExecutionResult(
            entity_name=entity_name,
            target_name=target_name,
            row_count=row_count,
            columns=columns,
            success=True,
        )

    def _run_transform_pipeline(self, entity_name: str, module: Any) -> ibis.Table:
        """Execute the layer functions from a generated module in order.

        Looks for: source_{name} -> prep_{name} -> dim_{name} -> snapshot_{name}
        Calls each function that exists, chaining the output.
        """
        conn = self._conn
        backend = "duckdb"

        # Prefer registered source (has correct resolved path from runner)
        source_name = f"source_{entity_name}"
        if source_name in self._registered_sources:
            t = conn.table(source_name)
        else:
            # Fall back to source function in generated code
            source_fn = getattr(module, f"source_{entity_name}", None)
            if source_fn is not None:
                t = source_fn(conn, backend)
            else:
                raise ExecutionError(
                    f"No source function or registered source for {entity_name}"
                )

        # Prep function
        prep_fn = getattr(module, f"prep_{entity_name}", None)
        if prep_fn is not None:
            t = prep_fn(t)

        # Dimension function
        dim_fn = getattr(module, f"dim_{entity_name}", None)
        if dim_fn is not None:
            t = dim_fn(t)

        # Snapshot function
        snapshot_fn = getattr(module, f"snapshot_{entity_name}", None)
        if snapshot_fn is not None:
            t = snapshot_fn(t)

        return t

    def close(self) -> None:
        """Close the DuckDB connection."""
        if self._conn is not None:
            self._conn.disconnect()
            log.info("DuckDB disconnected")

    def __enter__(self) -> DuckDBExecutor:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
