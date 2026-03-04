"""Ibis execution engine for fyrnheim pipelines."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import ibis

from fyrnheim.core.entity import Entity
from fyrnheim.core.source import AggregationSource, DerivedSource
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
    snapshot_target_name: str | None = None
    activity_row_count: int | None = None
    analytics_row_count: int | None = None


class IbisExecutor:
    """Execute entity transformations on any Ibis-supported backend.

    Usage::

        with IbisExecutor.duckdb() as executor:
            executor.register_parquet("source_customers", Path("data/customers.parquet"))
            result = executor.execute("customers", generated_dir=Path("generated/"))

    Args:
        conn: An Ibis backend connection.
        backend: Backend name (e.g. "duckdb", "bigquery").
        generated_dir: Default directory for generated transform modules.
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

    def execute(
        self,
        entity_name: str,
        generated_dir: str | Path | None = None,
        target_name: str | None = None,
        entity: Entity | None = None,
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

        # Detect snapshot function to determine target naming
        has_snapshot = getattr(module, f"snapshot_{entity_name}", None) is not None
        snapshot_target_name: str | None = None

        if target_name is None:
            if has_snapshot:
                target_name = f"snapshot_{entity_name}"
                snapshot_target_name = target_name
            else:
                target_name = f"dim_{entity_name}"

        try:
            result_table, activity_row_count, analytics_row_count = (
                self._run_transform_pipeline(entity_name, module, entity=entity)
            )
        except Exception as e:
            raise ExecutionError(
                f"Transform execution failed for {entity_name}: {e}"
            ) from e

        # Persist final result
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
            snapshot_target_name=snapshot_target_name,
            activity_row_count=activity_row_count,
            analytics_row_count=analytics_row_count,
        )

    def _run_transform_pipeline(
        self, entity_name: str, module: Any, *, entity: Entity | None = None
    ) -> tuple[ibis.Table, int | None, int | None]:
        """Execute the layer functions from a generated module in order.

        Looks for: source_{name} -> prep_{name} -> dim_{name} -> snapshot_{name}
        With optional branches from dim:
            dim_{name} -> activity_{name}
            dim_{name} -> analytics_{name}

        Calls each function that exists, chaining the output along the main
        pipeline.  Activity and analytics branch from the dim result (not
        snapshot) and are persisted as separate tables.

        When snapshot is present, dim_{name} is persisted as an intermediate
        table before snapshot runs, so both tables are available.

        Returns:
            Tuple of (result_table, activity_row_count, analytics_row_count).
        """
        conn = self._conn
        backend = self._backend
        activity_row_count: int | None = None
        analytics_row_count: int | None = None

        # Resolve source data for the entity
        if entity is not None and isinstance(entity.source, DerivedSource) and entity.source.identity_graph_config is not None:
            # DerivedSource path: build sources_dict from catalog
            source_fn = getattr(module, f"source_{entity_name}", None)
            if source_fn is None:
                raise ExecutionError(
                    f"No source function for DerivedSource entity {entity_name}"
                )
            sources_dict = self._build_sources_dict(entity)
            t = source_fn(sources_dict)
        elif entity is not None and isinstance(entity.source, AggregationSource):
            # AggregationSource path: resolve dependency table and pass to source fn
            source_fn = getattr(module, f"source_{entity_name}", None)
            if source_fn is None:
                raise ExecutionError(
                    f"No source function for AggregationSource entity {entity_name}"
                )
            dep_table_name = f"dim_{entity.source.source_entity}"
            try:
                dep_table = self._conn.table(dep_table_name)
            except Exception as err:
                raise ExecutionError(
                    f"Dependency table '{dep_table_name}' not found for "
                    f"AggregationSource entity '{entity_name}'. "
                    f"Ensure '{entity.source.source_entity}' executes before '{entity_name}'."
                ) from err
            t = source_fn(dep_table)
        else:
            # Call the generated source function — it handles registered tables
            # (via conn.table) and fallback to read_parquet internally
            source_fn = getattr(module, f"source_{entity_name}", None)
            source_name = f"source_{entity_name}"
            if source_fn is not None:
                t = source_fn(conn, backend)
            elif source_name in self._registered_sources:
                t = conn.table(source_name)
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

        # Capture dim result for branch layers (activity, analytics)
        dim_result = t

        # Snapshot function
        snapshot_fn = getattr(module, f"snapshot_{entity_name}", None)
        if snapshot_fn is not None:
            # Persist dim as intermediate table before snapshot
            conn.create_table(f"dim_{entity_name}", t, overwrite=True)
            t = snapshot_fn(t)

        # Activity layer (branch from dim, not snapshot)
        activity_fn = getattr(module, f"activity_{entity_name}", None)
        if activity_fn is not None:
            activity_table = activity_fn(dim_result)
            activity_target = f"activity_{entity_name}"
            conn.create_table(activity_target, activity_table, overwrite=True)
            activity_row_count = conn.table(activity_target).count().execute()

        # Analytics layer (branch from dim, not snapshot)
        analytics_fn = getattr(module, f"analytics_{entity_name}", None)
        if analytics_fn is not None:
            analytics_table = analytics_fn(dim_result)
            analytics_target = f"analytics_{entity_name}"
            conn.create_table(analytics_target, analytics_table, overwrite=True)
            analytics_row_count = conn.table(analytics_target).count().execute()

        return t, activity_row_count, analytics_row_count

    def _build_sources_dict(self, entity: Entity) -> dict[str, ibis.Table]:
        """Build sources dict for DerivedSource from dependency tables in catalog."""
        source = entity.source
        assert isinstance(source, DerivedSource)
        config = source.identity_graph_config
        if config is None:
            raise ExecutionError(
                f"DerivedSource entity {entity.name} has no identity_graph_config"
            )

        sources_dict: dict[str, ibis.Table] = {}
        for ig_source in config.sources:
            table_name = f"dim_{ig_source.entity}"
            try:
                sources_dict[ig_source.name] = self._conn.table(table_name)
            except Exception as err:
                raise ExecutionError(
                    f"Dependency table '{table_name}' not found for "
                    f"DerivedSource entity '{entity.name}'. "
                    f"Ensure '{ig_source.entity}' executes before '{entity.name}'."
                ) from err
        return sources_dict

    def close(self) -> None:
        """Close the backend connection."""
        if self._conn is not None:
            self._conn.disconnect()
            log.info("%s backend disconnected", self._backend)

    def __enter__(self) -> IbisExecutor:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
