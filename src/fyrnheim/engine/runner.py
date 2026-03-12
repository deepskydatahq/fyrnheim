"""Pipeline orchestration: run() and run_entity() functions."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import ibis

from fyrnheim.engine.connection import create_connection
from fyrnheim.engine.errors import SourceNotFoundError
from fyrnheim.engine.executor import IbisExecutor
from fyrnheim.engine.registry import EntityRegistry
from fyrnheim.engine.resolution import extract_dependencies, resolve_execution_order

if TYPE_CHECKING:
    from fyrnheim.core.entity import Entity
    from fyrnheim.core.source_mapping import SourceMapping
    from fyrnheim.quality.results import CheckResult

log = logging.getLogger("fyrnheim.engine")


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EntityRunResult:
    """Result of running a single entity through the pipeline."""

    entity_name: str
    status: Literal["success", "skipped", "error"]
    row_count: int | None = None
    activity_row_count: int | None = None
    analytics_row_count: int | None = None
    error: str | None = None
    duration_seconds: float = 0.0
    quality_results: list[CheckResult] | None = None


@dataclass(frozen=True)
class PushedTable:
    """Result of pushing a single table to the output backend."""

    table_name: str
    row_count: int = 0
    status: Literal["ok", "error"] = "ok"
    error: str | None = None


@dataclass(frozen=True)
class RunResult:
    """Result of running the full pipeline."""

    entities: list[EntityRunResult] = field(default_factory=list)
    total_duration_seconds: float = 0.0
    backend: str = "duckdb"
    pushed_tables: list[PushedTable] = field(default_factory=list)

    @property
    def success_count(self) -> int:
        return sum(1 for e in self.entities if e.status == "success")

    @property
    def error_count(self) -> int:
        return sum(1 for e in self.entities if e.status == "error")

    @property
    def skipped_count(self) -> int:
        return sum(1 for e in self.entities if e.status == "skipped")

    @property
    def ok(self) -> bool:
        """True if no errors occurred."""
        return self.error_count == 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_generated_dir(
    entities_dir: Path,
    generated_dir: Path | str | None,
) -> Path:
    """Resolve the generated code directory.

    If generated_dir is None, default to {entities_dir}/../generated/
    (a sibling directory to the entities directory).
    """
    if generated_dir is not None:
        return Path(generated_dir)
    return entities_dir.parent / "generated"


def _register_entity_source(
    executor: IbisExecutor,
    entity: Entity,
    data_dir: Path,
) -> None:
    """Register an entity's source parquet data with the executor.

    Resolves ``duckdb_path`` from the entity source relative to ``data_dir``.
    Handles both single TableSource and UnionSource (registers each sub-source).
    """
    from fyrnheim.core.source import UnionSource

    source = entity.source
    if source is None:
        return

    # Register single TableSource
    duckdb_path = getattr(source, "duckdb_path", None)
    if duckdb_path:
        resolved = data_dir / duckdb_path
        source_name = f"source_{entity.name}"
        try:
            executor.register_parquet(source_name, resolved)
        except SourceNotFoundError:
            raise SourceNotFoundError(
                f"Data file not found: {duckdb_path} "
                f"(entity: {entity.name}, data_dir: {data_dir})"
            ) from None

    # Register UnionSource sub-sources
    if isinstance(source, UnionSource):
        for sub_source in source.sources:
            if sub_source.duckdb_path:
                resolved = data_dir / sub_source.duckdb_path
                sub_name = f"source_{entity.name}_{sub_source.table}"
                try:
                    executor.register_parquet(sub_name, resolved)
                except SourceNotFoundError:
                    raise SourceNotFoundError(
                        f"Data file not found: {sub_source.duckdb_path} "
                        f"(entity: {entity.name}, source: {sub_source.table}, "
                        f"data_dir: {data_dir})"
                    ) from None


def validate_helper_entities(entities: list[Entity]) -> None:
    """Validate all HelperEntities are referenced by at least one other entity.

    Raises ValueError if any HelperEntity is not referenced in another
    entity's dependencies (via DerivedSource.depends_on or
    AggregationSource.source_entity).
    """
    from fyrnheim.core.entity import HelperEntity

    helper_names = {e.name for e in entities if isinstance(e, HelperEntity)}
    if not helper_names:
        return

    referenced: set[str] = set()
    for e in entities:
        referenced.update(extract_dependencies(e))

    orphaned = helper_names - referenced
    if orphaned:
        sorted_orphaned = sorted(orphaned)
        raise ValueError(
            f"HelperEntity(s) not referenced by any other entity: "
            f"{', '.join(sorted_orphaned)}. "
            "Helper entities must be depended on."
        )


_OUTPUT_TABLE_PREFIXES = ("dim_", "analytics_", "activity_")


def _push_tables(
    source_conn: ibis.BaseBackend,
    output_backend: str,
    output_config: dict[str, str] | None,
) -> list[PushedTable]:
    """Push output tables from source connection to output backend.

    Filters tables to dim_*, analytics_*, activity_* prefixes and copies
    each to the output backend with overwrite=True.
    """
    output_conn = create_connection(output_backend, **(output_config or {}))
    pushed: list[PushedTable] = []

    try:
        all_tables = source_conn.list_tables()
        output_tables = [t for t in all_tables if t.startswith(_OUTPUT_TABLE_PREFIXES)]
        log.info("Push phase: %d tables to push to %s", len(output_tables), output_backend)

        for table_name in sorted(output_tables):
            try:
                table = source_conn.table(table_name)
                row_count = table.count().execute()
                df = table.execute()
                output_conn.create_table(table_name, df, overwrite=True)
                pushed.append(PushedTable(table_name=table_name, row_count=row_count, status="ok"))
                log.info("Pushed %s (%d rows)", table_name, row_count)
            except Exception as exc:
                pushed.append(PushedTable(table_name=table_name, status="error", error=str(exc)))
                log.error("Failed to push %s: %s", table_name, exc)
    finally:
        try:
            output_conn.disconnect()
        except Exception:
            pass

    return pushed


# ---------------------------------------------------------------------------
# run_entity()
# ---------------------------------------------------------------------------


def run_entity(
    entity: Entity,
    data_dir: str | Path,
    *,
    backend: str = "duckdb",
    backend_config: dict[str, str] | None = None,
    generated_dir: str | Path | None = None,
    auto_generate: bool = True,
    quality_checks: bool = True,
    _executor: IbisExecutor | None = None,
    source_mapping: SourceMapping | None = None,
    output_backend: str | None = None,
    output_config: dict[str, str] | None = None,
) -> EntityRunResult:
    """Execute a single entity through the pipeline.

    Args:
        entity: The Entity to execute.
        data_dir: Base directory for parquet source data.
        backend: Backend engine ("duckdb", "bigquery").
        backend_config: Backend-specific connection arguments (e.g. project_id, dataset_id).
        generated_dir: Directory for generated transform code.
        auto_generate: If True, regenerate code before execution.
        quality_checks: If True, run quality checks after execution.
        _executor: Internal: shared executor from run().

    Returns:
        EntityRunResult with status, row count, and quality results.
    """
    start_time = time.monotonic()
    data_dir = Path(data_dir)
    gen_dir = Path(generated_dir) if generated_dir else Path("generated")

    # 1. Auto-generate
    if auto_generate:
        try:
            from fyrnheim._generate import generate

            log.debug("Generating: %s_transforms.py", entity.name)
            generate(entity, output_dir=gen_dir, source_mapping=source_mapping)
        except Exception as e:
            return EntityRunResult(
                entity_name=entity.name,
                status="error",
                error=f"Code generation failed: {e}",
                duration_seconds=time.monotonic() - start_time,
            )
    else:
        # Verify generated file exists
        transform_path = gen_dir / f"{entity.name}_transforms.py"
        if not transform_path.exists():
            log.warning("Generated file not found: %s (skipping)", transform_path)
            return EntityRunResult(
                entity_name=entity.name,
                status="skipped",
                error=f"Generated file not found: {transform_path}",
                duration_seconds=time.monotonic() - start_time,
            )

    # 2. Execute
    own_executor = _executor is None
    executor: IbisExecutor | None = _executor
    try:
        if own_executor:
            conn = create_connection(backend, **(backend_config or {}))
            executor = IbisExecutor(conn=conn, backend=backend, generated_dir=gen_dir)
        assert executor is not None  # guaranteed: either _executor was given or we just created one
        _register_entity_source(executor, entity, data_dir)

        log.info("Transforming: %s", entity.name)
        exec_result = executor.execute(entity.name, generated_dir=gen_dir, entity=entity)
        row_count = exec_result.row_count
        activity_row_count = exec_result.activity_row_count
        analytics_row_count = exec_result.analytics_row_count
    except Exception as e:
        return EntityRunResult(
            entity_name=entity.name,
            status="error",
            error=str(e),
            duration_seconds=time.monotonic() - start_time,
        )
    finally:
        if own_executor and executor is not None:
            executor.close()

    # 3. Quality checks
    quality_results = None
    if quality_checks and entity.quality and entity.quality.checks:
        try:
            from fyrnheim.quality import QualityRunner

            qr = QualityRunner(executor.connection)
            entity_result = qr.run_entity_checks(
                entity_name=entity.name,
                quality_config=entity.quality,
                primary_key=entity.quality.primary_key,
                table_name=exec_result.target_name,
            )
            quality_results = entity_result.results
        except Exception as e:
            log.warning("Quality check error for %s: %s", entity.name, e)

    duration = time.monotonic() - start_time
    log.info("%s: %d rows (%.1fs)", entity.name, row_count, duration)

    return EntityRunResult(
        entity_name=entity.name,
        status="success",
        row_count=row_count,
        activity_row_count=activity_row_count,
        analytics_row_count=analytics_row_count,
        duration_seconds=duration,
        quality_results=quality_results,
    )


# ---------------------------------------------------------------------------
# run()
# ---------------------------------------------------------------------------


def run(
    entities_dir: str | Path,
    data_dir: str | Path,
    *,
    backend: str = "duckdb",
    backend_config: dict[str, str] | None = None,
    generated_dir: str | Path | None = None,
    auto_generate: bool = True,
    quality_checks: bool = True,
    on_error: Literal["skip", "stop"] = "skip",
    output_backend: str | None = None,
    output_config: dict[str, str] | None = None,
) -> RunResult:
    """Run the full fyrnheim pipeline: discover, generate, execute, verify.

    Args:
        entities_dir: Directory containing entity .py files.
        data_dir: Base directory for parquet source data.
        backend: Backend engine ("duckdb", "bigquery").
        backend_config: Backend-specific connection arguments (e.g. project_id, dataset_id).
        generated_dir: Directory for generated transform code.
            None means {entities_dir}/../generated/.
        auto_generate: If True, regenerate code before execution.
        quality_checks: If True, run quality checks after execution.
        on_error: "skip" continues after errors; "stop" halts.

    Returns:
        RunResult with per-entity results.
    """
    pipeline_start = time.monotonic()
    entities_dir = Path(entities_dir)
    data_dir = Path(data_dir)
    gen_dir = _resolve_generated_dir(entities_dir, generated_dir)

    # Validate
    if not entities_dir.is_dir():
        raise FileNotFoundError(f"Entities directory not found: {entities_dir}")

    # Phase 1: Discover
    log.info("Discovering entities in %s", entities_dir)
    registry = EntityRegistry()
    registry.discover(entities_dir)

    if len(registry) == 0:
        log.warning("No entities found in %s", entities_dir)
        return RunResult(
            entities=[],
            total_duration_seconds=time.monotonic() - pipeline_start,
            backend=backend,
        )

    # Phase 1.5: Validate helper entities
    all_entities = [info.entity for _name, info in registry.items()]
    validate_helper_entities(all_entities)

    # Phase 2: Resolve dependency order
    sorted_entities = resolve_execution_order(registry)
    entity_names = [e.name for e in sorted_entities]
    log.debug("Execution order: %s", ", ".join(entity_names))
    log.info("Running %d entities on %s backend", len(sorted_entities), backend)

    # Phase 3: Execute
    results: list[EntityRunResult] = []
    failed_entities: set[str] = set()

    conn = create_connection(backend, **(backend_config or {}))
    with IbisExecutor(conn=conn, backend=backend, generated_dir=gen_dir) as executor:
        for entity_info in sorted_entities:
            entity = entity_info.entity
            entity_name = entity_info.name

            # Check dependency failures (skip mode)
            if on_error == "skip":
                deps = extract_dependencies(entity)
                failed_deps = [d for d in deps if d in failed_entities]
                if failed_deps:
                    result = EntityRunResult(
                        entity_name=entity_name,
                        status="skipped",
                        error=f"dependency failed: {', '.join(failed_deps)}",
                    )
                    results.append(result)
                    log.warning(
                        "%s: skipped (dependency failed: %s)",
                        entity_name,
                        ", ".join(failed_deps),
                    )
                    continue

            # Execute the entity
            try:
                result = run_entity(
                    entity,
                    data_dir,
                    backend=backend,
                    generated_dir=gen_dir,
                    auto_generate=auto_generate,
                    quality_checks=quality_checks,
                    _executor=executor,
                    source_mapping=entity_info.source_mapping,
                )
            except Exception as exc:
                result = EntityRunResult(
                    entity_name=entity_name,
                    status="error",
                    error=str(exc),
                )

            results.append(result)

            if result.status == "error":
                failed_entities.add(entity_name)
                log.error("%s: failed: %s", entity_name, result.error)
                if on_error == "stop":
                    remaining_idx = entity_names.index(entity_name) + 1
                    for remaining_name in entity_names[remaining_idx:]:
                        results.append(
                            EntityRunResult(
                                entity_name=remaining_name,
                                status="skipped",
                                error="execution stopped due to previous error",
                            )
                        )
                    break

        # Phase 4: Push to output backend (inside with block — conn still open)
        pushed_tables: list[PushedTable] = []
        if output_backend is not None:
            pushed_tables = _push_tables(conn, output_backend, output_config)

    total_duration = time.monotonic() - pipeline_start
    run_result = RunResult(
        entities=results,
        total_duration_seconds=total_duration,
        backend=backend,
        pushed_tables=pushed_tables,
    )
    log.info(
        "Pipeline complete: %d success, %d errors, %d skipped (%.1fs)",
        run_result.success_count,
        run_result.error_count,
        run_result.skipped_count,
        total_duration,
    )
    return run_result
