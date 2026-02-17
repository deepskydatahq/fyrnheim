"""Pipeline orchestration: run() and run_entity() functions."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from typedata.engine.executor import DuckDBExecutor
from typedata.engine.registry import EntityRegistry
from typedata.engine.resolution import _extract_dependencies, resolve_execution_order

if TYPE_CHECKING:
    from typedata.core.entity import Entity
    from typedata.quality.results import CheckResult

log = logging.getLogger("typedata.engine")


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EntityRunResult:
    """Result of running a single entity through the pipeline."""

    entity_name: str
    status: Literal["success", "skipped", "error"]
    row_count: int | None = None
    error: str | None = None
    duration_seconds: float = 0.0
    quality_results: list[CheckResult] | None = None


@dataclass(frozen=True)
class RunResult:
    """Result of running the full pipeline."""

    entities: list[EntityRunResult] = field(default_factory=list)
    total_duration_seconds: float = 0.0
    backend: str = "duckdb"

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
    executor: DuckDBExecutor,
    entity: Entity,
    data_dir: Path,
) -> None:
    """Register an entity's source parquet data with the executor.

    Resolves ``duckdb_path`` from the entity source relative to ``data_dir``.
    """
    source = entity.source
    if source is None:
        return

    duckdb_path = getattr(source, "duckdb_path", None)
    if duckdb_path:
        resolved = data_dir / duckdb_path
        source_name = f"source_{entity.name}"
        executor.register_parquet(source_name, resolved)


# ---------------------------------------------------------------------------
# run_entity()
# ---------------------------------------------------------------------------


def run_entity(
    entity: Entity,
    data_dir: str | Path,
    *,
    backend: str = "duckdb",
    generated_dir: str | Path | None = None,
    auto_generate: bool = True,
    quality_checks: bool = True,
    _executor: DuckDBExecutor | None = None,
) -> EntityRunResult:
    """Execute a single entity through the pipeline.

    Args:
        entity: The Entity to execute.
        data_dir: Base directory for parquet source data.
        backend: Backend engine (only "duckdb" supported).
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

    if backend != "duckdb":
        return EntityRunResult(
            entity_name=entity.name,
            status="error",
            error=f"Unsupported backend: {backend}",
            duration_seconds=time.monotonic() - start_time,
        )

    # 1. Auto-generate
    if auto_generate:
        try:
            from typedata._generate import generate

            log.debug("Generating: %s_transforms.py", entity.name)
            generate(entity, output_dir=gen_dir)
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
    executor = _executor or DuckDBExecutor(generated_dir=gen_dir)
    try:
        _register_entity_source(executor, entity, data_dir)

        log.info("Transforming: %s", entity.name)
        exec_result = executor.execute(entity.name, generated_dir=gen_dir)
        row_count = exec_result.row_count
    except Exception as e:
        return EntityRunResult(
            entity_name=entity.name,
            status="error",
            error=str(e),
            duration_seconds=time.monotonic() - start_time,
        )
    finally:
        if own_executor:
            executor.close()

    # 3. Quality checks
    quality_results = None
    if quality_checks and entity.quality and entity.quality.checks:
        try:
            from typedata.quality import QualityRunner

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
    generated_dir: str | Path | None = None,
    auto_generate: bool = True,
    quality_checks: bool = True,
    on_error: Literal["skip", "stop"] = "skip",
) -> RunResult:
    """Run the full typedata pipeline: discover, generate, execute, verify.

    Args:
        entities_dir: Directory containing entity .py files.
        data_dir: Base directory for parquet source data.
        backend: Backend engine (only "duckdb" supported).
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
    if backend != "duckdb":
        raise ValueError(f"Unsupported backend: {backend}. Supported: 'duckdb'")

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

    # Phase 2: Resolve dependency order
    sorted_entities = resolve_execution_order(registry)
    entity_names = [e.name for e in sorted_entities]
    log.debug("Execution order: %s", ", ".join(entity_names))
    log.info("Running %d entities on %s backend", len(sorted_entities), backend)

    # Phase 3: Execute
    results: list[EntityRunResult] = []
    failed_entities: set[str] = set()

    with DuckDBExecutor(generated_dir=gen_dir) as executor:
        for entity_info in sorted_entities:
            entity = entity_info.entity
            entity_name = entity_info.name

            # Check dependency failures (skip mode)
            if on_error == "skip":
                deps = _extract_dependencies(entity)
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

    total_duration = time.monotonic() - pipeline_start
    run_result = RunResult(
        entities=results,
        total_duration_seconds=total_duration,
        backend=backend,
    )
    log.info(
        "Pipeline complete: %d success, %d errors, %d skipped (%.1fs)",
        run_result.success_count,
        run_result.error_count,
        run_result.skipped_count,
        total_duration,
    )
    return run_result
