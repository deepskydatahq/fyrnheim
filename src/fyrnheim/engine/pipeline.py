"""Pipeline orchestrator: chains all engine components into an executable pipeline.

Flows: sources -> diff/events -> activities -> identity -> analytics entities + metrics -> parquet output.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path

import ibis
import pandas as pd

from fyrnheim.config import ResolvedConfig
from fyrnheim.core.source import EventSource, StateSource
from fyrnheim.engine.activity_engine import apply_activity_definitions
from fyrnheim.engine.analytics_entity_engine import project_analytics_entity
from fyrnheim.engine.event_source_loader import load_event_source
from fyrnheim.engine.executor import IbisExecutor
from fyrnheim.engine.identity_engine import enrich_events, resolve_identities
from fyrnheim.engine.metrics_engine import aggregate_metrics
from fyrnheim.engine.snapshot_diff import SnapshotDiffPipeline
from fyrnheim.engine.snapshot_store import SnapshotStore
from fyrnheim.engine.staging_runner import materialize_staging_views

log = logging.getLogger("fyrnheim.pipeline")


@dataclass
class PipelineTimings:
    """Per-phase / per-asset wall-clock timings for a pipeline run.

    Populated by ``run_pipeline`` via :func:`time.monotonic` deltas so
    downstream perf tooling (e.g. ``fyr bench``, the ``benchmark_result``
    pytest fixture) can reason about where wall-clock time is spent.

    All values are in seconds. Dict-valued fields are keyed by the
    corresponding asset's ``name``. Inner dicts for analytics entities
    and metrics models split work into ``project_s`` (projection +
    ``.execute()``) and ``write_s`` (``write_table`` or
    ``df.to_parquet``).
    """

    staging_views_s: float = 0.0
    source_loads: dict[str, float] = field(default_factory=dict)
    activities_s: float = 0.0
    identity_graphs: dict[str, float] = field(default_factory=dict)
    analytics_entities: dict[str, dict[str, float]] = field(default_factory=dict)
    metrics_models: dict[str, dict[str, float]] = field(default_factory=dict)


@dataclass
class PipelineResult:
    """Result of a pipeline execution."""

    source_count: int = 0
    output_count: int = 0
    errors: list[str] = field(default_factory=list)
    outputs: dict[str, int] = field(default_factory=dict)  # name -> row_count
    output_destinations: dict[str, str] = field(default_factory=dict)  # name -> destination
    elapsed_seconds: float = 0.0
    staging_materialized: list[str] = field(default_factory=list)
    staging_skipped: list[str] = field(default_factory=list)
    timings: PipelineTimings = field(default_factory=PipelineTimings)


def run_pipeline(
    assets: dict[str, list],
    config: ResolvedConfig,
    executor: IbisExecutor,
    *,
    no_state: bool = False,
) -> PipelineResult:
    """Execute the full pipeline: sources -> events -> activities -> identity -> outputs.

    Args:
        assets: Dict from _discover_assets() with keys: sources, activities,
            identity_graphs, analytics_entities, metrics_models.
        config: Resolved configuration with paths and backend info.
        executor: IbisExecutor with an active connection.

    Returns:
        PipelineResult with counts and any errors.
    """
    start = time.monotonic()
    result = PipelineResult()
    timings = result.timings
    conn = executor.connection

    sources = assets.get("sources", [])
    activities = assets.get("activities", [])
    identity_graphs = assets.get("identity_graphs", [])
    analytics_entities = assets.get("analytics_entities", [])
    metrics_models = assets.get("metrics_models", [])
    staging_views = assets.get("staging_views", [])

    # --- Phase 0: Materialize staging views (in-warehouse derived sources) ---
    if staging_views:
        # A staging view is "fixture-shadowed" when a source with a local
        # duckdb_path shares its name. In that case we skip materialization
        # since the source will load the fixture directly.
        fixture_names: set[str] = set()
        if config.backend == "duckdb":
            for src in sources:
                if getattr(src, "duckdb_path", None):
                    upstream = getattr(src, "upstream", None)
                    if upstream is not None:
                        fixture_names.add(upstream.name)
        t_phase0 = time.monotonic()
        try:
            staging_summary = materialize_staging_views(
                executor,
                list(staging_views),
                no_state=no_state,
                source_fixture_names=fixture_names,
            )
            result.staging_materialized = staging_summary.materialized
            result.staging_skipped = staging_summary.skipped
        except Exception as exc:
            result.errors.append(f"Staging views: {exc}")
            log.warning("Failed to materialize staging views: %s", exc)
        timings.staging_views_s = time.monotonic() - t_phase0

    if not sources:
        result.elapsed_seconds = time.monotonic() - start
        return result

    # --- Phase 1: Load sources into events ---
    event_tables: list[ibis.Table] = []

    for source in sources:
        t_source = time.monotonic()
        try:
            if isinstance(source, StateSource):
                events = _load_state_source(source, config, conn)
            elif isinstance(source, EventSource):
                events = load_event_source(
                    conn, source, data_dir=config.data_dir, backend=config.backend,
                )
            else:
                continue

            event_tables.append(events)
            result.source_count += 1
            log.info("Loaded source: %s", source.name)
        except Exception as exc:
            result.errors.append(f"Source '{source.name}': {exc}")
            log.warning("Failed to load source '%s': %s", source.name, exc)
        finally:
            timings.source_loads[source.name] = time.monotonic() - t_source

    if not event_tables:
        result.elapsed_seconds = time.monotonic() - start
        return result

    # Concatenate all events
    all_events = _concat_tables(event_tables)

    # --- Phase 2: Apply activity definitions ---
    if activities:
        t_phase2 = time.monotonic()
        try:
            all_events = apply_activity_definitions(all_events, activities)
        except Exception as exc:
            result.errors.append(f"Activity definitions: {exc}")
            log.warning("Failed to apply activity definitions: %s", exc)
        timings.activities_s = time.monotonic() - t_phase2

    # --- Phase 3: Identity resolution ---
    enriched_events = all_events
    if identity_graphs:
        for ig in identity_graphs:
            t_graph = time.monotonic()
            try:
                id_mapping = resolve_identities(all_events, ig)
                enriched_events = enrich_events(enriched_events, id_mapping)
            except Exception as exc:
                result.errors.append(f"Identity graph '{ig.name}': {exc}")
                log.warning("Failed identity resolution for '%s': %s", ig.name, exc)
            finally:
                timings.identity_graphs[ig.name] = time.monotonic() - t_graph

    # --- Phase 4: Analytics entities ---
    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for ae in analytics_entities:
        project_s = 0.0
        write_s = 0.0
        try:
            t_project = time.monotonic()
            projected = project_analytics_entity(enriched_events, ae)
            df = projected.execute()
            project_s = time.monotonic() - t_project

            t_write = time.monotonic()
            if ae.materialization == "table":
                table_name = ae.table or ae.name
                executor.write_table(ae.project, ae.dataset, table_name, df)
                destination = (
                    f"{config.backend}:{ae.project}.{ae.dataset}.{table_name}"
                )
                log.info(
                    "Wrote analytics entity to warehouse: %s (%d rows)",
                    destination,
                    len(df),
                )
            else:
                out_path = output_dir / f"{ae.name}.parquet"
                df.to_parquet(str(out_path))
                destination = f"parquet:{out_path}"
                log.info("Wrote analytics entity: %s (%d rows)", ae.name, len(df))
            write_s = time.monotonic() - t_write
            result.outputs[ae.name] = len(df)
            result.output_destinations[ae.name] = destination
            result.output_count += 1
        except Exception as exc:
            result.errors.append(f"Analytics entity '{ae.name}': {exc}")
            log.warning("Failed analytics entity '%s': %s", ae.name, exc)
        finally:
            timings.analytics_entities[ae.name] = {
                "project_s": project_s,
                "write_s": write_s,
            }

    # --- Phase 5: Metrics models ---
    for mm in metrics_models:
        project_s = 0.0
        write_s = 0.0
        try:
            t_project = time.monotonic()
            aggregated = aggregate_metrics(enriched_events, mm)
            df = aggregated.execute()
            project_s = time.monotonic() - t_project

            t_write = time.monotonic()
            if mm.materialization == "table":
                table_name = mm.table or mm.name
                executor.write_table(mm.project, mm.dataset, table_name, df)
                destination = (
                    f"{config.backend}:{mm.project}.{mm.dataset}.{table_name}"
                )
                log.info(
                    "Wrote metrics model to warehouse: %s (%d rows)",
                    destination,
                    len(df),
                )
            else:
                out_path = output_dir / f"{mm.name}.parquet"
                df.to_parquet(str(out_path))
                destination = f"parquet:{out_path}"
                log.info("Wrote metrics model: %s (%d rows)", mm.name, len(df))
            write_s = time.monotonic() - t_write
            result.outputs[mm.name] = len(df)
            result.output_destinations[mm.name] = destination
            result.output_count += 1
        except Exception as exc:
            result.errors.append(f"Metrics model '{mm.name}': {exc}")
            log.warning("Failed metrics model '%s': %s", mm.name, exc)
        finally:
            timings.metrics_models[mm.name] = {
                "project_s": project_s,
                "write_s": write_s,
            }

    result.elapsed_seconds = time.monotonic() - start
    return result


def _load_state_source(
    source: StateSource,
    config: ResolvedConfig,
    conn: ibis.BaseBackend,
) -> ibis.Table:
    """Load a StateSource through SnapshotDiffPipeline."""
    table = source.read_table(conn, config.backend, data_dir=config.data_dir)
    snapshot_dir = Path(config.output_dir) / "snapshots"
    store = SnapshotStore(snapshot_dir, conn)
    pipeline = SnapshotDiffPipeline(store, conn)
    return pipeline.run(
        source_name=source.name,
        current_table=table,
        id_field=source.id_field,
    )


def _concat_tables(tables: list[ibis.Table]) -> ibis.Table:
    """Concatenate multiple Ibis tables via pandas."""
    if len(tables) == 1:
        return tables[0]

    dfs = [t.execute() for t in tables]
    combined = pd.concat(dfs, ignore_index=True)
    return ibis.memtable(combined)
