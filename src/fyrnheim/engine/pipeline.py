"""Pipeline orchestrator: chains all engine components into an executable pipeline.

Flows: sources -> diff/events -> activities -> identity -> analytics entities + metrics -> parquet output.
"""

from __future__ import annotations

import datetime
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import ibis
import pandas as pd

from fyrnheim.config import ResolvedConfig
from fyrnheim.core.source import EventSource, StateSource
from fyrnheim.engine.activity_engine import apply_activity_definitions
from fyrnheim.engine.analytics_entity_engine import project_analytics_entity
from fyrnheim.engine.diff_engine import _make_appeared_events
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

    # --- Phase 1: Load sources into events (parallel) ---
    # Sources fan out via ThreadPoolExecutor bounded by max_parallel_io.
    # The resulting event_tables list preserves sources order via
    # index-based assignment (no as_completed). The first worker exception
    # surfaces to the caller of run_pipeline (future.result() re-raises).
    #
    # DuckDB's embedded Python connection is NOT thread-safe at the
    # connection level, so we reuse the executor's connection lock
    # (no-op nullcontext for BigQuery, real Lock for DuckDB) to serialize
    # connection access across workers. On BigQuery the underlying
    # google.cloud client is already thread-safe, so parallelism is
    # preserved.
    phase1_lock = executor._conn_lock

    def _load_one_source(src: object) -> ibis.Table | None:
        if isinstance(src, StateSource):
            with phase1_lock:
                return _load_state_source(src, config, conn)
        if isinstance(src, EventSource):
            with phase1_lock:
                return load_event_source(
                    conn, src, data_dir=config.data_dir, backend=config.backend,
                )
        return None

    def _phase1_worker(src: object) -> tuple[ibis.Table | None, float]:
        t_source = time.monotonic()
        tbl = _load_one_source(src)
        return tbl, time.monotonic() - t_source

    loaded_tables: list[ibis.Table | None] = [None] * len(sources)
    max_workers = max(1, int(config.max_parallel_io))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(_phase1_worker, src) for src in sources]
        for i, fut in enumerate(futures):
            source = sources[i]
            tbl, elapsed = fut.result()
            loaded_tables[i] = tbl
            timings.source_loads[source.name] = elapsed
            if tbl is not None:
                result.source_count += 1
                log.info("Loaded source: %s", source.name)

    event_tables: list[ibis.Table] = [t for t in loaded_tables if t is not None]

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

    # --- Phase 4: Analytics entities (projection sequential, writes parallel) ---
    # Projection (project_analytics_entity + .execute()) stays sequential in
    # the main thread — that axis is M060's territory. Only the write step
    # (executor.write_table or df.to_parquet) is submitted to the pool.
    # Worker exceptions propagate via future.result().
    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    def _write_frame(
        *,
        label: str,
        name: str,
        materialization: str | None,
        table: str | None,
        project: str,
        dataset: str,
        df: pd.DataFrame,
    ) -> tuple[str, int, float]:
        """Write a single DataFrame to its configured destination.

        Returns (destination, row_count, write_elapsed_seconds). Runs in a
        worker thread; must not touch pipeline-wide mutable state except
        for the already-captured DataFrame.
        """
        t_write = time.monotonic()
        if materialization == "table":
            table_name = table or name
            executor.write_table(project, dataset, table_name, df)
            destination = f"{config.backend}:{project}.{dataset}.{table_name}"
            log.info(
                "Wrote %s to warehouse: %s (%d rows)", label, destination, len(df)
            )
        else:
            out_path = output_dir / f"{name}.parquet"
            df.to_parquet(str(out_path))
            destination = f"parquet:{out_path}"
            log.info("Wrote %s: %s (%d rows)", label, name, len(df))
        return destination, len(df), time.monotonic() - t_write

    if analytics_entities:
        ae_pending: list[tuple[Any, pd.DataFrame, float]] = []
        for ae in analytics_entities:
            t_project = time.monotonic()
            projected = project_analytics_entity(enriched_events, ae)
            df = projected.execute()
            ae_pending.append((ae, df, time.monotonic() - t_project))

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            ae_futures = [
                pool.submit(
                    _write_frame,
                    label="analytics entity",
                    name=ae.name,
                    materialization=ae.materialization,
                    table=ae.table,
                    project=ae.project,
                    dataset=ae.dataset,
                    df=df,
                )
                for ae, df, _ in ae_pending
            ]
            for (ae, _df, project_s), ae_fut in zip(
                ae_pending, ae_futures, strict=True
            ):
                destination, row_count, write_s = ae_fut.result()
                # Dict-setitem is atomic in CPython, safe without a lock.
                timings.analytics_entities[ae.name] = {
                    "project_s": project_s,
                    "write_s": write_s,
                }
                result.outputs[ae.name] = row_count
                result.output_destinations[ae.name] = destination
                result.output_count += 1

    # --- Phase 5: Metrics models (projection sequential, writes parallel) ---
    if metrics_models:
        mm_pending: list[tuple[Any, pd.DataFrame, float]] = []
        for mm in metrics_models:
            t_project = time.monotonic()
            aggregated = aggregate_metrics(enriched_events, mm)
            df = aggregated.execute()
            mm_pending.append((mm, df, time.monotonic() - t_project))

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            mm_futures = [
                pool.submit(
                    _write_frame,
                    label="metrics model",
                    name=mm.name,
                    materialization=mm.materialization,
                    table=mm.table,
                    project=mm.project,
                    dataset=mm.dataset,
                    df=df,
                )
                for mm, df, _ in mm_pending
            ]
            for (mm, _df, project_s), mm_fut in zip(
                mm_pending, mm_futures, strict=True
            ):
                destination, row_count, write_s = mm_fut.result()
                timings.metrics_models[mm.name] = {
                    "project_s": project_s,
                    "write_s": write_s,
                }
                result.outputs[mm.name] = row_count
                result.output_destinations[mm.name] = destination
                result.output_count += 1

    result.elapsed_seconds = time.monotonic() - start
    return result


def _load_state_source(
    source: StateSource,
    config: ResolvedConfig,
    conn: ibis.BaseBackend,
) -> ibis.Table:
    """Load a StateSource through SnapshotDiffPipeline.

    When ``source.full_refresh`` is True, bypass the snapshot-diff machinery
    entirely and emit ``row_appeared`` for every current row. No snapshot
    read, no snapshot save — deterministic "entity reflects current state"
    behavior independent of any prior snapshot (M066 escape hatch).
    """
    table = source.read_table(conn, config.backend, data_dir=config.data_dir)

    if source.full_refresh:
        # M066: escape hatch — never consult the snapshot store.
        today = datetime.date.today().isoformat()
        replay_events = _make_appeared_events(
            table.execute(), source.name, source.id_field, today
        )
        events = ibis.memtable(pd.DataFrame(replay_events))
        log.info(
            "StateSource %s: full_refresh enabled, emitted %d rows as row_appeared",
            source.name,
            len(replay_events),
        )
        return events

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
