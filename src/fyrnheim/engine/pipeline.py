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

log = logging.getLogger("fyrnheim.pipeline")


@dataclass
class PipelineResult:
    """Result of a pipeline execution."""

    source_count: int = 0
    output_count: int = 0
    errors: list[str] = field(default_factory=list)
    outputs: dict[str, int] = field(default_factory=dict)  # name -> row_count
    elapsed_seconds: float = 0.0


def run_pipeline(
    assets: dict[str, list],
    config: ResolvedConfig,
    executor: IbisExecutor,
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
    conn = executor.connection

    sources = assets.get("sources", [])
    activities = assets.get("activities", [])
    identity_graphs = assets.get("identity_graphs", [])
    analytics_entities = assets.get("analytics_entities", [])
    metrics_models = assets.get("metrics_models", [])

    if not sources:
        result.elapsed_seconds = time.monotonic() - start
        return result

    # --- Phase 1: Load sources into events ---
    event_tables: list[ibis.Table] = []

    for source in sources:
        try:
            if isinstance(source, StateSource):
                events = _load_state_source(source, config, conn)
            elif isinstance(source, EventSource):
                events = load_event_source(conn, source, data_dir=config.data_dir)
            else:
                continue

            event_tables.append(events)
            result.source_count += 1
            log.info("Loaded source: %s", source.name)
        except Exception as exc:
            result.errors.append(f"Source '{source.name}': {exc}")
            log.warning("Failed to load source '%s': %s", source.name, exc)

    if not event_tables:
        result.elapsed_seconds = time.monotonic() - start
        return result

    # Concatenate all events
    all_events = _concat_tables(event_tables)

    # --- Phase 2: Apply activity definitions ---
    if activities:
        try:
            all_events = apply_activity_definitions(all_events, activities)
        except Exception as exc:
            result.errors.append(f"Activity definitions: {exc}")
            log.warning("Failed to apply activity definitions: %s", exc)

    # --- Phase 3: Identity resolution ---
    enriched_events = all_events
    if identity_graphs:
        for ig in identity_graphs:
            try:
                id_mapping = resolve_identities(all_events, ig)
                enriched_events = enrich_events(enriched_events, id_mapping)
            except Exception as exc:
                result.errors.append(f"Identity graph '{ig.name}': {exc}")
                log.warning("Failed identity resolution for '%s': %s", ig.name, exc)

    # --- Phase 4: Analytics entities ---
    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for ae in analytics_entities:
        try:
            projected = project_analytics_entity(enriched_events, ae)
            out_path = output_dir / f"{ae.name}.parquet"
            df = projected.execute()
            df.to_parquet(str(out_path))
            result.outputs[ae.name] = len(df)
            result.output_count += 1
            log.info("Wrote analytics entity: %s (%d rows)", ae.name, len(df))
        except Exception as exc:
            result.errors.append(f"Analytics entity '{ae.name}': {exc}")
            log.warning("Failed analytics entity '%s': %s", ae.name, exc)

    # --- Phase 5: Metrics models ---
    for mm in metrics_models:
        try:
            aggregated = aggregate_metrics(enriched_events, mm)
            out_path = output_dir / f"{mm.name}.parquet"
            df = aggregated.execute()
            df.to_parquet(str(out_path))
            result.outputs[mm.name] = len(df)
            result.output_count += 1
            log.info("Wrote metrics model: %s (%d rows)", mm.name, len(df))
        except Exception as exc:
            result.errors.append(f"Metrics model '{mm.name}': {exc}")
            log.warning("Failed metrics model '%s': %s", mm.name, exc)

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
