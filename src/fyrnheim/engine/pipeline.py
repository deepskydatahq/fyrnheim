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
from fyrnheim.engine.event_source_loader import (
    _build_event_source_table,
    load_event_source,
)
from fyrnheim.engine.executor import IbisExecutor
from fyrnheim.engine.identity_engine import enrich_events, resolve_identities
from fyrnheim.engine.metrics_engine import aggregate_metrics
from fyrnheim.engine.snapshot_diff import SnapshotDiffPipeline
from fyrnheim.engine.snapshot_store import SnapshotStore
from fyrnheim.engine.source_stage import build_source_stage_table
from fyrnheim.engine.staging_runner import materialize_staging_views

log = logging.getLogger("fyrnheim.pipeline")


class SourceJoinCycleError(ValueError):
    """Raised when the inferred source-level-join graph contains a cycle.

    Cycles surface at pipeline-load time (before Phase 1) so users see
    the structural problem before any source actually executes. The
    cycle path is preserved on the exception for debugging.
    """

    def __init__(self, cycle: list[str]) -> None:
        self.cycle = cycle
        super().__init__(
            "Cycle detected in source joins: "
            f"{' -> '.join(cycle)}"
        )


def _topo_sort_sources(sources: list[Any]) -> list[list[Any]]:
    """Topologically partition ``sources`` into levels by inferred joins.

    Mirrors the staging_runner Kahn-style sort but returns LEVELS so
    the pipeline runner can preserve Phase 1 parallelism within a
    level (sources at the same dependency depth load in parallel) and
    serialize between levels (a level only fires after every source in
    every prior level has its post-pipeline table in the registry).

    The dependency graph is inferred from each source's
    ``joins[i].source_name`` — both StateSources (since M070 / v0.12.0)
    and EventSources (since M076 / v0.13.0) can expose ``joins``; the
    sort uses ``getattr(src, 'joins', [])`` to remain source-type
    agnostic. Edges that point at sources outside the provided list
    are ignored (they would be caught by the missing-source error
    inside ``_apply_joins`` — surfaced there with the helpful
    "topological sort guarantees this name is present; if you see
    this it's a typo" message).

    Stability: within a level, sources retain their declaration order
    relative to ``sources``. So pipelines with NO joins declared see
    a single level whose order is exactly ``sources`` — no behavior
    change relative to v0.11.0.

    M076 (v0.13.0): EventSource as a join TARGET is OUT of scope. If
    any source's ``joins`` reference an EventSource, this function
    raises ``ValueError`` with a clear future-enhancement pointer
    BEFORE the topological partition runs — early failure beats
    runtime failure inside ``_apply_joins``.

    Args:
        sources: The flat declaration-order source list (StateSource
            and EventSource instances mixed).

    Returns:
        A list of levels; each level is a list of source instances.
        Concatenating the levels reconstructs a valid topological
        order.

    Raises:
        ValueError: when a join references an EventSource (M076
            future-enhancement guard).
        SourceJoinCycleError: when a cycle is detected. The exception
            ``cycle`` attribute holds the cycle path.
    """
    by_name = {getattr(s, "name", None): s for s in sources}
    by_name.pop(None, None)
    declaration_index = {id(s): i for i, s in enumerate(sources)}

    # M076 (v0.13.0): EventSource as join TARGET is out of scope.
    # `_apply_joins` resolves the right-side primary-key column from
    # the joined source's `id_field`, which only StateSource exposes.
    # Surface this as a ValueError at sort time (before any worker
    # runs) so users see the structural problem early.
    for s in sources:
        joins = getattr(s, "joins", None) or []
        for j in joins:
            target = by_name.get(j.source_name)
            if isinstance(target, EventSource):
                raise ValueError(
                    f"Join in source {s.name!r} references "
                    f"{j.source_name!r} which is an EventSource. "
                    "Joining TO an EventSource is not yet supported "
                    "(the right_pk_registry's id_field lookup assumes "
                    "StateSource semantics; EventSources expose "
                    "entity_id_field + timestamp_field instead). "
                    "File a future-enhancement request if you need "
                    "this. EventSource as the LEFT side of a join "
                    "(i.e. an EventSource declaring `joins=[...]`) "
                    "IS supported as of M076 / v0.13.0."
                )

    # remaining_deps[name] = set of as-yet-unresolved join sources that
    # this source depends on. Edges pointing outside `sources` (no name
    # match) are dropped — _apply_joins surfaces those at runtime.
    remaining_deps: dict[str, set[str]] = {}
    for s in sources:
        joins = getattr(s, "joins", None) or []
        deps = {j.source_name for j in joins if j.source_name in by_name}
        # A source must not list itself as a join dependency. ibis
        # would reject the resulting predicate with a column-name
        # collision long before we got to the pipeline run, but we
        # guard up front so the error message is structural.
        deps.discard(s.name)
        remaining_deps[s.name] = deps

    levels: list[list[Any]] = []
    placed: set[str] = set()

    while len(placed) < len(remaining_deps):
        # A "level" is every source whose remaining_deps is fully
        # contained in `placed`. Order within the level mirrors the
        # original declaration index so the no-joins-case is a no-op.
        level = [
            s
            for s in sources
            if s.name not in placed
            and remaining_deps[s.name].issubset(placed)
        ]
        if not level:
            # Cycle: at least one source still has unresolved deps but
            # no source is ready. Find the cycle path through the
            # remaining sub-graph.
            remaining_names = {
                n for n in remaining_deps if n not in placed
            }
            cycle = _find_source_cycle(remaining_names, sources)
            raise SourceJoinCycleError(cycle)
        # Stable sort by declaration order (already preserved by the
        # `for s in sources` iteration; explicit for clarity).
        level.sort(key=lambda s: declaration_index[id(s)])
        levels.append(level)
        placed.update(s.name for s in level)

    return levels


def _find_source_cycle(
    nodes: set[str], sources: list[Any]
) -> list[str]:
    """Return a cycle path among ``nodes`` in the source-join graph.

    DFS from each node in declaration order so the reported cycle is
    deterministic. The returned list is the cycle path with the start
    node repeated at the end (e.g. ``['a', 'b', 'a']``) — matches the
    staging_runner cycle-error convention.
    """
    by_name: dict[str, Any] = {
        s.name: s for s in sources if s.name in nodes
    }

    visited: set[str] = set()
    stack: list[str] = []
    on_stack: set[str] = set()

    def dfs(n: str) -> list[str] | None:
        if n in on_stack:
            idx = stack.index(n)
            return stack[idx:] + [n]
        if n in visited:
            return None
        visited.add(n)
        stack.append(n)
        on_stack.add(n)
        joins = getattr(by_name[n], "joins", None) or []
        for j in joins:
            if j.source_name in nodes:
                result = dfs(j.source_name)
                if result is not None:
                    return result
        stack.pop()
        on_stack.discard(n)
        return None

    # Iterate in declaration order for deterministic cycle reporting.
    for s in sources:
        if s.name in nodes:
            result = dfs(s.name)
            if result is not None:
                return result
    return sorted(nodes)


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

    # --- Phase 1: Load sources into events (level-parallel) ---
    # M070 (v0.12.0) introduces source-level joins, so Phase 1 now
    # topologically sorts sources by their inferred join dependencies
    # before loading. The sort produces "levels" — buckets of sources
    # that have no remaining dependencies at the same depth — and the
    # runner loads each level in parallel (mirrors staging_runner) and
    # serializes between levels. Pipelines with NO joins declared
    # collapse to a single level whose order is exactly declaration
    # order, so the v0.11.0 behavior is preserved bit-for-bit.
    #
    # Two registries flow alongside Phase 1:
    #   * loaded_sources: ``name → post-pipeline ibis.Table`` for each
    #     source that has finished its full transforms/joins/json_path/
    #     computed_columns/filter chain. Source-level joins read this
    #     registry to resolve the right-side joined table — populated
    #     AFTER each source's chain completes, so a join always sees
    #     the post-pipeline shape of its dependency.
    #   * right_pk_registry: ``name → id_field`` for StateSource targets
    #     so ``_apply_joins`` can build ``table[fk] == other[pk]``
    #     equality predicates without re-reading the source declaration.
    #     Only StateSources are valid join targets in v0.12.0;
    #     EventSources are absent from this registry, and any join
    #     pointing at one surfaces a clear ValueError from _apply_joins.
    #
    # DuckDB's embedded Python connection is NOT thread-safe at the
    # connection level, so we reuse the executor's connection lock
    # (no-op nullcontext for BigQuery, real Lock for DuckDB) to serialize
    # connection access across workers. On BigQuery the underlying
    # google.cloud client is already thread-safe, so parallelism is
    # preserved.
    phase1_lock = executor._conn_lock
    loaded_sources: dict[str, ibis.Table] = {}
    right_pk_registry: dict[str, str] = {
        s.name: s.id_field for s in sources if isinstance(s, StateSource)
    }

    try:
        source_levels = _topo_sort_sources(sources)
    except SourceJoinCycleError as exc:
        result.errors.append(f"Source joins: {exc}")
        log.error("%s", exc)
        result.elapsed_seconds = time.monotonic() - start
        return result
    except ValueError as exc:
        # M076: EventSource-as-join-target guard surfaces here too.
        result.errors.append(f"Source joins: {exc}")
        log.error("%s", exc)
        result.elapsed_seconds = time.monotonic() - start
        return result

    def _load_one_source(
        src: object,
    ) -> tuple[ibis.Table | None, ibis.Table | None]:
        """Load one source. Returns ``(events, pre_diff_table)`` where
        ``events`` is the universal-schema event table (or None for
        unsupported source types) and ``pre_diff_table`` is the
        post-transform / pre-snapshot-diff (or pre-event-shape, for
        EventSource) Ibis table that downstream consumers should consume
        from the registry. M076 (v0.13.0): EventSources also populate
        the registry with their post-pipeline (post-joins, pre-event-
        shape) table. Joins TO an EventSource remain blocked at sort
        time, so the registered EventSource entry is forward-compatible
        rather than load-bearing today."""
        if isinstance(src, StateSource):
            with phase1_lock:
                pre_diff = _build_state_source_table(
                    src,
                    config,
                    conn,
                    source_registry=loaded_sources,
                    right_pk_registry=right_pk_registry,
                )
                # Populate the registry BEFORE running snapshot-diff so
                # any same-level race-loser would still see this source
                # available — but topo-sort guarantees no same-level
                # source has a cross-level dep. The lock makes this
                # write race-safe with the registry reads of subsequent
                # levels.
                loaded_sources[src.name] = pre_diff
                events = _run_state_source_diff(src, config, conn, pre_diff)
                return events, pre_diff
        if isinstance(src, EventSource):
            with phase1_lock:
                # M076 (v0.13.0): build the post-pipeline table first so
                # the registry can hold it BEFORE event-shape conversion.
                # Joined columns are part of the post-pipeline table; the
                # event-shape conversion's payload-pack step (load_event_source
                # below) carries them into the emitted event payload.
                pre_event = _build_event_source_table(
                    conn,
                    src,
                    data_dir=config.data_dir,
                    backend=config.backend,
                    source_registry=loaded_sources,
                    right_pk_registry=right_pk_registry,
                )
                loaded_sources[src.name] = pre_event
                events = load_event_source(
                    conn,
                    src,
                    data_dir=config.data_dir,
                    backend=config.backend,
                    source_registry=loaded_sources,
                    right_pk_registry=right_pk_registry,
                    pre_built_table=pre_event,
                )
                return events, pre_event
        return None, None

    def _phase1_worker(
        src: object,
    ) -> tuple[ibis.Table | None, float]:
        t_source = time.monotonic()
        events, _pre_diff = _load_one_source(src)
        return events, time.monotonic() - t_source

    loaded_tables_by_name: dict[str, ibis.Table | None] = {}
    max_workers = max(1, int(config.max_parallel_io))
    for level in source_levels:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                src.name: pool.submit(_phase1_worker, src) for src in level
            }
            for src in level:
                tbl, elapsed = futures[src.name].result()
                loaded_tables_by_name[src.name] = tbl
                timings.source_loads[src.name] = elapsed
                if tbl is not None:
                    result.source_count += 1
                    log.info("Loaded source: %s", src.name)

    # Preserve declaration order in the event-table list so existing
    # downstream consumers (and tests that assume declaration order)
    # see no behavior change in the no-joins case.
    loaded_tables: list[ibis.Table | None] = [
        loaded_tables_by_name.get(src.name) for src in sources
    ]
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


def _build_state_source_table(
    source: StateSource,
    config: ResolvedConfig,
    conn: ibis.BaseBackend,
    *,
    source_registry: dict[str, ibis.Table] | None = None,
    right_pk_registry: dict[str, str] | None = None,
) -> ibis.Table:
    """Apply the shared source-stage chain to a StateSource.

    The shared chain is implemented by
    :func:`fyrnheim.engine.source_stage.build_source_stage_table`:

      read → transforms → joins → json_path → computed_columns → filter

    StateSource-specific snapshot diff remains in :func:`_run_state_source_diff`.
    """
    return build_source_stage_table(
        source,
        conn,
        config.backend,
        data_dir=config.data_dir,
        source_registry=source_registry,
        right_pk_registry=right_pk_registry,
        log=log,
        source_kind="StateSource",
    )


def _load_state_source(
    source: StateSource,
    config: ResolvedConfig,
    conn: ibis.BaseBackend,
    *,
    source_registry: dict[str, ibis.Table] | None = None,
    right_pk_registry: dict[str, str] | None = None,
) -> ibis.Table:
    """Load a StateSource through SnapshotDiffPipeline.

    When ``source.full_refresh`` is True, bypass the snapshot-diff machinery
    entirely and emit ``row_appeared`` for every current row. No snapshot
    read, no snapshot save — deterministic "entity reflects current state"
    behavior independent of any prior snapshot (M066 escape hatch).

    M070 (v0.12.0): when ``source.joins`` is non-empty, ``_apply_joins``
    fires AFTER ``_apply_source_transforms`` and BEFORE
    ``_apply_json_path_extractions`` — so users can join on
    transform-renamed columns and extract JSON from joined columns.
    The two registry dicts (``source_registry`` for ``name → table``
    and ``right_pk_registry`` for ``name → id_field``) are populated
    by ``run_pipeline`` Phase 1 and passed in here. When called
    directly (e.g. from tests with no joins declared), passing ``None``
    for both is fine — the helper short-circuits on empty joins.
    """
    table = _build_state_source_table(
        source,
        config,
        conn,
        source_registry=source_registry,
        right_pk_registry=right_pk_registry,
    )
    return _run_state_source_diff(source, config, conn, table)


def _run_state_source_diff(
    source: StateSource,
    config: ResolvedConfig,
    conn: ibis.BaseBackend,
    table: ibis.Table,
) -> ibis.Table:
    """Convert a post-pipeline StateSource table into the universal
    event-schema view (source/entity_id/ts/event_type/payload).

    Either replays every current row as ``row_appeared`` (when
    ``source.full_refresh`` is True) or runs the SnapshotDiffPipeline
    against the on-disk snapshot store. Extracted from
    :func:`_load_state_source` for M070 so the run_pipeline orchestrator
    can stash the post-pipeline ``table`` in the join-source registry
    BEFORE the diff turns it into events.
    """
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


def _canonical_event_table(table: ibis.Table) -> ibis.Table:
    """Normalize event stream columns before backend-side unioning."""
    return table.select(
        source=table["source"].cast("string"),
        entity_id=table["entity_id"].cast("string"),
        ts=table["ts"].cast("string"),
        event_type=table["event_type"].cast("string"),
        payload=table["payload"].cast("string"),
    )


def _concat_tables(tables: list[ibis.Table]) -> ibis.Table:
    """Concatenate event tables as a backend-executable UNION ALL."""
    canonical = [_canonical_event_table(t) for t in tables]
    if len(canonical) == 1:
        return canonical[0]
    return ibis.union(*canonical, distinct=False)
