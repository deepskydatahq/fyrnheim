"""Phase 0 staging view materialization with state-table idempotency.

Materializes StagingView instances before source loading. Uses a
`fyrnheim_state` table in the target dataset to content-hash skip views
whose SQL has not changed.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime

from fyrnheim import __version__ as _FYR_VERSION
from fyrnheim.core.staging_view import StagingView
from fyrnheim.engine.executor import IbisExecutor

log = logging.getLogger("fyrnheim.staging_runner")

STATE_TABLE_NAME = "fyrnheim_state"


class StagingCycleError(ValueError):
    """Raised when a cycle is detected in staging view depends_on graph."""

    def __init__(self, cycle: list[str]) -> None:
        self.cycle = cycle
        super().__init__(f"Cycle detected in staging views: {' -> '.join(cycle)}")


@dataclass
class StagingRunSummary:
    """Summary of a staging view materialization run."""

    materialized: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    fixture_skipped: list[str] = field(default_factory=list)


def _topo_levels(views: list[StagingView]) -> list[list[StagingView]]:
    """Group staging views into topological dependency levels.

    Only edges within the provided view set are considered; external deps are
    ignored. Views in the same level have no dependencies on each other and can
    be materialized concurrently. Level and intra-level ordering are stable and
    alphabetical, matching the previous topological order for serial runs.
    """
    by_name = {v.name: v for v in views}
    remaining_deps: dict[str, set[str]] = {
        v.name: {d for d in v.depends_on if d in by_name} for v in views
    }

    levels: list[list[StagingView]] = []
    placed: set[str] = set()

    while len(placed) < len(views):
        ready = sorted(
            name
            for name, deps in remaining_deps.items()
            if name not in placed and deps.issubset(placed)
        )
        if not ready:
            remaining = {n for n in by_name if n not in placed}
            cycle = _find_cycle(remaining, by_name)
            raise StagingCycleError(cycle)
        levels.append([by_name[name] for name in ready])
        placed.update(ready)

    return levels


def _topo_sort(views: list[StagingView]) -> list[StagingView]:
    """Kahn-style topological sort by depends_on.

    Kept for callers/tests that import the private helper; implemented by
    flattening the dependency levels used by the parallel runner.
    """
    return [view for level in _topo_levels(views) for view in level]


def _find_cycle(nodes: set[str], by_name: dict[str, StagingView]) -> list[str]:
    """Return a cycle path among `nodes` in the staging view graph."""
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
        for dep in by_name[n].depends_on:
            if dep in nodes:
                result = dfs(dep)
                if result is not None:
                    return result
        stack.pop()
        on_stack.discard(n)
        return None

    for node in sorted(nodes):
        result = dfs(node)
        if result is not None:
            return result
    return sorted(nodes)


def _resolve_state_target(views: list[StagingView]) -> tuple[str, str]:
    """Return (project, dataset) to host the state table. v1: derive from
    the first view and error if mixed.
    """
    first = views[0]
    project, dataset = first.project, first.dataset
    for v in views[1:]:
        if v.project != project or v.dataset != dataset:
            raise ValueError(
                "Mixed (project, dataset) across StagingViews is not supported "
                f"in v1: got {first.project}.{first.dataset} and "
                f"{v.project}.{v.dataset}"
            )
    return project, dataset


def _qualify_state_table(executor: IbisExecutor, project: str, dataset: str) -> str:
    """Return backend-appropriate fully qualified state table identifier."""
    backend = executor.connection.name
    if backend == "bigquery":
        return f"`{project}.{dataset}.{STATE_TABLE_NAME}`"
    # duckdb
    return f'"{dataset}"."{STATE_TABLE_NAME}"'


def _ensure_state_table(executor: IbisExecutor, project: str, dataset: str) -> None:
    """Create the fyrnheim_state table if it does not exist."""
    backend = executor.connection.name
    qualified = _qualify_state_table(executor, project, dataset)
    if backend == "duckdb":
        executor.connection.raw_sql(f'CREATE SCHEMA IF NOT EXISTS "{dataset}"')
        executor.connection.raw_sql(
            f"CREATE TABLE IF NOT EXISTS {qualified} ("
            "name VARCHAR PRIMARY KEY, "
            "hash VARCHAR, "
            "materialized_at TIMESTAMP, "
            "sql_excerpt VARCHAR, "
            "fyrnheim_version VARCHAR"
            ")"
        )
    elif backend == "bigquery":
        executor.connection.raw_sql(
            f"CREATE TABLE IF NOT EXISTS {qualified} ("
            "name STRING NOT NULL, "
            "`hash` STRING, "
            "materialized_at TIMESTAMP, "
            "sql_excerpt STRING, "
            "fyrnheim_version STRING"
            ")"
        )
    else:
        raise NotImplementedError(
            f"staging state table not supported for backend {backend!r}"
        )


def _load_state(
    executor: IbisExecutor, project: str, dataset: str
) -> dict[str, str]:
    """Return {name: hash} from the state table."""
    qualified = _qualify_state_table(executor, project, dataset)
    hash_col = "`hash`" if executor.connection.name == "bigquery" else "hash"
    rows = executor.execute_parameterized(
        f"SELECT name, {hash_col} FROM {qualified}",
        {},
    )
    return {row[0]: row[1] for row in rows} if rows else {}


def _write_state_row(
    executor: IbisExecutor,
    project: str,
    dataset: str,
    view: StagingView,
    content_hash: str,
) -> None:
    """Upsert a row in the state table for the given view (DELETE+INSERT)."""
    qualified = _qualify_state_table(executor, project, dataset)
    excerpt = view.rendered_sql[:500]
    ts = datetime.now(UTC)
    backend = executor.connection.name
    hash_col = "`hash`" if backend == "bigquery" else "hash"

    executor.execute_parameterized(
        f"DELETE FROM {qualified} WHERE name = @name",
        {"name": view.name},
    )
    executor.execute_parameterized(
        f"INSERT INTO {qualified} "
        f"(name, {hash_col}, materialized_at, sql_excerpt, fyrnheim_version) "
        f"VALUES (@name, @hash, @ts, @excerpt, @version)",
        {
            "name": view.name,
            "hash": content_hash,
            "ts": ts,
            "excerpt": excerpt,
            "version": _FYR_VERSION,
        },
    )


def materialize_staging_views(
    executor: IbisExecutor,
    views: list[StagingView],
    *,
    no_state: bool = False,
    source_fixture_names: set[str] | None = None,
    max_parallel_io: int = 1,
) -> StagingRunSummary:
    """Materialize staging views by topological level with state skip.

    Args:
        executor: IbisExecutor connected to the target backend.
        views: List of StagingView instances to materialize.
        no_state: If True, bypass state lookup AND skip state writes.
        source_fixture_names: Set of source names whose upstream fixture
            shadows a staging view; any view whose name appears in this
            set is skipped (fixture-shadowed).
        max_parallel_io: Maximum number of independent views to materialize
            concurrently within one dependency level. ``1`` preserves the
            historical serial execution order.

    Returns:
        StagingRunSummary with materialized / skipped / fixture_skipped lists.
    """
    summary = StagingRunSummary()
    if not views:
        return summary

    fixture_names = source_fixture_names or set()

    levels = _topo_levels(views)

    # Filter out fixture-shadowed views up front; they are neither
    # materialized nor state-tracked. Keep level boundaries intact so
    # dependency ordering remains explicit for the active views.
    active_levels: list[list[StagingView]] = []
    active: list[StagingView] = []
    for level in levels:
        active_level: list[StagingView] = []
        for v in level:
            if v.name in fixture_names:
                summary.fixture_skipped.append(v.name)
                log.info("StagingView '%s' shadowed by duckdb fixture; skipping", v.name)
            else:
                active_level.append(v)
                active.append(v)
        if active_level:
            active_levels.append(active_level)

    if not active:
        return summary

    project, dataset = _resolve_state_target(active)

    existing_state: dict[str, str] = {}
    if not no_state:
        _ensure_state_table(executor, project, dataset)
        existing_state = _load_state(executor, project, dataset)

    hashes = {view.name: view.content_hash() for view in active}
    runnable_levels: list[list[StagingView]] = []
    for level in active_levels:
        runnable_level: list[StagingView] = []
        for view in level:
            if not no_state and existing_state.get(view.name) == hashes[view.name]:
                summary.skipped.append(view.name)
                log.info("StagingView '%s' unchanged; skipping", view.name)
            else:
                runnable_level.append(view)
        if runnable_level:
            runnable_levels.append(runnable_level)

    workers = max(1, int(max_parallel_io))

    def materialize_one(view: StagingView) -> str:
        executor.materialize_view(
            view.project, view.dataset, view.name, view.rendered_sql
        )
        log.info("Materialized StagingView '%s'", view.name)
        if not no_state:
            _write_state_row(executor, project, dataset, view, hashes[view.name])
        return view.name

    for level in runnable_levels:
        if workers == 1 or len(level) == 1:
            for view in level:
                summary.materialized.append(materialize_one(view))
            continue

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(materialize_one, view) for view in level]
            for future in futures:
                # Iterating in submission order preserves deterministic summary
                # ordering. If any view fails, the exception propagates here and
                # later dependency levels are not started.
                summary.materialized.append(future.result())

    return summary
