"""Phase 0 staging view materialization with state-table idempotency.

Materializes StagingView instances before source loading. Uses a
`fyrnheim_state` table in the target dataset to content-hash skip views
whose SQL has not changed.
"""

from __future__ import annotations

import logging
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


def _topo_sort(views: list[StagingView]) -> list[StagingView]:
    """Kahn-style topological sort by depends_on. Only edges within the
    provided view set are considered; external deps are ignored. Raises
    StagingCycleError on cycle.
    """
    by_name = {v.name: v for v in views}
    # adjacency: dep -> dependents
    remaining_deps: dict[str, set[str]] = {
        v.name: {d for d in v.depends_on if d in by_name} for v in views
    }

    ordered: list[StagingView] = []
    ready = sorted(
        [name for name, deps in remaining_deps.items() if not deps]
    )

    while ready:
        name = ready.pop(0)
        ordered.append(by_name[name])
        for other, deps in remaining_deps.items():
            if name in deps:
                deps.discard(name)
                if not deps and other not in [o.name for o in ordered] and other not in ready:
                    ready.append(other)
        ready.sort()

    if len(ordered) != len(views):
        # Find a cycle in the remaining subgraph.
        remaining = {n for n in by_name if n not in {o.name for o in ordered}}
        cycle = _find_cycle(remaining, by_name)
        raise StagingCycleError(cycle)

    return ordered


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
    cursor = executor.connection.raw_sql(
        f"SELECT name, {hash_col} FROM {qualified}"
    )
    try:
        rows = list(cursor.fetchall())  # type: ignore[union-attr]
    except AttributeError:
        rows = list(cursor)
    return {row[0]: row[1] for row in rows}


def _escape(value: str, backend: str = "duckdb") -> str:
    # Order matters: double backslashes FIRST, then handle single quotes.
    # Otherwise the backslash introduced by the quote-escape step on BigQuery
    # would be re-processed and corrupted.
    value = value.replace("\\", "\\\\")
    if backend == "bigquery":
        # BigQuery standard SQL rejects SQL-standard quote doubling ('')
        # inside string literals; use backslash escape instead.
        value = value.replace("'", "\\'")
    else:
        value = value.replace("'", "''")
    return (
        value.replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )


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
    ts = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    backend = executor.connection.name
    name_s = _escape(view.name, backend)
    hash_s = _escape(content_hash, backend)
    excerpt_s = _escape(excerpt, backend)
    version_s = _escape(_FYR_VERSION, backend)

    executor.connection.raw_sql(
        f"DELETE FROM {qualified} WHERE name = '{name_s}'"
    )
    hash_col = "`hash`" if executor.connection.name == "bigquery" else "hash"
    executor.connection.raw_sql(
        f"INSERT INTO {qualified} "
        f"(name, {hash_col}, materialized_at, sql_excerpt, fyrnheim_version) VALUES ("
        f"'{name_s}', '{hash_s}', TIMESTAMP '{ts}', "
        f"'{excerpt_s}', '{version_s}')"
    )


def materialize_staging_views(
    executor: IbisExecutor,
    views: list[StagingView],
    *,
    no_state: bool = False,
    source_fixture_names: set[str] | None = None,
) -> StagingRunSummary:
    """Materialize staging views in topological order with state skip.

    Args:
        executor: IbisExecutor connected to the target backend.
        views: List of StagingView instances to materialize.
        no_state: If True, bypass state lookup AND skip state writes.
        source_fixture_names: Set of source names whose upstream fixture
            shadows a staging view; any view whose name appears in this
            set is skipped (fixture-shadowed).

    Returns:
        StagingRunSummary with materialized / skipped / fixture_skipped lists.
    """
    summary = StagingRunSummary()
    if not views:
        return summary

    fixture_names = source_fixture_names or set()

    ordered = _topo_sort(views)

    # Filter out fixture-shadowed views up front; they are neither
    # materialized nor state-tracked.
    active: list[StagingView] = []
    for v in ordered:
        if v.name in fixture_names:
            summary.fixture_skipped.append(v.name)
            log.info("StagingView '%s' shadowed by duckdb fixture; skipping", v.name)
        else:
            active.append(v)

    if not active:
        return summary

    project, dataset = _resolve_state_target(active)

    existing_state: dict[str, str] = {}
    if not no_state:
        _ensure_state_table(executor, project, dataset)
        existing_state = _load_state(executor, project, dataset)

    for view in active:
        h = view.content_hash()
        if not no_state and existing_state.get(view.name) == h:
            summary.skipped.append(view.name)
            log.info("StagingView '%s' unchanged; skipping", view.name)
            continue

        executor.materialize_view(
            view.project, view.dataset, view.name, view.rendered_sql
        )
        summary.materialized.append(view.name)
        log.info("Materialized StagingView '%s'", view.name)

        if not no_state:
            _write_state_row(executor, project, dataset, view, h)

    return summary
