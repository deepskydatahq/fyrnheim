"""Topological-sort tests for source-level joins (M070 / v0.12.0).

The pipeline runner pre-sorts sources into "levels" before Phase 1 so
that any source declaring a ``Join(source_name=X, ...)`` loads after
``X`` has finished its full transforms/joins/json_path/computed_columns/
filter chain. Pipelines with NO joins declared collapse to a single
level whose order is exactly declaration order — that's the v0.11.0
preservation guarantee that lets these tests run without tripping
existing e2e regressions.

The sort is exercised directly here so we can assert the structural
contract independent of the rest of Phase 1.
"""

from __future__ import annotations

import pytest

from fyrnheim.core.source import Join, StateSource
from fyrnheim.engine.pipeline import (
    SourceJoinCycleError,
    _topo_sort_sources,
)


def _state_source(
    name: str, *, joins: list[Join] | None = None
) -> StateSource:
    """Build a minimal StateSource for topo-sort fixtures."""
    return StateSource(
        name=name,
        project="p",
        dataset="d",
        table=name,
        id_field="id",
        joins=joins or [],
    )


def test_sources_no_joins_preserve_declaration_order() -> None:
    """When no source declares joins, the topo sort returns a single
    level whose order is exactly the input declaration order. This is
    the v0.11.0 preservation guarantee — pipelines that don't opt into
    joins must see no behavior change."""
    a = _state_source("a")
    b = _state_source("b")
    c = _state_source("c")
    levels = _topo_sort_sources([a, b, c])
    # Exactly one level (no edges).
    assert len(levels) == 1
    assert [s.name for s in levels[0]] == ["a", "b", "c"]


def test_source_with_join_to_earlier_source_orders_correctly() -> None:
    """When a source declares a join to an earlier-declared source, the
    sort places the dependency BEFORE the dependent (i.e. on a strictly
    earlier level). Declaration order already satisfies the topology
    here — confirm the runner produces the expected level shape."""
    stages = _state_source("lifecycle_stage")
    history = _state_source(
        "lifecycle_history",
        joins=[
            Join(source_name="lifecycle_stage", join_key="previous_stage_id"),
        ],
    )
    levels = _topo_sort_sources([stages, history])
    # Two levels: dep first, dependent second.
    assert len(levels) == 2
    assert [s.name for s in levels[0]] == ["lifecycle_stage"]
    assert [s.name for s in levels[1]] == ["lifecycle_history"]


def test_source_with_join_to_later_source_gets_reordered() -> None:
    """When a source declares a join to a LATER-declared source, the
    sort moves the dependency forward (the dependent's join would fail
    at runtime if we honored declaration order naively). This is the
    "reorder" case — declaration order changes."""
    history = _state_source(
        "lifecycle_history",
        joins=[
            Join(source_name="lifecycle_stage", join_key="previous_stage_id"),
        ],
    )
    stages = _state_source("lifecycle_stage")
    # Declaration order: history first, then stages.
    levels = _topo_sort_sources([history, stages])
    # Sort must place stages first (level 0) and history second (level 1).
    assert len(levels) == 2
    assert [s.name for s in levels[0]] == ["lifecycle_stage"]
    assert [s.name for s in levels[1]] == ["lifecycle_history"]


def test_cycle_in_joins_raises_clear_error() -> None:
    """A cycle (A joins B, B joins A) raises SourceJoinCycleError at
    topo-sort time with a message naming the cycle nodes. The error
    surfaces BEFORE Phase 1 starts so users see structural problems
    before any source actually executes."""
    a = _state_source(
        "a", joins=[Join(source_name="b", join_key="b_id")]
    )
    b = _state_source(
        "b", joins=[Join(source_name="a", join_key="a_id")]
    )
    with pytest.raises(SourceJoinCycleError) as excinfo:
        _topo_sort_sources([a, b])
    # Message includes both cycle nodes.
    msg = str(excinfo.value)
    assert "a" in msg and "b" in msg
    assert "Cycle detected in source joins" in msg


def test_diamond_join_dependency_levels() -> None:
    """A diamond shape (D joins B + C; B and C both join A) produces
    three levels: {A} → {B, C} → {D}. B and C share a level — they
    can load in parallel because they have no inter-dependency.

    This pins the "level-parallel" semantics: same-level sources are
    independent, cross-level edges serialize.
    """
    a = _state_source("a")
    b = _state_source(
        "b", joins=[Join(source_name="a", join_key="a_id")]
    )
    c = _state_source(
        "c", joins=[Join(source_name="a", join_key="a_id")]
    )
    d = _state_source(
        "d",
        joins=[
            Join(source_name="b", join_key="b_id"),
            Join(source_name="c", join_key="c_id"),
        ],
    )
    levels = _topo_sort_sources([a, b, c, d])
    assert [s.name for s in levels[0]] == ["a"]
    assert {s.name for s in levels[1]} == {"b", "c"}
    assert [s.name for s in levels[2]] == ["d"]
