"""Dependency resolution for entity execution ordering."""

from __future__ import annotations

from graphlib import CycleError, TopologicalSorter
from typing import TYPE_CHECKING

from fyrnheim.core.source import AggregationSource, DerivedSource

if TYPE_CHECKING:
    from fyrnheim.core.entity import Entity
    from fyrnheim.engine.registry import EntityInfo, EntityRegistry


class CircularDependencyError(Exception):
    """Raised when entities form a circular dependency."""


def _extract_dependencies(entity: Entity) -> list[str]:
    """Return entity names this entity depends on.

    Dependency edges come from the entity's source type:
    - AggregationSource: [source_entity] + depends_on
    - DerivedSource: depends_on list
    - All other source types: no entity dependencies.
    """
    source = entity.source
    if source is None:
        return []
    if isinstance(source, AggregationSource):
        return [source.source_entity] + list(source.depends_on)
    if isinstance(source, DerivedSource):
        return list(source.depends_on)
    return []


def resolve_execution_order(registry: EntityRegistry) -> list[EntityInfo]:
    """Return entities sorted in dependency order (dependencies first).

    Uses graphlib.TopologicalSorter to compute a valid execution order
    where every entity's dependencies appear before it in the list.

    Raises:
        CircularDependencyError: If entities form a dependency cycle.
    """
    graph: dict[str, set[str]] = {}

    for name, info in registry.items():
        deps = _extract_dependencies(info.entity)
        graph[name] = set(deps)

    ts = TopologicalSorter(graph)
    try:
        order = list(ts.static_order())
    except CycleError as e:
        raise CircularDependencyError(
            f"Circular dependency detected among entities: {e}"
        ) from e

    # Filter to entities that exist in the registry (deps may reference
    # entities not in this registry).
    return [registry.get(name) for name in order if registry.get(name) is not None]
