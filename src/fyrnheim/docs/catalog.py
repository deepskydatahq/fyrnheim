"""Catalog builder that extracts entity metadata into a JSON-serializable dict."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from fyrnheim.core.source import (
    AggregationSource,
    DerivedSource,
    EventAggregationSource,
    TableSource,
    UnionSource,
)
from fyrnheim.engine.registry import EntityRegistry
from fyrnheim.engine.resolution import extract_dependencies


def _detect_source_type(source: Any) -> str:
    """Return a human-readable source type string."""
    if source is None:
        return "none"
    if isinstance(source, TableSource):
        return "table"
    if isinstance(source, DerivedSource):
        return "derived"
    if isinstance(source, AggregationSource):
        return "aggregation"
    if isinstance(source, EventAggregationSource):
        return "event_aggregation"
    if isinstance(source, UnionSource):
        return "union"
    return "unknown"


def _serialize_fields(entity: Any) -> list[dict[str, Any]]:
    """Extract field definitions from an entity."""
    try:
        fields = entity.all_fields
    except (ValueError, AttributeError):
        return []
    return [
        {
            "name": f.name,
            "type": f.type,
            "description": f.description,
            "nullable": f.nullable,
        }
        for f in fields
    ]


def _serialize_computed_columns(entity: Any) -> list[dict[str, Any]]:
    """Extract computed column definitions."""
    columns = entity.all_computed_columns
    return [
        {
            "name": cc.name,
            "expression": cc.expression,
            "description": cc.description,
        }
        for cc in columns
    ]


def _serialize_measures(entity: Any) -> list[dict[str, Any]]:
    """Extract measure definitions."""
    measures = entity.all_measures
    return [
        {
            "name": m.name,
            "expression": m.expression,
            "description": m.description,
        }
        for m in measures
    ]


def _serialize_quality(entity: Any) -> dict[str, Any]:
    """Extract quality check information."""
    if entity.quality is None:
        return {"checks": [], "primary_key": None}
    qc = entity.quality
    checks = []
    for check in qc.checks:
        checks.append(
            {
                "type": type(check).__name__,
                "display_name": check.display_name,
            }
        )
    return {
        "checks": checks,
        "primary_key": qc.primary_key,
    }


def _serialize_layers(entity: Any, layers_list: list[str]) -> dict[str, Any]:
    """Extract layer configuration details."""
    result: dict[str, Any] = {"active": layers_list}
    for layer_name in layers_list:
        layer = entity.get_layer(layer_name)
        if layer is None:
            continue
        layer_info: dict[str, Any] = {}
        if hasattr(layer, "model_name"):
            layer_info["model_name"] = layer.model_name
        if hasattr(layer, "materialization"):
            layer_info["materialization"] = str(layer.materialization.value)
        if hasattr(layer, "target_schema"):
            layer_info["target_schema"] = layer.target_schema
        result[layer_name] = layer_info
    return result


def build_catalog(registry: EntityRegistry) -> dict[str, Any]:
    """Build a JSON-serializable catalog dict from an EntityRegistry.

    Walks all entities in the registry and extracts metadata including
    fields, computed columns, measures, quality checks, layers, and
    dependency relationships.

    Args:
        registry: The entity registry to catalog.

    Returns:
        A dict with 'entities' (list of entity dicts) and 'metadata'.
    """
    # Build dependency graph: entity_name -> list of dependency names
    dep_graph: dict[str, list[str]] = {}
    for name, info in registry.items():
        dep_graph[name] = extract_dependencies(info.entity)

    # Compute dependents by inverting the dependency graph
    dependents_graph: dict[str, list[str]] = {name: [] for name in registry}
    for name, deps in dep_graph.items():
        for dep in deps:
            if dep in dependents_graph:
                dependents_graph[dep].append(name)

    entities: list[dict[str, Any]] = []
    for name, info in registry.items():
        entity = info.entity
        entity_dict: dict[str, Any] = {
            "name": name,
            "description": entity.description,
            "source_type": _detect_source_type(entity.source),
            "is_internal": entity.is_internal,
            "fields": _serialize_fields(entity),
            "computed_columns": _serialize_computed_columns(entity),
            "measures": _serialize_measures(entity),
            "quality": _serialize_quality(entity),
            "layers": _serialize_layers(entity, info.layers),
            "dependencies": dep_graph.get(name, []),
            "dependents": dependents_graph.get(name, []),
        }
        entities.append(entity_dict)

    return {
        "entities": entities,
        "metadata": {
            "generated_at": datetime.now(tz=UTC).isoformat(),
            "entity_count": len(entities),
            "generator": "fyrnheim",
        },
    }
