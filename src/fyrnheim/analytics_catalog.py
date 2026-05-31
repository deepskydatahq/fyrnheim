"""Analytics catalog helpers derived from Fyrnheim manifests.

The catalog is intentionally read-only metadata. It answers which analytical
models, metrics, and dimensions a project exposes; it does not execute queries
or connect to warehouses.
"""

from __future__ import annotations

from typing import Any, Literal

CATALOG_SCHEMA_VERSION = "fyrnheim.analytics_catalog.v1"
ModelType = Literal["analytics_entity", "metrics_model"]


def build_analytics_catalog(manifest: dict[str, Any]) -> dict[str, Any]:
    """Build a JSON-compatible analytics catalog from a Fyrnheim manifest."""
    models = [_analytics_entity_model(record) for record in manifest.get("analytics_entities", [])]
    models.extend(_metrics_model(record) for record in manifest.get("metrics_models", []))
    models = sorted(models, key=lambda model: (model["model_type"], model["name"]))

    metrics = sorted(
        [metric for model in models for metric in model["metrics"]],
        key=lambda metric: (metric["name"], metric["model"], metric["metric_id"]),
    )
    dimensions = sorted(
        [dimension for model in models for dimension in model["dimensions"]],
        key=lambda dimension: (dimension["name"], dimension["model"], dimension["dimension_id"]),
    )
    property_bags = sorted(
        [property_bag for model in models for property_bag in model.get("property_bags", [])],
        key=lambda property_bag: (
            property_bag["name"],
            property_bag["model"],
            property_bag["property_bag_id"],
        ),
    )

    return {
        "schema_version": CATALOG_SCHEMA_VERSION,
        "manifest_schema_version": manifest.get("schema_version"),
        "manifest_git_commit": manifest.get("git_commit"),
        "project_path": manifest.get("project_path"),
        "entities_dir": manifest.get("entities_dir"),
        "models": models,
        "metrics": metrics,
        "dimensions": dimensions,
        "property_bags": property_bags,
    }


def list_analytics_models(catalog: dict[str, Any]) -> dict[str, Any]:
    """Return all queryable analytics models with metric/dimension summaries."""
    return {
        "schema_version": catalog["schema_version"],
        "models": [
            {
                "name": model["name"],
                "model_type": model["model_type"],
                "description": model.get("description"),
                "grain": model.get("grain"),
                "defining_entity": model.get("defining_entity"),
                "limitations": model.get("limitations", []),
                "recommended_questions": model.get("recommended_questions", []),
                "metric_count": len(model["metrics"]),
                "dimension_count": len(model["dimensions"]),
                "metrics": [metric["name"] for metric in model["metrics"]],
                "dimensions": [dimension["name"] for dimension in model["dimensions"]],
                "property_bags": [property_bag["name"] for property_bag in model.get("property_bags", [])],
            }
            for model in catalog["models"]
        ],
    }


def list_metrics(catalog: dict[str, Any], *, model: str | None = None) -> dict[str, Any]:
    """Return metric definitions, optionally filtered to one model."""
    metrics = catalog["metrics"]
    if model is not None:
        metrics = [metric for metric in metrics if metric["model"] == model]
    return {"metrics": metrics}


def list_dimensions(catalog: dict[str, Any], *, model: str | None = None) -> dict[str, Any]:
    """Return dimension definitions, optionally filtered to one model."""
    dimensions = catalog["dimensions"]
    if model is not None:
        dimensions = [dimension for dimension in dimensions if dimension["model"] == model]
    return {"dimensions": dimensions}


def describe_analytics_model(catalog: dict[str, Any], model: str) -> dict[str, Any]:
    """Describe one analytics model with concise agent-facing context."""
    matches = [candidate for candidate in catalog["models"] if candidate["name"] == model]
    response: dict[str, Any] = {"query": model, "matches": matches, "count": len(matches)}
    if len(matches) == 1:
        match = matches[0]
        response["model"] = match
        model_summary = {
            "name": match["name"],
            "model_type": match["model_type"],
            "description": match.get("description"),
            "grain": match.get("grain"),
            "defining_entity": match.get("defining_entity"),
            "metrics": [metric["name"] for metric in match["metrics"]],
            "dimensions": [dimension["name"] for dimension in match["dimensions"]],
            "limitations": match.get("limitations", []),
            "recommended_questions": match.get("recommended_questions", []),
        }
        if match.get("property_bags"):
            model_summary["property_bags"] = [
                property_bag["name"] for property_bag in match.get("property_bags", [])
            ]
        response["model_summary"] = model_summary
        response["ambiguous"] = False
    else:
        response["ambiguous"] = len(matches) > 1
    return response


def describe_metric(
    catalog: dict[str, Any],
    metric: str,
    *,
    model: str | None = None,
) -> dict[str, Any]:
    """Describe a metric by name or id.

    If a name is ambiguous across models or aggregations, the response includes
    all matches and sets ``ambiguous`` to True instead of guessing.
    """
    matches = [
        candidate
        for candidate in catalog["metrics"]
        if candidate["name"] == metric or candidate["metric_id"] == metric
    ]
    if model is not None:
        matches = [candidate for candidate in matches if candidate["model"] == model]
    matches = sorted(matches, key=lambda item: item["metric_id"])

    response: dict[str, Any] = {"query": metric, "matches": matches, "count": len(matches)}
    if len(matches) == 1:
        match = matches[0]
        response["metric"] = match
        response["available_dimensions"] = _model_dimensions(catalog, match["model"])
        response["ambiguous"] = False
    else:
        response["ambiguous"] = len(matches) > 1
    return response


def describe_dimension(
    catalog: dict[str, Any],
    dimension: str,
    *,
    model: str | None = None,
) -> dict[str, Any]:
    """Describe a dimension by name or id.

    If a name is ambiguous across models or dimension kinds, the response
    includes all matches and sets ``ambiguous`` to True instead of guessing.
    """
    matches = [
        candidate
        for candidate in catalog["dimensions"]
        if candidate["name"] == dimension or candidate["dimension_id"] == dimension
    ]
    if model is not None:
        matches = [candidate for candidate in matches if candidate["model"] == model]
    matches = sorted(matches, key=lambda item: item["dimension_id"])

    response: dict[str, Any] = {"query": dimension, "matches": matches, "count": len(matches)}
    if len(matches) == 1:
        match = matches[0]
        response["dimension"] = match
        response["usable_with_metrics"] = _model_metrics(catalog, match["model"])
        response["ambiguous"] = False
    else:
        response["ambiguous"] = len(matches) > 1
    return response


def _analytics_entity_model(record: dict[str, Any]) -> dict[str, Any]:
    config = record["config"]
    model_name = record["name"]
    metrics = [
        {
            "metric_id": f"analytics_entity:{model_name}:measure:{measure['name']}",
            "name": measure["name"],
            "model": model_name,
            "model_type": "analytics_entity",
            "kind": "measure",
            "aggregation": measure["aggregation"],
            "activity": measure["activity"],
            "field": measure.get("field"),
            "output_name": measure["name"],
            "description": _metric_description(measure["name"], measure["aggregation"]),
            "usage": _metric_usage(measure["aggregation"]),
        }
        for measure in config.get("measures", [])
    ]

    dimensions = [
        {
            "dimension_id": f"analytics_entity:{model_name}:identity:canonical_id",
            "name": "canonical_id",
            "model": model_name,
            "model_type": "analytics_entity",
            "kind": "identity",
            "source": config.get("identity_graph"),
            "field": "canonical_id",
            "strategy": None,
            "description": "Stable canonical identifier for the analytics entity row.",
            "usage": _dimension_usage(can_group=True, can_filter=True, can_sort=True),
        }
    ]
    dimensions.extend(
        {
            "dimension_id": f"analytics_entity:{model_name}:state_field:{field['name']}",
            "name": field["name"],
            "model": model_name,
            "model_type": "analytics_entity",
            "kind": "state_field",
            "source": field["source"],
            "field": field["field"],
            "strategy": field["strategy"],
            "description": _dimension_description(field["name"], field.get("source")),
            "usage": _dimension_usage(can_group=True, can_filter=True, can_sort=True),
        }
        for field in config.get("state_fields", [])
    )
    dimensions.extend(
        {
            "dimension_id": f"analytics_entity:{model_name}:computed_field:{field['name']}",
            "name": field["name"],
            "model": model_name,
            "model_type": "analytics_entity",
            "kind": "computed_field",
            "source": "computed_fields",
            "field": field["name"],
            "strategy": None,
            "expression": field.get("expression"),
            "description": field.get("description") or _dimension_description(field["name"], "computed_fields"),
            "usage": _dimension_usage(can_group=True, can_filter=True, can_sort=True),
        }
        for field in config.get("computed_fields", [])
    )
    property_bags = [
        {
            "property_bag_id": f"analytics_entity:{model_name}:property_bag:{property_bag['name']}",
            "name": property_bag["name"],
            "model": model_name,
            "model_type": "analytics_entity",
            "kind": "property_bag",
            "source": property_bag["source"],
            "field": property_bag["field"],
            "backend_type": property_bag.get("backend_type", "json"),
            "discoverable": property_bag.get("discoverable", True),
            "description": (
                f"Dynamic JSON property bag '{property_bag['name']}' from "
                f"{property_bag['source']}.{property_bag['field']}."
            ),
            "usage": {
                "can_discover_keys": property_bag.get("discoverable", True),
                "can_sample_values": property_bag.get("discoverable", True),
                "dynamic_dimension_syntax": [
                    f"{property_bag['name']}.<key>",
                    f"{property_bag['field']}.<key>",
                    f"{property_bag['field']}['<key>']",
                ],
            },
        }
        for property_bag in config.get("property_bags", [])
    ]

    return {
        "name": model_name,
        "model_type": "analytics_entity",
        "identity_graph": config.get("identity_graph"),
        "materialization": config.get("materialization"),
        "table": config.get("table") or model_name,
        "description": f"Analytics entity '{model_name}' with state fields and measures.",
        "defining_entity": model_name,
        "limitations": ["Entity-level row grain; measures are precomputed on the entity table."],
        "recommended_questions": [
            f"Which {model_name} have the highest measures?",
            f"How do {model_name} break down by declared dimensions?",
        ],
        "metrics": sorted(metrics, key=lambda metric: metric["metric_id"]),
        "dimensions": sorted(dimensions, key=lambda dimension: dimension["dimension_id"]),
        "property_bags": sorted(property_bags, key=lambda property_bag: property_bag["property_bag_id"]),
    }


def _metrics_model(record: dict[str, Any]) -> dict[str, Any]:
    config = record["config"]
    model_name = record["name"]
    metrics = [
        {
            "metric_id": _metrics_model_metric_id(model_name, field),
            "name": field["field_name"],
            "model": model_name,
            "model_type": "metrics_model",
            "kind": "metric_field",
            "aggregation": field["aggregation"],
            "distinct_field": field.get("distinct_field"),
            "output_name": f"{field['field_name']}_{field['aggregation']}",
            "description": _metric_description(field["field_name"], field["aggregation"]),
            "usage": _metric_usage(field["aggregation"]),
            "grain": config["grain"],
            "sources": list(config.get("sources", [])),
        }
        for field in config.get("metric_fields", [])
    ]
    dimensions = [
        {
            "dimension_id": f"metrics_model:{model_name}:time_grain:_date",
            "name": "_date",
            "model": model_name,
            "model_type": "metrics_model",
            "kind": "time_grain",
            "grain": config["grain"],
            "source": "event_timestamp",
            "field": "ts",
            "description": f"{config['grain'].title()} time bucket for the metrics model.",
            "usage": _dimension_usage(can_group=True, can_filter=True, can_sort=True),
        }
    ]
    dimensions.extend(
        {
            "dimension_id": f"metrics_model:{model_name}:dimension:{dimension}",
            "name": dimension,
            "model": model_name,
            "model_type": "metrics_model",
            "kind": "dimension",
            "source": "metrics_model.dimensions",
            "field": dimension,
            "grain": config["grain"],
            "description": _dimension_description(dimension, "metrics_model.dimensions"),
            "usage": _dimension_usage(can_group=True, can_filter=True, can_sort=True),
        }
        for dimension in config.get("dimensions", [])
    )

    return {
        "name": model_name,
        "model_type": "metrics_model",
        "grain": config["grain"],
        "sources": list(config.get("sources", [])),
        "materialization": config.get("materialization"),
        "table": config.get("table") or model_name,
        "description": _metrics_model_description(model_name, config["grain"], config.get("sources", [])),
        "defining_entity": _metrics_model_defining_entity(config.get("sources", []), config["grain"]),
        "limitations": [
            f"{config['grain']} aggregate grain; lower-grain records such as individual posts are not available unless declared as dimensions or exposed by another model."
        ],
        "recommended_questions": [
            f"Which dimensions drive {model_name} metrics?",
            f"How do metrics trend by _date at {config['grain']} grain?",
        ],
        "metrics": sorted(metrics, key=lambda metric: metric["metric_id"]),
        "dimensions": sorted(dimensions, key=lambda dimension: dimension["dimension_id"]),
    }


def _metric_description(name: str, aggregation: str) -> str:
    return f"Metric '{name}' using {aggregation} aggregation."


def _metric_usage(aggregation: str) -> dict[str, Any]:
    return {
        "safe_for_select": True,
        "safe_for_order_by": True,
        "safe_for_filter": False,
        "safe_for_group_by": False,
        "aggregation": aggregation,
    }


def _dimension_description(name: str, source: str | None) -> str:
    suffix = f" from {source}" if source else ""
    return f"Dimension '{name}'{suffix}."


def _dimension_usage(*, can_group: bool, can_filter: bool, can_sort: bool) -> dict[str, bool]:
    return {
        "safe_for_select": True,
        "safe_for_group_by": can_group,
        "safe_for_filter": can_filter,
        "safe_for_order_by": can_sort,
    }


def _metrics_model_description(model_name: str, grain: str, sources: list[str]) -> str:
    source_text = ", ".join(sources) if sources else "declared sources"
    return f"{grain.title()} metrics model '{model_name}' aggregated from {source_text}."


def _metrics_model_defining_entity(sources: list[str], grain: str) -> str:
    source_text = ", ".join(sources) if sources else "declared source events"
    return f"{grain} aggregate over {source_text}"


def _metrics_model_metric_id(model_name: str, field: dict[str, Any]) -> str:
    distinct = field.get("distinct_field") or ""
    return f"metrics_model:{model_name}:{field['aggregation']}:{field['field_name']}:{distinct}"


def _model_dimensions(catalog: dict[str, Any], model_name: str) -> list[dict[str, Any]]:
    return [dimension for dimension in catalog["dimensions"] if dimension["model"] == model_name]


def _model_metrics(catalog: dict[str, Any], model_name: str) -> list[dict[str, Any]]:
    return [metric for metric in catalog["metrics"] if metric["model"] == model_name]
