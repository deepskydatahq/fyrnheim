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

    return {
        "schema_version": CATALOG_SCHEMA_VERSION,
        "manifest_schema_version": manifest.get("schema_version"),
        "manifest_git_commit": manifest.get("git_commit"),
        "project_path": manifest.get("project_path"),
        "entities_dir": manifest.get("entities_dir"),
        "models": models,
        "metrics": metrics,
        "dimensions": dimensions,
    }


def list_analytics_models(catalog: dict[str, Any]) -> dict[str, Any]:
    """Return all queryable analytics models with metric/dimension summaries."""
    return {
        "schema_version": catalog["schema_version"],
        "models": [
            {
                "name": model["name"],
                "model_type": model["model_type"],
                "grain": model.get("grain"),
                "metric_count": len(model["metrics"]),
                "dimension_count": len(model["dimensions"]),
                "metrics": [metric["name"] for metric in model["metrics"]],
                "dimensions": [dimension["name"] for dimension in model["dimensions"]],
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
            "description": field.get("description"),
        }
        for field in config.get("computed_fields", [])
    )

    return {
        "name": model_name,
        "model_type": "analytics_entity",
        "identity_graph": config.get("identity_graph"),
        "materialization": config.get("materialization"),
        "metrics": sorted(metrics, key=lambda metric: metric["metric_id"]),
        "dimensions": sorted(dimensions, key=lambda dimension: dimension["dimension_id"]),
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
        }
        for dimension in config.get("dimensions", [])
    )

    return {
        "name": model_name,
        "model_type": "metrics_model",
        "grain": config["grain"],
        "sources": list(config.get("sources", [])),
        "materialization": config.get("materialization"),
        "metrics": sorted(metrics, key=lambda metric: metric["metric_id"]),
        "dimensions": sorted(dimensions, key=lambda dimension: dimension["dimension_id"]),
    }


def _metrics_model_metric_id(model_name: str, field: dict[str, Any]) -> str:
    distinct = field.get("distinct_field") or ""
    return f"metrics_model:{model_name}:{field['aggregation']}:{field['field_name']}:{distinct}"


def _model_dimensions(catalog: dict[str, Any], model_name: str) -> list[dict[str, Any]]:
    return [dimension for dimension in catalog["dimensions"] if dimension["model"] == model_name]


def _model_metrics(catalog: dict[str, Any], model_name: str) -> list[dict[str, Any]]:
    return [metric for metric in catalog["metrics"] if metric["model"] == model_name]
