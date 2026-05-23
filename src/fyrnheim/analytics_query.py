"""Safe read-only analytics model query helpers."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import ibis
import yaml

from fyrnheim.analytics_catalog import build_analytics_catalog, describe_analytics_model
from fyrnheim.engine.executor import IbisExecutor
from fyrnheim.inspect import build_manifest

QUERY_SCHEMA_VERSION = "fyrnheim.analytics_query.v1"
DEFAULT_LIMIT = 50
MAX_LIMIT = 500
FilterOperator = Literal["eq", "in", "gte", "lte", "gt", "lt"]


class AnalyticsQueryError(ValueError):
    """Raised when an analytics model query is invalid or cannot execute safely."""


@dataclass(frozen=True)
class QueryProject:
    """Loaded project pieces needed for safe analytics queries."""

    catalog: dict[str, Any]
    executor: IbisExecutor
    project_path: Path
    output_dir: Path


def load_query_project(
    config_path: Path | str,
    *,
    entities_dir: Path | str | None = None,
    project_path: Path | str | None = None,
) -> QueryProject:
    """Load project catalog and backend executor for analytics model queries."""
    config_file = Path(config_path)
    project = Path(project_path) if project_path is not None else config_file.parent
    raw = _load_yaml_config(config_file)
    raw_entities = entities_dir or raw.get("entities_dir", "entities")
    resolved_entities = _resolve_project_path(project, raw_entities)
    backend = str(raw.get("backend", "duckdb"))
    backend_config = raw.get("backend_config") if isinstance(raw.get("backend_config"), dict) else None
    output_dir = _resolve_project_path(project, raw.get("output_dir", "generated"))

    manifest = build_manifest(resolved_entities, project_path=project, include_git=False, strict=True)
    catalog = build_analytics_catalog(manifest)
    executor = IbisExecutor.from_config(backend, backend_config=backend_config)
    return QueryProject(catalog=catalog, executor=executor, project_path=project, output_dir=output_dir)


def query_analytics_model(
    catalog: dict[str, Any],
    connection: ibis.BaseBackend,
    *,
    model: str,
    metrics: list[str],
    dimensions: list[str] | None = None,
    filters: dict[str, Any] | None = None,
    order_by: list[dict[str, str]] | None = None,
    limit: int | None = None,
    parquet_dir: Path | str | None = None,
) -> dict[str, Any]:
    """Execute a bounded read-only query over declared analytics model fields."""
    expression, query_context = build_analytics_query_expression(
        catalog,
        connection,
        model=model,
        metrics=metrics,
        dimensions=dimensions,
        filters=filters,
        order_by=order_by,
        limit=limit,
        parquet_dir=parquet_dir,
    )
    rows = expression.execute().to_dict(orient="records")
    return {
        "schema_version": QUERY_SCHEMA_VERSION,
        **query_context,
        "row_count": len(rows),
        "rows": [_json_safe_row(row) for row in rows],
    }


def preview_analytics_query_sql(
    catalog: dict[str, Any],
    connection: ibis.BaseBackend,
    *,
    model: str,
    metrics: list[str],
    dimensions: list[str] | None = None,
    filters: dict[str, Any] | None = None,
    order_by: list[dict[str, str]] | None = None,
    limit: int | None = None,
    parquet_dir: Path | str | None = None,
) -> dict[str, Any]:
    """Compile a safe analytics model query to SQL without executing it."""
    expression, query_context = build_analytics_query_expression(
        catalog,
        connection,
        model=model,
        metrics=metrics,
        dimensions=dimensions,
        filters=filters,
        order_by=order_by,
        limit=limit,
        parquet_dir=parquet_dir,
    )
    return {"schema_version": QUERY_SCHEMA_VERSION, **query_context, "sql": expression.compile()}


def run_project_analytics_query(
    config_path: Path | str,
    *,
    model: str,
    metrics: list[str],
    dimensions: list[str] | None = None,
    filters: dict[str, Any] | None = None,
    order_by: list[dict[str, str]] | None = None,
    limit: int | None = None,
    entities_dir: Path | str | None = None,
    project_path: Path | str | None = None,
) -> dict[str, Any]:
    """Load project config and execute a safe analytics model query."""
    project = load_query_project(config_path, entities_dir=entities_dir, project_path=project_path)
    return query_analytics_model(
        project.catalog,
        project.executor.connection,
        model=model,
        metrics=metrics,
        dimensions=dimensions,
        filters=filters,
        order_by=order_by,
        limit=limit,
        parquet_dir=project.output_dir,
    )


def preview_project_analytics_query_sql(
    config_path: Path | str,
    *,
    model: str,
    metrics: list[str],
    dimensions: list[str] | None = None,
    filters: dict[str, Any] | None = None,
    order_by: list[dict[str, str]] | None = None,
    limit: int | None = None,
    entities_dir: Path | str | None = None,
    project_path: Path | str | None = None,
) -> dict[str, Any]:
    """Load project config and preview generated SQL for a safe analytics query."""
    project = load_query_project(config_path, entities_dir=entities_dir, project_path=project_path)
    return preview_analytics_query_sql(
        project.catalog,
        project.executor.connection,
        model=model,
        metrics=metrics,
        dimensions=dimensions,
        filters=filters,
        order_by=order_by,
        limit=limit,
        parquet_dir=project.output_dir,
    )


def build_analytics_query_expression(
    catalog: dict[str, Any],
    connection: ibis.BaseBackend,
    *,
    model: str,
    metrics: list[str],
    dimensions: list[str] | None = None,
    filters: dict[str, Any] | None = None,
    order_by: list[dict[str, str]] | None = None,
    limit: int | None = None,
    parquet_dir: Path | str | None = None,
) -> tuple[ibis.Table, dict[str, Any]]:
    """Build a validated Ibis expression for a model query."""
    model_context = describe_analytics_model(catalog, model)
    if not model_context.get("model"):
        raise AnalyticsQueryError(f"Unknown analytics model: {model!r}")
    model_record = model_context["model"]
    requested_metrics = _dedupe_required(metrics, "metrics")
    requested_dimensions = _dedupe(dimensions or [])
    capped_limit = _cap_limit(limit)

    metric_map = {metric["name"]: metric for metric in model_record["metrics"]}
    dimension_map = {dimension["name"]: dimension for dimension in model_record["dimensions"]}
    _validate_names(requested_metrics, metric_map, "metric", model)
    _validate_names(requested_dimensions, dimension_map, "dimension", model)

    table = _model_table(connection, model_record, parquet_dir=parquet_dir)
    table_columns = set(table.columns)
    table = _apply_filters(table, filters or {}, {**metric_map, **dimension_map}, model)
    aggregations: dict[str, Any] = {}

    if requested_dimensions:
        grouped = table.group_by([table[dimension] for dimension in requested_dimensions])
        for metric_name in requested_metrics:
            metric = metric_map[metric_name]
            source_column = _metric_source_column(metric)
            _require_column(source_column, table_columns, model)
            aggregations[metric_name] = _aggregate_metric(table[source_column], metric)
        expression = grouped.aggregate(**aggregations)
    else:
        for metric_name in requested_metrics:
            metric = metric_map[metric_name]
            source_column = _metric_source_column(metric)
            _require_column(source_column, table_columns, model)
            aggregations[metric_name] = _aggregate_metric(table[source_column], metric)
        expression = table.aggregate(**aggregations)

    for dimension_name in requested_dimensions:
        _require_column(dimension_name, table_columns, model)

    expression = _apply_ordering(expression, order_by, set(expression.columns), model)
    expression = expression.limit(capped_limit)

    context = {
        "model": model,
        "model_type": model_record["model_type"],
        "grain": model_record.get("grain"),
        "metrics": requested_metrics,
        "dimensions": requested_dimensions,
        "filters": filters or {},
        "order_by": order_by or [],
        "limit": capped_limit,
        "model_context": model_context["model_summary"],
    }
    return expression, context


def _model_table(
    connection: ibis.BaseBackend,
    model: dict[str, Any],
    *,
    parquet_dir: Path | str | None,
) -> ibis.Table:
    table_name = _physical_table_name(model)
    try:
        return connection.table(table_name)
    except Exception as exc:
        if model.get("materialization") != "parquet" or parquet_dir is None:
            raise AnalyticsQueryError(
                f"Model {model['name']!r} is not available as table {table_name!r}. "
                "Run the pipeline or configure the model materialization for the query backend."
            ) from exc
        parquet_path = Path(parquet_dir) / f"{model['name']}.parquet"
        if not parquet_path.exists():
            raise AnalyticsQueryError(
                f"Model {model['name']!r} is materialized as parquet, but {parquet_path} does not exist. "
                "Run the pipeline before querying this model."
            ) from exc
        try:
            return connection.read_parquet(parquet_path)
        except AttributeError as attr_exc:
            raise AnalyticsQueryError(
                f"Backend cannot read parquet materialization for model {model['name']!r}. "
                "Use a DuckDB-backed MCP query config or table materialization."
            ) from attr_exc


def _apply_filters(
    expression: ibis.Table,
    filters: dict[str, Any],
    allowed_fields: dict[str, dict[str, Any]],
    model: str,
) -> ibis.Table:
    for field_name, raw_filter in filters.items():
        if field_name not in allowed_fields:
            raise AnalyticsQueryError(f"Filter field {field_name!r} is not declared on model {model!r}")
        if field_name not in expression.columns:
            raise AnalyticsQueryError(f"Filter field {field_name!r} is not queryable before aggregation")
        op, value = _parse_filter(raw_filter)
        column = expression[field_name]
        predicate = _filter_predicate(column, op, value)
        expression = expression.filter(predicate)
    return expression


def _apply_ordering(
    expression: ibis.Table,
    order_by: list[dict[str, str]] | None,
    available_columns: set[str],
    model: str,
) -> ibis.Table:
    if not order_by:
        return expression
    sort_keys = []
    for item in order_by:
        field = item.get("field")
        direction = item.get("direction", "desc")
        if field not in available_columns:
            raise AnalyticsQueryError(f"order_by field {field!r} is not selected on model {model!r}")
        if direction not in {"asc", "desc"}:
            raise AnalyticsQueryError("order_by direction must be 'asc' or 'desc'")
        sort_keys.append(ibis.asc(field) if direction == "asc" else ibis.desc(field))
    return expression.order_by(sort_keys)


def _parse_filter(raw_filter: Any) -> tuple[FilterOperator, Any]:
    if isinstance(raw_filter, dict):
        if len(raw_filter) != 1:
            raise AnalyticsQueryError("Filter objects must contain exactly one operator")
        op, value = next(iter(raw_filter.items()))
        if op not in {"eq", "in", "gte", "lte", "gt", "lt"}:
            raise AnalyticsQueryError(f"Unsupported filter operator: {op!r}")
        return op, value  # type: ignore[return-value]
    return "eq", raw_filter


def _filter_predicate(column: Any, op: FilterOperator, value: Any) -> Any:
    if op == "eq":
        return column == value
    if op == "in":
        if not isinstance(value, list):
            raise AnalyticsQueryError("'in' filter value must be a list")
        return column.isin(value)
    if op == "gte":
        return column >= value
    if op == "lte":
        return column <= value
    if op == "gt":
        return column > value
    if op == "lt":
        return column < value
    raise AnalyticsQueryError(f"Unsupported filter operator: {op!r}")


def _aggregate_metric(column: Any, metric: dict[str, Any]) -> Any:
    aggregation = metric.get("aggregation")
    if aggregation in {"max_value"}:
        return column.max()
    return column.sum()


def _metric_source_column(metric: dict[str, Any]) -> str:
    if metric["model_type"] == "metrics_model":
        return str(metric.get("output_name") or metric["name"])
    return str(metric.get("output_name") or metric["name"])


def _physical_table_name(model: dict[str, Any]) -> str:
    table = model.get("table")
    return str(table or model["name"])


def _cap_limit(limit: int | None) -> int:
    requested = DEFAULT_LIMIT if limit is None else int(limit)
    if requested < 1:
        raise AnalyticsQueryError("limit must be >= 1")
    return min(requested, MAX_LIMIT)


def _dedupe_required(values: list[str], label: str) -> list[str]:
    deduped = _dedupe(values)
    if not deduped:
        raise AnalyticsQueryError(f"At least one {label} value is required")
    return deduped


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            deduped.append(value)
    return deduped


def _validate_names(
    requested: list[str],
    allowed: dict[str, dict[str, Any]],
    label: str,
    model: str,
) -> None:
    unknown = [name for name in requested if name not in allowed]
    if unknown:
        available = ", ".join(sorted(allowed))
        raise AnalyticsQueryError(
            f"Unknown {label}(s) for model {model!r}: {', '.join(unknown)}. Available: {available}"
        )


def _require_column(column: str, available_columns: set[str], model: str) -> None:
    if column not in available_columns:
        raise AnalyticsQueryError(f"Model {model!r} table is missing required column {column!r}")


def _json_safe_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: _json_safe_value(value) for key, value in row.items()}


def _json_safe_value(value: Any) -> Any:
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    return value


def _load_yaml_config(config_path: Path) -> dict[str, Any]:
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise AnalyticsQueryError(f"Expected mapping in {config_path}")
    return raw


def _resolve_project_path(project: Path, raw_path: Path | str) -> Path:
    path = Path(raw_path)
    return path if path.is_absolute() else project / path
