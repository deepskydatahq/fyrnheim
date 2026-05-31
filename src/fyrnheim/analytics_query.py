"""Safe read-only analytics model query helpers."""

from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

import ibis
import yaml
from typing_extensions import TypedDict

from fyrnheim.analytics_catalog import build_analytics_catalog, describe_analytics_model
from fyrnheim.engine.executor import IbisExecutor
from fyrnheim.inspect import build_manifest
from fyrnheim.primitives.json_ops import clickhouse_json_property_discovery_sql

QUERY_SCHEMA_VERSION = "fyrnheim.analytics_query.v1"
DEFAULT_LIMIT = 50
MAX_LIMIT = 500
QUERY_SYNTAX_SCHEMA_VERSION = "fyrnheim.analytics_query_syntax.v1"
FilterOperator = Literal["eq", "in", "gte", "lte", "gt", "lt"]
OrderDirection = Literal["asc", "desc"]


class OrderByInput(TypedDict, total=False):
    """Declared query sort key accepted by MCP analytics query tools."""

    field: str
    direction: OrderDirection


class AnalyticsQueryError(ValueError):
    """Raised when an analytics model query is invalid or cannot execute safely."""


@dataclass(frozen=True)
class QueryProject:
    """Loaded project pieces needed for safe analytics queries."""

    catalog: dict[str, Any]
    executor: IbisExecutor
    project_path: Path
    output_dir: Path


def describe_query_syntax() -> dict[str, Any]:
    """Return a compact cookbook for MCP analytics query arguments."""
    return {
        "schema_version": QUERY_SYNTAX_SCHEMA_VERSION,
        "tools": ["query_analytics_model", "preview_analytics_query_sql"],
        "contract": [
            "Use metric and dimension names exactly as returned by list_metrics/list_dimensions.",
            "Use semantic metric names such as 'reactions', not backing columns such as 'reactions_sum_delta'.",
            "order_by must be an array of objects: [{'field': 'reactions', 'direction': 'desc'}].",
            "order_by.field must be selected in metrics or dimensions.",
            "direction must be 'asc' or 'desc'; omit it to default to 'desc'.",
            "For latest rows, select dimensions ['_date'], order by _date descending, and set limit to 1.",
            "If a query fails, call preview_analytics_query_sql with the same arguments to inspect generated SQL.",
        ],
        "order_by_schema": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["field"],
                "properties": {
                    "field": {"type": "string", "description": "Selected metric or dimension name."},
                    "direction": {"enum": ["asc", "desc"], "default": "desc"},
                },
            },
            "example": [{"field": "reactions", "direction": "desc"}],
            "invalid_examples": [{"reactions": "desc"}, {"field": "reactions", "dir": "desc"}],
        },
        "examples": [
            {
                "title": "Query reactions by source",
                "arguments": {
                    "model": "content_metrics_daily",
                    "metrics": ["reactions"],
                    "dimensions": ["source"],
                    "order_by": [{"field": "reactions", "direction": "desc"}],
                    "limit": 5,
                },
            },
            {
                "title": "Latest available date",
                "arguments": {
                    "model": "content_metrics_daily",
                    "metrics": ["reactions"],
                    "dimensions": ["_date"],
                    "order_by": [{"field": "_date", "direction": "desc"}],
                    "limit": 1,
                },
            },
        ],
    }


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
    order_by: list[OrderByInput] | None = None,
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
    order_by: list[OrderByInput] | None = None,
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
    sql = query_context.get("sql") or expression.compile()
    return {"schema_version": QUERY_SCHEMA_VERSION, **query_context, "sql": sql}


def run_project_analytics_query(
    config_path: Path | str,
    *,
    model: str,
    metrics: list[str],
    dimensions: list[str] | None = None,
    filters: dict[str, Any] | None = None,
    order_by: list[OrderByInput] | None = None,
    limit: int | None = None,
    entities_dir: Path | str | None = None,
    project_path: Path | str | None = None,
) -> dict[str, Any]:
    """Load project config and execute a safe analytics model query."""
    project = load_query_project(config_path, entities_dir=entities_dir, project_path=project_path)
    with project.executor:
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
    order_by: list[OrderByInput] | None = None,
    limit: int | None = None,
    entities_dir: Path | str | None = None,
    project_path: Path | str | None = None,
) -> dict[str, Any]:
    """Load project config and preview generated SQL for a safe analytics query."""
    project = load_query_project(config_path, entities_dir=entities_dir, project_path=project_path)
    with project.executor:
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
    order_by: list[OrderByInput] | None = None,
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
    property_dimensions = [_parse_property_path(name, model_record) for name in requested_dimensions]
    declared_dimensions = [
        name for name, parsed in zip(requested_dimensions, property_dimensions, strict=True) if parsed is None
    ]
    property_dimensions = [parsed for parsed in property_dimensions if parsed is not None]
    _validate_names(requested_metrics, metric_map, "metric", model)
    _validate_names(declared_dimensions, dimension_map, "dimension", model)

    if property_dimensions or _filters_include_property(filters or {}, model_record):
        expression, query_context = _build_property_sql_query(
            connection,
            model_record,
            model=model,
            metrics=requested_metrics,
            dimensions=requested_dimensions,
            filters=filters or {},
            order_by=order_by,
            limit=capped_limit,
        )
        query_context["model_context"] = model_context["model_summary"]
        return expression, query_context

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


def list_property_bags(catalog: dict[str, Any], *, model: str | None = None) -> dict[str, Any]:
    """Return declared property bags, optionally filtered to one model."""
    property_bags = catalog.get("property_bags", [])
    if model is not None:
        property_bags = [property_bag for property_bag in property_bags if property_bag["model"] == model]
    return {"property_bags": property_bags}


def discover_property_keys(
    catalog: dict[str, Any],
    connection: ibis.BaseBackend,
    *,
    model: str,
    property_bag: str,
    limit: int | None = None,
) -> dict[str, Any]:
    """Discover keys for a declared property bag with bounded generated SQL."""
    model_record = _required_model(catalog, model)
    bag = _required_property_bag(model_record, property_bag, require_discoverable=True)
    capped_limit = _cap_limit(limit)
    sql = _property_discovery_sql(connection, model_record, bag, capped_limit)
    rows = connection.sql(sql).execute().to_dict(orient="records")
    return {
        "schema_version": QUERY_SCHEMA_VERSION,
        "model": model,
        "property_bag": bag["name"],
        "field": bag["field"],
        "limit": capped_limit,
        "row_count": len(rows),
        "keys": [_json_safe_row(row) for row in rows],
        "sql": sql,
    }


def sample_property_values(
    catalog: dict[str, Any],
    connection: ibis.BaseBackend,
    *,
    model: str,
    property_bag: str,
    key: str,
    limit: int | None = None,
) -> dict[str, Any]:
    """Sample values for a declared property key and infer a rough type."""
    model_record = _required_model(catalog, model)
    bag = _required_property_bag(model_record, property_bag, require_discoverable=True)
    _validate_property_key(key)
    capped_limit = _cap_limit(limit)
    sql = _property_sample_sql(connection, model_record, bag, key, capped_limit)
    rows = connection.sql(sql).execute().to_dict(orient="records")
    values = [row.get("value") for row in rows]
    return {
        "schema_version": QUERY_SCHEMA_VERSION,
        "model": model,
        "property_bag": bag["name"],
        "field": bag["field"],
        "key": key,
        "limit": capped_limit,
        "row_count": len(rows),
        "inferred_type": infer_property_type(values),
        "values": [_json_safe_value(value) for value in values],
        "sql": sql,
    }


def infer_property_type(values: list[Any]) -> str:
    """Infer a rough, agent-facing type label from sampled JSON property values."""
    non_null = [value for value in values if value not in (None, "")]
    if not values or not non_null:
        return "unknown"
    if len(non_null) / len(values) < 0.5:
        return "null-heavy"
    labels = {_infer_one_property_type(value) for value in non_null}
    if len(labels) == 1:
        return next(iter(labels))
    if labels <= {"number", "bool"}:
        return "mixed"
    return "mixed"


def _build_property_sql_query(
    connection: ibis.BaseBackend,
    model_record: dict[str, Any],
    *,
    model: str,
    metrics: list[str],
    dimensions: list[str],
    filters: dict[str, Any],
    order_by: list[OrderByInput] | None,
    limit: int,
) -> tuple[ibis.Table, dict[str, Any]]:
    metric_map = {metric["name"]: metric for metric in model_record["metrics"]}
    dimension_map = {dimension["name"]: dimension for dimension in model_record["dimensions"]}
    selected_exprs: list[str] = []
    group_exprs: list[str] = []
    available_aliases: set[str] = set()

    for dimension in dimensions:
        parsed = _parse_property_path(dimension, model_record)
        if parsed is None:
            if dimension not in dimension_map:
                raise AnalyticsQueryError(f"Unknown dimension(s) for model {model!r}: {dimension}")
            expr = _quote_identifier(dimension)
            alias = dimension
        else:
            bag, key = parsed
            expr = _json_extract_string_sql(connection, bag["field"], key)
            alias = _property_alias(bag, key)
        selected_exprs.append(f"{expr} AS {_quote_identifier(alias)}")
        group_exprs.append(expr)
        available_aliases.add(alias)

    for metric_name in metrics:
        metric = metric_map[metric_name]
        source_column = _metric_source_column(metric)
        selected_exprs.append(
            f"{_aggregate_metric_sql(source_column, metric)} AS {_quote_identifier(metric_name)}"
        )
        available_aliases.add(metric_name)

    where_clauses = [
        _filter_sql(connection, model_record, field, raw_filter, model=model)
        for field, raw_filter in filters.items()
    ]
    sql = f"SELECT {', '.join(selected_exprs)}\nFROM {_quote_identifier(_physical_table_name(model_record))}"
    if where_clauses:
        sql += "\nWHERE " + " AND ".join(where_clauses)
    if group_exprs:
        sql += "\nGROUP BY " + ", ".join(group_exprs)
    sql += _order_by_sql(order_by, available_aliases, model)
    sql += f"\nLIMIT {limit}"
    context = {
        "model": model,
        "model_type": model_record["model_type"],
        "grain": model_record.get("grain"),
        "metrics": metrics,
        "dimensions": dimensions,
        "filters": filters,
        "order_by": order_by or [],
        "limit": limit,
        "dynamic_property_dimensions": [
            _property_alias(*parsed)
            for dimension in dimensions
            if (parsed := _parse_property_path(dimension, model_record)) is not None
        ],
        "sql": sql,
    }
    return connection.sql(sql), context


def _filters_include_property(filters: dict[str, Any], model_record: dict[str, Any]) -> bool:
    return any(_parse_property_path(field, model_record) is not None for field in filters)


def _parse_property_path(field: str, model_record: dict[str, Any]) -> tuple[dict[str, Any], str] | None:
    match = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z0-9_ ?-]+)", field)
    if match is None:
        match = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)\[['\"](.+)['\"]\]", field)
    if match is None:
        return None
    bag_name, key = match.groups()
    for bag in model_record.get("property_bags", []):
        if bag_name in {bag["name"], bag["field"]}:
            _validate_property_key(key)
            return bag, key
    return None


def _required_model(catalog: dict[str, Any], model: str) -> dict[str, Any]:
    matches = [candidate for candidate in catalog["models"] if candidate["name"] == model]
    if len(matches) != 1:
        raise AnalyticsQueryError(f"Unknown analytics model: {model!r}")
    return cast(dict[str, Any], matches[0])


def _required_property_bag(
    model_record: dict[str, Any],
    property_bag: str,
    *,
    require_discoverable: bool = False,
) -> dict[str, Any]:
    for bag in model_record.get("property_bags", []):
        if property_bag in {bag["name"], bag["field"]}:
            if require_discoverable and not bag.get("discoverable", True):
                raise AnalyticsQueryError(f"Property bag {property_bag!r} is not discoverable")
            return cast(dict[str, Any], bag)
    raise AnalyticsQueryError(
        f"Unknown property bag {property_bag!r} on model {model_record['name']!r}"
    )


def _validate_property_key(key: str) -> None:
    if not key or len(key) > 128 or any(ord(char) < 32 for char in key):
        raise AnalyticsQueryError("Property key must be a non-empty printable string up to 128 chars")
    if any(token in key for token in (";", "--", "/*", "*/")):
        raise AnalyticsQueryError("Property key contains unsafe SQL token")


def _property_alias(bag: dict[str, Any], key: str) -> str:
    safe_key = re.sub(r"[^A-Za-z0-9_]+", "_", key).strip("_").lower() or "value"
    return f"{bag['field']}__{safe_key}"


def _json_extract_string_sql(connection: ibis.BaseBackend, field: str, key: str) -> str:
    backend = getattr(connection, "name", "")
    if backend == "clickhouse":
        return f"JSONExtractString(toString({_quote_identifier(field)}), {_sql_string(key)})"
    if backend == "duckdb":
        return f"json_extract_string({_quote_identifier(field)}, {_sql_string('$.' + _duckdb_json_path_key(key))})"
    raise AnalyticsQueryError(f"Dynamic property querying is not supported for backend {backend!r}")


def _property_discovery_sql(
    connection: ibis.BaseBackend,
    model_record: dict[str, Any],
    bag: dict[str, Any],
    limit: int,
) -> str:
    table = _physical_table_name(model_record)
    backend = getattr(connection, "name", "")
    if backend == "clickhouse":
        return clickhouse_json_property_discovery_sql(table, bag["field"], limit=limit)
    if backend == "duckdb":
        return (
            "SELECT je.key AS key, count(*) AS row_count, "
            "count(DISTINCT CAST(je.value AS VARCHAR)) AS distinct_value_count\n"
            f"FROM {_quote_identifier(table)}, json_each({_quote_identifier(bag['field'])}) AS je\n"
            "GROUP BY je.key\nORDER BY row_count DESC, key ASC\n"
            f"LIMIT {limit}"
        )
    raise AnalyticsQueryError(f"Property discovery is not supported for backend {backend!r}")


def _property_sample_sql(
    connection: ibis.BaseBackend,
    model_record: dict[str, Any],
    bag: dict[str, Any],
    key: str,
    limit: int,
) -> str:
    table = _physical_table_name(model_record)
    value_expr = _json_extract_string_sql(connection, bag["field"], key)
    return (
        f"SELECT {value_expr} AS value\n"
        f"FROM {_quote_identifier(table)}\n"
        f"WHERE {value_expr} IS NOT NULL\n"
        f"LIMIT {limit}"
    )


def _filter_sql(
    connection: ibis.BaseBackend,
    model_record: dict[str, Any],
    field_name: str,
    raw_filter: Any,
    *,
    model: str,
) -> str:
    parsed = _parse_property_path(field_name, model_record)
    if parsed is None:
        allowed_fields = {
            *[metric["name"] for metric in model_record["metrics"]],
            *[dimension["name"] for dimension in model_record["dimensions"]],
        }
        if field_name not in allowed_fields:
            raise AnalyticsQueryError(f"Filter field {field_name!r} is not declared on model {model!r}")
        op, value = _parse_filter(raw_filter)
        if op == "in":
            return f"{_quote_identifier(field_name)} IN ({', '.join(_sql_literal(item) for item in value)})"
        return f"{_quote_identifier(field_name)} {_sql_operator(op)} {_sql_literal(value)}"
    bag, key = parsed
    property_op, value = _parse_property_filter(raw_filter)
    expr = _json_extract_string_sql(connection, bag["field"], key)
    if property_op == "contains":
        return f"{expr} ILIKE {_sql_string('%' + str(value) + '%')}"
    if property_op == "is_null":
        return f"{expr} IS NULL"
    if property_op == "not_null":
        return f"{expr} IS NOT NULL"
    if property_op == "in":
        if not isinstance(value, list):
            raise AnalyticsQueryError("'in' filter value must be a list")
        return f"{expr} IN ({', '.join(_sql_literal(item) for item in value)})"
    return f"{expr} = {_sql_literal(value)}"


def _parse_property_filter(raw_filter: Any) -> tuple[str, Any]:
    if isinstance(raw_filter, dict):
        if len(raw_filter) != 1:
            raise AnalyticsQueryError("Filter objects must contain exactly one operator")
        op, value = next(iter(raw_filter.items()))
        if op not in {"eq", "in", "contains", "is_null", "not_null"}:
            raise AnalyticsQueryError(f"Unsupported property filter operator: {op!r}")
        return op, value
    return "eq", raw_filter


def _order_by_sql(
    order_by: list[OrderByInput] | None,
    available_aliases: set[str],
    model: str,
) -> str:
    if not order_by:
        return ""
    clauses = []
    for item in order_by:
        field = item.get("field")
        direction = item.get("direction", "desc")
        if field not in available_aliases:
            raise AnalyticsQueryError(f"order_by field {field!r} is not selected on model {model!r}")
        if direction not in {"asc", "desc"}:
            raise AnalyticsQueryError("order_by direction must be 'asc' or 'desc'")
        clauses.append(f"{_quote_identifier(str(field))} {direction.upper()}")
    return "\nORDER BY " + ", ".join(clauses)


def _aggregate_metric_sql(source_column: str, metric: dict[str, Any]) -> str:
    if metric.get("aggregation") in {"max_value"}:
        return f"max({_quote_identifier(source_column)})"
    return f"sum({_quote_identifier(source_column)})"


def _sql_operator(op: FilterOperator) -> str:
    return {"eq": "=", "gte": ">=", "lte": "<=", "gt": ">", "lt": "<"}[op]


def _sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return _sql_string(str(value))


def _sql_string(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("'", "''")
    return "'" + escaped + "'"


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _duckdb_json_path_key(key: str) -> str:
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
        return key
    return '"' + key.replace('"', '\\"') + '"'


def _infer_one_property_type(value: Any) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, (int, float)):
        return "number"
    text = str(value).strip().strip('"')
    if text.lower() in {"true", "false"}:
        return "bool"
    try:
        float(text)
    except ValueError:
        pass
    else:
        return "number"
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            dt.datetime.strptime(text[:19], fmt)
        except ValueError:
            continue
        return "date-ish"
    return "string"


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
    order_by: list[OrderByInput] | None,
    available_columns: set[str],
    model: str,
) -> ibis.Table:
    if not order_by:
        return expression
    if not isinstance(order_by, list):
        raise AnalyticsQueryError(
            "order_by must be an array of objects like "
            "[{'field': 'reactions', 'direction': 'desc'}]; "
            f"got {type(order_by).__name__}"
        )
    sort_keys = []
    for index, item in enumerate(order_by):
        if not isinstance(item, dict):
            raise AnalyticsQueryError(
                "order_by must be an array of objects like "
                "[{'field': 'reactions', 'direction': 'desc'}]; "
                f"item {index} is {type(item).__name__}"
            )
        unknown_keys = set(item) - {"field", "direction"}
        if unknown_keys:
            raise AnalyticsQueryError(
                "order_by items must use keys {'field', 'direction'}; "
                f"item {index} has unknown key(s): {', '.join(sorted(unknown_keys))}"
            )
        field = item.get("field")
        if not isinstance(field, str) or not field:
            raise AnalyticsQueryError(
                "order_by items must include a non-empty string field, e.g. "
                "{'field': 'reactions', 'direction': 'desc'}"
            )
        direction = item.get("direction", "desc")
        if field not in available_columns:
            raise AnalyticsQueryError(
                f"order_by field {field!r} is not selected on model {model!r}. "
                "Use a selected metric/dimension name, not a backing column name."
            )
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
