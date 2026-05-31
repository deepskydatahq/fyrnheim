"""JSON operation primitives."""

from __future__ import annotations


def to_json_struct(fields: dict[str, str]) -> str:
    """Convert field mappings to TO_JSON_STRING(STRUCT(...)).

    Args:
        fields: Dict of {json_key: sql_expression}

    Returns:
        SQL expression for JSON struct

    Example:
        >>> to_json_struct({"amount": "total_amount", "currency": "currency"})
        TO_JSON_STRING(STRUCT(
            total_amount AS amount,
            currency AS currency
        ))
    """
    struct_fields = [f"{sql_expr} AS {json_key}" for json_key, sql_expr in fields.items()]

    fields_str = ",\n            ".join(struct_fields)

    return f"""TO_JSON_STRING(STRUCT(
            {fields_str}
        ))"""


def json_extract_scalar(json_col: str, path: str) -> str:
    """Extract scalar value from JSON.

    Args:
        json_col: JSON column name
        path: JSON path (e.g., '$.amount')

    Returns:
        SQL JSON_EXTRACT_SCALAR expression
    """
    return f"JSON_EXTRACT_SCALAR({json_col}, '{path}')"


def json_value(json_col: str, path: str) -> str:
    """Extract value from JSON (BigQuery JSON_VALUE).

    Args:
        json_col: JSON column name
        path: JSON path

    Returns:
        SQL JSON_VALUE expression
    """
    return f"JSON_VALUE({json_col}, '{path}')"


def clickhouse_json_extract_string(json_col: str, key: str) -> str:
    """Generate ClickHouse SQL to extract a JSON property as string."""
    return f"JSONExtractString(toString({json_col}), {_sql_string(key)})"


def clickhouse_json_extract_bool(json_col: str, key: str) -> str:
    """Generate ClickHouse SQL to extract a JSON property as bool."""
    return f"JSONExtractBool(toString({json_col}), {_sql_string(key)})"


def clickhouse_json_extract_raw(json_col: str, key: str) -> str:
    """Generate ClickHouse SQL to extract a JSON property as raw JSON."""
    return f"JSONExtractRaw(toString({json_col}), {_sql_string(key)})"


def clickhouse_json_property_discovery_sql(
    table: str,
    json_col: str,
    *,
    limit: int = 100,
) -> str:
    """Generate bounded ClickHouse SQL for JSON property key discovery."""
    if limit < 1:
        raise ValueError("limit must be >= 1")
    return f"""SELECT
  kv.1 AS key,
  count() AS row_count,
  uniqExact(kv.2) AS distinct_value_count
FROM {table}
ARRAY JOIN JSONExtractKeysAndValuesRaw(toString({json_col})) AS kv
GROUP BY key
ORDER BY row_count DESC, key ASC
LIMIT {int(limit)}"""


def _sql_string(value: str) -> str:
    """Return a single-quoted SQL string literal."""
    return "'" + value.replace("'", "''") + "'"
