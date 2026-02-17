"""JSON operation primitives."""


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
