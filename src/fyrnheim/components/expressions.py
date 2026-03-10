"""Expression helper functions for computed columns.

These functions return ibis expression strings usable in ComputedColumn.expression.
"""

from pydantic import Field as PydanticField, model_validator

from fyrnheim.components.computed_column import ComputedColumn


class CaseColumn(ComputedColumn):
    """Computed column that generates ibis.cases() expressions from structured when/then pairs."""

    cases: list[tuple[str, str]] = PydanticField(min_length=1)
    default: str | None = None
    expression: str = ""

    @model_validator(mode="after")
    def build_expression(self) -> "CaseColumn":
        case_parts = ", ".join(f"({cond}, '{val}')" for cond, val in self.cases)
        expr = f"ibis.cases({case_parts})"
        if self.default is not None:
            expr += f".else_('{self.default}')"
        object.__setattr__(self, "expression", expr)
        return self


def contains_any(column: str, values: list[str]) -> str:
    """Generate chained .contains() expression for tag/group matching.

    Args:
        column: Column reference (e.g. "t.tags")
        values: Values to check for

    Returns:
        Expression string with chained .contains().fill_null(False) calls
    """
    if not values:
        raise ValueError("contains_any requires at least one value")
    parts = [f"{column}.contains('{v}').fill_null(False)" for v in values]
    if len(parts) == 1:
        return parts[0]
    return "(" + " | ".join(parts) + ")"


def _build_window(partition_by: str | list[str], order_by: str, descending: bool = False) -> str:
    """Build an ibis.window() expression string.

    Args:
        partition_by: Column(s) to partition by
        order_by: Column to order by
        descending: Whether to sort descending

    Returns:
        Expression string like "ibis.window(group_by='col', order_by='col')"
    """
    if isinstance(partition_by, list):
        group_by_str = repr(partition_by)
    else:
        group_by_str = repr(partition_by)

    if descending:
        order_by_str = f"ibis.desc({repr(order_by)})"
    else:
        order_by_str = repr(order_by)

    return f"ibis.window(group_by={group_by_str}, order_by={order_by_str})"


def dedup_by(partition_by: str | list[str], order_by: str, descending: bool = False) -> str:
    """Generate ibis ROW_NUMBER window expression for deduplication.

    Args:
        partition_by: Column(s) to partition by
        order_by: Column to order by
        descending: Whether to sort descending

    Returns:
        Expression string like "ibis.row_number().over(ibis.window(...))"
    """
    window = _build_window(partition_by, order_by, descending)
    return f"ibis.row_number().over({window})"


def first_value_by(
    column: str, partition_by: str | list[str], order_by: str, descending: bool = False
) -> str:
    """Generate ibis first() window expression for ordered value extraction.

    Args:
        column: Column reference (e.g. "t.channel")
        partition_by: Column(s) to partition by
        order_by: Column to order by
        descending: Whether to sort descending

    Returns:
        Expression string like "t.channel.first().over(ibis.window(...))"
    """
    window = _build_window(partition_by, order_by, descending)
    return f"{column}.first().over({window})"


def last_value_by(
    column: str, partition_by: str | list[str], order_by: str, descending: bool = False
) -> str:
    """Generate ibis last() window expression for ordered value extraction.

    Args:
        column: Column reference (e.g. "t.channel")
        partition_by: Column(s) to partition by
        order_by: Column to order by
        descending: Whether to sort descending

    Returns:
        Expression string like "t.channel.last().over(ibis.window(...))"
    """
    window = _build_window(partition_by, order_by, descending)
    return f"{column}.last().over({window})"


def isin_literal(column: str, values: list[str]) -> str:
    """Generate .isin() expression from a Python list.

    Args:
        column: Column reference (e.g. "t.domain")
        values: List of literal values

    Returns:
        Expression string like "t.domain.isin(['gmail.com', 'yahoo.com'])"
    """
    quoted = [repr(v) for v in values]
    return f"{column}.isin([{', '.join(quoted)}])"
