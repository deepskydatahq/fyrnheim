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
