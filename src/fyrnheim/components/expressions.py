"""Expression helper functions for computed columns.

These functions generate ibis expression strings that can be used
inside ComputedColumn.expression, eliminating fragile hand-written strings.
"""


def contains_any(column: str, values: list[str]) -> str:
    """Generate a chained .contains().fill_null(False) expression for tag/group matching.

    Args:
        column: The column reference (e.g. 't.tags')
        values: List of values to check for containment

    Returns:
        An expression string like:
        (t.tags.contains('masterdoc').fill_null(False) | t.tags.contains('cohort1').fill_null(False))

    Raises:
        ValueError: If values list is empty.
    """
    if not values:
        raise ValueError("contains_any requires at least one value")

    parts = [f"{column}.contains('{v}').fill_null(False)" for v in values]
    if len(parts) == 1:
        return parts[0]
    return "(" + " | ".join(parts) + ")"


def isin_literal(column: str, values: list[str]) -> str:
    """Generate an .isin() expression string from a Python list.

    Args:
        column: The column reference (e.g. 't.domain')
        values: List of string values to match against

    Returns:
        An expression string like:
        t.domain.isin(['gmail.com', 'yahoo.com', 'hotmail.com'])

    For an empty list, returns the column with .isin([]).
    """
    quoted = ", ".join(f"'{v.replace(chr(39), chr(92) + chr(39))}'" for v in values)
    return f"{column}.isin([{quoted}])"
