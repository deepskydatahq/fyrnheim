"""Categorization and bucketing primitives for Ibis code generation."""


def categorize(column: str, thresholds: list[tuple[float, str]], default: str = "unknown") -> str:
    """Generate Ibis code for numeric categorization using ibis.cases().

    Args:
        column: Column name to categorize
        thresholds: List of (threshold, label) tuples in ascending order
        default: Label for values above highest threshold

    Returns:
        Ibis cases expression code

    Example:
        >>> categorize("revenue", [(1000, "small"), (10000, "medium")], "large")
        'ibis.cases(
            (t.revenue < 1000, "small"),
            (t.revenue < 10000, "medium"),
            else_="large"
        )'
    """
    # Add t. prefix if not already present
    if not column.startswith(("t.", "ibis.")):
        column = f"t.{column}"

    # Build tuple conditions
    conditions = [f'({column} < {threshold}, "{label}")' for threshold, label in thresholds]
    conditions_str = ",\n    ".join(conditions)

    return f"""ibis.cases(
    {conditions_str},
    else_="{default}"
)"""


def categorize_contains(
    column: str,
    categories: dict[str, list[str]],
    default: str = "other",
) -> str:
    """Generate Ibis code for string pattern categorization using contains().

    Automatically applies .fill_null('').lower() for case-insensitive matching
    with null handling.

    Args:
        column: Column name to categorize
        categories: Dict mapping category name to list of patterns to match
        default: Label when no patterns match

    Returns:
        Ibis cases expression code

    Example:
        >>> categorize_contains("referrer", {
        ...     "social": ["linkedin.com", "twitter.com"],
        ...     "seo": ["google.com", "bing.com"],
        ... }, default="other")
    """
    if not column.startswith(("t.", "ibis.")):
        column = f"t.{column}"

    # Column with null handling and lowercase
    col_expr = f"{column}.fill_null('').lower()"

    conditions = []
    for category, patterns in categories.items():
        if len(patterns) == 1:
            cond = f"{col_expr}.contains('{patterns[0]}')"
        else:
            parts = [f"{col_expr}.contains('{p}')" for p in patterns]
            cond = " |\n     ".join(parts)
        conditions.append(f"    ({cond}, '{category}')")

    conditions_str = ",\n".join(conditions)
    return f"""ibis.cases(
{conditions_str},
    else_='{default}'
)"""


def lifecycle_flag(status_col: str, active_states: list[str]) -> str:
    """Generate Ibis code for lifecycle state check.

    Args:
        status_col: Status column name
        active_states: List of states considered "active"

    Returns:
        Ibis isin expression code

    Example:
        >>> lifecycle_flag("status", ["active", "on_trial"])
        't.status.isin(["active", "on_trial"])'
    """
    # Add t. prefix if not already present
    if not status_col.startswith(("t.", "ibis.")):
        status_col = f"t.{status_col}"

    states_list = ", ".join([f'"{s}"' for s in active_states])
    return f"{status_col}.isin([{states_list}])"


def boolean_to_int(bool_col: str) -> str:
    """Generate Ibis code for converting boolean to integer (0/1).

    Args:
        bool_col: Boolean column name

    Returns:
        Ibis cast expression code
    """
    # Add t. prefix if not already present
    if not bool_col.startswith(("t.", "ibis.")):
        bool_col = f"t.{bool_col}"
    return f'{bool_col}.cast("int64")'
