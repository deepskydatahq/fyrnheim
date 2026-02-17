"""Window function and aggregation primitives."""


def row_number_by(partition_by: str, order_by: str) -> str:
    """Generate ROW_NUMBER() OVER window function.

    Args:
        partition_by: Partition column(s)
        order_by: Order by expression

    Returns:
        SQL ROW_NUMBER expression
    """
    return f"""ROW_NUMBER() OVER (
        PARTITION BY {partition_by}
        ORDER BY {order_by}
    )"""


def cumulative_sum(value_col: str, partition_by: str, order_by: str) -> str:
    """Generate cumulative sum window function.

    Args:
        value_col: Column to sum
        partition_by: Partition column(s)
        order_by: Order by expression

    Returns:
        SQL SUM window expression
    """
    return f"""SUM({value_col}) OVER (
        PARTITION BY {partition_by}
        ORDER BY {order_by}
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )"""


def lag_value(value_col: str, partition_by: str, order_by: str, offset: int = 1) -> str:
    """Generate LAG window function.

    Args:
        value_col: Column to lag
        partition_by: Partition column(s)
        order_by: Order by expression
        offset: Number of rows to lag (default 1)

    Returns:
        SQL LAG expression
    """
    return f"""LAG({value_col}, {offset}) OVER (
        PARTITION BY {partition_by}
        ORDER BY {order_by}
    )"""


def lead_value(value_col: str, partition_by: str, order_by: str, offset: int = 1) -> str:
    """Generate LEAD window function."""
    return f"""LEAD({value_col}, {offset}) OVER (
        PARTITION BY {partition_by}
        ORDER BY {order_by}
    )"""


def sum_(column: str) -> str:
    """Generate SUM aggregation.

    Args:
        column: Column to sum

    Returns:
        SQL SUM expression
    """
    return f"SUM({column})"


def count_() -> str:
    """Generate COUNT(*) aggregation.

    Returns:
        SQL COUNT expression
    """
    return "COUNT(*)"


def count_distinct(column: str) -> str:
    """Generate COUNT(DISTINCT column) aggregation.

    Args:
        column: Column to count distinct values

    Returns:
        SQL COUNT DISTINCT expression
    """
    return f"COUNT(DISTINCT {column})"


def avg_(column: str) -> str:
    """Generate AVG aggregation.

    Args:
        column: Column to average

    Returns:
        SQL AVG expression
    """
    return f"AVG({column})"


def min_(column: str) -> str:
    """Generate MIN aggregation.

    Args:
        column: Column to find minimum

    Returns:
        SQL MIN expression
    """
    return f"MIN({column})"


def max_(column: str) -> str:
    """Generate MAX aggregation.

    Args:
        column: Column to find maximum

    Returns:
        SQL MAX expression
    """
    return f"MAX({column})"


def first_value(column: str, partition_by: str, order_by: str) -> str:
    """Generate FIRST_VALUE window function.

    Args:
        column: Column to get first value of
        partition_by: Partition column(s)
        order_by: Order by expression

    Returns:
        SQL FIRST_VALUE expression
    """
    return f"""FIRST_VALUE({column}) OVER (
        PARTITION BY {partition_by}
        ORDER BY {order_by}
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )"""


def last_value(column: str, partition_by: str, order_by: str) -> str:
    """Generate LAST_VALUE window function.

    Args:
        column: Column to get last value of
        partition_by: Partition column(s)
        order_by: Order by expression

    Returns:
        SQL LAST_VALUE expression
    """
    return f"""LAST_VALUE({column}) OVER (
        PARTITION BY {partition_by}
        ORDER BY {order_by}
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )"""


def any_value(column: str) -> str:
    """Generate ANY_VALUE aggregation.

    Args:
        column: Column to get any non-null value from

    Returns:
        SQL ANY_VALUE expression
    """
    return f"ANY_VALUE({column})"
