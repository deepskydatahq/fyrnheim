"""Date and time primitives for Ibis code generation."""


def date_diff_days(start_col: str, end_col: str = "ibis.now().date()") -> str:
    """Generate Ibis code for day difference between dates.

    Uses the .delta() method which works across all backends (DuckDB, BigQuery, etc).

    Args:
        start_col: Start date column name (the earlier date)
        end_col: End date column or expression (default: ibis.now().date())

    Returns:
        Ibis expression code as string

    Example:
        >>> date_diff_days("created_at")
        'ibis.now().date().delta(t.created_at.cast("date"), unit="day")'
    """
    # Add t. prefix if not already present
    if not start_col.startswith(("t.", "ibis.")):
        start_col = f"t.{start_col}"

    # Add t. prefix to end_col if it's a bare column name
    if not end_col.startswith(("t.", "ibis.")):
        end_col = f"t.{end_col}"

    # Use .delta() method which works across all backends (DuckDB, BigQuery, etc.)
    # The order is: end.delta(start, unit="day") = end - start in days
    return f'{end_col}.cast("date").delta({start_col}.cast("date"), unit="day")'


def date_trunc_month(date_col: str) -> str:
    """Generate Ibis code for truncating date to month.

    Args:
        date_col: Date column name

    Returns:
        Ibis expression code as string
    """
    if not date_col.startswith(("t.", "ibis.")):
        date_col = f"t.{date_col}"
    return f'{date_col}.truncate("M")'


def date_trunc_quarter(date_col: str) -> str:
    """Generate Ibis code for truncating date to quarter."""
    if not date_col.startswith(("t.", "ibis.")):
        date_col = f"t.{date_col}"
    return f'{date_col}.truncate("Q")'


def date_trunc_year(date_col: str) -> str:
    """Generate Ibis code for truncating date to year."""
    if not date_col.startswith(("t.", "ibis.")):
        date_col = f"t.{date_col}"
    return f'{date_col}.truncate("Y")'


def days_since(date_col: str, reference: str = "ibis.now().date()") -> str:
    """Generate Ibis code for days since a date.

    Convenience wrapper for date_diff_days.
    """
    return date_diff_days(date_col, reference)


def extract_year(date_col: str) -> str:
    """Generate Ibis code for extracting year from date."""
    if not date_col.startswith(("t.", "ibis.")):
        date_col = f"t.{date_col}"
    return f"{date_col}.year()"


def extract_month(date_col: str) -> str:
    """Generate Ibis code for extracting month from date."""
    if not date_col.startswith(("t.", "ibis.")):
        date_col = f"t.{date_col}"
    return f"{date_col}.month()"


def extract_day(date_col: str) -> str:
    """Generate Ibis code for extracting day from date."""
    if not date_col.startswith(("t.", "ibis.")):
        date_col = f"t.{date_col}"
    return f"{date_col}.day()"


def earliest_date(*date_cols: str) -> str:
    """Get earliest date across multiple columns.

    Args:
        date_cols: Date column names

    Returns:
        Ibis expression code for earliest date

    Example:
        >>> earliest_date("date1", "date2", "date3")
        'ibis.least(t.date1.fillna(...), t.date2.fillna(...), t.date3.fillna(...))'
    """
    # Add t. prefix if not already present and handle fillna for nulls
    processed_cols = []
    for col in date_cols:
        if not col.startswith(("t.", "ibis.")):
            col = f"t.{col}"
        # Fill nulls with far future date to exclude from LEAST
        processed_cols.append(f'{col}.fillna(ibis.date("9999-12-31"))')

    return f'ibis.least({", ".join(processed_cols)})'


def latest_date(*date_cols: str) -> str:
    """Get latest date across multiple columns.

    Args:
        date_cols: Date column names

    Returns:
        Ibis expression code for latest date

    Example:
        >>> latest_date("date1", "date2", "date3")
        'ibis.greatest(t.date1.fillna(...), t.date2.fillna(...), t.date3.fillna(...))'
    """
    # Add t. prefix if not already present and handle fillna for nulls
    processed_cols = []
    for col in date_cols:
        if not col.startswith(("t.", "ibis.")):
            col = f"t.{col}"
        # Fill nulls with far past date to exclude from GREATEST
        processed_cols.append(f'{col}.fillna(ibis.date("1970-01-01"))')

    return f'ibis.greatest({", ".join(processed_cols)})'
