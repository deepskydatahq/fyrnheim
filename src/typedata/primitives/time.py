"""Time duration primitives for Ibis code generation."""


def parse_iso8601_duration(col: str) -> str:
    """Parse ISO 8601 duration format to total seconds.

    Parses YouTube-style ISO 8601 duration strings (PT#H#M#S, PT#M#S, PT#S)
    into total seconds.

    Args:
        col: Column name containing ISO 8601 duration string

    Returns:
        Ibis expression code as string that calculates total seconds

    Examples:
        >>> parse_iso8601_duration("duration")
        # Handles: PT4M13S -> 253, PT1H2M3S -> 3723, PT30S -> 30

    Implementation uses Ibis regex operations to extract hours, minutes, and seconds,
    then calculates total seconds. Works across DuckDB and BigQuery backends.
    """
    if not col.startswith(("t.", "ibis.")):
        col = f"t.{col}"

    # The expression extracts H, M, S components using regex and converts to seconds
    # PT1H2M3S -> hours=1, minutes=2, seconds=3 -> 1*3600 + 2*60 + 3 = 3723
    # PT4M13S -> hours=null, minutes=4, seconds=13 -> 0 + 4*60 + 13 = 253
    # PT30S -> hours=null, minutes=null, seconds=30 -> 0 + 0 + 30 = 30

    # Build regex pattern for PT#H#M#S format
    pattern = r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?"

    # Helper to convert extracted group to int, handling empty strings from failed regex matches
    def safe_extract(group_num):
        extracted = f"{col}.re_extract(r'{pattern}', {group_num})"
        return f"ibis.ifelse({extracted} == '', ibis.literal(0), {extracted}.cast('int64'))"

    return (
        f"(" f"{safe_extract(1)} * 3600 + " f"{safe_extract(2)} * 60 + " f"{safe_extract(3)}" f")"
    )
