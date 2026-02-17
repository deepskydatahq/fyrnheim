"""Hashing primitives for identity resolution and data privacy.

These primitives generate Ibis code strings for hashing operations.
"""


def hash_email(col: str) -> str:
    """Generate Ibis code for SHA256 hash of lowercase trimmed email.

    Args:
        col: Column name (string reference)

    Returns:
        Ibis expression code as string

    Example:
        >>> hash_email("customer_email")
        't.customer_email.lower().strip().hash().cast("string")'
    """
    # Add t. prefix if not already present
    if not col.startswith(("t.", "ibis.")):
        col = f"t.{col}"
    return f'{col}.lower().strip().hash().cast("string")'


def hash_id(id_col: str, salt: str = "") -> str:
    """Generate Ibis code for SHA256 hash of ID with optional salt.

    Args:
        id_col: Column name containing ID
        salt: Optional salt string for hashing

    Returns:
        Ibis expression code as string
    """
    # Add t. prefix if not already present
    if not id_col.startswith(("t.", "ibis.")):
        id_col = f"t.{id_col}"
    if salt:
        return f'ibis.concat({id_col}.cast("string"), "{salt}").hash().cast("string")'
    return f'{id_col}.cast("string").hash().cast("string")'


def hash_sha256(value_expr: str) -> str:
    """Generate Ibis code for SHA256 hash of any expression.

    Args:
        value_expr: Column name or expression

    Returns:
        Ibis expression code as string
    """
    # Add t. prefix if not already present (for simple column names)
    if not value_expr.startswith(("t.", "ibis.")) and "." not in value_expr:
        value_expr = f"t.{value_expr}"
    return f'{value_expr}.hash().cast("string")'


def hash_md5(value_expr: str) -> str:
    """Generate Ibis code for MD5 hash (for non-cryptographic uses).

    Uses Ibis's hashbytes method with md5 algorithm, which returns binary.
    Result is cast to string for readability.

    Args:
        value_expr: Column name or expression

    Returns:
        Ibis expression code as string
    """
    # Add t. prefix if not already present (for simple column names)
    if not value_expr.startswith(("t.", "ibis.")) and "." not in value_expr:
        value_expr = f"t.{value_expr}"
    # Use hashbytes("md5") which returns binary, then cast to string
    return f'{value_expr}.hashbytes("md5").cast("string")'


def concat_hash(*cols: str) -> str:
    """Generate Ibis code for hash of concatenated columns.

    All columns are cast to string before concatenation to ensure
    consistent hashing across different data types.

    Args:
        *cols: Column names to concatenate and hash

    Returns:
        Ibis expression code as string

    Example:
        >>> concat_hash("person_id", "timestamp", "event_type")
        'ibis.concat(t.person_id.cast("string"), t.timestamp.cast("string"), t.event_type.cast("string")).hash().cast("string")'
    """
    col_exprs = []
    for col in cols:
        if not col.startswith(("t.", "ibis.")):
            col = f"t.{col}"
        col_exprs.append(f'{col}.cast("string")')

    return f'ibis.concat({", ".join(col_exprs)}).hash().cast("string")'
