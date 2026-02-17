"""String manipulation primitives for SQL generation."""


def extract_email_domain(email_col: str) -> str:
    """Extract domain from email address.

    Args:
        email_col: Email column name

    Returns:
        SQL expression to extract domain (DuckDB-compatible)
    """
    return f"split_part({email_col}, '@', 2)"


def is_personal_email_domain(domain_col: str) -> str:
    """Check if email domain is a personal email provider.

    Args:
        domain_col: Email domain column name

    Returns:
        SQL expression for personal email check
    """
    personal_domains = [
        "gmail.com",
        "yahoo.com",
        "outlook.com",
        "hotmail.com",
        "icloud.com",
        "aol.com",
        "protonmail.com",
        "mail.com",
    ]
    domain_list = ", ".join(f"'{d}'" for d in personal_domains)
    return f"{domain_col} IN ({domain_list})"


def account_id_from_domain(domain_col: str, is_personal_col: str) -> str:
    """Generate account_id from email domain for business emails.

    Args:
        domain_col: Email domain column name
        is_personal_col: Is personal email boolean column

    Returns:
        SQL expression for account_id generation
    """
    return f"CASE WHEN {is_personal_col} THEN NULL ELSE MD5({domain_col}) END"
