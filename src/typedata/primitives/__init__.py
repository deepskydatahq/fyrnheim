"""SQL primitive functions - building blocks for complex SQL."""

# Hashing
# Aggregations
from .aggregations import (
    any_value,
    avg_,
    count_,
    count_distinct,
    cumulative_sum,
    first_value,
    lag_value,
    last_value,
    lead_value,
    max_,
    min_,
    row_number_by,
    sum_,
)

# Categorization
from .categorization import (
    boolean_to_int,
    categorize,
    categorize_contains,
    lifecycle_flag,
)

# Dates
from .dates import (
    date_diff_days,
    date_trunc_month,
    date_trunc_quarter,
    date_trunc_year,
    days_since,
    earliest_date,
    extract_day,
    extract_month,
    extract_year,
    latest_date,
)
from .hashing import (
    concat_hash,
    hash_email,
    hash_id,
    hash_md5,
    hash_sha256,
)

# JSON
from .json_ops import (
    json_extract_scalar,
    json_value,
    to_json_struct,
)

# Strings
from .strings import (
    account_id_from_domain,
    extract_email_domain,
    is_personal_email_domain,
)

# Time durations
from .time import (
    parse_iso8601_duration,
)

__all__ = [
    # Hashing
    "concat_hash",
    "hash_email",
    "hash_id",
    "hash_md5",
    "hash_sha256",
    # Dates
    "date_diff_days",
    "date_trunc_month",
    "date_trunc_quarter",
    "date_trunc_year",
    "days_since",
    "extract_year",
    "extract_month",
    "extract_day",
    "earliest_date",
    "latest_date",
    # Categorization
    "categorize",
    "categorize_contains",
    "lifecycle_flag",
    "boolean_to_int",
    # JSON
    "to_json_struct",
    "json_extract_scalar",
    "json_value",
    # Aggregations
    "row_number_by",
    "cumulative_sum",
    "lag_value",
    "lead_value",
    "sum_",
    "count_",
    "count_distinct",
    "avg_",
    "min_",
    "max_",
    "first_value",
    "last_value",
    "any_value",
    # Strings
    "extract_email_domain",
    "is_personal_email_domain",
    "account_id_from_domain",
    # Time durations
    "parse_iso8601_duration",
]
