---
title: Primitives
description: Reusable Python functions that replace SQL snippets for hashing, dates, categorization, and more.
---

Primitives are reusable Python functions that replace SQL snippets. Hashing, date operations, categorization -- import and compose them instead of copy-pasting SQL.

## Usage

```python
from fyrnheim.primitives import hash_email, date_trunc_month
from fyrnheim import ComputedColumn

ComputedColumn(name="email_hash", expression=hash_email("email"))
ComputedColumn(name="signup_month", expression=date_trunc_month("created_at"))
```

Primitives generate Ibis expressions, so they compile to the correct SQL for every backend (DuckDB, BigQuery, ClickHouse, etc.).

## Available Primitives

### Hashing

- `hash_email(column)` -- SHA-256 hash of a lowered, trimmed email
- `hash_id(column)` -- SHA-256 hash of a column value
- `hash_md5(column)` -- MD5 hash of a column value
- `hash_sha256(column)` -- SHA-256 hash of a column value
- `concat_hash(*columns)` -- Concatenate columns and hash the result

### Dates

- `date_trunc_month(column)` -- Truncate a date to the first of the month
- `date_trunc_quarter(column)` -- Truncate a date to the first of the quarter
- `date_trunc_year(column)` -- Truncate a date to the first of the year
- `days_since(column)` -- Number of days between a date column and today
- `date_diff_days(start, end)` -- Number of days between two date columns
- `extract_year(column)` -- Extract year from a date
- `extract_month(column)` -- Extract month from a date
- `extract_day(column)` -- Extract day from a date
- `earliest_date(*columns)` -- The earliest (minimum) date across columns
- `latest_date(*columns)` -- The latest (maximum) date across columns

### Categorization

- `categorize(column, mapping)` -- Map column values to categories via a dictionary
- `categorize_contains(column, mapping)` -- Categorize by substring matching
- `lifecycle_flag(column, states)` -- Produce a boolean flag based on column value
- `boolean_to_int(column)` -- Convert a boolean column to 0/1

### JSON

- `to_json_struct(column)` -- Parse a JSON string column into a struct
- `json_extract_scalar(column, path)` -- Extract a scalar value from JSON
- `json_value(column, path)` -- Extract a value from JSON

### Aggregations

- `sum_(column)` -- Sum of a column
- `count_(column)` -- Count of a column
- `count_distinct(column)` -- Count of distinct values
- `avg_(column)` -- Average of a column
- `min_(column)` -- Minimum value
- `max_(column)` -- Maximum value
- `row_number_by(partition, order)` -- Row number within a partition
- `cumulative_sum(column)` -- Cumulative sum
- `lag_value(column)` -- Previous row's value
- `lead_value(column)` -- Next row's value
- `first_value(column)` -- First value in a window
- `last_value(column)` -- Last value in a window
- `any_value(column)` -- Any non-null value

### Strings

- `extract_email_domain(column)` -- Extract the domain from an email address
- `is_personal_email_domain(column)` -- Check if an email domain is a personal provider
- `account_id_from_domain(column)` -- Derive an account ID from an email domain

### Time

- `parse_iso8601_duration(column)` -- Parse an ISO 8601 duration string
