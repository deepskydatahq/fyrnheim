"""Snapshot layer runtime: daily snapshot with deduplication.

Provides apply_snapshot() for the daily snapshot pattern used by all
entity snapshot layers. The generator produces thin wrappers that call
this function with entity-specific configuration.
"""

from __future__ import annotations

import os

import ibis


def apply_snapshot(
    table: ibis.Table,
    natural_key: str | list[str],
    date_column: str = "ds",
    dedup_order_by: str = "updated_at",
    dedup_descending: bool = True,
    snapshot_date: str | None = None,
    include_validity_range: bool = False,
) -> ibis.Table:
    """Apply daily snapshot pattern to a dimension table.

    Steps:
    1. Add snapshot date column (from parameter, env var, or current date)
    2. Compute surrogate key: hash(natural_key + ds)
    3. Deduplicate: one row per entity per snapshot date
    4. Optionally add valid_from / valid_to columns

    Args:
        table: Input dimension table.
        natural_key: Column name(s) that identify a unique entity row.
        date_column: Name of the snapshot date column to add.
        dedup_order_by: Column to order by when deduplicating.
        dedup_descending: Whether dedup ordering is descending (most recent first).
        snapshot_date: Explicit snapshot date string (YYYY-MM-DD). If None,
            falls back to SNAPSHOT_DATE env var, then current date.
        include_validity_range: If True, add valid_from and valid_to columns.

    Returns:
        Snapshot table with ds column, surrogate key, and deduplication applied.
    """
    # Normalize natural_key to list
    key_cols = [natural_key] if isinstance(natural_key, str) else list(natural_key)

    # 1. Determine snapshot date
    ds_value = snapshot_date or os.getenv("SNAPSHOT_DATE")
    if ds_value:
        ds_expr = ibis.literal(ds_value).cast("date")
    else:
        ds_expr = ibis.now().date()

    t = table.mutate(**{date_column: ds_expr})

    # 2. Surrogate key: hash(concat(natural_key_parts, ds))
    key_parts = [t[k].cast("string") for k in key_cols]
    key_parts.append(t[date_column].cast("string"))

    concat_expr = key_parts[0]
    for part in key_parts[1:]:
        concat_expr = concat_expr.concat(part)
    t = t.mutate(snapshot_key=concat_expr.hash().cast("string"))

    # 3. Deduplicate: one row per entity per snapshot date
    group_cols = list(key_cols) + [date_column]
    order_col = ibis.desc(dedup_order_by) if dedup_descending else dedup_order_by
    window = ibis.window(group_by=group_cols, order_by=order_col)

    # ibis row_number() is 0-indexed (unlike SQL which is 1-indexed)
    t = t.mutate(_rn=ibis.row_number().over(window))
    t = t.filter(t._rn == 0)
    t = t.drop("_rn")

    # 4. Optional validity range
    if include_validity_range:
        validity_window = ibis.window(
            group_by=key_cols,
            order_by=date_column,
        )
        t = t.mutate(
            valid_from=t[date_column],
            valid_to=t[date_column].lead().over(validity_window),
        )

    return t


def _parse_dedup_order(order_by_str: str) -> tuple[str, bool]:
    """Parse 'column_name DESC' into (column_name, is_descending).

    Args:
        order_by_str: String like 'updated_at DESC' or 'updated_at'.

    Returns:
        Tuple of (column_name, descending_bool).
    """
    parts = order_by_str.strip().split()
    column = parts[0]
    descending = True  # default: most recent wins
    if len(parts) > 1:
        direction = parts[1].upper()
        descending = direction != "ASC"
    return column, descending
