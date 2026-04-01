"""Diff engine for comparing state source snapshots.

Compares two Ibis tables (current vs previous snapshot) by id_field
and produces a unified event table with row_appeared, field_changed,
and row_disappeared events following the universal event schema:
source, entity_id, ts, event_type, payload.
"""

from __future__ import annotations

import json

import ibis
import pandas as pd


def diff_snapshots(
    current: ibis.Table,
    previous: ibis.Table | None,
    *,
    source_name: str,
    id_field: str = "id",
    snapshot_date: str,
    exclude_fields: list[str] | None = None,
) -> ibis.Table:
    """Compare two snapshots and produce raw change events.

    Compares *current* against *previous* by *id_field* and emits:

    - **row_appeared** – ID exists in *current* but not in *previous*.
    - **row_disappeared** – ID exists in *previous* but not in *current*.
    - **field_changed** – ID exists in both but one or more field values
      differ (one event per changed field).

    When *previous* is ``None`` (cold start), every row in *current*
    produces a ``row_appeared`` event.

    Args:
        current: Current snapshot as an Ibis table expression.
        previous: Previous snapshot, or ``None`` for cold start.
        source_name: Logical name of the state source (written to
            the ``source`` column of each event).
        id_field: Column used as the entity identifier.
        snapshot_date: Date string written to the ``ts`` column.
        exclude_fields: Field names to ignore when detecting
            ``field_changed`` events.  These fields still appear in
            ``row_appeared`` / ``row_disappeared`` payloads.

    Returns:
        Ibis table with columns ``source``, ``entity_id``, ``ts``,
        ``event_type``, ``payload`` (JSON string).
    """
    if exclude_fields is None:
        exclude_fields = []

    cur_df = current.execute()

    if previous is None:
        # Cold start: every row is row_appeared
        events = _make_appeared_events(cur_df, source_name, id_field, snapshot_date)
    else:
        prev_df = previous.execute()
        events = _diff_dataframes(
            cur_df, prev_df, source_name, id_field, snapshot_date, exclude_fields
        )

    if not events:
        # Return an empty table with the correct schema
        empty = pd.DataFrame(
            columns=["source", "entity_id", "ts", "event_type", "payload"]
        )
        return ibis.memtable(empty)

    return ibis.memtable(pd.DataFrame(events))


def _make_appeared_events(
    df: pd.DataFrame,
    source_name: str,
    id_field: str,
    snapshot_date: str,
) -> list[dict[str, str]]:
    """Create row_appeared events for every row in *df*."""
    events: list[dict[str, str]] = []
    for _, row in df.iterrows():
        payload = {k: _serialize_value(v) for k, v in row.items() if k != id_field}
        events.append(
            {
                "source": source_name,
                "entity_id": str(row[id_field]),
                "ts": snapshot_date,
                "event_type": "row_appeared",
                "payload": json.dumps(payload),
            }
        )
    return events


def _make_disappeared_events(
    df: pd.DataFrame,
    source_name: str,
    id_field: str,
    snapshot_date: str,
) -> list[dict[str, str]]:
    """Create row_disappeared events for every row in *df*."""
    events: list[dict[str, str]] = []
    for _, row in df.iterrows():
        payload = {k: _serialize_value(v) for k, v in row.items() if k != id_field}
        events.append(
            {
                "source": source_name,
                "entity_id": str(row[id_field]),
                "ts": snapshot_date,
                "event_type": "row_disappeared",
                "payload": json.dumps(payload),
            }
        )
    return events


def _diff_dataframes(
    cur_df: pd.DataFrame,
    prev_df: pd.DataFrame,
    source_name: str,
    id_field: str,
    snapshot_date: str,
    exclude_fields: list[str],
) -> list[dict[str, str]]:
    """Diff two DataFrames and produce all event types."""
    events: list[dict[str, str]] = []

    cur_ids = set(cur_df[id_field])
    prev_ids = set(prev_df[id_field])

    # row_appeared: in current but not previous
    appeared_ids = cur_ids - prev_ids
    if appeared_ids:
        appeared_df = cur_df[cur_df[id_field].isin(appeared_ids)]
        events.extend(
            _make_appeared_events(appeared_df, source_name, id_field, snapshot_date)
        )

    # row_disappeared: in previous but not current
    disappeared_ids = prev_ids - cur_ids
    if disappeared_ids:
        disappeared_df = prev_df[prev_df[id_field].isin(disappeared_ids)]
        events.extend(
            _make_disappeared_events(
                disappeared_df, source_name, id_field, snapshot_date
            )
        )

    # field_changed: in both, compare field values
    common_ids = cur_ids & prev_ids
    if common_ids:
        # Index by id_field for fast lookup
        cur_indexed = cur_df.set_index(id_field)
        prev_indexed = prev_df.set_index(id_field)

        # Fields to compare (exclude id_field and excluded fields)
        compare_fields = [
            c
            for c in cur_df.columns
            if c != id_field and c not in exclude_fields
        ]

        for eid in common_ids:
            cur_row = cur_indexed.loc[eid]
            prev_row = prev_indexed.loc[eid]
            for field in compare_fields:
                cur_val = cur_row[field]
                prev_val = prev_row[field]
                if _values_differ(cur_val, prev_val):
                    payload = {
                        "field_name": field,
                        "old_value": _serialize_value(prev_val),
                        "new_value": _serialize_value(cur_val),
                    }
                    events.append(
                        {
                            "source": source_name,
                            "entity_id": str(eid),
                            "ts": snapshot_date,
                            "event_type": "field_changed",
                            "payload": json.dumps(payload),
                        }
                    )

    return events


def _values_differ(a: object, b: object) -> bool:
    """Check if two values are different, handling NaN correctly."""
    # Both NaN -> same
    if pd.isna(a) and pd.isna(b):
        return False
    # One NaN -> different
    if pd.isna(a) or pd.isna(b):
        return True
    return a != b


def _serialize_value(v: object) -> str:
    """Convert a value to a JSON-safe string representation."""
    if pd.isna(v):
        return "null"
    return str(v)
