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

_EVENT_SCHEMA = ibis.schema(
    {
        "source": "string",
        "entity_id": "string",
        "ts": "string",
        "event_type": "string",
        "payload": "string",
    }
)


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

    if previous is None:
        # Cold start: every row is row_appeared. This stays as an Ibis
        # expression so warehouse backends build events server-side.
        return build_row_appeared_events(
            current,
            source_name=source_name,
            id_field=id_field,
            snapshot_date=snapshot_date,
        )

    return build_snapshot_diff_events(
        current=current,
        previous=previous,
        source_name=source_name,
        id_field=id_field,
        snapshot_date=snapshot_date,
        exclude_fields=exclude_fields,
    )


def _canonical_empty_events() -> ibis.Table:
    """Return an empty canonical event table expression."""
    return ibis.memtable([], schema=_EVENT_SCHEMA)


def _payload_json_expr(table: ibis.Table, fields: list[str]) -> ibis.Value:
    """Build a JSON-string payload expression from ``fields`` on ``table``."""
    if not fields:
        return ibis.literal("{}")
    return ibis.struct({field: table[field] for field in fields}).cast("json").cast("string")


def _canonical_events_select(
    table: ibis.Table,
    *,
    source_name: str,
    id_field: str,
    snapshot_date: str,
    event_type: str,
    payload: ibis.Value,
) -> ibis.Table:
    """Select the universal event schema from ``table``."""
    return table.select(
        source=ibis.literal(source_name).cast("string"),
        entity_id=table[id_field].cast("string"),
        ts=ibis.literal(snapshot_date).cast("string"),
        event_type=ibis.literal(event_type).cast("string"),
        payload=payload.cast("string"),
    )


def build_row_appeared_events(
    table: ibis.Table,
    *,
    source_name: str,
    id_field: str,
    snapshot_date: str,
) -> ibis.Table:
    """Build row_appeared events as a backend-executable expression."""
    payload_fields = [col for col in table.columns if col != id_field]
    return _canonical_events_select(
        table,
        source_name=source_name,
        id_field=id_field,
        snapshot_date=snapshot_date,
        event_type="row_appeared",
        payload=_payload_json_expr(table, payload_fields),
    )


def build_row_disappeared_events(
    table: ibis.Table,
    *,
    source_name: str,
    id_field: str,
    snapshot_date: str,
) -> ibis.Table:
    """Build row_disappeared events as a backend-executable expression."""
    payload_fields = [col for col in table.columns if col != id_field]
    return _canonical_events_select(
        table,
        source_name=source_name,
        id_field=id_field,
        snapshot_date=snapshot_date,
        event_type="row_disappeared",
        payload=_payload_json_expr(table, payload_fields),
    )


def build_snapshot_diff_events(
    *,
    current: ibis.Table,
    previous: ibis.Table,
    source_name: str,
    id_field: str,
    snapshot_date: str,
    exclude_fields: list[str] | None = None,
) -> ibis.Table:
    """Build appeared/disappeared/field_changed events with Ibis joins.

    This is the warehouse-native counterpart to the legacy pandas diff below.
    It intentionally returns an expression and never materializes current or
    previous rows locally.
    """
    exclude = set(exclude_fields or [])
    compare_fields = [
        col for col in current.columns if col != id_field and col not in exclude
    ]

    previous_marker = "__fyrnheim_previous_id"
    current_marker = "__fyrnheim_current_id"
    previous_prefixed = {
        field: f"__fyrnheim_previous_{idx}" for idx, field in enumerate(compare_fields)
    }

    previous_ids = previous.select(
        **{previous_marker: previous[id_field].cast("string")}
    )
    appeared_join = current.left_join(
        previous_ids,
        current[id_field].cast("string") == previous_ids[previous_marker],
    )
    appeared_rows = appeared_join.filter(appeared_join[previous_marker].isnull()).select(
        current.columns
    )
    appeared = build_row_appeared_events(
        appeared_rows,
        source_name=source_name,
        id_field=id_field,
        snapshot_date=snapshot_date,
    )

    current_ids = current.select(**{current_marker: current[id_field].cast("string")})
    disappeared_join = previous.left_join(
        current_ids,
        previous[id_field].cast("string") == current_ids[current_marker],
    )
    disappeared_rows = disappeared_join.filter(
        disappeared_join[current_marker].isnull()
    ).select(previous.columns)
    disappeared = build_row_disappeared_events(
        disappeared_rows,
        source_name=source_name,
        id_field=id_field,
        snapshot_date=snapshot_date,
    )

    event_tables = [appeared, disappeared]
    if compare_fields:
        previous_for_changes = previous.select(
            **{previous_marker: previous[id_field].cast("string")},
            **{alias: previous[field] for field, alias in previous_prefixed.items()},
        )
        joined = current.inner_join(
            previous_for_changes,
            current[id_field].cast("string") == previous_for_changes[previous_marker],
        )
        for field in compare_fields:
            previous_field = previous_prefixed[field]
            changed = joined.filter(
                ~joined[field]
                .cast("string")
                .identical_to(joined[previous_field].cast("string"))
            )
            payload = ibis.struct(
                {
                    "field_name": ibis.literal(field),
                    "old_value": changed[previous_field],
                    "new_value": changed[field],
                }
            ).cast("json").cast("string")
            event_tables.append(
                _canonical_events_select(
                    changed,
                    source_name=source_name,
                    id_field=id_field,
                    snapshot_date=snapshot_date,
                    event_type="field_changed",
                    payload=payload,
                )
            )

    if not event_tables:
        return _canonical_empty_events()
    return ibis.union(*event_tables, distinct=False)


def _stringify_id(v: object) -> str:
    """Stringify an id-field value, preserving int-shape for pandas-
    promoted integers.

    Context (M071 / v0.9.1 patch): ``pd.DataFrame.iterrows()`` packs each
    row into a homogeneous-dtype ``Series``. When a DataFrame has an
    int64 id column AND any float column, the per-row Series upcasts to
    float64 — so ``row[id_field]`` arrives here as ``np.float64(1.0)``
    and a naive ``str(v)`` produces ``'1.0'`` instead of ``'1'``. That
    silently breaks identity resolution for any StateSource with integer
    ids.

    This helper detects float values with no fractional part and casts
    them to int before stringifying, so integer ids surface as ``'1'``
    end-to-end regardless of sibling-column dtype promotion.

    Preserves ``str()`` default behavior for all non-integral values
    (true floats, strings, bools, etc.). Note that ``bool`` is NOT
    special-cased: ``isinstance(True, float)`` is False, so bool values
    fall through to ``str(v)`` and produce ``'True'`` / ``'False'`` —
    bool-as-id is pathological and not worth its own branch.
    """
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return str(v)


def _make_appeared_events(
    df: pd.DataFrame,
    source_name: str,
    id_field: str,
    snapshot_date: str,
) -> list[dict[str, str]]:
    """Create row_appeared events for every row in *df*."""
    events: list[dict[str, str]] = []
    # Use df.to_dict(orient="records") rather than iterrows() to preserve
    # each column's original dtype. iterrows() packs every row into a
    # homogeneous-dtype Series, which upcasts int64 to float64 when any
    # sibling column is float — silently corrupting large int ids and any
    # integer-typed payload field before _stringify_id / _serialize_value
    # ever see the value.
    for record in df.to_dict(orient="records"):
        raw_id = record[id_field]
        payload = {
            k: _serialize_value(v) for k, v in record.items() if k != id_field
        }
        events.append(
            {
                "source": source_name,
                "entity_id": _stringify_id(raw_id),
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
    # See _make_appeared_events for why we iterate via to_dict("records")
    # rather than iterrows() — it preserves per-column dtypes.
    for record in df.to_dict(orient="records"):
        raw_id = record[id_field]
        payload = {
            k: _serialize_value(v) for k, v in record.items() if k != id_field
        }
        events.append(
            {
                "source": source_name,
                "entity_id": _stringify_id(raw_id),
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
                            # All event types must share one stringification
                            # path; otherwise field_changed emits "1" while
                            # row_appeared emits "1.0" (or vice versa for
                            # large int64 after float-promotion), and
                            # enrich_events join on [source, entity_id]
                            # misses.
                            "entity_id": _stringify_id(eid),
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


def _serialize_value(v: object) -> object:
    """Convert a value to a JSON-safe representation that round-trips through
    json.dumps / json.loads with the right Python type.

    Critically: None / NaN / NaT must round-trip as Python None (so downstream
    consumers can use 'is None' or 'pd.isna' checks). Returning the string
    "null" instead made downstream null-aware logic (e.g. _resolve_latest's
    "first non-null" semantics) treat missing data as present.
    """
    if pd.isna(v):
        return None
    if isinstance(v, (str, int, float, bool)):
        return v
    return str(v)
