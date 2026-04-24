"""Load EventSource tables into the standard event schema.

Converts an EventSource table into the universal event schema:
source, entity_id, ts, event_type, payload — all as strings.
"""

from __future__ import annotations

import json
import os

import ibis
import numpy as np
import pandas as pd

from fyrnheim.core.source import EventSource
from fyrnheim.engine.source_transforms import _apply_source_transforms


def load_event_source(
    conn: ibis.BaseBackend,
    event_source: EventSource,
    data_dir: str | os.PathLike[str] | None = None,
    backend: str = "duckdb",
) -> ibis.Table:
    """Read an EventSource and convert it to the standard event schema.

    Maps entity_id_field -> entity_id, timestamp_field -> ts.
    Determines event_type from: static event_type, event_type_field column,
    or falls back to the source name.
    Remaining columns are packed into a JSON payload string.

    Args:
        conn: Ibis backend connection (used for read_table).
        event_source: EventSource configuration.
        data_dir: Base directory for resolving relative duckdb_path values.
        backend: Backend name for read_table().

    Returns:
        Ibis table with columns: source, entity_id, ts, event_type, payload.
    """
    table = event_source.read_table(conn, backend, data_dir=data_dir)

    # M068: apply read-time transforms (type_casts, divides, multiplies, renames)
    # before computed_columns so user expressions can reference the transformed schema.
    table = _apply_source_transforms(table, event_source.transforms)

    # Apply computed columns after transforms (users reference post-transform columns)
    if event_source.computed_columns:
        for cc in event_source.computed_columns:
            table = table.mutate(**{cc.name: eval(cc.expression, {"ibis": ibis, "t": table})})  # noqa: S307

    df = table.execute()

    # Map entity_id and ts
    entity_id_col = event_source.entity_id_field
    ts_col = event_source.timestamp_field

    # Determine event_type strategy
    has_static = event_source.event_type is not None
    has_field = event_source.event_type_field is not None

    # Columns that are NOT packed into payload
    exclude_cols = {entity_id_col, ts_col}
    if has_field and event_source.event_type_field is not None:
        exclude_cols.add(event_source.event_type_field)
    # User-configured exclusions for noisy/large columns (e.g. GA4 event_params)
    exclude_cols.update(event_source.payload_exclude)

    events: list[dict[str, str]] = []
    for _, row in df.iterrows():
        entity_id = str(row[entity_id_col])
        ts = str(row[ts_col])

        if has_static:
            event_type: str = event_source.event_type  # type: ignore[assignment]
        elif has_field:
            event_type = str(row[event_source.event_type_field])  # type: ignore[index]
        else:
            event_type = event_source.name

        # Pack remaining columns into payload
        payload = {
            k: _serialize_value(v)
            for k, v in row.items()
            if k not in exclude_cols
        }

        events.append(
            {
                "source": event_source.name,
                "entity_id": entity_id,
                "ts": ts,
                "event_type": event_type,
                "payload": json.dumps(payload),
            }
        )

    if not events:
        empty_schema = ibis.schema(
            {
                "source": "string",
                "entity_id": "string",
                "ts": "string",
                "event_type": "string",
                "payload": "string",
            }
        )
        return ibis.memtable([], schema=empty_schema)

    return ibis.memtable(pd.DataFrame(events))


def _serialize_value(v: object) -> object:
    """Convert a value to a JSON-safe representation that round-trips
    through json.dumps / json.loads with the right Python type.

    - Array-like values (list, tuple, np.ndarray) are JSON-encoded to
      preserve them as JSON arrays for BigQuery REPEATED fields.
    - None / NaN / NaT round-trips as Python None.
    - Primitives (str, int, float, bool) are preserved.
    - Exotic types are stringified via str().

    Order matters: the array check must come BEFORE pd.isna, because
    pd.isna on an ndarray returns an ndarray (not a bool) and breaks `if`.
    """
    if isinstance(v, (list, tuple, np.ndarray)):
        try:
            seq = v.tolist() if hasattr(v, "tolist") else list(v)
            return json.dumps(seq, default=str)
        except (TypeError, ValueError):
            return str(v)
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(v, (str, int, float, bool)):
        return v
    return str(v)
