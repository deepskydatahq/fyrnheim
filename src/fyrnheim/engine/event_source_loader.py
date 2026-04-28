"""Load EventSource tables into the standard event schema.

Converts an EventSource table into the universal event schema:
source, entity_id, ts, event_type, payload — all as strings.
"""

from __future__ import annotations

import json
import logging
import os

import ibis
import numpy as np
import pandas as pd

from fyrnheim.core.source import EventSource
from fyrnheim.engine.source_stage import build_source_stage_table

log = logging.getLogger("fyrnheim.event_source_loader")


def _build_event_source_table(
    conn: ibis.BaseBackend,
    event_source: EventSource,
    data_dir: str | os.PathLike[str] | None = None,
    backend: str = "duckdb",
    *,
    source_registry: dict[str, ibis.Table] | None = None,
    right_pk_registry: dict[str, str] | None = None,
    required_columns: set[str] | frozenset[str] | None = None,
) -> ibis.Table:
    """Apply the shared source-stage chain to an EventSource.

    The shared chain is implemented by
    :func:`fyrnheim.engine.source_stage.build_source_stage_table`:

      read → transforms → joins → json_path → computed_columns → filter

    EventSource-specific payload packing remains in :func:`load_event_source`.
    """
    return build_source_stage_table(
        event_source,
        conn,
        backend,
        data_dir=data_dir,
        source_registry=source_registry,
        right_pk_registry=right_pk_registry,
        log=log,
        source_kind="EventSource",
        required_columns=required_columns,
    )


def load_event_source(
    conn: ibis.BaseBackend,
    event_source: EventSource,
    data_dir: str | os.PathLike[str] | None = None,
    backend: str = "duckdb",
    *,
    source_registry: dict[str, ibis.Table] | None = None,
    right_pk_registry: dict[str, str] | None = None,
    pre_built_table: ibis.Table | None = None,
    required_columns: set[str] | frozenset[str] | None = None,
) -> ibis.Table:
    """Read an EventSource and convert it to the standard event schema.

    Maps entity_id_field -> entity_id, timestamp_field -> ts.
    Determines event_type from: static event_type, event_type_field column,
    or falls back to the source name.
    Remaining columns are packed into a JSON payload string.

    M076 (v0.13.0): EventSource gained the same ``joins`` field that
    StateSource got in M070. The pipeline-shape work (read → transforms
    → joins → json_path → computed_columns → filter) is delegated to
    :func:`_build_event_source_table`; this function then converts the
    post-pipeline table into universal event-schema rows. Joined
    columns flow into the emitted event payload naturally via the
    existing payload-pack step.

    Args:
        conn: Ibis backend connection (used for read_table).
        event_source: EventSource configuration.
        data_dir: Base directory for resolving relative duckdb_path values.
        backend: Backend name for read_table().
        source_registry: Mapping ``source_name → post-pipeline ibis.Table``
            for sibling sources already loaded in this pipeline run.
            Populated by ``run_pipeline`` Phase 1; passing ``None`` is
            fine when the EventSource declares no joins (the helper
            short-circuits on empty joins).
        right_pk_registry: Mapping ``source_name → id_field`` for the
            right-side primary-key column. Same semantic as the
            StateSource path. Only StateSource targets are valid in
            v0.13.0; EventSource targets are blocked at sort time.
        pre_built_table: Optional. When provided, skip the pipeline-shape
            stage and use this table directly for the event-shape
            conversion. The pipeline runner uses this to avoid
            double-running the chain after stashing the post-pipeline
            table in the source registry.

    Returns:
        Ibis table with columns: source, entity_id, ts, event_type, payload.
    """
    if pre_built_table is not None:
        table = pre_built_table
    else:
        table = _build_event_source_table(
            conn,
            event_source,
            data_dir=data_dir,
            backend=backend,
            source_registry=source_registry,
            right_pk_registry=right_pk_registry,
            required_columns=required_columns,
        )

    return build_event_source_event_table(table, event_source)


def build_event_source_event_table(
    table: ibis.Table,
    event_source: EventSource,
) -> ibis.Table:
    """Convert a post-stage EventSource table to canonical events.

    This is intentionally expression-only: it does not call ``execute()``
    and it does not iterate rows in Python. On BigQuery, the returned Ibis
    expression compiles to SQL that maps IDs/timestamps, chooses the event
    type, and packs the payload in the warehouse.
    """
    entity_id_col = event_source.entity_id_field
    ts_col = event_source.timestamp_field

    has_field = event_source.event_type_field is not None
    exclude_cols = {entity_id_col, ts_col}
    if has_field and event_source.event_type_field is not None:
        exclude_cols.add(event_source.event_type_field)
    exclude_cols.update(event_source.payload_exclude)

    if event_source.event_type is not None:
        event_type_expr = ibis.literal(event_source.event_type).cast("string")
    elif event_source.event_type_field is not None:
        event_type_expr = table[event_source.event_type_field].cast("string")
    else:
        event_type_expr = ibis.literal(event_source.name).cast("string")

    payload_cols = [col for col in table.columns if col not in exclude_cols]
    if payload_cols:
        payload = ibis.struct(
            {
                col: _payload_expr(table[col])
                for col in payload_cols
            }
        ).cast("json").cast("string")
    else:
        payload = ibis.literal("{}")

    return table.select(
        source=ibis.literal(event_source.name).cast("string"),
        entity_id=table[entity_id_col].cast("string"),
        ts=table[ts_col].cast("string"),
        event_type=event_type_expr,
        payload=payload,
    )


def _payload_expr(expr: ibis.Value) -> ibis.Value:
    """Return an expression matching legacy payload serialization.

    Legacy pandas packing encoded array-like payload values as JSON strings
    nested inside the payload object. Preserve that shape so existing GA4
    payload consumers can continue to call ``json.loads(payload[key])``.
    """
    dtype = expr.type()
    if dtype.is_array() or dtype.is_struct() or dtype.is_map():
        return expr.cast("json").cast("string")
    return expr


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
