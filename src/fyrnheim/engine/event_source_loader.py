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
from fyrnheim.engine.source_transforms import (
    _apply_joins,
    _apply_json_path_extractions,
    _apply_source_transforms,
    _reads_duckdb_fixture,
)

log = logging.getLogger("fyrnheim.event_source_loader")


def _build_event_source_table(
    conn: ibis.BaseBackend,
    event_source: EventSource,
    data_dir: str | os.PathLike[str] | None = None,
    backend: str = "duckdb",
    *,
    source_registry: dict[str, ibis.Table] | None = None,
    right_pk_registry: dict[str, str] | None = None,
) -> ibis.Table:
    """Apply the full read → transforms → joins → json_path →
    computed_columns → filter chain to an EventSource and return the
    resulting Ibis table.

    Mirrors :func:`_build_state_source_table` (M076, v0.13.0) so the
    Phase 1 pipeline runner can stash the post-pipeline table in the
    join-source-registry BEFORE the event-shape conversion turns it into
    the universal event schema. Calling :func:`load_event_source`
    continues to work without a registry — registry args are optional
    and the helper short-circuits on empty joins.

    Stage order (load-bearing, mirrors StateSource):

      read → transforms → joins → json_path → computed_columns → filter

    M072 (FR-8): when the source opts into fixture-shadow mode AND the
    engine is reading the duckdb_path parquet, skip transforms/joins/
    json_path/filter. The fixture is assumed to be in post-transform
    (and post-join) shape. computed_columns STILL APPLY — they are
    backend-independent expressions.

    M075 (FR-9): refines the M072 computed_columns rule. When
    fixture-shadow fires AND the computed_column's output column is
    already present in the fixture, preserve the fixture's value
    instead of recomputing.
    """
    table = event_source.read_table(conn, backend, data_dir=data_dir)

    # M072 (FR-8): when the source opts into fixture-shadow mode AND the
    # engine is reading the duckdb_path parquet (i.e. backend=duckdb +
    # duckdb_path set — mirrors BaseTableSource.read_table's parquet-read
    # branch), skip transforms / json_path / filter. The fixture is
    # assumed to be in post-transform shape; re-applying transforms would
    # either fail (pre-rename columns missing) or double-transform.
    # computed_columns STILL APPLY on the skip path — they are
    # backend-independent expressions, not transforms.
    reads_fixture = _reads_duckdb_fixture(event_source, backend)

    if reads_fixture:
        log.info(
            "EventSource %s: duckdb_fixture_is_transformed=True, "
            "skipping transforms/joins/fields/filter (reading duckdb_path fixture)",
            event_source.name,
        )
    else:
        # M068: apply read-time transforms (type_casts, divides, multiplies, renames)
        # before computed_columns so user expressions can reference the transformed schema.
        table = _apply_source_transforms(table, event_source.transforms)

        # M076 (v0.13.0): apply source-level joins AFTER transforms (so users
        # can join on transform-renamed columns) and BEFORE json_path
        # extraction (so users can extract JSON from joined columns).
        # Same pipeline-stage placement as _build_state_source_table.
        if event_source.joins:
            log.info(
                "EventSource %s: applying %d join(s) to %s",
                event_source.name,
                len(event_source.joins),
                [j.source_name for j in event_source.joins],
            )
            table = _apply_joins(
                table,
                event_source.joins,
                source_registry or {},
                right_pk_registry or {},
            )

        # M069: extract json_path fields AFTER renames (users can rename a JSON
        # column and extract from the renamed name) and BEFORE computed_columns
        # (so user expressions can reference the extracted typed columns).
        table = _apply_json_path_extractions(table, event_source.fields)

    # Apply computed columns after transforms (users reference post-transform columns).
    # M072: computed_columns apply on the fixture-shadow path because they are
    # backend-independent expressions, evaluating the same way on any backend.
    # M075 (FR-9): refines that rule. When fixture-shadow fires AND the
    # computed_column's output column already exists in the fixture, preserve
    # the fixture's value instead of recomputing. This unblocks expressions
    # that reference upstream-pipeline outputs (joins, json_path, transforms)
    # that DID skip on the fixture-shadow path — the fixture has the final
    # value baked in, so re-evaluating against missing inputs would fail.
    # Per-column granularity: columns missing from the fixture still get
    # computed.
    if event_source.computed_columns:
        for cc in event_source.computed_columns:
            if reads_fixture and cc.name in table.columns:
                log.info(
                    "EventSource %s: computed_column %s skipped (output already in fixture)",
                    event_source.name,
                    cc.name,
                )
                continue
            table = table.mutate(**{cc.name: eval(cc.expression, {"ibis": ibis, "t": table})})  # noqa: S307

    # M069: source-level filter applied AFTER computed_columns so users can
    # filter on computed values. See BaseTableSource.filter docstring for
    # NULL-gotcha and the .fillna(False) escape hatch.
    # M072: skipped on the fixture-shadow path (the fixture is assumed to
    # already be filtered to the post-transform row set).
    if not reads_fixture and event_source.filter:
        table = table.filter(
            eval(event_source.filter, {"ibis": ibis, "t": table})  # noqa: S307
        )

    return table


def load_event_source(
    conn: ibis.BaseBackend,
    event_source: EventSource,
    data_dir: str | os.PathLike[str] | None = None,
    backend: str = "duckdb",
    *,
    source_registry: dict[str, ibis.Table] | None = None,
    right_pk_registry: dict[str, str] | None = None,
    pre_built_table: ibis.Table | None = None,
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
        )

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
