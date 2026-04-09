"""Projection engine for AnalyticsEntity: state fields + activity-derived measures."""

from __future__ import annotations

import json
from typing import Any

import ibis
import pandas as pd

from fyrnheim.core.analytics_entity import AnalyticsEntity, Measure, StateField


class _RowProxy:
    """Proxy object that allows t.field_name access on a dict row.

    Supports expressions like t.email.split('@')[1].lower() by returning
    the actual value from the row dict on attribute access.
    """

    def __init__(self, row_dict: dict[str, Any]) -> None:
        self._data = row_dict

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            return super().__getattribute__(name)
        try:
            return self._data[name]
        except KeyError:
            raise AttributeError(f"No field '{name}' in row") from None


def _extract_field_value(row: pd.Series, field_name: str) -> Any:
    """Extract a field value from an enriched event row's payload.

    Three event-shape paths:

    1. row_appeared (StateSource snapshot insert): payload is a flat dict of
       column → value from the source row. Direct lookup.
    2. field_changed (StateSource snapshot update): payload is
       {field_name, old_value, new_value}. Return new_value only if
       field_name matches.
    3. Anything else (EventSource events, OR row_appeared events that have
       been rewritten by apply_activity_definitions to carry the activity
       name as event_type): the payload is still a flat dict of column →
       value, since activity rewriting preserves the original payload.
       Fall back to a direct lookup.

    Without case (3), state_fields silently return None for any entity
    whose events flow through apply_activity_definitions, because the
    rewrite changes event_type away from 'row_appeared'.
    """
    try:
        payload = json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]
    except (json.JSONDecodeError, TypeError):
        return None

    event_type = row["event_type"]

    if event_type == "row_appeared":
        return payload.get(field_name) if isinstance(payload, dict) else None
    if event_type == "field_changed":
        if isinstance(payload, dict) and payload.get("field_name") == field_name:
            return payload.get("new_value")
        return None

    # Fallback: flat payload lookup for activity-rewritten events and
    # EventSource events.
    if isinstance(payload, dict):
        return payload.get(field_name)
    return None


def _resolve_latest(events: pd.DataFrame, field_name: str) -> Any:
    """Resolve a field value using the 'latest' strategy: most recent non-null."""
    if events.empty:
        return None
    sorted_events = events.sort_values("ts", ascending=False)
    for _, row in sorted_events.iterrows():
        value = _extract_field_value(row, field_name)
        if value is not None:
            return value
    return None


def _resolve_first(events: pd.DataFrame, field_name: str) -> Any:
    """Resolve a field value using the 'first' strategy: earliest non-null."""
    if events.empty:
        return None
    sorted_events = events.sort_values("ts", ascending=True)
    for _, row in sorted_events.iterrows():
        value = _extract_field_value(row, field_name)
        if value is not None:
            return value
    return None


def _resolve_coalesce(
    all_events: pd.DataFrame, field_name: str, priority: list[str]
) -> Any:
    """Resolve a field using 'coalesce' strategy: iterate sources in priority order."""
    for source_name in priority:
        source_events = all_events[all_events["source"] == source_name]
        value = _resolve_latest(source_events, field_name)
        if value is not None:
            return value
    return None


def _resolve_state_field(
    events: pd.DataFrame, state_field: StateField, all_events: pd.DataFrame
) -> Any:
    """Resolve a single state field value for an entity."""
    if state_field.strategy == "latest":
        return _resolve_latest(events, state_field.field)
    elif state_field.strategy == "first":
        return _resolve_first(events, state_field.field)
    elif state_field.strategy == "coalesce":
        return _resolve_coalesce(
            all_events, state_field.field, state_field.priority or []
        )
    return None


def _extract_payload_field(row: pd.Series, field_name: str) -> Any:
    """Extract a named field from a row's payload JSON."""
    try:
        payload = json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]
    except (json.JSONDecodeError, TypeError):
        return None
    if isinstance(payload, dict):
        return payload.get(field_name)
    return None


def _compute_measure(events: pd.DataFrame, measure: Measure) -> Any:
    """Compute a single measure from events matching the measure's activity."""
    activity_events = events[events["event_type"] == measure.activity]

    if measure.aggregation == "count":
        return len(activity_events)

    elif measure.aggregation == "sum":
        total = 0.0
        for _, row in activity_events.iterrows():
            val = _extract_payload_field(row, measure.field)  # type: ignore[arg-type]
            if val is not None:
                try:
                    total += float(val)
                except (ValueError, TypeError):
                    pass
        return total

    elif measure.aggregation == "latest":
        if activity_events.empty:
            return None
        sorted_events = activity_events.sort_values("ts", ascending=False)
        for _, row in sorted_events.iterrows():
            val = _extract_payload_field(row, measure.field)  # type: ignore[arg-type]
            if val is not None:
                return val
        return None

    return None


def project_analytics_entity(
    enriched_events: ibis.expr.types.Table,
    analytics_entity: AnalyticsEntity,
) -> ibis.expr.types.Table:
    """Project one row per entity from enriched events using an AnalyticsEntity.

    Combines state field projection (latest/first/coalesce from row_appeared/
    field_changed events) with activity-derived measures (count/sum/latest).

    Args:
        enriched_events: Ibis table with columns: source, entity_id, ts,
            event_type, payload, and optionally canonical_id.
        analytics_entity: AnalyticsEntity defining state fields, measures,
            and computed fields.

    Returns:
        Ibis memtable with one row per entity plus all state, measure,
        and computed field columns.
    """
    df = enriched_events.execute()

    # Determine grouping key: use canonical_id if present, else entity_id
    group_key = "canonical_id" if "canonical_id" in df.columns else "entity_id"

    group_ids = df[group_key].unique()
    rows: list[dict[str, Any]] = []

    for gid in group_ids:
        gid_events = df[df[group_key] == gid]
        row: dict[str, Any] = {group_key: gid}

        # Project state fields
        for sf in analytics_entity.state_fields:
            if sf.strategy == "coalesce":
                row[sf.name] = _resolve_state_field(gid_events, sf, gid_events)
            else:
                source_events = gid_events[gid_events["source"] == sf.source]
                row[sf.name] = _resolve_state_field(source_events, sf, gid_events)

        # Compute measures
        for measure in analytics_entity.measures:
            row[measure.name] = _compute_measure(gid_events, measure)

        rows.append(row)

    # Build column list for empty DataFrame case
    all_columns = (
        [group_key]
        + [sf.name for sf in analytics_entity.state_fields]
        + [m.name for m in analytics_entity.measures]
    )

    result_df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=all_columns)

    # Evaluate computed fields
    for cf in analytics_entity.computed_fields:
        result_df[cf.name] = result_df.apply(
            lambda r, expr=cf.expression: eval(  # noqa: S307
                expr,
                {"__builtins__": {}},
                {**r.to_dict(), "t": _RowProxy(r.to_dict())},
            ),
            axis=1,
        )

    return ibis.memtable(result_df)
