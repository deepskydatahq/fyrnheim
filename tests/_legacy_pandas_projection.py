"""Vendored from v0.6.2 for M060 equivalence testing.

This is a frozen copy of ``project_analytics_entity`` and its helpers as
they existed on main before the M060 push-down rewrite. Used ONLY by
``tests/test_analytics_entity_engine.py``'s equivalence suite. Delete
after the new implementation has been green on main for 2+ weeks.

The leading underscore in the module name prevents pytest from
collecting its contents as tests.
"""

from __future__ import annotations

import json
from typing import Any

import ibis
import pandas as pd

from fyrnheim.core.analytics_entity import AnalyticsEntity, Measure, StateField


class _LegacyRowProxy:
    """Proxy object that allows t.field_name access on a dict row."""

    def __init__(self, row_dict: dict[str, Any]) -> None:
        self._data = row_dict

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            return super().__getattribute__(name)
        try:
            return self._data[name]
        except KeyError:
            raise AttributeError(f"No field '{name}' in row") from None


def _legacy_extract_field_value(row: pd.Series, field_name: str) -> Any:
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

    if isinstance(payload, dict):
        return payload.get(field_name)
    return None


def _legacy_resolve_latest(events: pd.DataFrame, field_name: str) -> Any:
    if events.empty:
        return None
    sorted_events = events.sort_values("ts", ascending=False)
    for _, row in sorted_events.iterrows():
        value = _legacy_extract_field_value(row, field_name)
        if value is not None:
            return value
    return None


def _legacy_resolve_first(events: pd.DataFrame, field_name: str) -> Any:
    if events.empty:
        return None
    sorted_events = events.sort_values("ts", ascending=True)
    for _, row in sorted_events.iterrows():
        value = _legacy_extract_field_value(row, field_name)
        if value is not None:
            return value
    return None


def _legacy_resolve_coalesce(
    all_events: pd.DataFrame, field_name: str, priority: list[str]
) -> Any:
    for source_name in priority:
        source_events = all_events[all_events["source"] == source_name]
        value = _legacy_resolve_latest(source_events, field_name)
        if value is not None:
            return value
    return None


def _legacy_resolve_state_field(
    events: pd.DataFrame, state_field: StateField, all_events: pd.DataFrame
) -> Any:
    if state_field.strategy == "latest":
        return _legacy_resolve_latest(events, state_field.field)
    elif state_field.strategy == "first":
        return _legacy_resolve_first(events, state_field.field)
    elif state_field.strategy == "coalesce":
        return _legacy_resolve_coalesce(
            all_events, state_field.field, state_field.priority or []
        )
    return None


def _legacy_extract_payload_field(row: pd.Series, field_name: str) -> Any:
    try:
        payload = json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]
    except (json.JSONDecodeError, TypeError):
        return None
    if isinstance(payload, dict):
        return payload.get(field_name)
    return None


def _legacy_compute_measure(events: pd.DataFrame, measure: Measure) -> Any:
    activity_events = events[events["event_type"] == measure.activity]

    if measure.aggregation == "count":
        return len(activity_events)

    elif measure.aggregation == "sum":
        total = 0.0
        for _, row in activity_events.iterrows():
            val = _legacy_extract_payload_field(row, measure.field)  # type: ignore[arg-type]
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
            val = _legacy_extract_payload_field(row, measure.field)  # type: ignore[arg-type]
            if val is not None:
                return val
        return None

    return None


def legacy_project_analytics_entity(
    enriched_events: ibis.expr.types.Table,
    analytics_entity: AnalyticsEntity,
) -> ibis.expr.types.Table:
    """Frozen v0.6.2 implementation of ``project_analytics_entity``."""
    df = enriched_events.execute()

    relevant_sources: set[str] = set()
    for sf in analytics_entity.state_fields:
        if sf.strategy == "coalesce":
            relevant_sources.update(sf.priority or [])
        else:
            relevant_sources.add(sf.source)
    relevant_activities = {m.activity for m in analytics_entity.measures}

    if relevant_sources or relevant_activities:
        mask = df["source"].isin(relevant_sources) | df["event_type"].isin(
            relevant_activities
        )
        df = df[mask]

    group_key = "canonical_id" if "canonical_id" in df.columns else "entity_id"

    group_ids = df[group_key].unique()
    rows: list[dict[str, Any]] = []

    for gid in group_ids:
        gid_events = df[df[group_key] == gid]
        row: dict[str, Any] = {group_key: gid}

        for sf in analytics_entity.state_fields:
            if sf.strategy == "coalesce":
                row[sf.name] = _legacy_resolve_state_field(gid_events, sf, gid_events)
            else:
                source_events = gid_events[gid_events["source"] == sf.source]
                row[sf.name] = _legacy_resolve_state_field(source_events, sf, gid_events)

        for measure in analytics_entity.measures:
            row[measure.name] = _legacy_compute_measure(gid_events, measure)

        rows.append(row)

    all_columns = (
        [group_key]
        + [sf.name for sf in analytics_entity.state_fields]
        + [m.name for m in analytics_entity.measures]
        + [cf.name for cf in analytics_entity.computed_fields]
    )

    if not rows:
        return ibis.memtable(pd.DataFrame(columns=all_columns))

    result_df = pd.DataFrame(rows)

    for cf in analytics_entity.computed_fields:
        result_df[cf.name] = result_df.apply(
            lambda r, expr=cf.expression: eval(  # noqa: S307
                expr,
                {"__builtins__": {}},
                {**r.to_dict(), "t": _LegacyRowProxy(r.to_dict())},
            ),
            axis=1,
        )

    return ibis.memtable(result_df)
