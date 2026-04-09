"""Activity engine for applying ActivityDefinitions to raw event streams.

Filters raw events (from diff_snapshots) by activity definitions and
produces named business events.
"""

from __future__ import annotations

import json

import ibis
import pandas as pd

from fyrnheim.core.activity import (
    ActivityDefinition,
    EventOccurred,
    FieldChanged,
    RowAppeared,
    RowDisappeared,
)


def apply_activity_definitions(
    raw_events: ibis.Table,
    definitions: list[ActivityDefinition],
) -> ibis.Table:
    """Apply activity definitions to a raw event table and produce a stream of
    annotated + pass-through events.

    For each raw event in the input:
    - If one or more ActivityDefinitions match it, emit one renamed copy per
      matching definition (with the activity's name as event_type and,
      optionally, payload filtered via include_fields).
    - If no ActivityDefinition matches it, emit the raw event unchanged.

    No raw event is ever silently dropped. This is a change from pre-v0.5.0
    behavior, where unmatched events were dropped — see ADR-0001
    (docs/decisions/0001-activity-definitions-drop-vs-preserve.md) for the
    decision rationale.

    Args:
        raw_events: Ibis table with columns: source, entity_id, ts,
            event_type, payload.
        definitions: List of ActivityDefinition instances to apply.

    Returns:
        Ibis table with the same schema. Matched events have their
        event_type replaced by the activity definition name; unmatched
        events pass through with their original event_type and payload.
    """
    events_df = raw_events.execute()

    all_matched: list[dict[str, str]] = []
    matched_indices: set[int] = set()

    for defn in definitions:
        matched, indices = _apply_single_definition(events_df, defn)
        all_matched.extend(matched)
        matched_indices.update(indices)

    # Pass through any raw events that no definition matched.
    passthrough: list[dict[str, str]] = []
    for idx in events_df.index:
        if idx in matched_indices:
            continue
        row = events_df.loc[idx]
        passthrough.append(
            {
                "source": row["source"],
                "entity_id": row["entity_id"],
                "ts": row["ts"],
                "event_type": row["event_type"],
                "payload": row["payload"],
            }
        )

    all_rows = all_matched + passthrough

    if not all_rows:
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

    return ibis.memtable(pd.DataFrame(all_rows))


def _apply_single_definition(
    events_df: pd.DataFrame,
    defn: ActivityDefinition,
) -> tuple[list[dict[str, str]], set[int]]:
    """Apply a single ActivityDefinition to events.

    Returns:
        A tuple of (output rows, set of original DataFrame indices that
        were matched by this definition).
    """
    # Filter by source
    source_mask = events_df["source"] == defn.source
    filtered = events_df[source_mask]

    trigger = defn.trigger

    if isinstance(trigger, RowAppeared):
        matched = filtered[filtered["event_type"] == "row_appeared"]
    elif isinstance(trigger, RowDisappeared):
        matched = filtered[filtered["event_type"] == "row_disappeared"]
    elif isinstance(trigger, FieldChanged):
        matched = _match_field_changed(filtered, trigger)
    elif isinstance(trigger, EventOccurred):
        matched = _match_event_occurred(filtered, trigger)
    else:
        return [], set()

    # Build output events
    results: list[dict[str, str]] = []
    for _idx, row in matched.iterrows():
        payload_str = row["payload"]
        if defn.include_fields:
            payload_str = _filter_payload(payload_str, defn.include_fields)

        results.append(
            {
                "source": row["source"],
                "entity_id": row["entity_id"],
                "ts": row["ts"],
                "event_type": defn.name,
                "payload": payload_str,
            }
        )

    return results, set(matched.index)


def _match_field_changed(
    events: pd.DataFrame,
    trigger: FieldChanged,
) -> pd.DataFrame:
    """Filter field_changed events by trigger criteria."""
    fc_events = events[events["event_type"] == "field_changed"]

    if fc_events.empty:
        return fc_events

    mask = pd.Series(True, index=fc_events.index)

    for idx in fc_events.index:
        payload = json.loads(fc_events.at[idx, "payload"])
        if payload.get("field_name") != trigger.field:
            mask[idx] = False
            continue
        if trigger.to_values is not None:
            if payload.get("new_value") not in trigger.to_values:
                mask[idx] = False
                continue
        if trigger.from_values is not None:
            if payload.get("old_value") not in trigger.from_values:
                mask[idx] = False
                continue

    return fc_events[mask]


def _match_event_occurred(
    events: pd.DataFrame,
    trigger: EventOccurred,
) -> pd.DataFrame:
    """Filter events for EventOccurred trigger.

    EventOccurred matches events from event sources. If trigger.event_type
    is set, only events with that exact event_type match. Otherwise, all
    events from the source match.
    """
    if trigger.event_type is not None:
        return events[events["event_type"] == trigger.event_type]
    return events


def _filter_payload(payload_str: str, include_fields: list[str]) -> str:
    """Filter a JSON payload to only include specified fields."""
    payload = json.loads(payload_str)
    filtered = {k: v for k, v in payload.items() if k in include_fields}
    return json.dumps(filtered)
