"""Activity engine for applying ActivityDefinitions to raw event streams.

Filters raw events by activity definitions and produces named business events.
"""

from __future__ import annotations

import json
from functools import reduce

import ibis

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
    """Apply activity definitions to a raw event table.

    The implementation is expression-based: filtering, event renaming, payload
    projection, pass-through, and unioning compose as Ibis expressions so
    warehouse backends can execute the activity phase server-side.
    """
    if not definitions:
        return _canonical_event_table(raw_events)

    matched_tables: list[ibis.Table] = []
    match_predicates: list[ibis.Value] = []

    for definition in definitions:
        predicate = _definition_predicate(raw_events, definition).fill_null(False)
        match_predicates.append(predicate)
        matched_tables.append(_activity_projection(raw_events, definition, predicate))

    any_matched = reduce(lambda left, right: left | right, match_predicates)
    passthrough = _canonical_event_table(raw_events.filter(~any_matched))
    return ibis.union(*matched_tables, passthrough, distinct=False)


def _canonical_event_table(table: ibis.Table) -> ibis.Table:
    """Select the canonical event schema in a stable column order."""
    return table.select(
        source=table["source"].cast("string"),
        entity_id=table["entity_id"].cast("string"),
        ts=table["ts"].cast("string"),
        event_type=table["event_type"].cast("string"),
        payload=table["payload"].cast("string"),
    )


def _definition_predicate(
    events: ibis.Table,
    definition: ActivityDefinition,
) -> ibis.Value:
    """Build the backend predicate for one activity definition."""
    trigger = definition.trigger
    predicate = events["source"] == definition.source

    if isinstance(trigger, RowAppeared):
        return predicate & (events["event_type"] == "row_appeared")
    if isinstance(trigger, RowDisappeared):
        return predicate & (events["event_type"] == "row_disappeared")
    if isinstance(trigger, FieldChanged):
        predicate = predicate & (events["event_type"] == "field_changed")
        payload = events["payload"]
        predicate = predicate & (_json_scalar(payload, "field_name") == trigger.field)
        if trigger.to_values is not None:
            predicate = predicate & _json_scalar(payload, "new_value").isin(
                trigger.to_values
            )
        if trigger.from_values is not None:
            predicate = predicate & _json_scalar(payload, "old_value").isin(
                trigger.from_values
            )
        return predicate
    if isinstance(trigger, EventOccurred):
        if trigger.event_type is not None:
            return predicate & (events["event_type"] == trigger.event_type)
        return predicate
    return ibis.literal(False)


def _activity_projection(
    events: ibis.Table,
    definition: ActivityDefinition,
    predicate: ibis.Value,
) -> ibis.Table:
    """Project matched rows into named activity events."""
    matched = events.filter(predicate)
    return matched.select(
        source=matched["source"].cast("string"),
        entity_id=matched["entity_id"].cast("string"),
        ts=matched["ts"].cast("string"),
        event_type=ibis.literal(definition.name).cast("string"),
        payload=_project_payload(matched["payload"], definition.include_fields),
    )


def _project_payload(payload: ibis.Value, include_fields: list[str]) -> ibis.Value:
    """Return an expression for an activity payload.

    Empty ``include_fields`` preserves the original payload. Non-empty lists
    build a JSON object from the requested payload keys.
    """
    if not include_fields:
        return payload.cast("string")
    json_payload = payload.cast("json")
    return ibis.struct(
        {field: json_payload[field] for field in include_fields}
    ).cast("json").cast("string")


def _json_scalar(payload: ibis.Value, field_name: str) -> ibis.Value:
    """Extract a scalar JSON payload field as a string expression."""
    return payload.cast("json")[field_name].unwrap_as("string")


def _filter_payload(payload_str: str, include_fields: list[str]) -> str:
    """Filter a JSON payload to only include specified fields.

    Kept as a tiny compatibility helper for callers that imported the private
    function directly; the activity engine no longer uses it internally.
    """
    payload = json.loads(payload_str)
    filtered = {k: v for k, v in payload.items() if k in include_fields}
    return json.dumps(filtered)
