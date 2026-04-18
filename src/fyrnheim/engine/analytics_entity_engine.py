"""Projection engine for AnalyticsEntity: state fields + activity-derived measures.

M060 — Ibis push-down rewrite. The heart of this module,
:func:`project_analytics_entity`, returns an Ibis expression composed of
``filter`` -> ``group_by`` -> ``aggregate`` over JSON payload extracts.
The backend (DuckDB or BigQuery) executes the projection server-side; only
the small per-group aggregation result is ever materialised on the client.

## JSON-extract portability (spike notes)

The JSON payload arrives as a string column. We use Ibis's JSON primitives
to extract scalar fields portably:

* ``col.cast("json")["key"].unwrap_as("string")`` compiles to
  ``CASE WHEN JSON_TYPE(...) = 'VARCHAR' THEN ... ->> '$' ELSE NULL END``
  on DuckDB, and to ``SAFE.STRING(CAST(... AS JSON)['key'])`` on BigQuery.
* ``col.cast("json")["key"].unwrap_as("float64")`` compiles to the
  equivalent numeric coercion — both backends return ``NULL`` on type
  mismatch, matching the legacy pandas ``try/except float(val)`` behaviour.

These primitives have been confirmed to compile successfully for both
DuckDB and BigQuery dialects. No backend-specific branch is required.

## ``field_changed`` event shape

``_extract_field_value`` in the legacy pandas implementation branched on
``event_type``:

* ``row_appeared`` -> ``payload[field_name]``
* ``field_changed`` -> ``payload.new_value`` iff ``payload.field_name == field_name``
* anything else -> ``payload[field_name]`` (flat lookup fallback)

In Ibis this is a per-event ``CASE WHEN`` producing a ``resolved_value``
sub-expression consumed by the aggregation — see ``_resolved_value_for_field``.

## ``computed_fields``

Still evaluated in Python (``eval``). After the aggregation expression is
built, if any computed fields exist we ``.execute()`` the (now small)
result, apply the computed fields in pandas, and wrap the frame back in
``ibis.memtable``. The heavy grouping work still pushes down.
"""

from __future__ import annotations

from typing import Any

import ibis
import pandas as pd
from ibis.expr.types import BooleanValue, StringValue, Table

from fyrnheim.core.analytics_entity import AnalyticsEntity, Measure, StateField


class _RowProxy:
    """Proxy object that allows ``t.field_name`` access on a dict row.

    Supports expressions like ``t.email.split('@')[1].lower()`` by returning
    the actual value from the row dict on attribute access. Retained for
    the Python-evaluated ``computed_fields`` path.
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


# ---------------------------------------------------------------------------
# Ibis expression helpers
# ---------------------------------------------------------------------------


def _payload_json(events: Table) -> Any:
    """Cast the raw payload string column to Ibis JSON."""
    return events.payload.cast("json")


def _extract_as_string(events: Table, field: str) -> StringValue:
    """Extract ``payload[field]`` as a string scalar, NULL on type mismatch."""
    return _payload_json(events)[field].unwrap_as("string")


def _extract_as_float(events: Table, field: str) -> Any:
    """Extract ``payload[field]`` as a float scalar, NULL on non-numeric."""
    return _payload_json(events)[field].unwrap_as("float64")


def _resolved_value_for_field(events: Table, field: str) -> StringValue:
    """Per-event ``resolved_value`` CASE WHEN matching ``_extract_field_value``.

    * ``event_type == 'field_changed' AND payload.field_name == <field>`` ->
      ``payload.new_value`` as string
    * otherwise -> ``payload[<field>]`` as string

    Both the ``row_appeared`` branch and the "anything else" fallback in the
    legacy code use the flat payload lookup, so they collapse into the
    ELSE branch here.
    """
    pj = _payload_json(events)
    field_name_match = (
        (events.event_type == "field_changed")
        & (pj["field_name"].unwrap_as("string") == field)
    )
    new_value = pj["new_value"].unwrap_as("string")
    fallback = pj[field].unwrap_as("string")
    return ibis.cases((field_name_match, new_value), else_=fallback)


def _latest_non_null(
    events: Table, resolved: StringValue, where: BooleanValue | None = None
) -> StringValue:
    """Latest non-null ``resolved`` value by ``events.ts``.

    Implemented as ``argmax(resolved, ts, where=<non-null AND extra>)``.
    When no matching non-null rows exist for a group, the aggregate yields
    NULL — matching ``_resolve_latest`` returning ``None``.
    """
    cond = resolved.notnull()
    if where is not None:
        cond = cond & where
    return resolved.argmax(events.ts, where=cond)


def _first_non_null(
    events: Table, resolved: StringValue, where: BooleanValue | None = None
) -> StringValue:
    """Earliest non-null ``resolved`` value by ``events.ts`` (argmin)."""
    cond = resolved.notnull()
    if where is not None:
        cond = cond & where
    return resolved.argmin(events.ts, where=cond)


def _state_field_expr(events: Table, sf: StateField) -> Any:
    """Build the aggregation expression for a single state field."""
    if sf.strategy == "latest":
        resolved = _resolved_value_for_field(events, sf.field)
        return _latest_non_null(events, resolved, where=events.source == sf.source)
    if sf.strategy == "first":
        resolved = _resolved_value_for_field(events, sf.field)
        return _first_non_null(events, resolved, where=events.source == sf.source)
    if sf.strategy == "coalesce":
        # Nested coalesce(latest_from(source[0]), latest_from(source[1]), ...).
        # Each arm is a per-source latest aggregation. Because each arm
        # filters on source=X, argmax ignores rows from other sources.
        resolved = _resolved_value_for_field(events, sf.field)
        per_source = [
            _latest_non_null(events, resolved, where=events.source == src)
            for src in (sf.priority or [])
        ]
        if not per_source:
            return ibis.null().cast("string")
        if len(per_source) == 1:
            return per_source[0]
        return ibis.coalesce(*per_source)
    # pragma: no cover — StateField.strategy is a Literal of the three above
    return ibis.null().cast("string")


def _measure_expr(events: Table, measure: Measure) -> Any:
    """Build the aggregation expression for a single measure."""
    is_activity = events.event_type == measure.activity

    if measure.aggregation == "count":
        # Count rows matching the activity. Groups with no match emit 0.
        return events.count(where=is_activity)

    if measure.aggregation == "sum":
        # Sum payload[field] as float, filtered to activity rows. Non-numeric
        # values coerce to NULL via ``unwrap_as("float64")`` and are ignored
        # by SUM. Empty groups emit 0.0 to match the legacy pandas branch.
        field = measure.field  # Pydantic guarantees this for sum
        assert field is not None
        amount = _extract_as_float(events, field)
        return ibis.coalesce(amount.sum(where=is_activity), 0.0)

    if measure.aggregation == "latest":
        # Latest non-null payload[field] among activity rows. NULL if none.
        #
        # Measure `latest` is typically used for numeric carry-forward
        # semantics (e.g. "last purchase amount"). We extract as float64 via
        # ``unwrap_as('float64')`` — this returns NULL for non-numeric values
        # and for missing keys, matching the legacy pandas
        # ``_extract_payload_field`` returning ``None``. The legacy pandas
        # impl returned the raw JSON-parsed Python value (int/float/str)
        # via ``object`` dtype; SQL requires a fixed type. Float is the
        # predominant case and compares equal (``250.0 == 250`` in Python)
        # to the legacy-returned int.
        field = measure.field
        assert field is not None
        amount = _extract_as_float(events, field)
        return _latest_non_null(events, amount, where=is_activity)

    # pragma: no cover — Measure.aggregation is a Literal
    return ibis.null()


def _relevance_filter(
    events: Table, analytics_entity: AnalyticsEntity
) -> Table:
    """Apply the M057 relevance filter as an Ibis ``.filter()``.

    An event is relevant if its ``source`` is referenced by any state field
    (including every source in a coalesce priority list) OR its
    ``event_type`` matches any measure activity. When both sets are empty
    (defensive; Pydantic requires at least one state_field or measure)
    no filter is applied.
    """
    relevant_sources: set[str] = set()
    for sf in analytics_entity.state_fields:
        if sf.strategy == "coalesce":
            relevant_sources.update(sf.priority or [])
        else:
            relevant_sources.add(sf.source)
    relevant_activities = {m.activity for m in analytics_entity.measures}

    if not relevant_sources and not relevant_activities:
        return events

    # Build the predicate as source.isin(...) | event_type.isin(...).
    # ``isin`` over an empty collection evaluates to false, so an entity with
    # no referenced sources still correctly reduces to event_type matching.
    sources_list = sorted(relevant_sources)
    activities_list = sorted(relevant_activities)

    if sources_list and activities_list:
        pred = events.source.isin(sources_list) | events.event_type.isin(
            activities_list
        )
    elif sources_list:
        pred = events.source.isin(sources_list)
    else:
        pred = events.event_type.isin(activities_list)

    return events.filter(pred)


def _build_aggregation_expression(
    enriched_events: Table, analytics_entity: AnalyticsEntity
) -> Table:
    """Build the full filter -> group_by -> aggregate Ibis expression.

    The returned expression produces one row per group with columns
    [group_key, *state_field.name, *measure.name]. ``computed_fields`` are
    NOT applied here.
    """
    filtered = _relevance_filter(enriched_events, analytics_entity)

    group_key = (
        "canonical_id" if "canonical_id" in filtered.schema().names else "entity_id"
    )

    agg_kwargs: dict[str, Any] = {}
    for sf in analytics_entity.state_fields:
        agg_kwargs[sf.name] = _state_field_expr(filtered, sf)
    for m in analytics_entity.measures:
        agg_kwargs[m.name] = _measure_expr(filtered, m)

    return filtered.group_by(group_key).aggregate(**agg_kwargs)


def project_analytics_entity(
    enriched_events: Table,
    analytics_entity: AnalyticsEntity,
) -> Table:
    """Project one row per entity from enriched events using an AnalyticsEntity.

    Returns an Ibis expression composed of ``filter`` -> ``group_by`` ->
    ``aggregate`` over JSON payload extraction. When the entity declares
    ``computed_fields``, the aggregation is executed and the (small)
    resulting DataFrame is post-processed in Python (``eval``) before being
    wrapped back in an ``ibis.memtable`` — the heavy grouping still pushes
    down to the backend.

    Args:
        enriched_events: Ibis table with columns ``source``, ``entity_id``,
            ``ts``, ``event_type``, ``payload``, and optionally
            ``canonical_id``.
        analytics_entity: :class:`AnalyticsEntity` defining state fields,
            measures, and computed fields.

    Returns:
        Ibis ``Table`` with one row per entity plus all state, measure, and
        computed field columns.
    """
    agg_expr = _build_aggregation_expression(enriched_events, analytics_entity)

    if not analytics_entity.computed_fields:
        # Empty-input path: the aggregation naturally yields zero rows with
        # the right schema, so callers that .execute() see an empty frame
        # with [group_key, state_fields, measures] columns. The M057
        # scoping tests exercise this path.
        return agg_expr

    # computed_fields require Python eval — materialise the small
    # aggregation result, apply, and wrap back.
    df = agg_expr.execute()

    # Preserve exact column order used by the legacy implementation:
    # group_key, state_fields, measures, computed_fields.
    group_key = (
        "canonical_id"
        if "canonical_id" in enriched_events.schema().names
        else "entity_id"
    )
    ordered_cols = (
        [group_key]
        + [sf.name for sf in analytics_entity.state_fields]
        + [m.name for m in analytics_entity.measures]
    )
    df = df[ordered_cols]

    if df.empty:
        # Pre-allocate computed-field columns so the empty frame has the
        # right shape even when there are no rows to apply over.
        for cf in analytics_entity.computed_fields:
            df[cf.name] = pd.Series([], dtype=object)
        return ibis.memtable(df)

    for cf in analytics_entity.computed_fields:
        df[cf.name] = df.apply(
            lambda r, expr=cf.expression: eval(  # noqa: S307
                expr,
                {"__builtins__": {}},
                {**r.to_dict(), "t": _RowProxy(r.to_dict())},
            ),
            axis=1,
        )

    return ibis.memtable(df)
