"""Projection engine for AnalyticsEntity: state fields + activity-derived measures.

M060 — Ibis push-down rewrite. The heart of this module,
:func:`project_analytics_entity`, returns an Ibis expression composed of
``filter`` -> ``group_by`` -> ``aggregate`` over JSON payload extracts.
The backend (DuckDB or BigQuery) executes the projection server-side; only
the small per-group aggregation result is ever materialised on the client.

## JSON-extract portability

The JSON payload arrives as a string column. We use Ibis's JSON primitives
to extract scalar fields portably:

* ``col.cast("json")["key"].unwrap_as("string")`` — extracts the *unquoted*
  string value of a JSON string scalar, returning NULL for non-string JSON
  scalars (numbers, booleans, JSON null). Compiles to a typed projection on
  both DuckDB and BigQuery.
* ``col.cast("json")["key"].try_cast("string")`` — returns the raw JSON
  text of the scalar (``"hello"`` with quotes, ``30``, ``true``, ``null``).
  Portable: compiles to ``TRY_CAST(... AS TEXT)`` on DuckDB and
  ``SAFE_CAST(... AS STRING)`` on BigQuery. Used for **type-preserving**
  extraction — the text is parsed with ``json.loads`` client-side to
  restore the original Python scalar type (str / int / float / bool).
* ``col.cast("json")["key"].unwrap_as("float64")`` — numeric coercion;
  returns NULL for string or boolean JSON scalars. For ``sum`` parity with
  the legacy pandas ``float(val)`` behaviour, numeric-strings (e.g.
  ``{"amount": "250"}``) are recovered via a ``coalesce`` with
  ``unwrap_as("string").try_cast("float64")``.

## Type preservation for state fields and the ``latest`` measure

Legacy pandas returned the raw JSON-decoded Python value for state fields
(``latest``/``first``/``coalesce``) and the ``latest`` measure. SQL columns
are fixed-type, so we push down the aggregation producing **JSON text**
(via ``try_cast("string")``) and parse each value back to its original
Python type on the client after ``.execute()``. This preserves the
mixed-type contract (e.g. ``{"age": 30}`` → Python ``int`` 30,
``{"is_active": true}`` → Python ``bool`` True, ``{"plan": "pro"}`` →
Python ``str`` "pro"). The heavy grouping work still pushes down to the
backend — only the one-row-per-group result is post-processed.

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

import json
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
    """Extract ``payload[field]`` as a float scalar, NULL on non-numeric.

    For parity with the legacy pandas ``sum`` aggregation (which accepted
    numeric strings via ``float(val)``), numeric-string values are recovered
    by coalescing with ``unwrap_as("string").try_cast("float64")``.
    """
    pj = _payload_json(events)[field]
    direct = pj.unwrap_as("float64")
    from_string = pj.unwrap_as("string").try_cast("float64")
    return ibis.coalesce(direct, from_string)


def _extract_as_json_text(events: Table, field: str) -> StringValue:
    """Extract ``payload[field]`` as raw JSON text, preserving scalar type.

    Uses ``try_cast("string")`` on the JSON value: produces quoted string
    text (``"hello"``), numeric text (``30``), boolean text (``true``), or
    (on DuckDB) the text ``"null"`` for JSON null values. Missing keys
    produce SQL NULL. The caller parses the text back with ``json.loads``
    to recover the original Python scalar type.
    """
    return _payload_json(events)[field].try_cast("string")


def _is_non_null_json_text(json_text: StringValue) -> BooleanValue:
    """Predicate: the JSON-text value is neither SQL NULL nor JSON ``null``.

    On DuckDB, ``TRY_CAST(JSON 'null' AS TEXT)`` returns the string
    ``'null'``; on BigQuery, ``SAFE_CAST(JSON 'null' AS STRING)`` returns
    SQL NULL. This predicate handles both cases uniformly so downstream
    aggregations skip JSON null values (matching legacy behaviour of
    ignoring ``None`` values in the pandas loop).
    """
    return json_text.notnull() & (json_text != "null")


def _resolved_value_for_field(events: Table, field: str) -> StringValue:
    """Per-event ``resolved_value`` CASE WHEN matching ``_extract_field_value``.

    Returns the JSON-text representation of the resolved value, to be parsed
    back to the original Python scalar type on the client.

    * ``event_type == 'field_changed' AND payload.field_name == <field>`` ->
      ``payload.new_value`` as JSON text
    * otherwise -> ``payload[<field>]`` as JSON text

    Both the ``row_appeared`` branch and the "anything else" fallback in the
    legacy code use the flat payload lookup, so they collapse into the
    ELSE branch here.
    """
    pj = _payload_json(events)
    field_name_match = (
        (events.event_type == "field_changed")
        & (pj["field_name"].unwrap_as("string") == field)
    )
    new_value = pj["new_value"].try_cast("string")
    fallback = pj[field].try_cast("string")
    return ibis.cases((field_name_match, new_value), else_=fallback)


def _latest_non_null(
    events: Table, resolved: StringValue, where: BooleanValue | None = None
) -> StringValue:
    """Latest non-null ``resolved`` value by ``events.ts``.

    ``resolved`` is assumed to be the JSON-text representation, so
    "non-null" means both SQL-NOT-NULL and JSON-value-not-null (see
    :func:`_is_non_null_json_text`).
    """
    cond = _is_non_null_json_text(resolved)
    if where is not None:
        cond = cond & where
    return resolved.argmax(events.ts, where=cond)


def _first_non_null(
    events: Table, resolved: StringValue, where: BooleanValue | None = None
) -> StringValue:
    """Earliest non-null ``resolved`` value by ``events.ts`` (argmin)."""
    cond = _is_non_null_json_text(resolved)
    if where is not None:
        cond = cond & where
    return resolved.argmin(events.ts, where=cond)


def _state_field_expr(events: Table, sf: StateField) -> Any:
    """Build the aggregation expression for a single state field.

    Produces a JSON-text column — the caller parses it back to a Python
    scalar (see :func:`_parse_json_text_column`).
    """
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
        # Sum payload[field] as float, filtered to activity rows. Uses
        # ``_extract_as_float`` which coalesces a direct float unwrap with a
        # string-to-float try_cast — so numeric-strings (e.g.
        # ``{"amount": "250"}``) are included for parity with the legacy
        # pandas ``float(val)`` behaviour. Truly non-numeric values still
        # coerce to NULL and are ignored by SUM. Empty groups emit 0.0.
        field = measure.field  # Pydantic guarantees this for sum
        assert field is not None
        amount = _extract_as_float(events, field)
        return ibis.coalesce(amount.sum(where=is_activity), 0.0)

    if measure.aggregation == "latest":
        # Latest non-null payload[field] among activity rows, returned as
        # JSON text. The caller parses it back to its original Python type
        # (int/float/str/bool) to match the legacy pandas semantics of
        # returning the raw JSON-decoded value.
        field = measure.field
        assert field is not None
        resolved = _extract_as_json_text(events, field)
        return _latest_non_null(events, resolved, where=is_activity)

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


def _parse_json_text_cell(value: Any) -> Any:
    """Parse a JSON-text scalar back to its Python type.

    ``None`` / ``NaN`` pass through unchanged. A malformed value (should
    not occur for data that originated as a valid JSON payload scalar) is
    returned as-is to keep the engine forgiving.
    """
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if not isinstance(value, str):
        # Some backends may already return a typed value — pass through.
        return value
    try:
        return json.loads(value)
    except (ValueError, TypeError):
        return value


def _parse_json_text_columns(
    df: pd.DataFrame, columns: list[str]
) -> pd.DataFrame:
    """Apply :func:`_parse_json_text_cell` to the given JSON-text columns.

    Operates in place on a copy and returns the updated frame. Columns
    missing from ``df`` (e.g. on an empty aggregation result) are ignored.
    """
    out = df.copy()
    for col in columns:
        if col in out.columns:
            out[col] = out[col].map(_parse_json_text_cell).astype(object)
    return out


def project_analytics_entity(
    enriched_events: Table,
    analytics_entity: AnalyticsEntity,
) -> Table:
    """Project one row per entity from enriched events using an AnalyticsEntity.

    Returns an Ibis expression composed of ``filter`` -> ``group_by`` ->
    ``aggregate`` over JSON payload extraction. State fields and the
    ``latest`` measure are pushed down as JSON-text extracts and parsed
    back to Python scalars on the client (preserving mixed int/float/str/
    bool types). When the entity declares ``computed_fields``, the
    post-processed frame is additionally passed through ``eval`` for each
    computed column. In all cases the heavy grouping work pushes down to
    the backend — only the one-row-per-group result is materialised.

    Args:
        enriched_events: Ibis table with columns ``source``, ``entity_id``,
            ``ts``, ``event_type``, ``payload``, and optionally
            ``canonical_id``.
        analytics_entity: :class:`AnalyticsEntity` defining state fields,
            measures, and computed fields.

    Returns:
        Ibis ``Table`` (an ``ibis.memtable``) with one row per entity plus
        all state, measure, and computed field columns. State and
        ``latest``-measure columns carry Python-scalar values (object
        dtype) to match the legacy pandas contract.
    """
    agg_expr = _build_aggregation_expression(enriched_events, analytics_entity)

    # Columns that were pushed down as JSON text and need client-side parsing.
    json_text_columns = [sf.name for sf in analytics_entity.state_fields] + [
        m.name for m in analytics_entity.measures if m.aggregation == "latest"
    ]

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

    df = agg_expr.execute()
    if not df.empty:
        df = df[ordered_cols]
        df = _parse_json_text_columns(df, json_text_columns)
    else:
        # Preserve column set even when the aggregation yielded zero rows.
        df = df.reindex(columns=ordered_cols)

    if not analytics_entity.computed_fields:
        return ibis.memtable(df)

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
