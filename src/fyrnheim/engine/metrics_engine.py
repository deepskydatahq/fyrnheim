"""Metrics engine for aggregating metric deltas and event counts."""

from __future__ import annotations

from datetime import datetime, timedelta
from functools import reduce

import ibis
import pandas as pd

from fyrnheim.core.metrics_model import MetricField, MetricsModel


def _truncate_to_grain(ts: str, grain: str) -> str:
    """Truncate a timestamp string to the specified grain."""
    if grain == "hourly":
        return ts[:10] + "T" + ts[11:13] if len(ts) >= 13 else ts[:10] + "T00"
    elif grain == "daily":
        return ts[:10]
    elif grain == "weekly":
        dt = datetime.fromisoformat(ts[:19])
        monday = dt - timedelta(days=dt.weekday())
        return monday.strftime("%Y-%m-%d")
    elif grain == "monthly":
        return ts[:7] + "-01"
    else:
        return ts[:10]


_STATE_AGGREGATIONS = {"sum_delta", "last_value", "max_value"}
_EVENT_AGGREGATIONS = {"count", "count_distinct"}


def aggregate_metrics(
    events: ibis.expr.types.Table, metrics_model: MetricsModel
) -> ibis.expr.types.Table:
    """Aggregate events into time-grain metric tables.

    Builds a backend-executable Ibis group-by expression. The large event
    stream is not materialized locally; only the caller's final ``execute()``
    fetches the aggregated result rows.
    """
    source_events = events.filter(events["source"].isin(metrics_model.sources))
    grouped_events = source_events.mutate(_date=_date_grain(source_events["ts"], metrics_model.grain))
    group_cols = ["_date", *[dim for dim in metrics_model.dimensions if dim in grouped_events.columns]]

    relevant_predicates = [_metric_predicate(grouped_events, mf) for mf in metrics_model.metric_fields]
    any_relevant = reduce(lambda left, right: left | right, relevant_predicates).fill_null(False)
    grouped_events = grouped_events.filter(any_relevant)

    aggregations: dict[str, ibis.Value] = {}
    for metric_field in metrics_model.metric_fields:
        aggregations[f"{metric_field.field_name}_{metric_field.aggregation}"] = _metric_aggregation(
            grouped_events, metric_field
        )

    return grouped_events.group_by(group_cols).agg(**aggregations).order_by(group_cols)


def _date_grain(ts: ibis.Value, grain: str) -> ibis.Value:
    """Return a string date-grain expression matching legacy output shapes."""
    timestamp = ts.cast("timestamp")
    if grain == "hourly":
        return timestamp.strftime("%Y-%m-%dT%H")
    if grain == "daily":
        return timestamp.strftime("%Y-%m-%d")
    if grain == "weekly":
        return timestamp.truncate("week").strftime("%Y-%m-%d")
    if grain == "monthly":
        return timestamp.truncate("month").strftime("%Y-%m-%d")
    return timestamp.strftime("%Y-%m-%d")


def _metric_predicate(events: ibis.Table, metric_field: MetricField) -> ibis.Value:
    if metric_field.aggregation in _STATE_AGGREGATIONS:
        return (
            (events["event_type"] == "field_changed")
            & (_json_scalar(events["payload"], "field_name") == metric_field.field_name)
        ).fill_null(False)
    if metric_field.aggregation in _EVENT_AGGREGATIONS:
        return (events["event_type"] == metric_field.field_name).fill_null(False)
    return ibis.literal(False)


def _metric_aggregation(events: ibis.Table, metric_field: MetricField) -> ibis.Value:
    predicate = _metric_predicate(events, metric_field)
    col_name = metric_field.aggregation

    if col_name == "sum_delta":
        delta = _json_number(events["payload"], "new_value") - _json_number(
            events["payload"], "old_value"
        )
        return delta.sum(where=predicate)
    if col_name == "last_value":
        return _json_number(events["payload"], "new_value").argmax(
            events["ts"].cast("timestamp"), where=predicate
        )
    if col_name == "max_value":
        return _json_number(events["payload"], "new_value").max(where=predicate)
    if col_name == "count":
        count = events["event_type"].count(where=predicate)
        return ibis.ifelse(count > 0, count, ibis.null().cast("int64"))
    if col_name == "count_distinct":
        if metric_field.distinct_field is None:
            return ibis.literal(0)
        return _json_scalar(events["payload"], metric_field.distinct_field).nunique(
            where=predicate
        )
    return ibis.literal(None)


def _payload_json(payload: ibis.Value) -> ibis.Value:
    return payload.cast("json")


def _json_scalar(payload: ibis.Value, field_name: str) -> ibis.Value:
    return _payload_json(payload)[field_name].unwrap_as("string")


def _json_number(payload: ibis.Value, field_name: str) -> ibis.Value:
    value = _payload_json(payload)[field_name]
    return value.unwrap_as("float64").coalesce(
        value.unwrap_as("string").try_cast("float64"),
        value.unwrap_as("bool").cast("float64"),
    )


def _empty_result(metrics_model: MetricsModel) -> ibis.expr.types.Table:
    """Create an empty result table with the correct schema.

    Kept for compatibility with private imports; aggregate_metrics now returns
    an empty backend expression naturally when no rows match.
    """
    cols: dict[str, list] = {"_date": []}
    for dim in metrics_model.dimensions:
        cols[dim] = []
    for mf in metrics_model.metric_fields:
        cols[f"{mf.field_name}_{mf.aggregation}"] = []
    return ibis.memtable(pd.DataFrame(cols))
