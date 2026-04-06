"""Metrics engine for aggregating metric deltas and event counts."""

from __future__ import annotations

import json
from datetime import datetime, timedelta

import ibis
import pandas as pd

from fyrnheim.core.metrics_model import MetricsModel


def _truncate_to_grain(ts: str, grain: str) -> str:
    """Truncate a timestamp string to the specified grain."""
    if grain == "hourly":
        # YYYY-MM-DDTHH -> take first 13 chars
        return ts[:10] + "T" + ts[11:13] if len(ts) >= 13 else ts[:10] + "T00"
    elif grain == "daily":
        return ts[:10]
    elif grain == "weekly":
        # Compute Monday of the week
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

    Supports two families of aggregation:
    - State-based (sum_delta, last_value, max_value): operates on field_changed events,
      extracting old_value/new_value from the payload.
    - Event-based (count, count_distinct): operates on events where event_type matches
      the metric field's field_name.

    Args:
        events: Ibis table with columns: source, entity_id, ts, event_type, payload
        metrics_model: MetricsModel defining the aggregation

    Returns:
        Ibis memtable with _date column plus one column per metric_field.
    """
    # 1. Materialize to pandas
    df = events.execute()

    # Filter to the specified sources
    source_df = df[df["source"].isin(metrics_model.sources)].copy()

    if source_df.empty:
        return _empty_result(metrics_model)

    # 2. Truncate ts to grain
    source_df["_date"] = source_df["ts"].apply(
        lambda t: _truncate_to_grain(str(t), metrics_model.grain)
    )

    # Determine group-by columns
    group_cols = ["_date"]
    if metrics_model.dimensions:
        for dim in metrics_model.dimensions:
            if dim in source_df.columns:
                group_cols.append(dim)

    # Separate metric fields by type
    state_fields = [mf for mf in metrics_model.metric_fields if mf.aggregation in _STATE_AGGREGATIONS]
    event_fields = [mf for mf in metrics_model.metric_fields if mf.aggregation in _EVENT_AGGREGATIONS]

    result_frames: list[pd.DataFrame] = []

    # 3. Process state-based aggregations (field_changed events)
    if state_fields:
        result_frames.extend(_aggregate_state_fields(source_df, state_fields, group_cols))

    # 4. Process event-based aggregations (count/count_distinct)
    if event_fields:
        result_frames.extend(_aggregate_event_fields(source_df, event_fields, group_cols))

    if not result_frames:
        return _empty_result(metrics_model)

    # 5. Merge all metric columns
    merged = result_frames[0]
    for frame in result_frames[1:]:
        merged = merged.merge(frame, on=group_cols, how="outer")

    # Sort by group columns
    merged = merged.sort_values(group_cols).reset_index(drop=True)

    # 6. Return as ibis.memtable
    return ibis.memtable(merged)


def _aggregate_state_fields(
    source_df: pd.DataFrame,
    state_fields: list,
    group_cols: list[str],
) -> list[pd.DataFrame]:
    """Aggregate field_changed events for state-based metrics."""
    fc_df = source_df[source_df["event_type"] == "field_changed"].copy()

    if fc_df.empty:
        return []

    # Parse payload JSON
    def _parse_payload(payload: str) -> dict[str, object]:
        if isinstance(payload, dict):
            return payload
        try:
            result: dict[str, object] = json.loads(payload)
            return result
        except (json.JSONDecodeError, TypeError):
            return {}

    parsed = fc_df["payload"].apply(_parse_payload)
    fc_df["parsed_field_name"] = parsed.apply(lambda p: p.get("field_name", ""))
    fc_df["old_value"] = parsed.apply(lambda p: p.get("old_value", None))
    fc_df["new_value"] = parsed.apply(lambda p: p.get("new_value", None))

    result_frames: list[pd.DataFrame] = []

    for mf in state_fields:
        field_df = fc_df[fc_df["parsed_field_name"] == mf.field_name].copy()

        if field_df.empty:
            continue

        field_df["old_value_f"] = pd.to_numeric(field_df["old_value"], errors="coerce")
        field_df["new_value_f"] = pd.to_numeric(field_df["new_value"], errors="coerce")

        col_name = f"{mf.field_name}_{mf.aggregation}"

        if mf.aggregation == "sum_delta":
            field_df["_delta"] = field_df["new_value_f"] - field_df["old_value_f"]
            agg_df = field_df.groupby(group_cols, as_index=False).agg(
                **{col_name: ("_delta", "sum")}
            )
        elif mf.aggregation == "last_value":
            idx = field_df.groupby(group_cols)["ts"].transform("max") == field_df["ts"]
            last_df = field_df[idx].copy()
            last_df = last_df.drop_duplicates(subset=group_cols, keep="last")
            agg_df = last_df[group_cols + ["new_value_f"]].rename(
                columns={"new_value_f": col_name}
            )
        elif mf.aggregation == "max_value":
            agg_df = field_df.groupby(group_cols, as_index=False).agg(
                **{col_name: ("new_value_f", "max")}
            )
        else:
            continue

        result_frames.append(agg_df)

    return result_frames


def _aggregate_event_fields(
    source_df: pd.DataFrame,
    event_fields: list,
    group_cols: list[str],
) -> list[pd.DataFrame]:
    """Aggregate event-type matching events for count-based metrics."""
    result_frames: list[pd.DataFrame] = []

    def _parse_payload(payload: str) -> dict[str, object]:
        if isinstance(payload, dict):
            return payload
        try:
            result: dict[str, object] = json.loads(payload)
            return result
        except (json.JSONDecodeError, TypeError):
            return {}

    for mf in event_fields:
        # Match events where event_type == field_name
        event_df = source_df[source_df["event_type"] == mf.field_name].copy()

        if event_df.empty:
            continue

        col_name = f"{mf.field_name}_{mf.aggregation}"

        if mf.aggregation == "count":
            agg_df = event_df.groupby(group_cols, as_index=False).size()
            agg_df = agg_df.rename(columns={"size": col_name})
        elif mf.aggregation == "count_distinct":
            # Extract the distinct_field from payload
            parsed = event_df["payload"].apply(_parse_payload)
            event_df["_distinct_val"] = parsed.apply(
                lambda p, key=mf.distinct_field: p.get(key, None)
            )
            agg_df = event_df.groupby(group_cols, as_index=False).agg(
                **{col_name: ("_distinct_val", "nunique")}
            )
        else:
            continue

        result_frames.append(agg_df)

    return result_frames


def _empty_result(metrics_model: MetricsModel) -> ibis.expr.types.Table:
    """Create an empty result table with the correct schema."""
    cols: dict[str, list] = {"_date": []}
    for dim in metrics_model.dimensions:
        cols[dim] = []
    for mf in metrics_model.metric_fields:
        cols[f"{mf.field_name}_{mf.aggregation}"] = []
    return ibis.memtable(pd.DataFrame(cols))
