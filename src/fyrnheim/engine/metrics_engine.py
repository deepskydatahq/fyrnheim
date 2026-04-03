"""Metrics engine for aggregating metric deltas from field_changed events."""

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


def aggregate_metrics(
    events: ibis.expr.types.Table, metrics_model: MetricsModel
) -> ibis.expr.types.Table:
    """Aggregate field_changed events into time-grain metric tables.

    Args:
        events: Ibis table with columns: source, entity_id, ts, event_type, payload
        metrics_model: MetricsModel defining the aggregation

    Returns:
        Ibis memtable with _date column plus one column per metric_field.
    """
    # 1. Materialize to pandas
    df = events.execute()

    # 2. Filter to field_changed events for the specified sources
    df = df[
        (df["event_type"] == "field_changed")
        & (df["source"].isin(metrics_model.sources))
    ].copy()

    if df.empty:
        # Return empty table with correct schema
        return _empty_result(metrics_model)

    # 3. Parse payload JSON
    def _parse_payload(payload: str) -> dict:
        if isinstance(payload, dict):
            return payload
        try:
            return json.loads(payload)
        except (json.JSONDecodeError, TypeError):
            return {}

    parsed = df["payload"].apply(_parse_payload)
    df["parsed_field_name"] = parsed.apply(lambda p: p.get("field_name", ""))
    df["old_value"] = parsed.apply(lambda p: p.get("old_value", None))
    df["new_value"] = parsed.apply(lambda p: p.get("new_value", None))

    # 4. Truncate ts to grain
    df["_date"] = df["ts"].apply(lambda t: _truncate_to_grain(str(t), metrics_model.grain))

    # Determine group-by columns
    group_cols = ["_date"]
    if metrics_model.dimensions:
        for dim in metrics_model.dimensions:
            if dim in df.columns:
                group_cols.append(dim)

    # 5. Compute each metric field
    result_frames: list[pd.DataFrame] = []

    for mf in metrics_model.metric_fields:
        field_df = df[df["parsed_field_name"] == mf.field_name].copy()

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
            # Get the row with max ts per group
            idx = field_df.groupby(group_cols)["ts"].transform("max") == field_df["ts"]
            last_df = field_df[idx].copy()
            # If there are ties, take the last one
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

    if not result_frames:
        return _empty_result(metrics_model)

    # 6. Merge all metric columns
    merged = result_frames[0]
    for frame in result_frames[1:]:
        merged = merged.merge(frame, on=group_cols, how="outer")

    # Sort by _date
    merged = merged.sort_values(group_cols).reset_index(drop=True)

    # 7. Return as ibis.memtable
    return ibis.memtable(merged)


def _empty_result(metrics_model: MetricsModel) -> ibis.expr.types.Table:
    """Create an empty result table with the correct schema."""
    cols: dict[str, list] = {"_date": []}
    for dim in metrics_model.dimensions:
        cols[dim] = []
    for mf in metrics_model.metric_fields:
        cols[f"{mf.field_name}_{mf.aggregation}"] = []
    return ibis.memtable(pd.DataFrame(cols))
