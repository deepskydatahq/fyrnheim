"""Analytics aggregation engine for stream analytics models.

Aggregates enriched activity stream events into time-grain metrics
grouped by date and optional dimensions.
"""

from __future__ import annotations

import json
from datetime import datetime

import ibis
import pandas as pd

from fyrnheim.core.analytics_model import StreamAnalyticsModel, StreamMetric


def _truncate_date(date_str: str, grain: str) -> str:
    """Truncate a date string to the given grain.

    Args:
        date_str: Date string in YYYY-MM-DD format.
        grain: One of 'daily', 'weekly', 'monthly'.

    Returns:
        Truncated date string in YYYY-MM-DD format.
    """
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    if grain == "daily":
        return date_str
    elif grain == "weekly":
        # Truncate to Monday of that week
        monday = dt - pd.Timedelta(days=dt.weekday())
        return monday.strftime("%Y-%m-%d")
    elif grain == "monthly":
        return dt.strftime("%Y-%m-01")
    else:
        raise ValueError(f"Unsupported date grain: {grain}")


def _compute_metric(
    df: pd.DataFrame, metric: StreamMetric, group_cols: list[str]
) -> pd.DataFrame:
    """Compute a single metric over grouped data.

    Args:
        df: DataFrame with enriched events (already date-truncated).
        metric: The StreamMetric to compute.
        group_cols: Columns to group by (_date + dimensions).

    Returns:
        DataFrame with group_cols and the metric column.
    """
    filtered = df
    if metric.event_filter:
        filtered = df[df["event_type"] == metric.event_filter]

    if metric.metric_type == "count":
        result = filtered.groupby(group_cols, as_index=False).size()
        result = result.rename(columns={"size": metric.name})
    elif metric.metric_type == "sum":
        # Parse payload JSON and extract the field named in expression
        field_name = metric.expression

        def _extract_field(payload: str) -> float:
            try:
                data = json.loads(payload)
                return float(data.get(field_name, 0))
            except (json.JSONDecodeError, TypeError, ValueError):
                return 0.0

        filtered = filtered.copy()
        filtered["_metric_value"] = filtered["payload"].apply(_extract_field)
        result = filtered.groupby(group_cols, as_index=False)["_metric_value"].sum()
        result = result.rename(columns={"_metric_value": metric.name})
    elif metric.metric_type == "snapshot":
        # Count distinct canonical_ids
        result = filtered.groupby(group_cols, as_index=False)[
            "canonical_id"
        ].nunique()
        result = result.rename(columns={"canonical_id": metric.name})
    else:
        raise ValueError(f"Unsupported metric type: {metric.metric_type}")

    return result


def aggregate_analytics(
    enriched_events: ibis.expr.types.Table,
    analytics_model: StreamAnalyticsModel,
) -> ibis.expr.types.Table:
    """Aggregate enriched events into time-grain metrics.

    Args:
        enriched_events: Ibis table with columns: source, entity_id, ts,
            event_type, payload, canonical_id.
        analytics_model: The analytics model defining metrics and dimensions.

    Returns:
        Ibis memtable with _date, dimensions, and metric columns.
    """
    # 1. Materialize to pandas
    df = enriched_events.execute()

    # 2. Truncate ts to date grain
    df["_date"] = df["ts"].apply(
        lambda ts: _truncate_date(ts, analytics_model.date_grain)
    )

    # 3. Define group columns
    group_cols = ["_date"] + analytics_model.dimensions

    # 4. Compute each metric and merge results
    result_df: pd.DataFrame | None = None

    for metric in analytics_model.metrics:
        metric_df = _compute_metric(df, metric, group_cols)

        if result_df is None:
            result_df = metric_df
        else:
            result_df = result_df.merge(metric_df, on=group_cols, how="outer")

    if result_df is None:
        # Should not happen since model requires at least one metric
        raise ValueError("No metrics to compute")

    # Sort for deterministic output
    result_df = result_df.sort_values(group_cols).reset_index(drop=True)

    # 5. Return as ibis memtable
    return ibis.memtable(result_df)
