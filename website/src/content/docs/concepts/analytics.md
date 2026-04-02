---
title: Analytics
description: Aggregate time-grain metrics from the enriched activity stream.
---

Analytics models produce time-grain metric tables from the enriched activity stream. They are top-level assets, not nested inside entities.

## StreamAnalyticsModel

```python
from fyrnheim.core import StreamAnalyticsModel, StreamMetric

daily_metrics = StreamAnalyticsModel(
    name="customer_metrics_daily",
    identity_graph="customer_identity",
    date_grain="daily",
    metrics=[
        StreamMetric(
            name="new_signups",
            expression="count()",
            event_filter="signup",
            metric_type="count",
        ),
        StreamMetric(
            name="total_customers",
            expression="count()",
            metric_type="snapshot",
        ),
    ],
)
```

## Date grains

| Grain | Aggregation period |
|-------|-------------------|
| `daily` | One row per entity per day |
| `weekly` | One row per entity per week |
| `monthly` | One row per entity per month |

## StreamMetric

Each metric declares what to compute and how.

| Parameter | Description |
|-----------|-------------|
| `name` | Output column name |
| `expression` | Aggregation expression (e.g., `count()`, `sum(amount)`) |
| `event_filter` | Optional: only count events matching this activity name |
| `metric_type` | How the metric accumulates (see below) |

## Metric types

| Type | Behavior | Example |
|------|----------|---------|
| `count` | Counts events in the time grain | New signups per day |
| `sum` | Sums a numeric field across events | Revenue per month |
| `snapshot` | Cumulative point-in-time value | Total active customers |

`snapshot` metrics represent state at the end of each period, not incremental activity. Use them for KPIs like "total customers" or "current MRR".

## Event filtering

The `event_filter` parameter filters to events from a specific [activity definition](/concepts/activities/). Without it, the metric aggregates across all events.

```python
StreamMetric(
    name="upgrades",
    expression="count()",
    event_filter="became_paying",
    metric_type="count",
)
```

## How analytics models connect to the pipeline

Analytics models depend on an [identity graph](/concepts/identity/) and consume the enriched activity stream. They run alongside [entity models](/concepts/entity-models/) -- both are downstream projections of the same event data.
