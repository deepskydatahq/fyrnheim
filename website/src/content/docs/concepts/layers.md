---
title: Layers
description: Composable transformation stages that an entity flows through.
---

Layers are composable transformation stages that an entity flows through. Each layer takes the output of the previous layer and applies additional transformations.

## Available Layers

| Layer | Purpose |
|-------|---------|
| **PrepLayer** | Clean raw data: type casts, renames, computed columns |
| **DimensionLayer** | Add business logic columns (is_paying, account_type) |
| **SnapshotLayer** | Track changes over time (daily snapshots, SCD) |
| **ActivityConfig** | Detect events from state changes (row_appears, status_becomes, field_changes) |
| **AnalyticsLayer** | Date-grain metric aggregation (snapshot and event metrics) |

## Layer Configuration

All layers are configured inside `LayersConfig` on the entity:

```python
from fyrnheim import (
    LayersConfig, PrepLayer, DimensionLayer, SnapshotLayer,
    ActivityConfig, ActivityType, AnalyticsLayer, AnalyticsMetric,
    ComputedColumn,
)

layers = LayersConfig(
    prep=PrepLayer(
        model_name="prep_customers",
        computed_columns=[
            ComputedColumn(name="amount_dollars", expression="t.amount_cents / 100.0"),
        ],
    ),
    dimension=DimensionLayer(
        model_name="dim_customers",
        computed_columns=[
            ComputedColumn(name="is_paying", expression="t.plan != 'free'"),
        ],
    ),
    activity=ActivityConfig(
        model_name="activity_customers",
        entity_id_field="customer_id",
        types=[
            ActivityType(
                name="signed_up",
                trigger="row_appears",
                timestamp_field="created_at",
            ),
        ],
    ),
    analytics=AnalyticsLayer(
        model_name="analytics_customers",
        date_expression="t.created_at.date()",
        metrics=[
            AnalyticsMetric(
                name="new_customers",
                expression="t.count()",
                metric_type="event",
            ),
        ],
    ),
)
```

## PrepLayer

The first transformation stage. Use it to clean raw data with type casts, renames, and computed columns.

```python
PrepLayer(
    model_name="prep_customers",
    computed_columns=[
        ComputedColumn(name="email_hash", expression=hash_email("email")),
        ComputedColumn(name="amount_dollars", expression="t.amount_cents / 100.0"),
    ],
)
```

## DimensionLayer

Add business logic columns derived from prep layer output.

```python
DimensionLayer(
    model_name="dim_customers",
    computed_columns=[
        ComputedColumn(name="is_paying", expression="t.plan != 'free'"),
        ComputedColumn(name="account_type", expression=categorize("t.plan", {"free": "free", "pro": "paid", "enterprise": "paid"})),
    ],
)
```

## SnapshotLayer

Track changes over time with daily snapshots, useful for slowly changing dimensions (SCD).

## ActivityConfig

Detect events from state changes. Supports triggers like `row_appears`, `status_becomes`, and `field_changes`.

```python
ActivityConfig(
    model_name="activity_customers",
    entity_id_field="customer_id",
    types=[
        ActivityType(name="signed_up", trigger="row_appears", timestamp_field="created_at"),
    ],
)
```

## AnalyticsLayer

Date-grain metric aggregation for both snapshot and event metrics.

```python
AnalyticsLayer(
    model_name="analytics_customers",
    date_expression="t.created_at.date()",
    metrics=[
        AnalyticsMetric(name="new_customers", expression="t.count()", metric_type="event"),
    ],
)
```
