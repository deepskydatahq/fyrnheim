---
title: Analytics Entities
description: Define business entities with state fields, activity-derived measures, and computed fields.
---

An `AnalyticsEntity` is the central model in Fyrnheim. It combines state field projection from sources with activity-derived measures and computed fields into a single, typed asset -- one row per entity.

## Defining an Analytics Entity

```python
from fyrnheim.core import (
    AnalyticsEntity,
    ComputedColumn,
    Measure,
    StateField,
)
from fyrnheim.quality import NotNull, Unique

customers = AnalyticsEntity(
    name="customers",
    identity_graph="customer_identity",
    state_fields=[
        StateField(name="email", source="crm_contacts", field="email", strategy="latest"),
        StateField(name="name", source="crm_contacts", field="full_name", strategy="latest"),
        StateField(name="plan", source="crm_contacts", field="plan", strategy="latest"),
    ],
    measures=[
        Measure(name="purchase_count", activity="purchase", aggregation="count"),
        Measure(name="total_spent", activity="purchase", aggregation="sum", field="amount"),
        Measure(name="last_purchase_amount", activity="purchase", aggregation="latest", field="amount"),
    ],
    computed_fields=[
        ComputedColumn(name="is_paying", expression="t.plan != 'free'"),
    ],
    quality_checks=[NotNull("email"), Unique("email")],
)
```

## State Fields

State fields project values from source tables into the entity row. Each field specifies:

- **name** -- the output column name
- **source** -- which source table to read from
- **field** -- the source column name
- **strategy** -- how to resolve the value: `latest`, `first`, or `coalesce`

The `coalesce` strategy requires a `priority` list to specify source precedence:

```python
StateField(
    name="name",
    source="ghost_members",
    field="name",
    strategy="coalesce",
    priority=["ghost_members", "mailerlite_subscribers"],
)
```

## Measures

Measures are activity-derived aggregations computed from the activity stream. Each measure specifies:

- **name** -- the output column name
- **activity** -- which activity event type to aggregate
- **aggregation** -- `count`, `sum`, or `latest`
- **field** -- the payload field to aggregate (required for `sum` and `latest`)

Measures let you combine state (who is this entity?) with behavior (what have they done?) in a single model.

## Computed Fields

Computed fields are Ibis expressions evaluated after state fields and measures are projected. They can reference any column produced by state fields or measures:

```python
ComputedColumn(
    name="is_high_value",
    expression="t.total_spent > 1000",
    description="True if customer has spent more than $1000",
)
```

## Quality Checks

Quality checks validate the final entity output. See [Quality Checks](/concepts/quality/) for the full list of available checks.

## Identity Graph

When an entity resolves identities across multiple sources, set `identity_graph` to the name of an `IdentityGraph` definition. When there is only one source, set it to `None`.
