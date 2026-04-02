---
title: Entity Models
description: Project current entity state from the enriched activity stream.
---

An entity model answers the question: "Given all events for a canonical ID, what is the current state?" It is a declared projection over the enriched activity stream, not a raw source query.

## EntityModel

```python
from fyrnheim.core import EntityModel, StateField, ComputedColumn

customers = EntityModel(
    name="customers",
    identity_graph="customer_identity",
    state_fields=[
        StateField(name="email", source="crm_contacts", field="email", strategy="latest"),
        StateField(name="name", source="crm_contacts", field="full_name", strategy="latest"),
        StateField(name="plan", source="crm_contacts", field="plan", strategy="latest"),
        StateField(
            name="first_seen", source="crm_contacts", field="created_at", strategy="first"
        ),
    ],
    computed_fields=[
        ComputedColumn(name="is_paying", expression="plan != 'free'"),
    ],
)
```

## StateField

Each `StateField` declares which source field to pull and how to resolve its value from the event history.

| Parameter | Description |
|-----------|-------------|
| `name` | Output column name in the entity model |
| `source` | Which source owns this field |
| `field` | Field name in that source |
| `strategy` | How to resolve the value (see below) |

## Resolution strategies

| Strategy | Behavior | Example use |
|----------|----------|-------------|
| `latest` | Most recent value from the source | Current email, current plan |
| `first` | Earliest observed value | First seen date, original signup source |
| `coalesce` | Latest non-null across multiple sources, ordered by priority | Email from CRM, falling back to billing |

For `coalesce`, provide a `priority` list to control which sources take precedence:

```python
StateField(
    name="email",
    source="crm_contacts",
    field="email",
    strategy="coalesce",
    priority=["crm_contacts", "billing_events"],
)
```

## Computed fields

Use `ComputedColumn` to derive values from resolved state fields:

```python
computed_fields=[
    ComputedColumn(name="is_paying", expression="plan != 'free'"),
    ComputedColumn(name="name_upper", expression="upper(name)"),
]
```

## Materialization

Entity models are materialized as tables for fast reads. They are recomputed from the activity stream on each run, not incrementally mutated. This guarantees consistency with the underlying events.

## How entity models connect to the pipeline

Entity models depend on an [identity graph](/concepts/identity/) to provide canonical IDs. They consume the enriched activity stream produced by [sources](/concepts/sources/) and [activity definitions](/concepts/activities/). For time-series metrics, see [analytics models](/concepts/analytics/).
