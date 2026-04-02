---
title: Activities
description: Define business events from raw data changes using the snapshot-diff engine and activity definitions.
---

Activities are the core of Fyrnheim's architecture. Every piece of data -- whether from a state source or an event source -- becomes an event in the activity stream. Entity models and analytics models are derived views over this stream.

## The snapshot-diff engine

For [state sources](/concepts/sources/), Fyrnheim automatically detects changes by comparing daily snapshots. This is infrastructure -- you do not configure it. The engine produces three raw event types:

- **row_appeared** -- a new ID was detected in the source
- **field_changed** -- a field value differs from the previous snapshot (includes `old_value` and `new_value`)
- **row_disappeared** -- an ID is no longer present

This means you get full change history from sources that only expose current state (like CRM exports), without relying on unreliable webhooks.

## ActivityDefinition

Activity definitions interpret raw changes into named business events. The diff engine detects all changes mechanically; you decide which ones matter.

```python
from fyrnheim.core import ActivityDefinition, RowAppeared, FieldChanged

signup = ActivityDefinition(
    name="signup",
    source="crm_contacts",
    trigger=RowAppeared(),
)

became_paying = ActivityDefinition(
    name="became_paying",
    source="crm_contacts",
    trigger=FieldChanged(
        field="plan",
        to_values=["pro", "enterprise"],
    ),
    include_fields=["plan_amount", "currency"],
)
```

## Trigger types

| Trigger | Fires when | Use case |
|---------|-----------|----------|
| `RowAppeared()` | A new row appears in a state source | Signups, new records |
| `FieldChanged(field, from_values, to_values)` | A field value changes, optionally filtered by old/new values | Plan upgrades, status transitions |
| `RowDisappeared()` | A row is no longer present in a state source | Churn, deletions |
| `EventOccurred(event_types)` | An event source row matches a type | Purchases, page views |

`from_values` and `to_values` on `FieldChanged` are optional. Omit them to fire on any change to that field.

For event sources, use `EventOccurred` to pass through events by type:

```python
from fyrnheim.core import ActivityDefinition, EventOccurred

purchase = ActivityDefinition(
    name="purchase",
    source="billing_events",
    trigger=EventOccurred(event_types=["purchase"]),
)
```

## How activities connect to the pipeline

All activity events (from both state and event sources) flow into the [identity graph](/concepts/identity/), which resolves raw source IDs to canonical entity IDs. From there, the enriched activity stream feeds [entity models](/concepts/entity-models/) and [analytics models](/concepts/analytics/).
