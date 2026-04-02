---
title: Sources
description: Declare raw data inputs as StateSource or EventSource.
---

Sources are the entry point of every Fyrnheim pipeline. They declare where data comes from and what shape it has. There are two types.

## StateSource

Use `StateSource` for data that represents current state -- CRM exports, dimension tables, configuration tables. These are tables where rows represent entities and columns represent their current attributes.

```python
from fyrnheim.core import StateSource, Field

crm_source = StateSource(
    name="crm_contacts",
    project="my_project",
    dataset="crm",
    table="contacts",
    id_field="contact_id",
    fields=[
        Field(name="email", type="string"),
        Field(name="plan", type="string"),
        Field(name="created_at", type="timestamp"),
    ],
)
```

The `id_field` tells the snapshot-diff engine which column uniquely identifies each row. On every run, Fyrnheim compares the current snapshot to the previous one and produces change events automatically.

## EventSource

Use `EventSource` for data that is already event-shaped -- page views, transactions, webhook logs. Each row is an occurrence with a timestamp.

```python
from fyrnheim.core import EventSource

billing_source = EventSource(
    name="billing_events",
    project="my_project",
    dataset="billing",
    table="transactions",
    entity_id_field="customer_id",
    timestamp_field="created_at",
    event_type_field="event_type",
)
```

Event sources pass through directly into the activity stream. The `event_type_field` (or a static `event_type` string) identifies what kind of event each row represents.

## SourceTransforms

Both source types accept an optional `transforms` parameter for cleaning data at ingestion time -- renaming columns, casting types, or applying simple arithmetic.

```python
from fyrnheim.core import StateSource, SourceTransforms, Rename, TypeCast

source = StateSource(
    name="raw_accounts",
    project="warehouse",
    dataset="crm",
    table="accounts",
    id_field="account_id",
    transforms=SourceTransforms(
        renames=[Rename(from_name="acct_id", to_name="account_id")],
        casts=[TypeCast(column="revenue", to_type="float64")],
    ),
)
```

## How sources connect to the pipeline

State sources feed into the **snapshot-diff engine**, which detects changes and produces raw events. Event sources flow directly into the activity stream. Both converge at the [activity definition](/concepts/activities/) step, where raw changes become named business events.
