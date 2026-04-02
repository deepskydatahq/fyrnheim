---
title: Getting Started
description: Install Fyrnheim and build your first activities-first data pipeline.
---

Fyrnheim is a Python-native dbt alternative. You define sources, activity definitions, identity graphs, entity models, and analytics models in Python. Fyrnheim generates Ibis transformation code and runs it on DuckDB, BigQuery, ClickHouse, or Postgres.

## Install

```bash
pip install fyrnheim[duckdb]
```

Requires Python 3.11 or later.

## Quick Start

```bash
fyr init myproject && cd myproject
```

Then open `entities/customers.py` and build a pipeline step by step.

### 1. Define sources

Declare where your data lives and what shape it has.

```python
from fyrnheim.core import StateSource, EventSource

crm_source = StateSource(
    name="crm_contacts",
    project="my_project",
    dataset="crm",
    table="contacts",
    id_field="contact_id",
)

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

State sources get automatic change detection via the snapshot-diff engine. Event sources pass through directly.

### 2. Define activities

Tell Fyrnheim which changes matter to your business.

```python
from fyrnheim.core import ActivityDefinition, RowAppeared, FieldChanged, EventOccurred

signup = ActivityDefinition(
    name="signup",
    source="crm_contacts",
    trigger=RowAppeared(),
)

became_paying = ActivityDefinition(
    name="became_paying",
    source="crm_contacts",
    trigger=FieldChanged(field="plan", to_values=["pro", "enterprise"]),
)

purchase = ActivityDefinition(
    name="purchase",
    source="billing_events",
    trigger=EventOccurred(event_types=["purchase"]),
)
```

### 3. Build an identity graph

Link IDs across sources to a single canonical ID.

```python
from fyrnheim.core import IdentityGraph, IdentitySource

customer_identity = IdentityGraph(
    name="customer_identity",
    canonical_id="customer_id",
    sources=[
        IdentitySource(source="crm_contacts", id_field="contact_id", match_key_field="email_hash"),
        IdentitySource(source="billing_events", id_field="customer_id", match_key_field="email_hash"),
    ],
)
```

### 4. Create an entity model

Project current state from the enriched activity stream.

```python
from fyrnheim.core import EntityModel, StateField, ComputedColumn

customers = EntityModel(
    name="customers",
    identity_graph="customer_identity",
    state_fields=[
        StateField(name="email", source="crm_contacts", field="email", strategy="latest"),
        StateField(name="name", source="crm_contacts", field="full_name", strategy="latest"),
        StateField(name="plan", source="crm_contacts", field="plan", strategy="latest"),
        StateField(name="first_seen", source="crm_contacts", field="created_at", strategy="first"),
    ],
    computed_fields=[
        ComputedColumn(name="is_paying", expression="plan != 'free'"),
    ],
)
```

### 5. Add analytics

Aggregate time-grain metrics from the activity stream.

```python
from fyrnheim.core import StreamAnalyticsModel, StreamMetric

daily_metrics = StreamAnalyticsModel(
    name="customer_metrics_daily",
    identity_graph="customer_identity",
    date_grain="daily",
    metrics=[
        StreamMetric(name="new_signups", expression="count()", event_filter="signup", metric_type="count"),
        StreamMetric(name="total_customers", expression="count()", metric_type="snapshot"),
    ],
)
```

### 6. Generate and run

```bash
fyr generate
fyr run
```

Fyrnheim reads your definitions, generates Ibis transformation code, and executes the full pipeline.

## Next steps

- [Sources](/concepts/sources/) -- StateSource vs EventSource in detail
- [Activities](/concepts/activities/) -- The snapshot-diff engine and trigger types
- [Identity](/concepts/identity/) -- How ID resolution works
- [Entity Models](/concepts/entity-models/) -- Resolution strategies for current state
- [Analytics](/concepts/analytics/) -- Time-grain metric aggregation
