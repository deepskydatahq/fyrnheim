# Fyrnheim

Activities-first data transformation framework.

Built on Pydantic + Ibis. Define typed sources, detect business events from state changes, resolve identities across systems, and project entity models -- all in Python.

## Install

```bash
pip install fyrnheim[duckdb]
```

## Quick Start

**1. Create a project:**

```bash
fyr init myproject && cd myproject
```

**2. Define your pipeline** in `entities/customers.py`:

```python
from fyrnheim import (
    StateSource, ActivityDefinition, RowAppeared, FieldChanged,
    IdentityGraph, IdentitySource, EntityModel, StateField,
)

# Source -- a slowly-changing state table
crm = StateSource(name="crm_contacts", project="p", dataset="raw", table="contacts", id_field="id")

# Activities -- named business events from state changes
signup = ActivityDefinition(name="signup", source="crm_contacts", trigger=RowAppeared())
became_paying = ActivityDefinition(
    name="became_paying", source="crm_contacts",
    trigger=FieldChanged(field="plan", to_values=["pro", "enterprise"]),
)

# Identity -- resolve across sources
identity = IdentityGraph(
    name="customer_identity", canonical_id="customer_id",
    sources=[IdentitySource(source="crm_contacts", id_field="id", match_key_field="email")],
)

# Entity -- derived current state
customers = EntityModel(
    name="customers", identity_graph="customer_identity",
    state_fields=[
        StateField(name="email", source="crm_contacts", field="email", strategy="latest"),
        StateField(name="plan", source="crm_contacts", field="plan", strategy="latest"),
    ],
)
```

**3. Run tests:**

```bash
pytest tests/
```

## Core Concepts

### Sources

**StateSource** -- a slowly-changing table (CRM contacts, subscription records). The diff engine automatically detects row appearances, disappearances, and field changes between snapshots.

```python
StateSource(name="crm_contacts", project="p", dataset="d", table="contacts", id_field="contact_id")
```

**EventSource** -- an append-only event stream (page views, transactions).

```python
EventSource(
    name="billing_events", project="p", dataset="d", table="transactions",
    entity_id_field="customer_id", timestamp_field="created_at", event_type_field="event_type",
)
```

### Activity Definitions

Named business events detected from raw data changes. Each activity ties to a source and a trigger:

| Trigger | Detects |
|---------|---------|
| `RowAppeared()` | New row in a state source |
| `RowDisappeared()` | Row removed from a state source |
| `FieldChanged(field, to_values)` | Field value changed (optionally to specific values) |
| `EventOccurred(event_types)` | Specific event types in an event source |

```python
signup = ActivityDefinition(name="signup", source="crm_contacts", trigger=RowAppeared())
became_paying = ActivityDefinition(
    name="became_paying", source="crm_contacts",
    trigger=FieldChanged(field="plan", to_values=["pro", "enterprise"]),
)
```

### Identity Graph

Cross-source identity resolution. Link records from different systems by a shared match key:

```python
IdentityGraph(
    name="customer_identity",
    canonical_id="customer_id",
    sources=[
        IdentitySource(source="crm_contacts", id_field="contact_id", match_key_field="email_hash"),
        IdentitySource(source="billing_events", id_field="customer_id", match_key_field="email_hash"),
    ],
)
```

### Entity Model

Derived current-state projection from resolved identities. Each field picks a source, a column, and a merge strategy (`latest`, `first`):

```python
EntityModel(
    name="customers",
    identity_graph="customer_identity",
    state_fields=[
        StateField(name="email", source="crm_contacts", field="email", strategy="latest"),
        StateField(name="first_seen", source="crm_contacts", field="created_at", strategy="first"),
    ],
    computed_fields=[ComputedColumn(name="is_paying", expression="plan != 'free'")],
)
```

### Analytics Model

Time-grain metric aggregation over the activity stream:

```python
StreamAnalyticsModel(
    name="daily_metrics",
    identity_graph="customer_identity",
    date_grain="daily",
    metrics=[
        StreamMetric(name="new_signups", expression="count()", event_filter="signup", metric_type="count"),
        StreamMetric(name="total_customers", expression="count()", metric_type="snapshot"),
    ],
)
```

## CLI

```bash
fyr init [project_name]   # Scaffold a new project
fyr run                   # Run the pipeline
fyr bench                 # Run the pipeline and print per-phase timings
fyr bench --json          # Same, but emit PipelineTimings as JSON on stdout
fyr --version             # Show version
fyr --help                # Show available commands
```

`fyr bench` reports wall-clock time per phase, per source, per identity graph,
and per analytics entity / metrics model (split into projection vs. write),
making it easy to spot where a pipeline spends its time.

## Why Fyrnheim?

| | dbt | Fyrnheim |
|---|---|---|
| Language | SQL + Jinja | Python |
| Type safety | Runtime errors | Pydantic validation at definition time |
| Local dev | Requires warehouse connection | DuckDB on local parquet files |
| Backend portability | Dialect-specific SQL | Ibis compiles to 15+ backends |
| Testing | Custom schema tests | pytest |
| Identity resolution | Manual SQL joins | Built-in identity graph |

## Status

- **Alpha** -- API may change before 1.0
- **DuckDB backend** -- fully supported
- **BigQuery backend** -- supported
- **ClickHouse output** -- supported as output sink
- **Postgres backend** -- supported
- **Python 3.11+** required

## License

MIT
