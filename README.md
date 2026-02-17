# Fyrnheim

Define typed Python entities, generate transformations, run anywhere.

A dbt alternative built on Pydantic + Ibis.

Fyrnheim lets data teams define business entities as typed Pydantic models and automatically generates Ibis transformation code from those definitions. The same entity runs on DuckDB for instant local development and deploys to BigQuery, Snowflake, or Postgres in production with zero changes. No SQL, no Jinja, no vendor lock-in.

## Install

```bash
pip install fyrnheim[duckdb]
```

## Quick Start

**1. Create a project:**

```bash
fyr init myproject && cd myproject
```

```
Created myproject/
  created  entities/
  created  data/
  created  generated/
  created  fyrnheim.yaml
  created  entities/customers.py
  created  data/customers.parquet
```

**2. Look at the sample entity** in `entities/customers.py`:

```python
entity = Entity(
    name="customers",
    source=TableSource(..., duckdb_path="customers.parquet"),
    layers=LayersConfig(
        prep=PrepLayer(model_name="prep_customers", computed_columns=[
            ComputedColumn(name="email_hash", expression=hash_email("email")),
            ComputedColumn(name="amount_dollars", expression="t.amount_cents / 100.0"),
        ]),
        dimension=DimensionLayer(model_name="dim_customers", computed_columns=[
            ComputedColumn(name="is_paying", expression="t.plan != 'free'"),
        ]),
    ),
    quality=QualityConfig(checks=[NotNull("email"), Unique("email_hash")]),
)
```

**3. Generate transformation code:**

```bash
fyr generate
```

```
Generating transforms from entities
  customers            generated/customers_transforms.py   written

Generated: 1 written, 0 unchanged
```

**4. Run the pipeline:**

```bash
fyr run
```

```
Discovering entities... 1 found
Running on duckdb

  customers        prep -> dim            12 rows    0.1s  ok

Done: 1 success, 0 errors (0.2s)
```

Add your own entities to `entities/` and data to `data/`. See `examples/` for more.

## Core Concepts

### Entities

An entity is a Pydantic model describing a business object -- customers, orders, products. It declares its source, transformation layers, and quality rules in one place.

```python
entity = Entity(
    name="customers",
    description="...",
    source=TableSource(...),
    layers=LayersConfig(prep=..., dimension=...),
    quality=QualityConfig(checks=[...]),
)
```

### Layers

Composable transformation stages that an entity flows through. PrepLayer cleans raw data, DimensionLayer adds business logic columns, SnapshotLayer tracks changes over time.

```python
layers=LayersConfig(
    prep=PrepLayer(
        model_name="prep_customers",
        computed_columns=[ComputedColumn(name="amount_dollars", expression="t.amount_cents / 100.0")],
    ),
    dimension=DimensionLayer(
        model_name="dim_customers",
        computed_columns=[ComputedColumn(name="is_paying", expression="t.plan != 'free'")],
    ),
)
```

### Primitives

Reusable Python functions that replace SQL snippets. Hashing, date operations, categorization -- import and compose them instead of copy-pasting SQL.

```python
from fyrnheim.primitives import hash_email, date_trunc_month

ComputedColumn(name="email_hash", expression=hash_email("email"))
ComputedColumn(name="signup_month", expression=date_trunc_month("created_at"))
```

### Components

Multi-column patterns that generate related fields from a single config. LifecycleFlags produces `is_active`, `is_churned`, `is_at_risk` from a status column. TimeBasedMetrics computes tenure and recency.

```python
from fyrnheim import LifecycleFlags

flags = LifecycleFlags(
    status_column="status",
    active_states=["active"],
    churned_states=["cancelled"],
)
```

### Quality Checks

Declarative data quality rules that run after transformations. Built-in checks include NotNull, Unique, InRange, InSet, MatchesPattern, and ForeignKey.

```python
quality=QualityConfig(
    primary_key="email_hash",
    checks=[
        NotNull("email"),
        Unique("email_hash"),
        InRange("amount_cents", min=0),
    ],
)
```

## Why Fyrnheim?

| | dbt | Fyrnheim |
|---|---|---|
| Language | SQL + Jinja | Python |
| Type safety | Runtime errors | Pydantic validation at definition time |
| Local dev | Requires warehouse connection | DuckDB on local parquet files |
| Backend portability | Dialect-specific SQL | Ibis compiles to 15+ backends |
| Testing | Custom schema tests | pytest + quality checks |
| Boilerplate | Jinja macros, YAML configs | Python functions, Pydantic models |

Fyrnheim is not an orchestrator, not an extraction tool, and not a BI layer. It handles the transformation step: raw data in, clean business entities out.

## Status

- **Alpha** -- API may change before 1.0
- **DuckDB backend** -- fully supported
- **BigQuery backend** -- planned
- **Python 3.11+** required

## License

MIT
