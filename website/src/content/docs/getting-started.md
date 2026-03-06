---
title: Getting Started
description: Install Fyrnheim and run your first entity pipeline in minutes.
---

Fyrnheim lets data teams define business entities as typed Pydantic models and automatically generates Ibis transformation code from those definitions. The same entity runs on DuckDB for instant local development and deploys to BigQuery, ClickHouse, or Postgres in production with zero changes.

## Install

```bash
pip install fyrnheim[duckdb]
```

Fyrnheim requires Python 3.11 or later.

## Quick Start

### 1. Create a project

```bash
fyr init myproject && cd myproject
```

This scaffolds a project directory with everything you need:

```
Created myproject/
  created  entities/
  created  data/
  created  generated/
  created  fyrnheim.yaml
  created  entities/customers.py
  created  data/customers.parquet
```

### 2. Look at the sample entity

Open `entities/customers.py` to see a complete entity definition:

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

An entity declares its source, transformation layers, and quality rules in one place.

### 3. Generate transformation code

```bash
fyr generate
```

```
Generating transforms from entities
  customers            generated/customers_transforms.py   written

Generated: 1 written, 0 unchanged
```

Fyrnheim reads your entity definitions and generates Ibis transformation code into the `generated/` directory.

### 4. Run the pipeline

```bash
fyr run
```

```
Discovering entities... 1 found
Running on duckdb

  customers        prep -> dim            12 rows    0.1s  ok

Done: 1 success, 0 errors (0.2s)
```

Add your own entities to `entities/` and data to `data/`. See the [Core Concepts](/concepts/entities/) section to learn about entities, layers, sources, and more.
