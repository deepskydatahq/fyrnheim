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
from fyrnheim.core import AnalyticsEntity, Measure, StateField, ComputedColumn
from fyrnheim.quality import NotNull, Unique

customers = AnalyticsEntity(
    name="customers",
    identity_graph="customer_identity",
    state_fields=[
        StateField(name="email", source="crm_contacts", field="email", strategy="latest"),
        StateField(name="plan", source="crm_contacts", field="plan", strategy="latest"),
    ],
    measures=[
        Measure(name="purchase_count", activity="purchase", aggregation="count"),
    ],
    computed_fields=[
        ComputedColumn(name="is_paying", expression="t.plan != 'free'"),
    ],
    quality_checks=[NotNull("email"), Unique("email")],
)
```

An `AnalyticsEntity` combines state fields from sources, activity-derived measures, and computed fields into a single typed model.

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

Add your own entities to `entities/` and data to `data/`. See the [Core Concepts](/concepts/analytics-entities/) section to learn about analytics entities, measures, sources, and more.
