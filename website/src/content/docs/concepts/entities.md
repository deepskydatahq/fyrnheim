---
title: Entities
description: Define business objects as typed Pydantic models with source, layers, and quality rules.
---

An entity is a Pydantic model describing a business object -- customers, orders, products. It declares its source, transformation layers, and quality rules in one place.

## Defining an Entity

```python
from fyrnheim import (
    Entity, TableSource, LayersConfig, PrepLayer,
    DimensionLayer, QualityConfig, NotNull, Unique,
    ComputedColumn, hash_email,
)

entity = Entity(
    name="customers",
    description="Customer records from the CRM",
    source=TableSource(
        project="myproject", dataset="raw", table="customers",
        duckdb_path="data/customers.parquet",
    ),
    layers=LayersConfig(
        prep=PrepLayer(
            model_name="prep_customers",
            computed_columns=[
                ComputedColumn(name="email_hash", expression=hash_email("email")),
            ],
        ),
        dimension=DimensionLayer(
            model_name="dim_customers",
            computed_columns=[
                ComputedColumn(name="is_paying", expression="t.plan != 'free'"),
            ],
        ),
    ),
    quality=QualityConfig(checks=[NotNull("email"), Unique("email_hash")]),
)
```

## Entity Fields

You can declare `required_fields` on an entity to define a contract between the entity and its source. This is especially useful with [Source Mapping](/concepts/source-mapping/) to decouple entity field names from source column names.

```python
entity = Entity(
    name="transactions",
    description="Customer transactions",
    required_fields=[
        Field(name="transaction_id", type="STRING"),
        Field(name="amount_cents", type="INT64"),
    ],
    source=TableSource(project="p", dataset="d", table="orders", duckdb_path="orders/*.parquet"),
    layers=LayersConfig(prep=PrepLayer(model_name="prep_transactions")),
)
```

## Multi-Entity Dependency Resolution

When entities depend on each other (via `DerivedSource` or `AggregationSource`), `fyr run` automatically resolves the execution order using topological sort. Dependencies run first:

```
transactions, subscriptions   (TableSource -- no dependencies)
         |           |
         v           v
        person               (DerivedSource -- identity graph)
           |
           v
        account              (AggregationSource -- groups person by account_id)
```

No manual ordering needed. Define your entities and Fyrnheim figures out the DAG.
