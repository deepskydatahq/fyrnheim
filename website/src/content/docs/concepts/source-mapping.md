---
title: Source Mapping
description: Decouple entity field names from source column names with SourceMapping.
---

SourceMapping lets you decouple entity field names from source column names. Define a contract of `required_fields` on the entity, then map source columns to those fields.

## Defining a Source Mapping

```python
from fyrnheim import Entity, TableSource, LayersConfig, PrepLayer, Field, SourceMapping

entity = Entity(
    name="transactions",
    description="Customer transactions",
    required_fields=[
        Field(name="transaction_id", type="STRING"),
        Field(name="amount_cents", type="INT64"),
    ],
    source=TableSource(
        project="p", dataset="d", table="orders",
        duckdb_path="orders/*.parquet",
    ),
    layers=LayersConfig(prep=PrepLayer(model_name="prep_transactions")),
)

source_mapping = SourceMapping(
    entity=entity,
    source=entity.source,
    field_mappings={"transaction_id": "id", "amount_cents": "subtotal"},
)
```

## How It Works

- `required_fields` defines the contract: the set of fields the entity expects to exist.
- `field_mappings` maps entity field names (left) to source column names (right).
- Fyrnheim validates that all required fields have mappings at definition time, catching mismatches before any data flows.

This pattern is useful when:

- Source column names are unclear or inconsistent (e.g., `id` instead of `transaction_id`).
- You want to swap sources without changing entity logic.
- Multiple sources feed the same entity with different column names.
