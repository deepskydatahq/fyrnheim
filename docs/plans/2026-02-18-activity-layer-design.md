# Activity Layer Design

## Overview
Activity layer transforms dimension rows into an event stream with (entity_id, activity_type, ts) columns. Each ActivityType generates a filtered/projected SELECT, all UNION'd together.

## Generator (_generate_activity_function)

### Trigger Implementation
- **row_appears**: Select all rows, project to (entity_id, activity_type=literal, ts)
- **status_becomes**: Filter where field.isin(values), project to same schema
- **field_changes**: SKIP for now (requires lag/window, defer to future)

### Generated Code Pattern
```python
def activity_customers(dim_customers: ibis.Table) -> ibis.Table:
    t = dim_customers
    signup = t.select(
        entity_id=t.id,
        activity_type=ibis.literal("signup"),
        ts=t.created_at,
    )
    became_paying = t.filter(t.plan.isin(["pro", "enterprise"])).select(
        entity_id=t.id,
        activity_type=ibis.literal("became_paying"),
        ts=t.updated_at,
    )
    return ibis.union(signup, became_paying)
```

## Executor
After dim is persisted, check for activity_{name} function:
```python
activity_fn = getattr(module, f"activity_{entity_name}", None)
if activity_fn:
    activity_t = activity_fn(dim_table)
    conn.create_table(f"activity_{entity_name}", activity_t, overwrite=True)
```
Activity reads from dim output (not the chained pipeline result).

## E2E Test
- 8-row parquet with mix of free/pro/enterprise plans
- Entity with DimensionLayer + ActivityConfig (signup + became_paying)
- Verify: 8 signup rows + 3 paying rows = 11 total activity rows
- Verify columns: entity_id, activity_type, ts

## Files
- Generator: src/fyrnheim/generators/ibis_code_generator.py
- Executor: src/fyrnheim/engine/executor.py
- Test: tests/test_e2e_activity.py

## Simplification Notes
- Skip field_changes trigger (complex, low value for v1)
- Activity always reads from dim (no conditional input resolution)
- No deduplication in activity layer
