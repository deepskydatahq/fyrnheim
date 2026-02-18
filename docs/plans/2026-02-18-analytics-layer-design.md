# Analytics Layer Design

## Overview
Analytics layer aggregates dimension data at date grain. Groups by date expression + optional dimensions, applies metric aggregations (count, sum, etc.) using Ibis expressions.

## Generator (_generate_analytics_function)

### Expression Handling
Use raw metric expressions directly in generated code (consistent with ComputedColumn pattern). Bind with existing `_bind_expression()` helper.

### Generated Code Pattern
```python
def analytics_customers(dim_customers: ibis.Table) -> ibis.Table:
    t = dim_customers
    date_col = t.created_at.cast("date")
    analytics = t.group_by(
        date=date_col,
        plan=t.plan,
    ).aggregate(
        signup_count=t.count(),
        total_revenue=t['amount_cents'].sum(),
    )
    return analytics
```

### Key Decision
metric_type ("snapshot"/"event") is semantic only — no difference in code generation. Both are aggregation expressions.

## Executor
Same multi-table pattern as activity:
```python
analytics_fn = getattr(module, f"analytics_{entity_name}", None)
if analytics_fn:
    analytics_t = analytics_fn(dim_table)
    conn.create_table(f"analytics_{entity_name}", analytics_t, overwrite=True)
```

## E2E Test
- 20-row parquet: 5 rows per date across 4 dates, 3 plan types
- Entity with AnalyticsLayer: date expression + plan dimension + count/sum metrics
- Verify: 12 rows (4 dates x 3 plans), correct aggregation values

## Files
- Generator: src/fyrnheim/generators/ibis_code_generator.py
- Executor: src/fyrnheim/engine/executor.py
- Test: tests/test_analytics_e2e.py
