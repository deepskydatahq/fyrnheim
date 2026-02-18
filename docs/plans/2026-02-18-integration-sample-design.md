# Integration & Sample Entity Design

## Sample Entity Update

### Layers to Add
- **SnapshotLayer**: natural_key="email_hash", dedup by created_at DESC
- **ActivityConfig**: signup (row_appears) + became_paying (status_becomes on plan field)
- **NOT analytics**: 12-row monthly data doesn't demonstrate daily aggregation well

### Parquet Changes
None needed — existing 12-row customers.parquet has all required columns.

### Scaffold Changes
Update src/fyrnheim/_scaffold/customers_entity.py to mirror the same layers.

## Full-Stack E2E Test

### Overview
New test file (tests/test_e2e_full_pipeline.py) with synthetic 100-row "orders" entity exercising all 5 layers.

### Test Data
100 rows over 10 days (10 per day):
- Columns: id, email, name, status, created_at, updated_at, revenue_cents
- Status mix: pending/active/inactive
- Revenue: 0-9900 cents

### Entity: orders
1. PrepLayer: hash email, convert cents to dollars
2. DimensionLayer: email_domain, is_active flag, cohort month
3. SnapshotLayer: natural_key=id, dedup by updated_at DESC
4. ActivityConfig: order_created (row_appears) + became_active (status_becomes)
5. AnalyticsLayer: daily metrics (total_revenue, order_count)

### Verification
- dim_orders: all rows with computed columns
- snapshot_orders: deduped with ds + snapshot_key
- activity_orders: correct event counts per trigger
- analytics_orders: 10 date rows with correct aggregations
- Quality checks pass on dim table
