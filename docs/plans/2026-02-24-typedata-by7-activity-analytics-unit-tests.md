# Plan: Unit tests for activity/analytics code generation

**Task:** typedata-by7
**Title:** [M003-retro] Unit tests for activity/analytics code generation
**Date:** 2026-02-24

## Goal

Add unit tests to `tests/test_ibis_code_generator.py` for `_generate_activity_function()` and `_generate_analytics_function()`. These methods currently have no unit-level coverage — they're only exercised through E2E tests.

## Context

The existing test file covers: imports, bind_expression, single source, union source, prep, dim, snapshot, generate_module, and write_module. The activity and analytics generators are the only untested methods.

Both methods follow the same pattern as the existing generators:
- Accept an `ibis.Table` input parameter
- Determine input name from layer hierarchy (dim → prep → source)
- Return generated Python code as a string

## Approach

Add two new test classes and two new fixtures to `tests/test_ibis_code_generator.py`, following existing conventions.

### New Fixtures

**1. `activity_entity`** — Entity with activity layer configured:
- Name: `members`
- Source: TableSource with basic fields
- Layers: PrepLayer + ActivityConfig with:
  - `signup` trigger (row_appears)
  - `became_paying` trigger (status_becomes on `plan` field, values `["pro", "enterprise"]`)
- entity_id_field: `id`, person_id_field: `email`

**2. `analytics_entity`** — Entity with analytics layer configured:
- Name: `sales`
- Source: TableSource with basic fields
- Layers: PrepLayer + AnalyticsLayer with:
  - date_expression: `t.created_at.cast("date")`
  - Two metrics: `total_revenue` (t.amount.sum()), `order_count` (t.id.count())
  - Dimensions: `["plan"]`

**3. `analytics_no_dim_entity`** — Entity with analytics but no dimension layer:
- Same as analytics_entity but without dimension layer
- Used to verify input falls back to prep/source

### New Test Classes

#### `TestActivityGeneration`

| Test | What it verifies |
|------|-----------------|
| `test_function_signature` | Function named `activity_members`, takes `prep_members: ibis.Table` |
| `test_row_appears_trigger` | `act_signup` select with entity_id, identity, ts, activity_type literal |
| `test_status_becomes_trigger` | `act_became_paying` with `.filter()` and `.isin(["pro", "enterprise"])` |
| `test_union_of_triggers` | `ibis.union(act_signup, act_became_paying)` when multiple triggers |
| `test_is_valid_python` | `ast.parse()` succeeds on imports + activity code |
| `test_input_from_dim_layer` | When entity has dim layer, input param is `dim_members` |

#### `TestAnalyticsGeneration`

| Test | What it verifies |
|------|-----------------|
| `test_function_signature` | Function named `analytics_sales`, takes `prep_sales: ibis.Table` |
| `test_date_expression` | `t.mutate(_date=t.created_at.cast("date"))` |
| `test_metrics` | `total_revenue=` and `order_count=` in aggregate call |
| `test_dimensions_in_group_by` | `group_by("_date", "plan")` |
| `test_is_valid_python` | `ast.parse()` succeeds on imports + analytics code |
| `test_no_dim_falls_back_to_prep` | When no dim layer, input is `prep_sales` |

## Files Modified

- `tests/test_ibis_code_generator.py` — add fixtures + 2 test classes (~12 test methods)

## Imports Needed

Add to existing imports:
```python
from fyrnheim.core.activity import ActivityConfig, ActivityType
from fyrnheim.core.analytics import AnalyticsLayer, AnalyticsMetric
```

(Entity, Field, LayersConfig, PrepLayer, TableSource already imported)

## Verification

```bash
uv run pytest tests/test_ibis_code_generator.py -v
```

All new tests should pass. Existing tests should remain green.
