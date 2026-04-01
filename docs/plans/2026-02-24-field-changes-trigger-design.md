# field_changes Activity Trigger Design

## Overview

Implement the `field_changes` activity trigger in the Ibis code generator. This trigger detects when a field value changes between consecutive rows for the same entity, turning mutation history into an activity event stream.

## Problem Statement

The `ActivityType` model defines `trigger: Literal['row_appears', 'status_becomes', 'field_changes']` but only `row_appears` and `status_becomes` are implemented. The `field_changes` trigger is skipped with a `# field_changes: skip for v1` comment. This leaves a gap for detecting field-level changes in time-series data (e.g., plan upgrades, status transitions).

## Expert Perspectives

### Product
- `field_changes` operates on time-series data where the same entity appears multiple times with different timestamps (e.g., subscription rows over months).
- The core job: turning mutation history into an event stream. "Did this field's value change?" — yes or no.
- Existing single-row-per-entity test fixtures won't exercise this trigger; multi-row test data is needed.

### Technical
- Use **inline code generation** (Approach A), matching how `row_appears` and `status_becomes` are already implemented. No new runtime helper abstraction needed.
- Window function: `lag(field).over(partition_by=entity_id, order_by=ts)` — same pattern used in the snapshot engine.
- First rows (NULL lag) are naturally excluded by the filter `prev_field IS NOT NULL`.

### Simplification Review
- **Removed `values` filter from `field_changes`**: The `values` parameter has different semantics on `field_changes` vs `status_becomes`. If you want "changed TO premium," use `status_becomes` with `values=["premium"]`. `field_changes` should purely detect any change. This avoids semantic inconsistency and conditional complexity.
- **Streamlined tests**: Focus on core change detection scenarios, not values-filter edge cases.
- **NULL handling is implementation detail**, not a design principle — the filter naturally excludes first rows.

## Proposed Solution

Add an `elif trigger == "field_changes":` block in `_generate_activity_function` that generates inline Ibis code using a window function with `lag()` to compare each row's field value against the previous row for that entity.

### Generated Code Shape

```python
# For activity with trigger='field_changes', field='plan'
w = ibis.window(group_by="entity_id", order_by="ts")
t = t.mutate(prev_plan=t["plan"].lag().over(w))
t = t.filter(t["prev_plan"].notnull() & (t["plan"] != t["prev_plan"]))
return t.select("entity_id", "identity", "ts", ibis.literal("plan_changed").name("activity_type"))
```

## Design Details

### Code Generator Changes (`src/fyrnheim/generators/ibis_code_generator.py`)

1. Replace `# field_changes: skip for v1` with an `elif trigger == "field_changes":` block
2. Generate window definition: `partition_by=entity_id_field, order_by=timestamp_field`
3. Generate `mutate` call adding `prev_{field}` column via `lag()`
4. Generate `filter` call: `prev_{field}.notnull() & (field != prev_{field})`
5. Generate standard activity output select: `(entity_id, identity, ts, activity_type)`
6. Require `field` attribute to be set (validate or assert)

### Validation (`src/fyrnheim/core/activity.py`)

Ensure `field` is required when `trigger == "field_changes"`. May already be enforced by Pydantic; verify and add validator if not.

### Tests

- **Basic change detection**: Entity with 3 rows where field changes between rows. Expect activity events for each change.
- **No change**: Entity with 3 rows, field stays the same. Expect 0 events.
- **First row excluded**: Single-row entity produces 0 events.
- **Multiple entities**: Two entities with interleaved timestamps. Changes detected per-entity, not globally.
- **Multiple changes**: Entity with 5 rows and 3 field changes. Expect 3 events.

### Not Changed

- Snapshot engine (no modifications needed)
- Activity output schema (same `(entity_id, identity, ts, activity_type)` tuple)
- Other triggers (`row_appears`, `status_becomes` untouched)

## Alternatives Considered

1. **Runtime helper function** (like `apply_snapshot()`): Rejected because `field_changes` is specific to individual activity configurations and doesn't benefit from a shared abstraction. Inline generation is consistent with existing triggers.
2. **`values` filter on `field_changes`**: Rejected during simplification review — semantically inconsistent with `status_becomes`. Use `status_becomes` for "changed TO specific value" cases.

## Success Criteria

- `field_changes` trigger generates correct Ibis code with window function
- Change detection works on multi-row-per-entity time-series data
- First rows (no previous value) are excluded
- All existing tests continue to pass
- New tests cover the core scenarios listed above
