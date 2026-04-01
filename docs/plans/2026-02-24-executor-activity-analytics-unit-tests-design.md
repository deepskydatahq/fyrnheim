# Plan: Unit Tests for Executor Activity/Analytics Branch Execution

**Task:** typedata-0a7 — [M003-retro] Unit tests for executor activity/analytics branch execution
**Date:** 2026-02-24

## Problem

The `_run_transform_pipeline()` method in `DuckDBExecutor` has activity and analytics branch
execution (lines 201-211 in `executor.py`), but the unit tests in `test_engine_executor.py` only
cover the `source → prep → dim → snapshot` pipeline. The activity/analytics branches are only
validated through E2E tests (`test_e2e_activity.py`, `test_e2e_analytics.py`, `test_e2e_full_pipeline.py`).

Unit tests are needed to verify the executor's branching logic directly, independent of the
full generate → execute pipeline.

## Current State

### Executor pipeline logic (`executor.py:155-213`)

```
source → prep → dim → (capture dim_result)
                     ├─ snapshot (from dim)
                     ├─ activity (from dim_result, NOT snapshot)
                     └─ analytics (from dim_result, NOT snapshot)
```

Key behaviors to test:
1. Activity function receives `dim_result` and output is persisted as `activity_{name}`
2. Analytics function receives `dim_result` and output is persisted as `analytics_{name}`
3. Both branch from dim, not from snapshot (even when snapshot exists)
4. Activity/analytics work with or without snapshot present
5. Activity/analytics are independent of each other

### Existing unit test patterns (`test_engine_executor.py`)

- Helper `_create_transform_module()` writes inline Python code as a generated module
- Tests use `DuckDBExecutor` with `tmp_path` for generated dirs
- Source data from either `register_parquet()` or inline `source_fn`
- `SIMPLE_TRANSFORM` constant provides the basic source → prep → dim template

## Implementation Plan

### Step 1: Add transform module templates for activity/analytics

Add new template constants alongside `SIMPLE_TRANSFORM` that include activity and/or analytics
functions. These use the same pattern (inline Python string with `{parquet_path}` placeholder).

**Templates needed:**
- `TRANSFORM_WITH_ACTIVITY` — source + prep + dim + activity function
- `TRANSFORM_WITH_ANALYTICS` — source + prep + dim + analytics function
- `TRANSFORM_WITH_ACTIVITY_AND_ANALYTICS` — source + prep + dim + activity + analytics
- `TRANSFORM_WITH_SNAPSHOT_AND_ACTIVITY` — source + prep + dim + snapshot + activity
- `TRANSFORM_WITH_SNAPSHOT_AND_ANALYTICS` — source + prep + dim + snapshot + analytics

The activity function should create a simple activity stream (e.g., one row per entity with
an `activity_type` and `entity_id` column). The analytics function should create a simple
aggregation (e.g., count + sum grouped by some column).

### Step 2: Add `TestDuckDBExecutorActivityBranch` test class

New test class in `test_engine_executor.py`:

| Test | Verifies |
|------|----------|
| `test_activity_table_created` | `activity_{name}` table exists after execution |
| `test_activity_receives_dim_result` | Activity function gets dim output (has `amount_dollars` from prep) |
| `test_activity_without_snapshot` | Activity works when no snapshot function exists |
| `test_activity_with_snapshot` | Activity still receives dim_result (not snapshot output) when snapshot exists |
| `test_activity_row_count` | Activity table has expected row count |

### Step 3: Add `TestDuckDBExecutorAnalyticsBranch` test class

| Test | Verifies |
|------|----------|
| `test_analytics_table_created` | `analytics_{name}` table exists after execution |
| `test_analytics_receives_dim_result` | Analytics function gets dim output |
| `test_analytics_without_snapshot` | Analytics works when no snapshot function exists |
| `test_analytics_with_snapshot` | Analytics still receives dim_result when snapshot exists |
| `test_analytics_aggregation_correct` | Analytics table has expected aggregated values |

### Step 4: Add `TestDuckDBExecutorBranchCombinations` test class

| Test | Verifies |
|------|----------|
| `test_both_activity_and_analytics` | Both tables created when both functions exist |
| `test_activity_only_no_analytics` | Only activity table created (no analytics function) |
| `test_analytics_only_no_activity` | Only analytics table created (no activity function) |
| `test_branches_with_snapshot` | All three (snapshot + activity + analytics) work together |

## Files Modified

| File | Change |
|------|--------|
| `tests/test_engine_executor.py` | Add ~4 template constants + 3 test classes (~15 tests) |

## Testing

```bash
# Run just the new tests
uv run pytest tests/test_engine_executor.py -v

# Run full suite to ensure no regressions
uv run pytest
```

## TDD Steps

1. **RED:** Write `test_activity_table_created` — fails because no template exists yet
2. **GREEN:** Add `TRANSFORM_WITH_ACTIVITY` template and wire up test → passes
3. **RED:** Write remaining activity tests
4. **GREEN:** Add `TRANSFORM_WITH_SNAPSHOT_AND_ACTIVITY` template → all activity tests pass
5. **RED:** Write analytics tests
6. **GREEN:** Add analytics templates → all analytics tests pass
7. **RED:** Write combination tests
8. **GREEN:** Add combined template → all tests pass
9. **REFACTOR:** Review for duplication, extract shared fixtures if beneficial

## Notes

- No changes to production code — this is purely adding test coverage
- Templates should be minimal (simplest possible transforms that exercise the branches)
- Activity/analytics functions in templates should be trivially verifiable (known row counts, specific column names)
