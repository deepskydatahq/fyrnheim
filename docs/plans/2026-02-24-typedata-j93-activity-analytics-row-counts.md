# Design: typedata-j93 -- ExecutionResult activity/analytics row counts

**Task:** typedata-j93 -- ExecutionResult lacks activity/analytics row counts
**Date:** 2026-02-24
**Status:** plan

---

## 1. Summary

Add optional `activity_row_count` and `analytics_row_count` fields to `ExecutionResult` and `EntityRunResult`. Capture row counts when activity/analytics tables are persisted in `_run_transform_pipeline`, propagate them through the execution chain, and display them in CLI output.

---

## 2. Problem

When entities have activity and/or analytics layers, `_run_transform_pipeline` creates and persists these tables silently. The row counts are never captured. Users running `fyr run` only see the main target (dim/snapshot) row count — activity and analytics counts are invisible.

---

## 3. Approach

### 3a. Pipeline return type

`_run_transform_pipeline` currently returns a single Ibis table expression. Change it to return a dataclass that bundles the main table with optional row counts:

```python
@dataclass(frozen=True)
class _PipelineResult:
    table: ibis.expr.types.Table
    activity_row_count: int | None = None
    analytics_row_count: int | None = None
```

This keeps the change local — `execute()` destructures the result and populates `ExecutionResult`.

### 3b. Capturing row counts in `_run_transform_pipeline`

After each `conn.create_table(...)` call for activity/analytics, call `.count().execute()` on the persisted table to get the row count. This mirrors how the main target's row count is already captured in `execute()`.

**executor.py lines 202-211** — after creating activity table:
```python
activity_table = activity_fn(dim_result)
persisted_activity = conn.create_table(f"activity_{entity_name}", activity_table, overwrite=True)
activity_row_count = persisted_activity.count().execute()
```

Same pattern for analytics.

### 3c. ExecutionResult changes

Add two optional fields (executor.py line ~27):

```python
activity_row_count: int | None = None
analytics_row_count: int | None = None
```

In `execute()` (line ~146), populate them from `_PipelineResult`.

### 3d. EntityRunResult changes

Add two optional fields (runner.py line ~36):

```python
activity_row_count: int | None = None
analytics_row_count: int | None = None
```

In `run_entity()` (line ~221), copy from `ExecutionResult`.

### 3e. CLI display

In `_print_entity_result` (cli.py line ~168), add a sub-line when activity or analytics counts are present:

```
  customers        source→dim→snap     42 rows   0.3s  ok
    activity: 128 rows  analytics: 12 rows
```

Only print the sub-line when at least one branch count is non-None.

---

## 4. Files to Change

| File | Change |
|------|--------|
| `src/fyrnheim/engine/executor.py` | Add `_PipelineResult` dataclass; update `_run_transform_pipeline` to capture counts and return `_PipelineResult`; add fields to `ExecutionResult`; update `execute()` to destructure pipeline result |
| `src/fyrnheim/engine/runner.py` | Add fields to `EntityRunResult`; update `run_entity()` to propagate counts |
| `src/fyrnheim/cli.py` | Update `_print_entity_result` to display branch counts |

---

## 5. Tests

| Test file | What to add/update |
|-----------|--------------------|
| `tests/test_engine_executor.py` | Test `ExecutionResult` new fields default to None; test that `execute()` returns counts when activity/analytics layers exist |
| `tests/test_engine_runner.py` | Test `EntityRunResult` new fields default to None; test that `run_entity()` propagates counts |
| `tests/test_cli_run.py` | Test CLI output includes activity/analytics sub-line when counts are present |
| `tests/test_e2e_activity.py` | Add assertion that `ExecutionResult.activity_row_count` matches expected count |
| `tests/test_e2e_analytics.py` | Add assertion that `ExecutionResult.analytics_row_count` matches expected count |

---

## 6. Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Extra `.count().execute()` adds latency | Negligible — one scalar query per branch table, same pattern as main target |
| Frozen dataclass change breaks existing code | New fields have defaults (`None`), fully backward compatible |
| `_PipelineResult` adds internal complexity | Private dataclass, only used between `_run_transform_pipeline` and `execute()` |

---

## 7. Acceptance Criteria

1. `fyr run` on an entity with activity layer shows activity row count
2. `fyr run` on an entity with analytics layer shows analytics row count
3. Entities without activity/analytics layers show no extra output (no regression)
4. All existing tests pass
5. New tests cover the added fields and CLI output
