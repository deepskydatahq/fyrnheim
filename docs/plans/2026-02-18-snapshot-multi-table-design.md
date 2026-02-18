# Snapshot Multi-Table Output Design

## Overview
The executor currently chains all layer functions and persists only the final result. We split the pipeline so dim_{name} is persisted BEFORE snapshot runs, then snapshot_{name} is persisted as a separate table. This establishes the multi-table pattern for activity and analytics layers.

## Key Decisions
- Preserve backward compatibility: entities without snapshots work unchanged
- Minimal API change: add optional `snapshot_target_name` to ExecutionResult
- Split `_run_transform_pipeline()` into `_run_through_dimension()` and `_apply_snapshot_layer()`

## Approach

### Refactor executor._run_transform_pipeline()

Split into two internal methods:
```python
def _run_through_dimension(self, entity_name, module):
    """Run source -> prep -> dim (everything before snapshot)."""

def _apply_snapshot_layer(self, entity_name, module, dim_table):
    """Apply snapshot if exists. Returns snapshot table or None."""
```

### Update execute() for dual outputs

```python
# Phase 1: Run through dimension
dim_table = self._run_through_dimension(entity_name, module)

# Phase 2: Persist dim
conn.create_table(f"dim_{name}", dim_table, overwrite=True)

# Phase 3: Snapshot (separate output)
snapshot_table = self._apply_snapshot_layer(entity_name, module, dim_table)
if snapshot_table is not None:
    conn.create_table(f"snapshot_{name}", snapshot_table, overwrite=True)
```

### ExecutionResult change
```python
snapshot_target_name: str | None = None  # NEW field
```

## Files to Modify
- `src/fyrnheim/engine/executor.py`: Split pipeline, update execute(), add field to ExecutionResult

## Simplification Notes
- Single ExecutionResult with optional snapshot field (not multiple results)
- No changes to runner.py needed
- No quality checks on snapshot table (defer to future)
