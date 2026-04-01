# Design: Extend Executor for Multi-Input DerivedSource Entities

**Task:** typedata-dbv (M005-E003-S003)
**Date:** 2026-02-28

## Problem

The executor currently handles all source functions with a single-input pattern: `source_fn(conn, backend)`. DerivedSource entities need a multi-input pattern: `source_fn(sources_dict)` where `sources_dict` maps source names to Ibis tables already persisted in the connection catalog by prior entity executions.

The runner already executes entities in topological order (via `resolution.py`), so dependency tables will exist in the DuckDB catalog when a DerivedSource entity runs. The executor just needs to detect DerivedSource, build the dict, and call the source function with the correct signature.

## Codebase Findings

### Current Execution Flow

1. **`runner.run()`** resolves execution order via `resolve_execution_order()` (topological sort using `_extract_dependencies()`).
2. For each entity, `runner.run_entity()` calls `executor.execute(entity_name, generated_dir=gen_dir)` (line 186 of runner.py).
3. **`executor.execute()`** loads the generated module, then calls `_run_transform_pipeline(entity_name, module)`.
4. **`_run_transform_pipeline()`** looks for a registered source or falls back to `source_fn(conn, backend)`, then chains prep -> dim -> snapshot -> activity -> analytics.

### DerivedSource Model (source.py)

```python
class DerivedSource(BaseModel):
    identity_graph: str
    depends_on: list[str] = []
```

Story M005-E003-S001 will add `identity_graph_config: IdentityGraphConfig | None` with `sources: list[IdentityGraphSource]` where each source has `name`, `entity`, `match_key_field`, `fields`, etc. The `depends_on` will be auto-populated from `identity_graph_config.sources[*].entity`.

### Generated Source Function Signature (from S002 story)

For DerivedSource entities, the codegen (S002) will generate:

```python
def source_{name}(sources: dict[str, ibis.Table]) -> ibis.Table:
    """..."""
    t_hubspot = sources["hubspot"]
    t_stripe = sources["stripe"]
    # ... join logic
```

This differs from the standard `source_{name}(conn, backend)` signature.

### Table Naming in the Executor

When the executor persists an entity result, it uses the target name (line 148):
- Default: `dim_{entity_name}` (line 135)
- Snapshot: `snapshot_{entity_name}` (line 132)

So dependency entity tables will be available as `dim_{dep_name}` in the connection catalog after they execute.

### Key Observation: Source Names vs Entity Names

The `IdentityGraphConfig.sources` list has both a `name` (e.g., "hubspot") and an `entity` (e.g., "hubspot_person"). The `depends_on` list uses entity names. The generated `source_fn(sources_dict)` uses source names as dict keys. The executor needs to map source names to the correct `dim_{entity_name}` tables.

This means the executor needs access to the `IdentityGraphConfig` to know the mapping from source name -> entity name -> `dim_{entity}` table name.

## Design Decision: How the Executor Detects DerivedSource

### Option A: Add `entity: Entity | None` param to `execute()` (CHOSEN)

Add an optional `entity` parameter to `execute()`:

```python
def execute(
    self,
    entity_name: str,
    generated_dir: str | Path | None = None,
    target_name: str | None = None,
    entity: Entity | None = None,  # NEW
) -> ExecutionResult:
```

The executor checks `isinstance(entity.source, DerivedSource)` to branch.

**Pros:**
- Explicit. The caller decides what info the executor gets.
- Backward compatible. All existing calls without `entity` continue working.
- The entity carries the full `DerivedSource` (and its `identity_graph_config`) so the executor can build `sources_dict` correctly.

**Cons:**
- Slightly widens the `execute()` API.

### Option B: Inspect generated function signature (REJECTED)

Use `inspect.signature()` on the loaded `source_fn` to detect `(sources: dict)` vs `(conn, backend)`.

**Pros:** No API change to `execute()`.
**Cons:** Fragile. Relies on exact parameter naming. Cannot determine the source-name-to-entity-name mapping needed to build `sources_dict`.

### Option C: Add metadata to the generated module (REJECTED)

Add a module-level attribute like `__source_type__ = "derived"` and a `__source_mapping__ = {"hubspot": "hubspot_person", ...}`.

**Pros:** Self-contained in generated code.
**Cons:** Adds a second channel of truth alongside the entity model. More codegen surface to maintain.

**Verdict: Option A.** Passing `entity` is the minimal, explicit change. The entity already carries all the information needed.

## Implementation Plan

### 1. Executor Changes (`executor.py`)

#### 1a. Update `execute()` signature

Add `entity: Entity | None = None` parameter. Pass it through to `_run_transform_pipeline()`.

```python
def execute(
    self,
    entity_name: str,
    generated_dir: str | Path | None = None,
    target_name: str | None = None,
    entity: Entity | None = None,
) -> ExecutionResult:
    ...
    result_table, activity_row_count, analytics_row_count = (
        self._run_transform_pipeline(entity_name, module, entity=entity)
    )
```

#### 1b. Update `_run_transform_pipeline()` to accept and branch on entity

```python
def _run_transform_pipeline(
    self, entity_name: str, module: Any, *, entity: Entity | None = None
) -> tuple[ibis.Table, int | None, int | None]:
```

At the source resolution step (currently lines 198-209), add a DerivedSource branch **before** the existing logic:

```python
from fyrnheim.core.source import DerivedSource

# DerivedSource path: build sources_dict from catalog
if entity is not None and isinstance(entity.source, DerivedSource):
    source_fn = getattr(module, f"source_{entity_name}", None)
    if source_fn is None:
        raise ExecutionError(
            f"No source function for DerivedSource entity {entity_name}"
        )
    sources_dict = self._build_sources_dict(entity)
    t = source_fn(sources_dict)
else:
    # Existing path: registered source or source_fn(conn, backend)
    ...
```

#### 1c. New method: `_build_sources_dict()`

Build the `{source_name: ibis.Table}` dict from the entity's `identity_graph_config`.

```python
def _build_sources_dict(self, entity: Entity) -> dict[str, ibis.Table]:
    """Build sources dict for DerivedSource from dependency tables in catalog."""
    source = entity.source  # DerivedSource, already type-checked by caller
    config = source.identity_graph_config
    if config is None:
        raise ExecutionError(
            f"DerivedSource entity {entity.name} has no identity_graph_config"
        )

    sources_dict: dict[str, ibis.Table] = {}
    for ig_source in config.sources:
        table_name = f"dim_{ig_source.entity}"
        try:
            sources_dict[ig_source.name] = self._conn.table(table_name)
        except Exception:
            raise ExecutionError(
                f"Dependency table '{table_name}' not found for "
                f"DerivedSource entity '{entity.name}'. "
                f"Ensure '{ig_source.entity}' executes before '{entity.name}'."
            )
    return sources_dict
```

Key behaviors:
- Uses `ig_source.name` as dict key (matches generated code expectations: `sources["hubspot"]`).
- Looks up `dim_{ig_source.entity}` from the connection catalog.
- Raises clear `ExecutionError` with both the missing table name and the entity name if a dependency is missing.

#### 1d. Import addition

Add `DerivedSource` import at the top of `_run_transform_pipeline` (or at module level in the `TYPE_CHECKING` block to avoid circular imports):

```python
from fyrnheim.core.source import DerivedSource
```

### 2. Runner Changes (`runner.py`)

#### 2a. Update `run_entity()` to pass entity to `execute()`

Change line 186 from:

```python
exec_result = executor.execute(entity.name, generated_dir=gen_dir)
```

To:

```python
exec_result = executor.execute(entity.name, generated_dir=gen_dir, entity=entity)
```

This is the only runner change needed. The runner already handles:
- Topological ordering via `resolve_execution_order()`.
- Shared executor instance in `run()` (so all entities share one connection/catalog).
- Dependency failure skipping.

### 3. No Changes Needed

- **`resolution.py`**: Already handles DerivedSource dependencies correctly. `_extract_dependencies()` returns `source.depends_on`.
- **`_loader.py`**: Module loading is source-type-agnostic.
- **`errors.py`**: `ExecutionError` already exists and is sufficient.
- **Codegen**: S002 handles generating the DerivedSource source function. This story only consumes the generated code.

## Branching Logic Summary

The source resolution in `_run_transform_pipeline()` will have three paths:

| Condition | Path | What happens |
|-----------|------|-------------|
| Registered source exists (`source_{name}` in `_registered_sources`) | Existing | `conn.table(source_name)` |
| `entity` provided and `entity.source` is `DerivedSource` | **New** | `_build_sources_dict()` -> `source_fn(sources_dict)` |
| Source function exists in module | Existing | `source_fn(conn, backend)` |
| None of the above | Error | `ExecutionError` |

**Order of precedence:** The DerivedSource check should come after the registered-source check but before the standard `source_fn(conn, backend)` fallback. In practice, DerivedSource entities will never have registered parquet sources, but maintaining the precedence keeps the logic defensive.

## Error Scenarios

1. **Missing dependency table**: DerivedSource entity "person" depends on "hubspot_person" but `dim_hubspot_person` is not in the catalog. -> `ExecutionError("Dependency table 'dim_hubspot_person' not found for DerivedSource entity 'person'. Ensure 'hubspot_person' executes before 'person'.")`

2. **DerivedSource without `identity_graph_config`**: Backward compat case where `DerivedSource(identity_graph="person_graph")` has no config. -> `ExecutionError("DerivedSource entity {name} has no identity_graph_config")`. This is correct: without config, codegen produces empty string, so there is no source function to call.

3. **Entity not passed to execute()**: For backward compatibility, if `entity` is `None`, the executor falls through to the existing `source_fn(conn, backend)` path. All existing tests continue to work unchanged.

## Test Strategy

### Unit tests (in `tests/test_engine_executor.py`)

1. **DerivedSource detection and dispatch**: Create a DerivedSource entity with `identity_graph_config`, pre-populate connection catalog with dependency dim tables, generate a mock module with `source_{name}(sources)` signature. Verify `source_fn` is called with the correct dict.

2. **sources_dict construction**: Verify dict keys match `ig_source.name` and values are `conn.table(f"dim_{ig_source.entity}")`.

3. **Missing dependency table error**: Remove one dependency table from catalog. Verify `ExecutionError` with entity name in message.

4. **Non-DerivedSource backward compatibility**: Existing entity (TableSource) with `entity=entity` passed still uses `source_fn(conn, backend)` path.

5. **No entity parameter**: Existing call pattern `execute(entity_name)` without `entity` still works (default `None` -> old path).

6. **Regression**: Run all existing executor tests unchanged. They do not pass `entity`, so they hit the existing path.

### Integration tests (in `tests/test_e2e_ibis_executor.py`)

After S001 and S002 are implemented, add an E2E test that:
- Defines two TableSource entities (e.g., `hubspot_person`, `stripe_person`)
- Defines a DerivedSource entity (`person`) with `identity_graph_config` referencing both
- Generates code for all three
- Executes all three in dependency order with shared executor
- Verifies `dim_person` contains joined data

(This may live in S004 -- the E2E story.)

## Simplification Review

**What would you remove?**
- Nothing. Every piece is load-bearing: the `entity` param enables detection, `_build_sources_dict` maps source names to catalog tables, the branching keeps existing paths untouched.

**Is the change minimal and focused?**
- Yes. The change touches exactly two methods in `executor.py` (`execute()` signature, `_run_transform_pipeline()` branching), adds one private helper (`_build_sources_dict`), and updates one call site in `runner.py`. Total delta: ~30-40 lines of production code.

**Any over-engineering?**
- No new abstractions, no new classes, no protocol changes. The entity parameter is the simplest possible way to pass type information.

**Verdict: APPROVED.**

## File Change Summary

| File | Change | Lines |
|------|--------|-------|
| `src/fyrnheim/engine/executor.py` | Add `entity` param to `execute()`, DerivedSource branch in `_run_transform_pipeline()`, new `_build_sources_dict()` | ~35 |
| `src/fyrnheim/engine/runner.py` | Pass `entity=entity` to `executor.execute()` | 1 |
| `tests/test_engine_executor.py` | New test class for DerivedSource executor behavior | ~60 |
| `tests/test_e2e_ibis_executor.py` | (Deferred to S004 -- needs S001+S002 implemented first) | 0 |

## Dependencies

This story depends on:
- **M005-E003-S001**: `IdentityGraphConfig` model exists on `DerivedSource` (needed for `_build_sources_dict` to read `config.sources`).
- **M005-E003-S002**: Codegen generates `source_{name}(sources: dict)` function (needed so the executor has something to call).

Both are currently "in-progress". This story's implementation can begin once S001 lands (for the model), and can be fully tested once S002 lands (for the generated code).
