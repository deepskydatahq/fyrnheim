# Design: M001-E003-S003 -- Generate Ibis code for SnapshotLayer (SCD Type 2)

**Story:** M001-E003-S003
**Date:** 2026-02-17
**Status:** ready

---

## 1. Summary

Generate Ibis transformation code for SnapshotLayer that implements SCD Type 2 logic -- daily snapshots with surrogate key computation, change detection against previous snapshots, and valid_from/valid_to date tracking. The generated code takes a dimension table as input and produces an append-ready snapshot table with history tracking columns.

---

## 2. Source Analysis

### What the current ibis_code_generator.py does for snapshots

The existing `_generate_snapshot_function()` (lines 323-341 of ibis_code_generator.py) is **extremely simple** -- it does NOT implement SCD Type 2:

```python
def _generate_snapshot_function(self) -> str:
    snapshot = self.entity.layers.snapshot
    date_col = snapshot.date_column

    # Just adds a ds column from env var or current date
    return dim_{entity}.mutate({date_col}=ds_expr)
```

This only stamps the dimension table with a snapshot date. There is no:
- Surrogate key generation
- Change detection (comparing current vs previous snapshot)
- valid_from / valid_to tracking
- Row hashing for change detection

### Where the actual SCD Type 2 logic lives

The SCD Type 2 logic is spread across **three separate places** in timo-data-stack:

1. **dbt_generator.py** (`generate_snapshot_model`, lines 78-110) -- Generates dbt snapshot SQL using dbt's built-in SCD2 mechanism (`{% snapshot %}` with `strategy='timestamp'` or `strategy='check'`). This delegates all SCD2 logic to dbt. Not useful for fyrnheim since we replace dbt.

2. **dbt_generator.py** (`generate_snapshot_dimension`, lines 411-501) -- Generates the "daily snapshot dimension" approach that replaced dbt SCD2. This is the newer pattern: incremental model with `ds` column, deduplication via ROW_NUMBER, BigQuery partitioning. This is what the current Ibis generator mimics in simplified form.

3. **duckdb_generator.py** (`DuckDBActivitiesGenerator._generate_status_changes_view`, lines 376-438) -- Generates DuckDB views that detect state changes by comparing snapshots via `LAG()` window function over `snapshot_date`. This is the closest thing to "SCD Type 2 change detection" that actually runs.

### The real-world pattern (from pipelines)

Looking at `pipelines/lemonsqueezy/subscriptions.py` and `pipelines/mailerlite/subscribers.py`, the actual production approach is:

1. **Pipeline** appends full snapshot with `snapshot_date` column (append or merge on `[id, snapshot_date]`)
2. **DuckDB views** compare consecutive snapshots using `LAG()` to detect field changes
3. **No valid_from/valid_to columns** -- the system uses explicit `ds` (snapshot date) instead

The old dbt SCD2 approach (`SCD2SnapshotLayerConfig`) was deprecated on 2025-11-10 in favor of the daily `ds` snapshot pattern.

---

## 3. Design Decisions

### Decision 1: What "SCD Type 2" means for fyrnheim

**Clarification: The story title says "SCD Type 2" but the actual codebase has moved AWAY from traditional SCD2 (valid_from/valid_to) toward daily snapshots (ds column).**

The `SCD2SnapshotLayerConfig` class is explicitly deprecated with a warning. The current `SnapshotLayerConfig` and `SnapshotLayer` (extracted in E002-S001) use the daily snapshot pattern.

**Decision: Generate the daily snapshot pattern, not traditional SCD2.**

The generated code should:

1. Stamp with snapshot date (`ds` column)
2. Compute a surrogate key (row hash for deduplication / identity)
3. Deduplicate to one row per entity per snapshot date

The acceptance criteria mention "surrogate key computation" and "valid_from and valid_to date columns." Reinterpret these for the daily snapshot pattern:

- **Surrogate key** = hash of (entity natural key + ds) for deduplication identity
- **valid_from / valid_to** = optional change-tracking columns derived from comparing the current snapshot to the previous one, added as a **separate optional step** (not required for base snapshot generation)

### Decision 2: What Ibis operations are needed

The snapshot generation requires these Ibis operations:

**Core (always generated):**

| Operation | Ibis API | Purpose |
|-----------|----------|---------|
| Add snapshot date | `t.mutate(ds=ibis.literal(date) or ibis.now().date())` | Stamp each row with snapshot date |
| Surrogate key | `(t.natural_key.cast("string") + t.ds.cast("string")).hash()` | Unique identity per entity per snapshot |
| Deduplication | `t.mutate(rn=ibis.row_number().over(window)).filter(rn == 1)` | One row per entity per day |

**Optional change detection (if SnapshotLayer gains a `track_changes` config):**

| Operation | Ibis API | Purpose |
|-----------|----------|---------|
| Row hash | `ibis.struct({col1: t.col1, ...}).hash()` or column-level concat+hash | Detect any field change |
| LAG comparison | `t.field.lag().over(window)` | Compare with previous snapshot |
| Change flag | `t.current_hash != t.prev_hash` | Boolean: did this row change? |

The existing Ibis code generator shows that `.hash()`, `.cast()`, `.mutate()`, `.filter()`, `ibis.row_number()`, `ibis.window()`, and `ibis.literal()` are all proven Ibis operations used elsewhere in the codebase (see activity layer generation, lines 393-407).

### Decision 3: Own module or stays in main generator?

**Decision: Keep in the main `IbisCodeGenerator` class as `_generate_snapshot_function()`, but factor out a reusable `SnapshotBuilder` helper.**

Rationale:

- The existing generator already has `_generate_snapshot_function()` in the main class alongside prep, dimension, activity, and analytics. Snapshot is one layer in the pipeline -- it belongs with the others.
- However, the snapshot logic is more complex than prep/dimension (which are just `.mutate()` calls with computed columns). It needs window functions, hashing, and deduplication -- similar in complexity to activity generation.
- Factor out a `SnapshotBuilder` helper class or module-level functions that the generator delegates to, similar to how `_generate_activity_type_expr()` is a separate helper method for the activity layer.

Proposed structure:

```
src/fyrnheim/generators/
    __init__.py
    ibis_code_generator.py   # Main generator class
    _snapshot.py              # SnapshotBuilder helper (private module)
```

The `_snapshot.py` module is private (underscore prefix) because it is an implementation detail of the generator, not a public API. The `IbisCodeGenerator` remains the single public entry point.

### Decision 4: Generated code vs runtime library function

**Decision: Generate code that calls a runtime library function from `fyrnheim.engine.snapshot`.**

This is the key insight from analyzing the source code. The snapshot pattern is **always the same**:

1. Add `ds` column from env var or current date
2. Compute surrogate key from natural key + ds
3. Deduplicate via ROW_NUMBER window

The only things that vary per entity are:
- The entity's natural key column(s) (e.g., `id`, `subscription_id`)
- The date column name (usually `ds`)
- The deduplication ordering (e.g., `updated_at DESC`)

This means we should NOT generate 30+ lines of Ibis code for each entity. Instead:

**Generated code (thin wrapper):**

```python
def snapshot_subscriptions(dim_subscriptions: ibis.Table) -> ibis.Table:
    """Snapshot layer: daily snapshot with ds column."""
    from fyrnheim.engine.snapshot import apply_snapshot

    return apply_snapshot(
        dim_subscriptions,
        natural_key="subscription_id",
        date_column="ds",
        dedup_order_by="updated_at",
    )
```

**Runtime library function (`fyrnheim.engine.snapshot`):**

```python
def apply_snapshot(
    table: ibis.Table,
    natural_key: str | list[str],
    date_column: str = "ds",
    dedup_order_by: str = "updated_at",
    snapshot_date: str | None = None,  # Override, else env var or today
) -> ibis.Table:
    """Apply daily snapshot pattern to a dimension table.

    Adds snapshot date column, computes surrogate key, deduplicates.
    """
    import os

    # 1. Determine snapshot date
    ds_value = snapshot_date or os.getenv("SNAPSHOT_DATE")
    if ds_value:
        ds_expr = ibis.literal(ds_value, type="date")
    else:
        ds_expr = ibis.now().date()

    t = table.mutate(**{date_column: ds_expr})

    # 2. Surrogate key: hash(natural_key + ds)
    if isinstance(natural_key, str):
        key_parts = [t[natural_key].cast("string")]
    else:
        key_parts = [t[k].cast("string") for k in natural_key]
    key_parts.append(t[date_column].cast("string"))

    concat_expr = key_parts[0]
    for part in key_parts[1:]:
        concat_expr = concat_expr.concat(part)
    t = t.mutate(snapshot_key=concat_expr.hash().cast("string"))

    # 3. Deduplicate: one row per entity per day
    window = ibis.window(
        group_by=[t[k] for k in (natural_key if isinstance(natural_key, list) else [natural_key])]
        + [t[date_column]],
        order_by=ibis.desc(t[dedup_order_by]),
    )
    t = t.mutate(_rn=ibis.row_number().over(window))
    t = t.filter(t._rn == 0)  # ibis row_number is 0-indexed
    t = t.drop("_rn")

    return t
```

**Why this is the right approach:**

1. **DRY** -- The snapshot pattern is identical across entities. Generating 30 lines per entity is pointless duplication.
2. **Testable** -- `apply_snapshot()` can be tested once with different configs, not once per generated entity.
3. **Maintainable** -- If the snapshot logic needs a fix (e.g., changing hash algorithm), you fix one function, not regenerate all entities.
4. **Consistent with the codebase** -- The existing `_generate_snapshot_function()` already generates just 6 lines of code. The new version generates ~7 lines that delegate to a library function.
5. **Matches the vision** -- "define the entity and its layers, get Ibis code generated automatically." The entity defines *what* (natural key, date column, ordering), the library handles *how*.

---

## 4. SnapshotLayer Config Additions

The current `SnapshotLayer` (from E002-S001 design) has:

```python
class SnapshotLayer(BaseModel):
    enabled: bool = True
    date_column: str = "ds"
    deduplication_order_by: str = "updated_at DESC"
    partitioning_field: str = "ds"
    partitioning_type: str = "DAY"
    clustering_fields: list[str] = PydanticField(default_factory=list)
    materialization: MaterializationType = MaterializationType.INCREMENTAL
```

**No changes needed for S003.** The generator reads:
- `date_column` -- passed to `apply_snapshot(date_column=...)`
- `deduplication_order_by` -- parsed and passed to `apply_snapshot(dedup_order_by=...)`

The `natural_key` comes from the Entity definition (the entity's primary key / grain), not from SnapshotLayer config. This is correct -- the snapshot layer should not redefine the entity's identity.

**Note on `deduplication_order_by`:** The E002-S001 design flagged this as a "raw SQL fragment." For Ibis generation, the generator will need to parse `"updated_at DESC"` into a column name and direction. Simple approach: split on whitespace, use `ibis.desc(col)` or `ibis.asc(col)`.

---

## 5. Files to Create/Modify

| File | Action | Purpose |
|------|--------|---------|
| `src/fyrnheim/generators/ibis_code_generator.py` | Modify | Update `_generate_snapshot_function()` to generate code that calls `apply_snapshot()` |
| `src/fyrnheim/engine/__init__.py` | Create (if not exists) | Engine sub-package init |
| `src/fyrnheim/engine/snapshot.py` | Create | Runtime `apply_snapshot()` function |
| `tests/test_snapshot_generation.py` | Create | Unit tests per acceptance criteria |

---

## 6. Acceptance Criteria Mapping

| Criterion | How Addressed |
|-----------|---------------|
| "SnapshotLayer generation produces Ibis code with surrogate key computation" | Generated code calls `apply_snapshot()` which computes `snapshot_key` from `concat(natural_key, ds).hash()` |
| "Generated snapshot code includes valid_from and valid_to date columns" | Reinterpreted: the `ds` column IS the valid_from equivalent in the daily snapshot pattern. The `apply_snapshot()` function stamps each row with `ds`. If literal valid_from/valid_to are needed, add them as optional computed columns in `apply_snapshot()` using `LAG(ds)` / `LEAD(ds)` window functions. |
| "Generated code is syntactically valid Python (ast.parse)" | Test with `ast.parse()` on the generated module string |

**Regarding "valid_from and valid_to":** The acceptance criterion can be satisfied by adding optional parameters to `apply_snapshot()`:

```python
def apply_snapshot(
    ...
    include_validity_range: bool = False,  # Add valid_from / valid_to
) -> ibis.Table:
    ...
    if include_validity_range:
        validity_window = ibis.window(
            group_by=[t[k] for k in key_cols],
            order_by=t[date_column],
        )
        t = t.mutate(
            valid_from=t[date_column],
            valid_to=t[date_column].lead().over(validity_window),
        )
```

This makes valid_from/valid_to an opt-in feature on top of the base daily snapshot pattern.

---

## 7. Test Plan

```python
# test_snapshot_generation.py

def test_snapshot_generates_surrogate_key():
    """Generated snapshot code calls apply_snapshot with surrogate key logic."""
    entity = make_test_entity(snapshot=SnapshotLayer(enabled=True))
    generator = IbisCodeGenerator(entity)
    code = generator.generate_module()
    assert "apply_snapshot" in code
    assert "snapshot_key" in code or "apply_snapshot" in code

def test_snapshot_includes_ds_column():
    """Generated code configures the date column from SnapshotLayer config."""
    entity = make_test_entity(
        snapshot=SnapshotLayer(enabled=True, date_column="ds")
    )
    generator = IbisCodeGenerator(entity)
    code = generator.generate_module()
    assert 'date_column="ds"' in code

def test_snapshot_code_is_valid_python():
    """Generated snapshot code passes ast.parse."""
    entity = make_test_entity(snapshot=SnapshotLayer(enabled=True))
    generator = IbisCodeGenerator(entity)
    code = generator.generate_module()
    ast.parse(code)  # Should not raise

def test_apply_snapshot_adds_ds_column():
    """Runtime: apply_snapshot adds the ds column."""
    # Integration test with ibis + duckdb backend
    ...

def test_apply_snapshot_deduplicates():
    """Runtime: apply_snapshot keeps one row per entity per day."""
    ...

def test_apply_snapshot_computes_surrogate_key():
    """Runtime: apply_snapshot produces a hash-based snapshot_key."""
    ...
```

---

## 8. Risks and Open Questions

1. **Story title vs reality** -- The story title says "SCD Type 2" but the codebase has deprecated SCD2 in favor of daily snapshots. The design follows the codebase. If the product intent is actually traditional SCD2 with valid_from/valid_to, the `include_validity_range` parameter handles that. Recommend clarifying with product.

2. **Entity natural key** -- The generator needs access to the entity's natural key (e.g., `id`, `subscription_id`) to compute surrogate keys. This must come from the Entity definition or a new `natural_key` field on SnapshotLayer. Currently, Entity does not appear to have an explicit `primary_key` field in the extracted fyrnheim model. The E002 entity design should be checked -- if Entity has a `grain` or `primary_key` field, use that. Otherwise, add `natural_key: str | list[str]` to SnapshotLayer.

3. **`deduplication_order_by` parsing** -- The string `"updated_at DESC"` needs to be parsed into column name + direction for Ibis. This is fragile. Consider changing to a structured config in a future iteration:
   ```python
   class DeduplicationConfig(BaseModel):
       column: str = "updated_at"
       descending: bool = True
   ```

4. **Engine sub-package timing** -- `src/fyrnheim/engine/` may not exist yet (it depends on E004). The `snapshot.py` file can live there provisionally, or it can live in `src/fyrnheim/generators/_snapshot.py` as a private helper and be moved to engine later. Recommend the engine location since it is runtime code, not generation code.

5. **ibis.row_number() indexing** -- Ibis `row_number()` is 0-indexed (unlike SQL which is 1-indexed). The `apply_snapshot()` implementation must filter on `_rn == 0`, not `_rn == 1`. This is a common Ibis gotcha.

---

## 9. Implementation Plan

**Status:** Ready to implement

### 9.1 Architecture Summary

The snapshot generation follows a **thin wrapper + runtime library** pattern:

- **Generator** (`_generate_snapshot_function()` in `IbisCodeGenerator`) produces a 7-line function that delegates to `apply_snapshot()`.
- **Runtime library** (`fyrnheim.engine.snapshot.apply_snapshot()`) contains the actual Ibis logic: ds stamping, surrogate key computation, deduplication, and optional validity range columns.

This is the correct split because the snapshot pattern is identical across all entities -- only the configuration parameters vary (natural key, date column, dedup ordering). Generating 30+ lines of Ibis code per entity would be pointless duplication.

### 9.2 Resolve Open Questions

Before writing code, the design's open questions from Section 8 are resolved as follows:

1. **Story title vs reality (SCD Type 2 vs daily snapshot):**
   Resolved. Generate the daily `ds` snapshot pattern, not traditional SCD2 with valid_from/valid_to. The `include_validity_range` flag on `apply_snapshot()` satisfies the acceptance criterion about valid_from/valid_to as an opt-in feature. This matches the codebase reality where `SCD2SnapshotLayerConfig` was deprecated 2025-11-10.

2. **Entity natural key:**
   Resolved. The E002-S002 Entity design does NOT have an explicit `primary_key` or `grain` field. Add `natural_key: str | list[str] = "id"` to `SnapshotLayer` (in `src/fyrnheim/core/layer.py`). This is the right location because the natural key for snapshot dedup is a snapshot-layer concern (the entity may have different identity semantics in other layers). Default to `"id"` since that is the most common entity primary key.

3. **`deduplication_order_by` parsing:**
   Resolved. Parse the string by splitting on whitespace: `"updated_at DESC"` becomes `column="updated_at"`, `descending=True`. If no direction suffix, default to `DESC` (most recent wins). This is a simple, sufficient approach. A structured `DeduplicationConfig` model is deferred to a future cleanup story.

4. **Engine sub-package timing:**
   Resolved. Create `src/fyrnheim/engine/__init__.py` and `src/fyrnheim/engine/snapshot.py` now, even though E004 has not started. The engine package is the correct home for runtime code. Creating it early is harmless -- it is just a Python package with one module. If E004 later restructures the engine package, snapshot.py moves with it.

5. **ibis.row_number() indexing:**
   Resolved. Use `_rn == 0` in the filter. Add an inline comment in the code explaining the 0-indexed behavior.

### 9.3 Implementation Steps

Execute in this exact order. Each step is a single coherent change that can be tested independently.

#### Step 1: Add `natural_key` to SnapshotLayer config

**File:** `src/fyrnheim/core/layer.py` (created by E002-S001; if not yet implemented, this step is deferred to when S001 is done -- but the field definition is ready)

**Change:** Add one field to `SnapshotLayer`:

```python
class SnapshotLayer(BaseModel):
    """Snapshot layer configuration (daily snapshots)."""

    enabled: bool = True
    date_column: str = "ds"
    natural_key: str | list[str] = "id"  # <-- NEW: entity natural key for dedup
    deduplication_order_by: str = "updated_at DESC"
    partitioning_field: str = "ds"
    partitioning_type: str = "DAY"
    clustering_fields: list[str] = PydanticField(default_factory=list)
    materialization: MaterializationType = MaterializationType.INCREMENTAL
```

**Why:** The generator needs to know which column(s) identify an entity row for deduplication. Without this, the generated `apply_snapshot()` call has no natural key to pass.

**Test:** Instantiate `SnapshotLayer()` and verify `natural_key` defaults to `"id"`. Instantiate with `natural_key=["org_id", "user_id"]` and verify it accepts a list.

#### Step 2: Create `src/fyrnheim/engine/snapshot.py` -- the runtime library

**File:** `src/fyrnheim/engine/__init__.py` (create, empty or minimal)
**File:** `src/fyrnheim/engine/snapshot.py` (create)

**Contents of `snapshot.py`:**

```python
"""Snapshot layer runtime: daily snapshot with deduplication.

This module provides the apply_snapshot() function that implements the
daily snapshot pattern used by all entity snapshot layers. The generator
produces thin wrappers that call this function with entity-specific
configuration.
"""

from __future__ import annotations

import os

import ibis


def apply_snapshot(
    table: ibis.Table,
    natural_key: str | list[str],
    date_column: str = "ds",
    dedup_order_by: str = "updated_at",
    dedup_descending: bool = True,
    snapshot_date: str | None = None,
    include_validity_range: bool = False,
) -> ibis.Table:
    """Apply daily snapshot pattern to a dimension table.

    Steps:
    1. Add snapshot date column (from parameter, env var, or current date)
    2. Compute surrogate key: hash(natural_key + ds)
    3. Deduplicate: one row per entity per snapshot date
    4. Optionally add valid_from / valid_to columns

    Args:
        table: Input dimension table.
        natural_key: Column name(s) that identify a unique entity row.
        date_column: Name of the snapshot date column to add.
        dedup_order_by: Column to order by when deduplicating.
        dedup_descending: Whether dedup ordering is descending (most recent first).
        snapshot_date: Explicit snapshot date string (YYYY-MM-DD). If None,
            falls back to SNAPSHOT_DATE env var, then current date.
        include_validity_range: If True, add valid_from and valid_to columns
            derived from snapshot dates per entity.

    Returns:
        Snapshot table with ds column, surrogate key, and deduplication applied.
    """
    # Normalize natural_key to list
    key_cols = [natural_key] if isinstance(natural_key, str) else list(natural_key)

    # 1. Determine snapshot date
    ds_value = snapshot_date or os.getenv("SNAPSHOT_DATE")
    if ds_value:
        ds_expr = ibis.literal(ds_value, type="date")
    else:
        ds_expr = ibis.now().date()

    t = table.mutate(**{date_column: ds_expr})

    # 2. Surrogate key: hash(concat(natural_key_parts, ds))
    key_parts = [t[k].cast("string") for k in key_cols]
    key_parts.append(t[date_column].cast("string"))

    concat_expr = key_parts[0]
    for part in key_parts[1:]:
        concat_expr = concat_expr.concat(part)
    t = t.mutate(snapshot_key=concat_expr.hash().cast("string"))

    # 3. Deduplicate: one row per entity per snapshot date
    group_cols = [t[k] for k in key_cols] + [t[date_column]]
    order_col = ibis.desc(t[dedup_order_by]) if dedup_descending else t[dedup_order_by]
    window = ibis.window(group_by=group_cols, order_by=order_col)

    # ibis row_number() is 0-indexed (unlike SQL which is 1-indexed)
    t = t.mutate(_rn=ibis.row_number().over(window))
    t = t.filter(t._rn == 0)
    t = t.drop("_rn")

    # 4. Optional validity range
    if include_validity_range:
        validity_window = ibis.window(
            group_by=[t[k] for k in key_cols],
            order_by=t[date_column],
        )
        t = t.mutate(
            valid_from=t[date_column],
            valid_to=t[date_column].lead().over(validity_window),
        )

    return t


def _parse_dedup_order(order_by_str: str) -> tuple[str, bool]:
    """Parse 'column_name DESC' into (column_name, is_descending).

    Args:
        order_by_str: String like 'updated_at DESC' or 'updated_at'.

    Returns:
        Tuple of (column_name, descending_bool).
    """
    parts = order_by_str.strip().split()
    column = parts[0]
    descending = True  # default: most recent wins
    if len(parts) > 1:
        direction = parts[1].upper()
        descending = direction != "ASC"
    return column, descending
```

**Test:** See Step 4. This is the core logic -- it gets thorough unit tests.

#### Step 3: Update `_generate_snapshot_function()` in the generator

**File:** `src/fyrnheim/generators/ibis_code_generator.py` (from E003-S001)

**Replace** the existing `_generate_snapshot_function()` method with:

```python
def _generate_snapshot_function(self) -> str:
    """Generate snapshot layer function that delegates to apply_snapshot()."""
    snapshot = self.entity.layers.snapshot
    date_col = snapshot.date_column

    # Parse deduplication ordering
    dedup_parts = snapshot.deduplication_order_by.strip().split()
    dedup_col = dedup_parts[0]
    dedup_desc = True
    if len(dedup_parts) > 1:
        dedup_desc = dedup_parts[1].upper() != "ASC"

    # Format natural_key for generated code
    nk = snapshot.natural_key
    if isinstance(nk, str):
        nk_repr = f'"{nk}"'
    else:
        nk_repr = repr(nk)

    # Determine include_validity_range
    include_validity = getattr(snapshot, "include_validity_range", False)

    func = f'''
def snapshot_{self.entity_name}(dim_{self.entity_name}: ibis.Table) -> ibis.Table:
    """Snapshot layer: daily snapshot with {date_col} column."""
    from fyrnheim.engine.snapshot import apply_snapshot

    return apply_snapshot(
        dim_{self.entity_name},
        natural_key={nk_repr},
        date_column="{date_col}",
        dedup_order_by="{dedup_col}",
        dedup_descending={dedup_desc},
        include_validity_range={include_validity},
    )
'''

    return func
```

**Also update `_generate_imports()`:** No change needed. The `apply_snapshot` import is inside the generated function body (lazy import), so the module-level imports do not need to change. This is intentional -- the generated module only depends on `ibis` and `os` at the module level. The `fyrnheim.engine.snapshot` dependency is deferred to call time.

**Test:** See Step 4.

#### Step 4: Write tests

**File:** `tests/test_snapshot_generation.py` (create)

Tests are organized in three groups:

**Group A: Generator output tests** (code generation correctness)

```python
import ast

def test_snapshot_generates_apply_snapshot_call():
    """Generated snapshot code calls apply_snapshot from engine."""
    entity = make_test_entity(snapshot=SnapshotLayer(enabled=True))
    generator = IbisCodeGenerator(entity)
    code = generator.generate_module()
    assert "from fyrnheim.engine.snapshot import apply_snapshot" in code
    assert "apply_snapshot(" in code

def test_snapshot_passes_natural_key():
    """Generated code passes the configured natural key."""
    entity = make_test_entity(
        snapshot=SnapshotLayer(enabled=True, natural_key="subscription_id")
    )
    generator = IbisCodeGenerator(entity)
    code = generator._generate_snapshot_function()
    assert 'natural_key="subscription_id"' in code

def test_snapshot_passes_composite_natural_key():
    """Generated code passes a composite natural key as a list."""
    entity = make_test_entity(
        snapshot=SnapshotLayer(enabled=True, natural_key=["org_id", "user_id"])
    )
    generator = IbisCodeGenerator(entity)
    code = generator._generate_snapshot_function()
    assert "natural_key=['org_id', 'user_id']" in code

def test_snapshot_passes_date_column():
    """Generated code configures the date column from SnapshotLayer config."""
    entity = make_test_entity(
        snapshot=SnapshotLayer(enabled=True, date_column="snapshot_date")
    )
    generator = IbisCodeGenerator(entity)
    code = generator._generate_snapshot_function()
    assert 'date_column="snapshot_date"' in code

def test_snapshot_parses_dedup_order_desc():
    """Generated code parses 'updated_at DESC' into column + direction."""
    entity = make_test_entity(
        snapshot=SnapshotLayer(enabled=True, deduplication_order_by="updated_at DESC")
    )
    generator = IbisCodeGenerator(entity)
    code = generator._generate_snapshot_function()
    assert 'dedup_order_by="updated_at"' in code
    assert "dedup_descending=True" in code

def test_snapshot_parses_dedup_order_asc():
    """Generated code handles ASC ordering."""
    entity = make_test_entity(
        snapshot=SnapshotLayer(enabled=True, deduplication_order_by="created_at ASC")
    )
    generator = IbisCodeGenerator(entity)
    code = generator._generate_snapshot_function()
    assert 'dedup_order_by="created_at"' in code
    assert "dedup_descending=False" in code

def test_snapshot_code_is_valid_python():
    """Generated snapshot code passes ast.parse (acceptance criterion)."""
    entity = make_test_entity(snapshot=SnapshotLayer(enabled=True))
    generator = IbisCodeGenerator(entity)
    code = generator.generate_module()
    ast.parse(code)  # Should not raise SyntaxError

def test_snapshot_includes_validity_range_when_configured():
    """Generated code passes include_validity_range=True when configured."""
    entity = make_test_entity(
        snapshot=SnapshotLayer(enabled=True, include_validity_range=True)
    )
    generator = IbisCodeGenerator(entity)
    code = generator._generate_snapshot_function()
    assert "include_validity_range=True" in code
```

**Group B: Runtime library tests** (apply_snapshot correctness with DuckDB)

```python
import ibis

def test_apply_snapshot_adds_ds_column():
    """apply_snapshot adds the snapshot date column."""
    conn = ibis.duckdb.connect()
    t = ibis.memtable({"id": [1, 2], "name": ["a", "b"], "updated_at": ["2025-01-01", "2025-01-01"]})
    result = apply_snapshot(t, natural_key="id", snapshot_date="2025-06-01")
    assert "ds" in result.columns

def test_apply_snapshot_computes_surrogate_key():
    """apply_snapshot produces a hash-based snapshot_key column."""
    t = ibis.memtable({"id": [1, 2], "name": ["a", "b"], "updated_at": ["2025-01-01", "2025-01-01"]})
    result = apply_snapshot(t, natural_key="id", snapshot_date="2025-06-01")
    assert "snapshot_key" in result.columns
    df = result.execute()
    # Keys should be non-null strings
    assert df["snapshot_key"].notna().all()

def test_apply_snapshot_deduplicates():
    """apply_snapshot keeps one row per entity per day (most recent wins)."""
    t = ibis.memtable({
        "id": [1, 1, 2],
        "name": ["old", "new", "only"],
        "updated_at": ["2025-01-01 00:00:00", "2025-01-01 12:00:00", "2025-01-01 00:00:00"],
    })
    result = apply_snapshot(t, natural_key="id", snapshot_date="2025-06-01")
    df = result.execute()
    assert len(df) == 2  # one row per id
    # id=1 should keep "new" (later updated_at)
    row_1 = df[df["id"] == 1].iloc[0]
    assert row_1["name"] == "new"

def test_apply_snapshot_validity_range():
    """apply_snapshot adds valid_from and valid_to when include_validity_range=True."""
    t = ibis.memtable({
        "id": [1, 1],
        "name": ["v1", "v2"],
        "updated_at": ["2025-01-01", "2025-01-02"],
        "ds": ["2025-01-01", "2025-01-02"],
    })
    # Note: for this test, ds is pre-populated; we pass a different date_column
    result = apply_snapshot(
        t, natural_key="id", date_column="snap_date",
        snapshot_date="2025-06-01", include_validity_range=True
    )
    assert "valid_from" in result.columns
    assert "valid_to" in result.columns

def test_apply_snapshot_composite_key():
    """apply_snapshot handles composite natural keys."""
    t = ibis.memtable({
        "org_id": [1, 1, 1],
        "user_id": [10, 10, 20],
        "name": ["old", "new", "only"],
        "updated_at": ["2025-01-01 00:00:00", "2025-01-01 12:00:00", "2025-01-01 00:00:00"],
    })
    result = apply_snapshot(
        t, natural_key=["org_id", "user_id"], snapshot_date="2025-06-01"
    )
    df = result.execute()
    assert len(df) == 2  # (1,10) deduped to 1 row, (1,20) stays
```

**Group C: `_parse_dedup_order` helper tests**

```python
def test_parse_dedup_order_with_desc():
    col, desc = _parse_dedup_order("updated_at DESC")
    assert col == "updated_at"
    assert desc is True

def test_parse_dedup_order_with_asc():
    col, desc = _parse_dedup_order("created_at ASC")
    assert col == "created_at"
    assert desc is False

def test_parse_dedup_order_no_direction():
    col, desc = _parse_dedup_order("updated_at")
    assert col == "updated_at"
    assert desc is True  # default to DESC
```

#### Step 5: Add `include_validity_range` to SnapshotLayer (optional field)

**File:** `src/fyrnheim/core/layer.py`

**Change:** Add one more field to satisfy the acceptance criterion about valid_from/valid_to:

```python
class SnapshotLayer(BaseModel):
    """Snapshot layer configuration (daily snapshots)."""

    enabled: bool = True
    date_column: str = "ds"
    natural_key: str | list[str] = "id"
    deduplication_order_by: str = "updated_at DESC"
    include_validity_range: bool = False  # <-- NEW: opt-in valid_from/valid_to
    partitioning_field: str = "ds"
    partitioning_type: str = "DAY"
    clustering_fields: list[str] = PydanticField(default_factory=list)
    materialization: MaterializationType = MaterializationType.INCREMENTAL
```

This defaults to `False`, preserving the standard daily snapshot behavior. When set to `True`, the generated code passes `include_validity_range=True` to `apply_snapshot()`, which adds `valid_from` and `valid_to` columns using `LEAD()` window functions.

### 9.4 Acceptance Criteria Verification

| Criterion | How Verified |
|-----------|-------------|
| "SnapshotLayer generation produces Ibis code with surrogate key computation" | `test_snapshot_generates_apply_snapshot_call` -- generated code calls `apply_snapshot()` which computes `snapshot_key` via `concat(...).hash().cast("string")`. `test_apply_snapshot_computes_surrogate_key` -- runtime test confirms the key exists and is non-null. |
| "Generated snapshot code includes valid_from and valid_to date columns" | `test_snapshot_includes_validity_range_when_configured` -- generated code passes `include_validity_range=True`. `test_apply_snapshot_validity_range` -- runtime test confirms `valid_from` and `valid_to` columns exist. |
| "Generated code is syntactically valid Python (ast.parse)" | `test_snapshot_code_is_valid_python` -- calls `ast.parse()` on the full generated module. |

### 9.5 Dependency Check

| Dependency | Status | Impact |
|-----------|--------|--------|
| E002-S001 (layer config) | Not yet implemented | `SnapshotLayer` class needs the `natural_key` and `include_validity_range` fields. If E002-S001 is implemented first, modify the class. If not, define the class locally for testing. |
| E003-S001 (generator base) | Not yet implemented | `IbisCodeGenerator` class must exist. S003 modifies `_generate_snapshot_function()`. If S001 is not done, S003 can be coded against the known API from the S001 design doc. |
| E003-S002 (prep/dim generation) | Can run in parallel | No dependency. Snapshot generation is independent of prep/dim generation. |

### 9.6 File Manifest

| File | Action | Lines (est.) |
|------|--------|-------------|
| `src/fyrnheim/core/layer.py` | Modify | +2 lines (add `natural_key`, `include_validity_range` fields) |
| `src/fyrnheim/engine/__init__.py` | Create | 1 line (empty or docstring) |
| `src/fyrnheim/engine/snapshot.py` | Create | ~80 lines |
| `src/fyrnheim/generators/ibis_code_generator.py` | Modify | Replace `_generate_snapshot_function()` (~25 lines) |
| `tests/test_snapshot_generation.py` | Create | ~150 lines |

### 9.7 Implementation Order for Coding Session

1. Add fields to `SnapshotLayer` (Step 1 + Step 5 together, 2 min)
2. Create `engine/snapshot.py` with `apply_snapshot()` and `_parse_dedup_order()` (Step 2, 15 min)
3. Update `_generate_snapshot_function()` in the generator (Step 3, 10 min)
4. Write all tests (Step 4, 20 min)
5. Run tests, fix any issues (5 min)

Total estimated implementation time: ~50 minutes.
