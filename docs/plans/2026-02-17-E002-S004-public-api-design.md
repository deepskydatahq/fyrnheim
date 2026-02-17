# Design: M001-E002-S004 -- Wire up model_rebuild and public API exports

**Date:** 2026-02-17
**Story:** M001-E002-S004
**Beads task:** typedata-88g
**Status:** Ready

---

## 1. Context

This story is the integration capstone for the entity model epic (M001-E002). All
predecessor stories (S001-S005 from E001 for package scaffolding, S001-S003 from
E002 for entity/layers/source-mapping) will have created the individual modules.
This story wires them together: resolve Pydantic forward references via
`model_rebuild()`, then re-export the public API from the top-level
`typedata/__init__.py`.

The reference implementation is `timo-data-stack/metadata/__init__.py`, which
imports everything, calls `model_rebuild()`, then imports concrete entity
instances. Typedata is a generic framework -- we drop the concrete entities and
the entity registry, keeping only the type system and re-exports.

---

## 2. Decision 1: Public API Surface

### Principle

Export everything a user needs to **define** an entity, its layers, sources,
quality checks, computed columns, and primitives. Do NOT export internal
implementation details, base classes that users never instantiate directly, or
timo-data-stack-specific concrete entities.

### Tier 1 -- Core Entity Definition (always needed)

These are the symbols a user touches in every entity definition file:

| Symbol | Module | Purpose |
|--------|--------|---------|
| `Entity` | `typedata.core` | The central model |
| `LayersConfig` | `typedata.core` | Container for layer configuration |
| `Source` | `typedata.core` | Union type alias for all source configs |
| `Field` | `typedata.core` | Field definition in source/entity contract |

### Tier 2 -- Layer Configuration

| Symbol | Module | Purpose |
|--------|--------|---------|
| `PrepLayer` | `typedata.core` | Prep/staging layer |
| `DimensionLayer` | `typedata.core` | Dimension layer (Type 1/2 SCD) |
| `SnapshotLayer` | `typedata.core` | Daily snapshot layer |
| `ActivityLayer` | `typedata.core` | Legacy dbt-based activity layer |
| `ActivityConfig` | `typedata.core` | Ibis-based activity config |
| `ActivityType` | `typedata.core` | Activity type definition |
| `AnalyticsLayer` | `typedata.core` | Analytics aggregation layer |
| `AnalyticsMetric` | `typedata.core` | Metric within analytics layer |
| `AnalyticsModel` | `typedata.core` | Combines analytics layers |
| `AnalyticsSource` | `typedata.core` | Reference to entity analytics |
| `ComputedMetric` | `typedata.core` | Computed from combined analytics |

### Tier 3 -- Source Configuration

| Symbol | Module | Purpose |
|--------|--------|---------|
| `BigQuerySource` | `typedata.core` | Standard BQ table source |
| `DerivedSource` | `typedata.core` | Identity-graph derived source |
| `DerivedEntitySource` | `typedata.core` | Derived entity source |
| `AggregationSource` | `typedata.core` | Entity-to-entity aggregation |
| `EventAggregationSource` | `typedata.core` | Event stream aggregation |
| `UnionSource` | `typedata.core` | Multi-source union |
| `BaseSourceConfig` | `typedata.core` | Base for BQ/DuckDB sources |
| `SourceOverrides` | `typedata.core` | Type casts, renames, etc. |
| `TypeCast` | `typedata.core` | Type cast helper |
| `Rename` | `typedata.core` | Column rename helper |
| `Divide` | `typedata.core` | Divide-by-constant helper |
| `Multiply` | `typedata.core` | Multiply-by-constant helper |
| `SignalSourceConfig` | `typedata.core` | Signal source within union |
| `ProductSourceConfig` | `typedata.core` | Product source within union |
| `ProductUnionSource` | `typedata.core` | Multi-product union |
| `AnonSourceConfig` | `typedata.core` | Anonymous visitor source |

**Decision on timo-specific source types:** Keep `SignalSourceConfig`,
`ProductSourceConfig`, `ProductUnionSource`, `AnonSourceConfig`, and
`EventAggregationSource` in the initial extraction. These are concrete patterns
that demonstrate what the framework supports. If they become a maintenance burden
we can move them to a `typedata.contrib` module later. The alternative (stripping
them now) risks breaking the extraction goal of "same behavior, new package."

### Tier 4 -- Enums and Types

| Symbol | Module | Purpose |
|--------|--------|---------|
| `MaterializationType` | `typedata.core` | TABLE, VIEW, INCREMENTAL, etc. |
| `IncrementalStrategy` | `typedata.core` | Merge, append, etc. |
| `SourcePriority` | `typedata.core` | Source ordering |

### Tier 5 -- Source Mapping and Tenant Extension

| Symbol | Module | Purpose |
|--------|--------|---------|
| `SourceMapping` | `typedata.core` | Entity-to-source bridge |
| `TenantExtension` | `typedata.core` | Tenant-specific overrides |

### Tier 6 -- Components (multi-column patterns)

| Symbol | Module | Purpose |
|--------|--------|---------|
| `ComputedColumn` | `typedata.components` | Single computed column |
| `Measure` | `typedata.components` | Aggregation measure |
| `LifecycleFlags` | `typedata.components` | Lifecycle boolean flags |
| `TimeBasedMetrics` | `typedata.components` | Time-based metric columns |
| `DataQualityChecks` | `typedata.components` | Quality check component |

### Tier 7 -- Quality Framework

| Symbol | Module | Purpose |
|--------|--------|---------|
| `QualityConfig` | `typedata.quality` | Quality check container |
| `QualityCheck` | `typedata.quality` | Base check class |
| `NotNull` | `typedata.quality` | Not-null check |
| `NotEmpty` | `typedata.quality` | Not-empty check |
| `InRange` | `typedata.quality` | Range check |
| `InSet` | `typedata.quality` | Set membership check |
| `MatchesPattern` | `typedata.quality` | Regex pattern check |
| `ForeignKey` | `typedata.quality` | Foreign key check |
| `Unique` | `typedata.quality` | Uniqueness check |
| `MaxAge` | `typedata.quality` | Freshness check |
| `CustomSQL` | `typedata.quality` | Custom SQL check |
| `QualityRunner` | `typedata.quality` | Check executor |
| `CheckResult` | `typedata.quality` | Single check result |
| `EntityResult` | `typedata.quality` | Entity-level result |

### Tier 8 -- Primitives (SQL building blocks)

All primitives from `typedata.primitives` are re-exported at the top level.
Full list (30+ functions):

**Hashing:** `concat_hash`, `hash_email`, `hash_id`, `hash_md5`, `hash_sha256`
**Dates:** `date_diff_days`, `date_trunc_month`, `date_trunc_quarter`,
`date_trunc_year`, `days_since`, `extract_year`, `extract_month`, `extract_day`,
`earliest_date`, `latest_date`
**Categorization:** `categorize`, `categorize_contains`, `lifecycle_flag`,
`boolean_to_int`
**JSON:** `to_json_struct`, `json_extract_scalar`, `json_value`
**Aggregations:** `sum_`, `count_`, `count_distinct`, `avg_`, `min_`, `max_`,
`row_number_by`, `cumulative_sum`, `lag_value`, `lead_value`, `first_value`,
`last_value`, `any_value`
**Strings:** `extract_email_domain`, `is_personal_email_domain`,
`account_id_from_domain`
**Time:** `parse_iso8601_duration`

### NOT Exported (differs from timo-data-stack)

| Symbol | Reason |
|--------|--------|
| `EntityRegistry` | timo-specific orchestration concern, not a framework type |
| `product_entity`, `leads_entity`, etc. | Concrete entity instances, user-land |
| `get_all_entities` | Registry/discovery, belongs in engine layer (M001-E004) |
| `standard_dimension_entity` | Template, future `typedata.templates` module |

---

## 3. Decision 2: model_rebuild() Call Order

### The Problem

Pydantic v2 uses `model_rebuild()` to resolve forward references (string
annotations used under `TYPE_CHECKING`). Several models reference types that are
not imported at definition time to avoid circular imports:

| Model | Forward References | Declared In |
|-------|-------------------|-------------|
| `Entity` | `ComputedColumn`, `Measure`, `QualityConfig` | `typedata.core.entity` via `TYPE_CHECKING` from `typedata.components`, `typedata.quality` |
| `SourceMapping` | `Entity`, `Source` | `typedata.core.source_mapping` via `TYPE_CHECKING` from `.entity` |
| `TenantExtension` | `ComputedColumn`, `Measure`, `Entity`, `SourceMapping` | `typedata.core.tenant_extension` via `TYPE_CHECKING` from `typedata.components`, `.entity`, `.source_mapping` |

### Required Call Order

The `model_rebuild()` calls must happen AFTER all referenced modules are
imported into the process, so that Python's namespace contains the actual
classes for the string annotations to resolve against.

```python
# Phase 1: Import all modules (triggers submodule loading)
from typedata.components import ComputedColumn, Measure, ...
from typedata.core import Entity, SourceMapping, TenantExtension, ...
from typedata.quality import QualityConfig, ...
from typedata.primitives import ...

# Phase 2: Rebuild models (order matters for dependencies)
Entity.model_rebuild()            # Resolves: ComputedColumn, Measure, QualityConfig
SourceMapping.model_rebuild()     # Resolves: Entity, Source
TenantExtension.model_rebuild()   # Resolves: ComputedColumn, Measure, Entity, SourceMapping
```

**Entity must rebuild first** because SourceMapping and TenantExtension
reference `Entity` in their annotations. If SourceMapping rebuilt before Entity,
its `Entity` forward ref would resolve to the not-yet-rebuilt Entity class,
which would still have unresolved forward refs for ComputedColumn/Measure -- and
Pydantic would raise errors when validating nested models.

**SourceMapping must rebuild before TenantExtension** because TenantExtension
has a `source_mapping: "SourceMapping"` field.

### Minimal model_rebuild Set

Only three models need `model_rebuild()`:

1. `Entity` -- has `TYPE_CHECKING` forward refs
2. `SourceMapping` -- has `TYPE_CHECKING` forward refs
3. `TenantExtension` -- has `TYPE_CHECKING` forward refs

Other models (PrepLayer, DimensionLayer, etc.) use `ComputedColumn = Any` as a
runtime placeholder rather than `TYPE_CHECKING` string annotations, so they do
NOT need `model_rebuild()`. This is a quirk of the original implementation --
layer.py defines `ComputedColumn = Any` at module level as a type alias rather
than using a proper forward reference. This means layers accept any value for
`computed_columns` at runtime. The `model_rebuild()` on Entity is what ensures
the full Entity validation chain works correctly.

**Future consideration:** When layers are refactored to use proper
`TYPE_CHECKING` forward refs instead of `ComputedColumn = Any`, they will also
need `model_rebuild()` calls. This is tech debt to track but not block on.

---

## 4. Decision 3: Use `__all__` for Explicit Exports

**Decision: Yes, use `__all__`.**

Reasons:

1. **IDE support.** Tools like PyCharm, VS Code/Pylance, and mypy use `__all__`
   to determine what `from typedata import *` exposes and to flag invalid
   imports. Without `__all__`, star-imports pull in every name in the namespace
   including internal helpers and re-exported stdlib types.

2. **Documentation.** `__all__` serves as machine-readable API documentation.
   Sphinx autodoc, pdoc, and mkdocstrings all use it to determine what to
   document.

3. **Accidental export prevention.** As the package grows, new internal imports
   added to `__init__.py` (e.g., `from typing import TYPE_CHECKING`) would leak
   into the public namespace without `__all__`.

4. **Precedent.** The timo-data-stack `metadata/__init__.py` already uses
   `__all__`. Maintaining the pattern reduces cognitive overhead.

**Convention:** Every subpackage (`typedata.core`, `typedata.components`,
`typedata.quality`, `typedata.primitives`) should also define its own `__all__`
in its `__init__.py`. The top-level `typedata/__all__` is the union of what we
choose to re-export.

---

## 5. Decision 4: Import Organization -- Flat vs Nested

**Decision: Both. Flat at the top level for convenience, nested always works.**

### User-facing API (flat)

```python
# The recommended way -- everything from one place
from typedata import Entity, LayersConfig, PrepLayer, DimensionLayer
from typedata import BigQuerySource, Field, ComputedColumn
from typedata import NotNull, Unique, QualityConfig
from typedata import hash_email, categorize, sum_
```

### Power-user / explicit API (nested)

```python
# Also works -- for when you want to be explicit about provenance
from typedata.core import Entity, LayersConfig
from typedata.components import ComputedColumn, Measure
from typedata.quality import NotNull, QualityConfig
from typedata.primitives import hash_email
```

### Why both?

- **Flat is ergonomic.** The acceptance criteria in the story explicitly require
  `from typedata import Entity, LayersConfig, PrepLayer, DimensionLayer` to
  work. This is the primary developer experience.
- **Nested is precise.** When reading unfamiliar code, `from typedata.quality
  import NotNull` instantly tells you the domain. It also avoids name collisions
  if a user has their own `Field` class.
- **No performance cost.** The top-level `__init__.py` eagerly imports all
  subpackages anyway (required for `model_rebuild()`), so flat imports add zero
  overhead.

### Implementation

`typedata/__init__.py` imports from subpackages and re-exports:

```python
from typedata.core import Entity, LayersConfig, Source, ...
from typedata.components import ComputedColumn, Measure, ...
from typedata.quality import QualityConfig, NotNull, ...
from typedata.primitives import hash_email, categorize, ...
```

Subpackage `__init__.py` files (e.g., `typedata/core/__init__.py`) handle
internal re-exports from their own submodules.

---

## 6. Decision 5: Lazy Loading for Optional Backends

**Decision: NOT needed for this story. Defer to M001-E004 (execution engine).**

### Analysis

The typedata package has two categories of dependencies:

1. **Core (always required):** `pydantic` -- used by every model class.
2. **Backend-specific (optional):** `ibis-framework[duckdb]`,
   `ibis-framework[bigquery]`, `google-cloud-bigquery`, `duckdb` -- only needed
   at execution time.

The modules in scope for this story (`core`, `components`, `quality`,
`primitives`) depend only on:
- `pydantic` (core, components, quality)
- Python stdlib (primitives -- they return SQL strings)

No module in the current public API surface imports `ibis` or any backend
library. Therefore **there is nothing to lazy-load**. The entire public API can
be eagerly imported with zero optional-dependency risk.

### When Lazy Loading Will Matter

In M001-E004 (execution engine), the `typedata.engine` module will import `ibis`
and backend connectors. At that point:

- `typedata.engine` should NOT be imported in `typedata/__init__.py`
- Users will explicitly `from typedata.engine import run` or
  `from typedata.engine.duckdb import DuckDBExecutor`
- The top-level `typedata` import remains lightweight

If we later want `from typedata import run` to work, we can add a lazy import
using `__getattr__` at the module level:

```python
def __getattr__(name):
    if name == "run":
        from typedata.engine import run
        return run
    raise AttributeError(f"module 'typedata' has no attribute {name}")
```

But this is a future concern, not needed for S004.

---

## 7. Implementation Skeleton

```python
# src/typedata/__init__.py
"""
typedata - Typed entity framework for data transformations

Define entities once in Python, run them anywhere.
"""

# --- Phase 1: Import all modules ---

# Components (must be imported before model_rebuild)
from typedata.components import (
    ComputedColumn,
    DataQualityChecks,
    LifecycleFlags,
    Measure,
    TimeBasedMetrics,
)

# Core entity and configuration classes
from typedata.core import (
    ActivityConfig,
    ActivityLayer,
    ActivityType,
    AggregationSource,
    AnalyticsLayer,
    AnalyticsMetric,
    AnalyticsModel,
    AnalyticsSource,
    AnonSourceConfig,
    BaseSourceConfig,
    BigQuerySource,
    ComputedMetric,
    DerivedEntitySource,
    DerivedSource,
    DimensionLayer,
    Divide,
    Entity,
    EventAggregationSource,
    Field,
    IncrementalStrategy,
    LayersConfig,
    MaterializationType,
    Multiply,
    PrepLayer,
    ProductSourceConfig,
    ProductUnionSource,
    Rename,
    SignalSourceConfig,
    SnapshotLayer,
    Source,
    SourceMapping,
    SourceOverrides,
    SourcePriority,
    TenantExtension,
    TypeCast,
    UnionSource,
)

# Quality checks
from typedata.quality import (
    CheckResult,
    CustomSQL,
    EntityResult,
    ForeignKey,
    InRange,
    InSet,
    MatchesPattern,
    MaxAge,
    NotEmpty,
    NotNull,
    QualityCheck,
    QualityConfig,
    QualityRunner,
    Unique,
)

# Primitives - SQL building blocks
from typedata.primitives import (
    account_id_from_domain,
    any_value,
    avg_,
    boolean_to_int,
    categorize,
    categorize_contains,
    concat_hash,
    count_,
    count_distinct,
    cumulative_sum,
    date_diff_days,
    date_trunc_month,
    date_trunc_quarter,
    date_trunc_year,
    days_since,
    earliest_date,
    extract_day,
    extract_email_domain,
    extract_month,
    extract_year,
    first_value,
    hash_email,
    hash_id,
    hash_md5,
    hash_sha256,
    is_personal_email_domain,
    json_extract_scalar,
    json_value,
    lag_value,
    last_value,
    latest_date,
    lead_value,
    lifecycle_flag,
    max_,
    min_,
    parse_iso8601_duration,
    row_number_by,
    sum_,
    to_json_struct,
)

# --- Phase 2: Resolve forward references ---
# Order matters: Entity first (referenced by others), then SourceMapping, then TenantExtension
Entity.model_rebuild()
SourceMapping.model_rebuild()
TenantExtension.model_rebuild()

# --- Package metadata ---
__version__ = "0.1.0"

__all__ = [
    # --- Core ---
    "Entity",
    "LayersConfig",
    "Source",
    "Field",
    # --- Types/Enums ---
    "MaterializationType",
    "IncrementalStrategy",
    "SourcePriority",
    # --- Sources ---
    "BaseSourceConfig",
    "BigQuerySource",
    "DerivedSource",
    "DerivedEntitySource",
    "AggregationSource",
    "EventAggregationSource",
    "UnionSource",
    "AnonSourceConfig",
    "SignalSourceConfig",
    "ProductSourceConfig",
    "ProductUnionSource",
    "SourceOverrides",
    "TypeCast",
    "Rename",
    "Divide",
    "Multiply",
    # --- Layers ---
    "PrepLayer",
    "DimensionLayer",
    "SnapshotLayer",
    "ActivityLayer",
    "ActivityConfig",
    "ActivityType",
    "AnalyticsLayer",
    "AnalyticsMetric",
    "AnalyticsModel",
    "AnalyticsSource",
    "ComputedMetric",
    # --- Source Mapping / Tenant ---
    "SourceMapping",
    "TenantExtension",
    # --- Components ---
    "ComputedColumn",
    "Measure",
    "LifecycleFlags",
    "TimeBasedMetrics",
    "DataQualityChecks",
    # --- Quality ---
    "QualityConfig",
    "QualityCheck",
    "NotNull",
    "NotEmpty",
    "InRange",
    "InSet",
    "MatchesPattern",
    "ForeignKey",
    "Unique",
    "MaxAge",
    "CustomSQL",
    "QualityRunner",
    "CheckResult",
    "EntityResult",
    # --- Primitives: Hashing ---
    "concat_hash",
    "hash_email",
    "hash_id",
    "hash_md5",
    "hash_sha256",
    # --- Primitives: Dates ---
    "date_diff_days",
    "date_trunc_month",
    "date_trunc_quarter",
    "date_trunc_year",
    "days_since",
    "extract_year",
    "extract_month",
    "extract_day",
    "earliest_date",
    "latest_date",
    # --- Primitives: Categorization ---
    "categorize",
    "categorize_contains",
    "lifecycle_flag",
    "boolean_to_int",
    # --- Primitives: JSON ---
    "to_json_struct",
    "json_extract_scalar",
    "json_value",
    # --- Primitives: Aggregations ---
    "sum_",
    "count_",
    "count_distinct",
    "avg_",
    "min_",
    "max_",
    "row_number_by",
    "cumulative_sum",
    "lag_value",
    "lead_value",
    "first_value",
    "last_value",
    "any_value",
    # --- Primitives: Strings ---
    "extract_email_domain",
    "is_personal_email_domain",
    "account_id_from_domain",
    # --- Primitives: Time ---
    "parse_iso8601_duration",
]
```

---

## 8. Test Strategy

Tests should verify the acceptance criteria from the story TOML:

### Unit Test: model_rebuild resolves forward refs

```python
def test_entity_model_rebuild_resolves_forward_refs():
    """Entity.model_rebuild() resolves ComputedColumn, Measure, QualityConfig forward refs."""
    from typedata import Entity, ComputedColumn, Measure, QualityConfig

    # Verify the model fields accept actual types (not strings)
    field_info = Entity.model_fields
    # core_computed should accept list[ComputedColumn], not list["ComputedColumn"]
    # This would fail if model_rebuild() hadn't been called
    assert field_info["core_computed"].annotation is not str
```

### Integration Tests: Import paths work

```python
def test_tier1_imports():
    from typedata import Entity, LayersConfig, PrepLayer, DimensionLayer

def test_tier2_imports():
    from typedata import ComputedColumn, Measure, NotNull, Unique

def test_tier3_imports():
    from typedata import BigQuerySource, DerivedSource, Field

def test_nested_imports_also_work():
    from typedata.core import Entity
    from typedata.components import ComputedColumn
    from typedata.quality import NotNull
    from typedata.primitives import hash_email
```

### Integration Test: Realistic entity with forward refs resolved

```python
def test_entity_with_computed_columns_and_quality():
    """Creating Entity with ComputedColumn and QualityConfig validates correctly."""
    from typedata import (
        Entity, LayersConfig, PrepLayer, DimensionLayer,
        BigQuerySource, Field, ComputedColumn, QualityConfig, NotNull,
    )

    entity = Entity(
        name="test_entity",
        description="Test entity with all features",
        layers=LayersConfig(
            prep=PrepLayer(model_name="prep_test"),
            dimension=DimensionLayer(
                model_name="dim_test",
                computed_columns=[
                    ComputedColumn(name="full_name", expression="first || ' ' || last"),
                ],
            ),
        ),
        required_fields=[
            Field(name="id", type="STRING"),
            Field(name="email", type="STRING"),
        ],
        core_computed=[
            ComputedColumn(name="email_hash", expression="MD5(email)"),
        ],
        quality=QualityConfig(checks=[NotNull("id")]),
    )

    assert entity.name == "test_entity"
    assert len(entity.all_computed_columns) == 2  # core + dimension
```

---

## 9. Open Questions (non-blocking)

1. **Should `DataQualityChecks` (component) be renamed to avoid confusion with
   `QualityCheck` (base class)?** Both exist in the original codebase. The
   component generates multiple checks; the base class is a single check.
   Recommendation: keep as-is for extraction, rename in a later story if
   confusing.

2. **Layer `to_sql()` methods.** These generate dbt-specific SQL. Should they
   be stripped in extraction or kept for backward compatibility? The story
   dependencies (E002-S001) mention evaluating this. Recommendation: keep them
   as-is in extraction, mark with deprecation comments pointing to future Ibis
   generators (M001-E003).

3. **`ComputedColumn = Any` in layer.py.** This is a runtime type erasure that
   means layers accept any value for computed_columns. Should this story also
   fix layers to use proper forward refs? Recommendation: no -- that is a
   separate concern. Track as tech debt.

---

## 10. Implementation Plan

### 10.1 Summary

This story creates/modifies two files:

1. **`src/typedata/__init__.py`** -- The main deliverable. Replace the current
   minimal stub (which only exports `__version__`) with the full public API:
   imports from all four subpackages, `model_rebuild()` calls in the correct
   order, and an `__all__` list.

2. **`tests/test_public_api.py`** -- New test file verifying all acceptance
   criteria: import paths, forward reference resolution, and realistic entity
   construction.

No other files are created or modified. All predecessor stories (E001-S001
through E001-S005, E002-S001 through E002-S003) must be complete before this
story can be implemented. Those stories create the subpackage modules and their
`__init__.py` re-exports; this story wires them together at the top level.

### 10.2 Naming reconciliation

The design docs for predecessor stories introduced several renames from the
original timo-data-stack names. The S004 public API design doc (sections 2-7
above) was written with the original timo-data-stack names for maximum clarity
against the reference implementation. The actual implementation MUST use the
names as they exist in the implemented predecessor stories.

Renames decided in predecessor designs:

| Original (timo-data-stack) | Typedata name | Decided in |
|---|---|---|
| `BigQuerySource` | `TableSource` | E001-S002, E002-S002 |
| `BaseSourceConfig` | `BaseTableSource` | E001-S002 |
| `SourceOverrides` | `SourceTransforms` | E001-S002 |
| `Field` | `FieldDef` (considered) | E002-S002 (recommendation, may or may not be adopted) |
| `ActivityLayer` (dbt-based) | Dropped entirely | E002-S001, E002-S002 |
| `activities: ActivityConfig` | `activity: ActivityConfig` | E002-S002 |

Symbols removed from typedata (present in timo-data-stack but not extracted):

| Symbol | Reason |
|---|---|
| `SignalSourceConfig` | timo-specific (E001-S002 decision) |
| `ProductSourceConfig` | timo-specific (E001-S002 decision) |
| `ProductUnionSource` | timo-specific (E001-S002 decision) |
| `AnonSourceConfig` | timo-specific (E001-S002 decision) |
| `TenantExtension` | Deferred (E002-S003 decision) |
| `ActivityLayer` | dbt-only, replaced by `ActivityConfig` (E002-S001 decision) |
| `EntityRegistry` | Engine-layer concern, not a framework type |
| Concrete entities | User-land, not framework |

**Implementation note:** When implementing, the actual symbol names used in
imports and `__all__` MUST match whatever the predecessor stories actually
implemented. If a predecessor deferred a rename (e.g., kept `Field` instead of
`FieldDef`), this story uses the name that exists. The implementation skeleton
in section 7 should be adapted accordingly at implementation time.

### 10.3 Prerequisite verification

Before writing any code, verify all four subpackages export their symbols
correctly. Run these checks:

```bash
python -c "from typedata.components import ComputedColumn, Measure, LifecycleFlags, TimeBasedMetrics, DataQualityChecks"
python -c "from typedata.core import Entity, SourceMapping, LayersConfig"
python -c "from typedata.quality import QualityConfig, QualityCheck, NotNull, Unique"
python -c "from typedata.primitives import hash_email, categorize, sum_"
```

If any fail, the predecessor story is not complete. Do not proceed.

### 10.4 Implementation tasks

**Task 1: Write `src/typedata/__init__.py`**

Target: `/home/tmo/roadtothebeach/tmo/typedata/src/typedata/__init__.py`

This replaces the existing stub. Structure follows three phases:

```
Phase 1: Import all symbols from subpackages
  - typedata.components (5 symbols)
  - typedata.core (all entity, layer, source, mapping, type symbols)
  - typedata.quality (13 symbols)
  - typedata.primitives (30+ functions)

Phase 2: model_rebuild() calls IN ORDER
  1. Entity.model_rebuild()           -- resolves ComputedColumn, Measure, QualityConfig
  2. SourceMapping.model_rebuild()    -- resolves Entity, Source

Phase 3: Package metadata + __all__
  - __version__ = "0.1.0"
  - __all__ = [...every public symbol...]
```

Key ordering constraints for Phase 2:
- Entity MUST rebuild first because SourceMapping references Entity
- SourceMapping MUST rebuild second because it references Entity and Source
- TenantExtension is NOT included (deferred per E002-S003 decision)
- Only two model_rebuild() calls are needed (not three as in the reference
  implementation)

Phase 1 import grouping follows the skeleton in section 7, adapted for the
actual symbol names from predecessor stories. Components are imported first
because Entity's forward references point to ComputedColumn and Measure --
having those modules loaded into the Python process before model_rebuild()
is what allows the forward reference strings to resolve.

**Task 2: Write `tests/test_public_api.py`**

Target: `/home/tmo/roadtothebeach/tmo/typedata/tests/test_public_api.py`

Test cases (mapped to acceptance criteria from the story TOML):

```
test_entity_model_rebuild_resolves_forward_refs
  AC: "Entity.model_rebuild() resolves ComputedColumn, Measure, QualityConfig forward refs"
  Method: Import Entity, inspect model_fields, verify forward ref annotations
          are resolved to actual classes (not strings).

test_flat_import_tier1_core
  AC: "from typedata import Entity, LayersConfig, PrepLayer, DimensionLayer works"
  Method: Single import statement, assert all four are callable classes.

test_flat_import_tier2_components_quality
  AC: "from typedata import ComputedColumn, Measure, NotNull, Unique works"
  Method: Single import statement, assert all four are importable.

test_flat_import_tier3_sources
  AC: "from typedata import BigQuerySource/TableSource, DerivedSource, Field works"
  Method: Single import statement, assert all three are importable.

test_nested_imports_work
  AC: implicit (both flat and nested must work)
  Method: Import same symbols via typedata.core, typedata.components,
          typedata.quality, typedata.primitives. Assert identity (same object).

test_entity_with_computed_columns_validates
  AC: "Creating Entity with ComputedColumn in dimension layer validates correctly"
  Method: Construct an Entity with LayersConfig containing a DimensionLayer
          that has computed_columns=[ComputedColumn(...)]. Assert no
          ValidationError. Verify entity.all_computed_columns returns the
          expected list.

test_entity_with_quality_config_validates
  AC: "Creating Entity with quality=QualityConfig(checks=[NotNull('id')]) validates"
  Method: Construct an Entity with quality=QualityConfig(checks=[NotNull("id")]).
          Assert no ValidationError. Verify entity.quality.checks has one item.

test_all_exports_match_dir
  AC: implicit (__all__ must be accurate)
  Method: Import typedata, verify every name in __all__ is actually accessible
          as an attribute. Verify no extra non-underscore public names exist
          that are not in __all__ (excluding dunder names and imported modules).

test_source_mapping_model_rebuild
  AC: implicit (SourceMapping forward refs must resolve)
  Method: Import SourceMapping, construct one with a real Entity and Source
          instance. Assert validation passes.
```

**Task 3: Run quality gates**

After implementation:

```bash
# Verify all tests pass
pytest tests/test_public_api.py -v

# Verify existing tests still pass
pytest tests/ -v

# Lint check
ruff check src/typedata/__init__.py

# Type check
mypy src/typedata/__init__.py
```

### 10.5 Exact `__all__` list

The `__all__` list is the union of all public symbols. Organized by tier
(matching section 2 above) for readability. The actual symbol names below use
the names from the S004 design skeleton; substitute with actual implemented
names at implementation time.

Estimated count: ~70 symbols (5 components + ~25 core + 13 quality +
~35 primitives).

Every symbol in `__all__` MUST:
1. Be imported in Phase 1 (no lazy loading)
2. Be a class, function, or type alias (not a module or internal helper)
3. Appear in the subpackage's own `__all__` if the subpackage defines one

### 10.6 Edge cases and error handling

1. **Import order within Phase 1 does not matter for Python** -- all imports
   execute at module load time. The grouping (components, core, quality,
   primitives) is for human readability only.

2. **Import order DOES matter for model_rebuild()** -- Phase 2 must come
   after ALL Phase 1 imports. If any import is moved after model_rebuild(),
   the forward references it provides would not be available.

3. **If a primitive or quality check is added in a future story**, it must be
   added to both the subpackage `__init__.py` and the top-level `__init__.py`
   imports + `__all__`. Consider adding a test that verifies `typedata.__all__`
   is a superset of each subpackage's `__all__`.

4. **Circular import risk:** None. The top-level `__init__.py` only imports
   FROM subpackages. No subpackage imports from the top-level. The
   TYPE_CHECKING forward references in entity.py and source_mapping.py resolve
   via model_rebuild(), not via runtime imports.

### 10.7 Definition of done

- [ ] `src/typedata/__init__.py` contains all Phase 1 imports, Phase 2
      model_rebuild() calls, and Phase 3 `__all__` + `__version__`
- [ ] `from typedata import Entity, LayersConfig, PrepLayer, DimensionLayer`
      works
- [ ] `from typedata import ComputedColumn, Measure, NotNull, Unique` works
- [ ] `from typedata.core import Entity` returns the same object as
      `from typedata import Entity`
- [ ] Entity with ComputedColumn in dimension layer validates without error
- [ ] Entity with QualityConfig validates without error
- [ ] SourceMapping with Entity and Source validates without error
- [ ] All tests in `tests/test_public_api.py` pass
- [ ] `ruff check src/typedata/__init__.py` passes
- [ ] `pytest tests/ -v` passes (all existing + new tests)
