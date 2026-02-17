# Design: M001-E001-S002 -- Extract core types, enums, and source configuration classes

**Date:** 2026-02-17
**Story:** M001-E001-S002
**Status:** ready

## Source Analysis

Two files from `timo-data-stack/metadata/core/`:

- **types.py** (30 lines) -- Three enums: `MaterializationType`, `IncrementalStrategy`, `SourcePriority`. Pure Python, zero internal dependencies.
- **source.py** (352 lines) -- 14 classes: field definitions, transformation helpers, a base source config, and 9 concrete source types. Depends only on `pydantic` and `os`.

## Decision 1: Which source types to KEEP vs SKIP

### KEEP (generic transformation concepts)

| Class | Rationale |
|---|---|
| **Field** | Universal. Every data framework needs field metadata (name, type, nullable, description). The `json_path` attribute is useful for JSON extraction patterns -- not timo-specific. |
| **TypeCast** | Universal. Type casting is a standard transformation primitive. |
| **Rename** | Universal. Column renaming is a standard transformation primitive. |
| **Divide** | Universal. Unit conversion (cents to dollars, etc.) is a common pattern. |
| **Multiply** | Universal. Scaling transformation, pairs with Divide. |
| **SourceOverrides** | Universal. Container for the four transform helpers above. Any source might need type casts, renames, and arithmetic transforms applied at read time. Keep it. |
| **BaseSourceConfig** | Universal. The project/dataset/table + duckdb_path pattern with `read_table()` is the core Ibis dual-backend abstraction. This IS the typedata value prop. |
| **BigQuerySource** | Universal. Standard warehouse table source with optional field list and overrides. Despite the name, `BaseSourceConfig.read_table()` already handles DuckDB too -- this is really "WarehouseTableSource." See rename discussion below. |
| **DerivedSource** | Generic pattern. Identity-graph-based entity derivation is a framework concept (merge N dimension tables into one via matching keys). The class itself is tiny and clean: one validated string field pointing to a graph config. |
| **DerivedEntitySource** | Generic pattern. Placeholder/marker for identity-graph-derived sources. Currently a stub (`identity_graph: Any`). Keep as a framework extension point. |
| **AggregationSource** | Generic pattern. "Aggregate from one entity to produce another" (e.g., Person -> Account) is a standard entity-framework concept: source_entity + group_by_column + optional filter. Not timo-specific at all. |
| **EventAggregationSource** | Generic pattern. "Read raw events, optionally transform the group key, filter, then GROUP BY" is a standard event-to-entity pattern. The class has no timo-specific field names -- it's project/dataset/table + group_by_column + group_by_expression + filter_expression + fields. Keep. |
| **UnionSource** | Generic pattern. Unioning multiple sources into a common schema is a standard data integration pattern. However, it currently takes `list[SignalSourceConfig]` which is timo-specific. See redesign below. |

### SKIP (timo-specific domain concepts)

| Class | Rationale |
|---|---|
| **SignalSourceConfig** | Highly timo-specific. Fields like `signal_type_column`, `signal_type_value`, `email_column`, `metadata_columns` encode a particular "signals" domain schema. A generic union source should not prescribe the target schema. |
| **ProductSourceConfig** | Highly timo-specific. Fields like `id_prefix`, `product_type`, `title_expression`, `view_count_column`, `like_count_column` encode a particular "content product" domain schema. |
| **ProductUnionSource** | Timo-specific. Wrapper for `list[ProductSourceConfig]`. |
| **AnonSourceConfig** | Timo-specific. Fields like `identifier_column`, `referrer_column` encode a particular "anonymous visitor" tracking domain. |

**Summary:** 10 classes kept, 4 classes skipped.

## Decision 2: Renames and API cleanup

### Renames

1. **`BigQuerySource` -> `TableSource`**
   - The class (via `BaseSourceConfig`) already handles both BigQuery AND DuckDB. The name "BigQuerySource" is misleading in a multi-backend library. `TableSource` communicates "reads from a table" regardless of backend.

2. **`BaseSourceConfig` -> `BaseTableSource`**
   - Aligns with the `TableSource` rename. "Config" is redundant when the whole framework is config-as-code.

3. **`EventAggregationSource` -> `EventAggregationSource`** (no change)
   - The name is already descriptive and generic.

4. **`SourceOverrides` -> `SourceTransforms`**
   - "Overrides" implies patching/monkey-patching. "Transforms" is more accurate -- these are read-time transformations applied to source data.

### API cleanup

1. **`UnionSource` redesign** -- Currently hardcoded to `list[SignalSourceConfig]`. Redesign as a generic union over `list[TableSource]` (the renamed BigQuerySource). Each member source points to a table; the union combines them. Field mapping and schema normalization will be handled at a higher layer (entity definitions), not baked into the source config.

2. **`BaseTableSource.read_table()` stays** -- This method is the core Ibis dual-backend abstraction. It belongs here. The `os.path.expanduser` call for duckdb_path is fine.

3. **Remove the `import os` dependency** -- Move `os.path.expanduser` into `read_table()` only (it's already only used there). This is just a note that the import is legitimate and minimal.

4. **Docstring cleanup** -- Update all docstrings to say "typedata" not "dbt" or "BigQuery-specific". Remove timo-specific examples from docstrings.

## Decision 3: SourceOverrides (renamed SourceTransforms)

**Include.** Rationale:

- It's the container for TypeCast, Rename, Divide, Multiply -- all of which we're keeping.
- Without it, every source that needs read-time transforms would need four separate optional lists, which is worse API design.
- It's clean, generic, and has no internal dependencies.
- Rename to `SourceTransforms` to better communicate intent.

## Decision 4: Exact file contents plan

### `typedata/core/__init__.py`

Re-exports from both modules for convenience:

```python
from typedata.core.types import IncrementalStrategy, MaterializationType, SourcePriority
from typedata.core.source import (
    AggregationSource,
    BaseTableSource,
    DerivedEntitySource,
    DerivedSource,
    Divide,
    EventAggregationSource,
    Field,
    Multiply,
    Rename,
    SourceTransforms,
    TableSource,
    TypeCast,
    UnionSource,
)
```

### `typedata/core/types.py`

Copy verbatim from timo-data-stack. No changes needed -- it's three clean enums with no internal dependencies.

```python
"""Core type definitions and enums."""

from enum import Enum


class MaterializationType(str, Enum):
    """Materialization strategies for transformed entities."""
    TABLE = "table"
    VIEW = "view"
    INCREMENTAL = "incremental"
    EPHEMERAL = "ephemeral"


class IncrementalStrategy(str, Enum):
    """Strategies for incremental materialization."""
    MERGE = "merge"
    APPEND = "append"
    DELETE_INSERT = "delete+insert"


class SourcePriority(int, Enum):
    """Priority levels for identity graph field resolution."""
    PRIMARY = 1
    SECONDARY = 2
    TERTIARY = 3
    QUATERNARY = 4
```

### `typedata/core/source.py`

Extracted and cleaned. Key changes from original:

1. `BaseSourceConfig` -> `BaseTableSource`
2. `BigQuerySource` -> `TableSource`
3. `SourceOverrides` -> `SourceTransforms`
4. `UnionSource` takes `list[TableSource]` instead of `list[SignalSourceConfig]`
5. Remove `SignalSourceConfig`, `ProductSourceConfig`, `ProductUnionSource`, `AnonSourceConfig`
6. Clean docstrings (remove timo references, remove "dbt" references)

```python
"""Source configuration classes for typedata entities."""

import os
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, field_validator
from pydantic import Field as PydanticField


class Field(BaseModel):
    """Defines a source field with its type and metadata."""
    name: str
    type: str  # STRING, INT64, FLOAT64, TIMESTAMP, BOOLEAN, DATE, BYTES, etc.
    description: str | None = None
    nullable: bool = True
    json_path: str | None = None  # JSON path for extraction (e.g., "$.utm_source")


class TypeCast(BaseModel):
    """Type cast configuration."""
    field: str
    target_type: str


class Rename(BaseModel):
    """Column rename configuration."""
    from_name: str
    to_name: str


class Divide(BaseModel):
    """Divide column by constant (e.g., cents to dollars)."""
    field: str
    divisor: float
    target_type: str = "decimal"
    suffix: str = "_amount"


class Multiply(BaseModel):
    """Multiply column by constant."""
    field: str
    multiplier: float
    target_type: str = "decimal"
    suffix: str = "_value"


class SourceTransforms(BaseModel):
    """Read-time transformations applied to source data."""
    type_casts: list[TypeCast] = PydanticField(default_factory=list)
    renames: list[Rename] = PydanticField(default_factory=list)
    divides: list[Divide] = PydanticField(default_factory=list)
    multiplies: list[Multiply] = PydanticField(default_factory=list)


class BaseTableSource(BaseModel):
    """Base configuration for table sources.

    Provides common fields for sources that read from a warehouse table
    or local parquet files (via duckdb_path).
    """
    project: str = PydanticField(min_length=1)
    dataset: str = PydanticField(min_length=1)
    table: str = PydanticField(min_length=1)
    duckdb_path: str | None = None

    @field_validator("project", "dataset", "table")
    @classmethod
    def validate_not_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("project, dataset, and table are required")
        return v

    def read_table(self, conn: Any, backend: str) -> Any:
        """Read table from warehouse or DuckDB based on backend.

        Args:
            conn: Ibis connection
            backend: "bigquery", "duckdb", etc.

        Returns:
            Ibis table expression
        """
        if backend == "duckdb":
            if not self.duckdb_path:
                raise ValueError("duckdb_path is required for duckdb backend")
            parquet_path = os.path.expanduser(self.duckdb_path)
            return conn.read_parquet(parquet_path)
        else:
            return conn.table(self.table, database=(self.project, self.dataset))


class TableSource(BaseTableSource):
    """Standard table source with optional field definitions and transforms."""
    transforms: SourceTransforms | None = None
    fields: list[Field] | None = None


class DerivedEntitySource(BaseModel):
    """Source from identity graph or derived logic."""
    type: Literal["identity_graph"]
    identity_graph: Any = None
    fields: list[Field] | None = None


class DerivedSource(BaseModel):
    """Source for derived entities created via identity graph resolution.

    Derived entities merge multiple dimension sources using identity matching
    to create unified views.

    Attributes:
        identity_graph: Name of the identity graph configuration
                       that defines source merging logic.
    """
    model_config = ConfigDict(frozen=True)

    identity_graph: str = PydanticField(min_length=1)

    @field_validator("identity_graph")
    @classmethod
    def validate_identity_graph(cls, v: str) -> str:
        if not isinstance(v, str) or not v:
            raise ValueError("identity_graph must be a non-empty string")
        return v


class AggregationSource(BaseModel):
    """Source for entities aggregated from other entities.

    Example: Account entity aggregating from Person entity.
    """
    source_entity: str
    group_by_column: str
    filter_expression: str | None = None
    fields: list[Field] | None = None


class EventAggregationSource(BaseTableSource):
    """Source for entities aggregated from raw event streams.

    Handles the pattern: raw events -> pre-processing -> GROUP BY -> entity.
    Used for transforming event-level data into entity-level records.
    """
    group_by_column: str = PydanticField(min_length=1)
    group_by_expression: str | None = None
    filter_expression: str | None = None
    fields: list[Field] | None = None

    @field_validator("group_by_column")
    @classmethod
    def validate_group_by_column(cls, v: str) -> str:
        if not v:
            raise ValueError("group_by_column is required")
        return v


class UnionSource(BaseModel):
    """Source that unions multiple table sources into a common schema.

    Used for entities that combine data from multiple upstream sources
    into a single unified table.
    """
    sources: list[TableSource]

    @field_validator("sources")
    @classmethod
    def validate_sources(cls, v: list) -> list:
        if not v:
            raise ValueError("UnionSource requires at least one source")
        return v
```

## Risks and Open Questions

1. **UnionSource field mapping** -- The original `UnionSource` relied on `SignalSourceConfig` which included `field_mappings` for schema normalization. The simplified `UnionSource` over `TableSource` defers field mapping to entity-level logic. This is the right call for the library (sources describe where data lives, not how to reshape it), but the entity layer (future story) needs to provide the mapping mechanism.

2. **`read_table()` backend dispatch** -- Currently uses a string `backend` parameter. Future stories may want an enum or protocol-based dispatch. Good enough for now; don't over-engineer.

3. **SourcePriority** -- This enum is specific to identity graph resolution. It's still generic enough to keep (priority-based field resolution is a common pattern), but if identity graphs get their own module later, it could move there. Keep in types.py for now since the story acceptance criteria require it.

---

## Implementation Plan

### Summary

Extract three enums (`MaterializationType`, `IncrementalStrategy`, `SourcePriority`) from `timo-data-stack/metadata/core/types.py` and ten source configuration classes from `timo-data-stack/metadata/core/source.py` into the typedata package. Four timo-specific classes are dropped (`SignalSourceConfig`, `ProductSourceConfig`, `ProductUnionSource`, `AnonSourceConfig`). Three classes are renamed for backend-neutrality (`BaseSourceConfig` to `BaseTableSource`, `BigQuerySource` to `TableSource`, `SourceOverrides` to `SourceTransforms`). `UnionSource` is redesigned to accept `list[TableSource]` instead of `list[SignalSourceConfig]`. All docstrings are cleaned of timo-specific and dbt-specific references.

### Acceptance Criteria (Agent-Verifiable)

1. **AC-1:** `from typedata.core.types import MaterializationType, IncrementalStrategy, SourcePriority` succeeds without error.
2. **AC-2:** `from typedata.core.source import Field, TypeCast, Rename, Divide, Multiply` succeeds without error.
3. **AC-3:** `from typedata.core.source import BaseTableSource, TableSource, DerivedSource, DerivedEntitySource, AggregationSource, EventAggregationSource, UnionSource, SourceTransforms` succeeds without error.
4. **AC-4:** `from typedata.core import MaterializationType, IncrementalStrategy, SourcePriority, Field, TableSource, BaseTableSource, DerivedSource, UnionSource` succeeds (re-exports from `__init__.py`).
5. **AC-5:** `TableSource(project="p", dataset="d", table="t")` validates successfully; `TableSource(project="", dataset="d", table="t")` raises `ValidationError`.
6. **AC-6:** `Field(name="email", type="STRING")` validates successfully; default `nullable=True`, `description=None`, `json_path=None`.
7. **AC-7:** `DerivedSource(identity_graph="person_graph")` validates successfully; `DerivedSource(identity_graph="")` raises `ValidationError`. Instance is frozen (immutable).
8. **AC-8:** `UnionSource(sources=[TableSource(project="p", dataset="d", table="t")])` validates successfully; `UnionSource(sources=[])` raises `ValidationError`.
9. **AC-9:** `EventAggregationSource(project="p", dataset="d", table="t", group_by_column="col")` validates successfully; missing `group_by_column` raises error.
10. **AC-10:** `BaseTableSource.read_table()` with `backend="duckdb"` and no `duckdb_path` raises `ValueError`.
11. **AC-11:** `MaterializationType.TABLE.value == "table"`, `IncrementalStrategy.MERGE.value == "merge"`, `SourcePriority.PRIMARY.value == 1`.
12. **AC-12:** `SourceTransforms()` creates an empty transform container with four empty lists. `SourceTransforms(type_casts=[TypeCast(field="id", target_type="INT64")])` works.
13. **AC-13:** None of the following names are importable from `typedata.core.source`: `SignalSourceConfig`, `ProductSourceConfig`, `ProductUnionSource`, `AnonSourceConfig`, `BigQuerySource`, `BaseSourceConfig`, `SourceOverrides`.
14. **AC-14:** `ruff check src/typedata/core/` passes with no errors.
15. **AC-15:** `pytest tests/test_core_types.py tests/test_core_source.py -v` passes with all tests green.

### Implementation Tasks

**Prerequisite:** Story M001-E001-S001 (package structure) must be completed first. The `src/typedata/core/__init__.py` file must already exist. If not yet done, implement S001 first.

#### Task 1: Create `src/typedata/core/types.py`

**File:** `/home/tmo/roadtothebeach/tmo/typedata/src/typedata/core/types.py` (new file)

**Action:** Copy from `/home/tmo/roadtothebeach/tmo/timo-data-stack/metadata/core/types.py` with docstring cleanup.

**Changes from original:**
- `MaterializationType` docstring: `"dbt materialization types."` becomes `"Materialization strategies for transformed entities."`
- `IncrementalStrategy` docstring: `"dbt incremental strategies."` becomes `"Strategies for incremental materialization."`
- `SourcePriority` docstring: unchanged (already generic)

**Exact content:** Use the code block from Decision 4 / `typedata/core/types.py` section of this design doc.

#### Task 2: Create `src/typedata/core/source.py`

**File:** `/home/tmo/roadtothebeach/tmo/typedata/src/typedata/core/source.py` (new file)

**Action:** Extract from `/home/tmo/roadtothebeach/tmo/timo-data-stack/metadata/core/source.py` with renames, deletions, and docstring cleanup.

**Renames applied:**
| Original class | New class | Field renames |
|---|---|---|
| `SourceOverrides` | `SourceTransforms` | none |
| `BaseSourceConfig` | `BaseTableSource` | none |
| `BigQuerySource` | `TableSource` | `overrides` becomes `transforms` (type changes from `SourceOverrides` to `SourceTransforms`) |

**Classes KEPT (10):** `Field`, `TypeCast`, `Rename`, `Divide`, `Multiply`, `SourceTransforms`, `BaseTableSource`, `TableSource`, `DerivedEntitySource`, `DerivedSource`, `AggregationSource`, `EventAggregationSource`, `UnionSource`

**Classes DROPPED (4):** `SignalSourceConfig`, `ProductSourceConfig`, `ProductUnionSource`, `AnonSourceConfig`

**`UnionSource` redesign:** `sources: list[SignalSourceConfig]` becomes `sources: list[TableSource]`

**Docstring changes:**
- `BaseTableSource`: `"Base configuration for BigQuery/DuckDB table sources."` becomes `"Base configuration for table sources."`; `"Provides common fields for sources that read from a BigQuery table"` becomes `"Provides common fields for sources that read from a warehouse table"`
- `BaseTableSource.read_table()`: `"Read table from BigQuery or DuckDB based on backend."` becomes `"Read table from warehouse or DuckDB based on backend."`; `"conn: Ibis connection (BigQuery or DuckDB)"` becomes `"conn: Ibis connection"`; `"backend: \"bigquery\" or \"duckdb\""` becomes `"backend: \"bigquery\", \"duckdb\", etc."`
- `TableSource`: `"Standard BigQuery table source."` becomes `"Standard table source with optional field definitions and transforms."`
- `EventAggregationSource`: Remove timo-specific example (the entire `Example:` block with `deepskydata`/`walker_timodechau_com`); simplify `Attributes:` section to remove `"BigQuery project ID"` references -- just use the shorter docstring from the design doc.
- `DerivedSource`: Remove `"customer 360"` reference; remove `Example:` block with `person_identity_graph`; simplify docstring.
- `UnionSource`: Remove signal-specific docstring and example; use the generic docstring from design doc.

**Exact content:** Use the code block from Decision 4 / `typedata/core/source.py` section of this design doc.

#### Task 3: Update `src/typedata/core/__init__.py`

**File:** `/home/tmo/roadtothebeach/tmo/typedata/src/typedata/core/__init__.py` (exists, currently empty from S001)

**Action:** Add re-exports for all public symbols from both `types.py` and `source.py`.

**Exact content:** Use the code block from Decision 4 / `typedata/core/__init__.py` section of this design doc.

#### Task 4: Create `tests/test_core_types.py`

**File:** `/home/tmo/roadtothebeach/tmo/typedata/tests/test_core_types.py` (new file)

**Tests:**
- `test_materialization_type_values` -- verify all 4 enum members and their string values
- `test_materialization_type_is_str_enum` -- verify `isinstance(MaterializationType.TABLE, str)` is True
- `test_incremental_strategy_values` -- verify all 3 enum members and their string values
- `test_source_priority_values` -- verify all 4 enum members and their int values
- `test_source_priority_is_int_enum` -- verify `isinstance(SourcePriority.PRIMARY, int)` is True
- `test_import_from_core_package` -- verify re-exports from `typedata.core` work

#### Task 5: Create `tests/test_core_source.py`

**File:** `/home/tmo/roadtothebeach/tmo/typedata/tests/test_core_source.py` (new file)

**Tests organized by class:**

*Field:*
- `test_field_basic` -- minimal creation with name + type
- `test_field_defaults` -- verify nullable=True, description=None, json_path=None
- `test_field_with_json_path` -- creation with json_path set
- `test_field_full` -- all fields specified

*TypeCast, Rename, Divide, Multiply:*
- `test_typecast_creation` -- field + target_type
- `test_rename_creation` -- from_name + to_name
- `test_divide_creation` -- field + divisor, check defaults (target_type="decimal", suffix="_amount")
- `test_multiply_creation` -- field + multiplier, check defaults (target_type="decimal", suffix="_value")

*SourceTransforms:*
- `test_source_transforms_empty` -- default creation has 4 empty lists
- `test_source_transforms_with_type_casts` -- populate type_casts list
- `test_source_transforms_with_all` -- populate all four transform lists

*BaseTableSource:*
- `test_base_table_source_valid` -- project + dataset + table
- `test_base_table_source_empty_project_rejected` -- empty string raises ValidationError
- `test_base_table_source_duckdb_path_optional` -- None by default
- `test_base_table_source_read_table_duckdb_no_path` -- raises ValueError

*TableSource:*
- `test_table_source_valid` -- basic creation
- `test_table_source_with_transforms` -- creation with SourceTransforms
- `test_table_source_with_fields` -- creation with list of Field
- `test_table_source_inherits_base` -- isinstance(TableSource(...), BaseTableSource)

*DerivedSource:*
- `test_derived_source_valid` -- identity_graph="some_graph"
- `test_derived_source_empty_rejected` -- empty string raises ValidationError
- `test_derived_source_frozen` -- assignment after creation raises error

*DerivedEntitySource:*
- `test_derived_entity_source_valid` -- type="identity_graph"

*AggregationSource:*
- `test_aggregation_source_valid` -- source_entity + group_by_column
- `test_aggregation_source_with_filter` -- filter_expression set

*EventAggregationSource:*
- `test_event_aggregation_source_valid` -- project + dataset + table + group_by_column
- `test_event_aggregation_source_inherits_base` -- isinstance check
- `test_event_aggregation_source_empty_group_by_rejected` -- empty string raises

*UnionSource:*
- `test_union_source_valid` -- list with one TableSource
- `test_union_source_multiple` -- list with two TableSources
- `test_union_source_empty_rejected` -- empty list raises ValidationError

*Negative / boundary:*
- `test_skipped_classes_not_importable` -- verify `SignalSourceConfig`, `ProductSourceConfig`, `ProductUnionSource`, `AnonSourceConfig` are not in `typedata.core.source`
- `test_old_names_not_importable` -- verify `BigQuerySource`, `BaseSourceConfig`, `SourceOverrides` are not in `typedata.core.source`

#### Task 6: Run quality gates

```bash
cd /home/tmo/roadtothebeach/tmo/typedata
uv pip install -e ".[dev]"
ruff check src/typedata/core/
pytest tests/test_core_types.py tests/test_core_source.py -v
```

All tests must pass. Ruff must report no errors.

### Test Plan

| Test Category | File | Count | Covers ACs |
|---|---|---|---|
| Enum values + types | `tests/test_core_types.py` | ~6 tests | AC-1, AC-4, AC-11 |
| Field + transform helpers | `tests/test_core_source.py` | ~8 tests | AC-2, AC-6, AC-12 |
| Base + concrete sources | `tests/test_core_source.py` | ~12 tests | AC-3, AC-5, AC-7, AC-8, AC-9, AC-10 |
| Negative / dropped classes | `tests/test_core_source.py` | ~2 tests | AC-13 |
| Re-exports from core | `tests/test_core_types.py` | ~1 test | AC-4 |
| Lint | CLI | ruff check | AC-14 |
| Full suite | CLI | pytest | AC-15 |

**Total:** approximately 29 tests across 2 test files.

**Test execution order:** Task 6 runs after all files are created. No mocking required -- all classes are pure Pydantic models with no external I/O (except `read_table()` which is tested only for the ValueError path, not for actual Ibis connections).
