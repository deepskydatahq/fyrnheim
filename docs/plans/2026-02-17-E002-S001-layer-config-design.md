# Design: M001-E002-S001 -- Extract layer configuration classes

**Date:** 2026-02-17
**Story:** M001-E002-S001-layer-config
**Source:** `/home/tmo/roadtothebeach/tmo/timo-data-stack/metadata/core/layer.py`
**Target:** `src/typedata/core/layer.py`

---

## Source Analysis

The source file (`layer.py`, 230 lines) defines four Pydantic configuration models:

| Class | Lines | Fields | Methods |
|-------|-------|--------|---------|
| `PrepLayer` | 14-61 | 6 data fields | `model_post_init`, `to_sql` |
| `DimensionLayer` | 64-105 | 6 data fields | `to_sql` |
| `SnapshotLayer` | 108-118 | 7 data fields | (none) |
| `ActivityLayer` | 120-229 | 6 data fields | `standard_computed_columns`, `standard_additional_columns`, `to_sql` |

All four classes inherit from `pydantic.BaseModel`. The module imports `MaterializationType` from `core/types` and sets `ComputedColumn = Any` as a forward-reference placeholder.

---

## Design Decisions

### Decision 1: Strip all `to_sql()` methods entirely

**Recommendation: Remove. Do not keep as utility.**

Rationale:

- The `to_sql()` methods on `PrepLayer`, `DimensionLayer`, and `ActivityLayer` generate **dbt Jinja SQL** (e.g., `{{ config(...) }}`, `{{ ref('...') }}`, `{{ dbt_utils.incremental_where(...) }}`). This is not portable SQL -- it is dbt-specific templating that has no meaning outside dbt.
- Typedata's architecture is `Entity -> Layers -> Ibis expressions -> Any backend`. Code generation lives in Epic E003 (`generators/ibis_code_generator.py`), not in the layer config classes. The layer classes are **configuration**, not **execution**.
- Keeping `to_sql()` even as a utility would create a confusing duality: the "real" path is Ibis generation (E003), but there would also be a dead dbt SQL path sitting in the config classes. This violates the vision statement: "Not a dbt wrapper -- we replace dbt's approach entirely."
- The SQL generation logic is tightly coupled to dbt conventions (Jinja config blocks, `ref()` macros, `dbt_utils` package). Generalizing it would mean rewriting it entirely, at which point it is no longer "keeping" the old code.
- If a dbt export target is ever needed in the future, it belongs in a separate `typedata.exporters.dbt` module, not on the config classes themselves.

**Action:** Delete `to_sql()` from all three classes.

### Decision 2: Strip `standard_computed_columns()` and `standard_additional_columns()` from ActivityLayer

**Recommendation: Remove.**

Rationale:

- These methods return **raw SQL strings** with BigQuery-specific syntax (`EXTRACT(HOUR FROM ts)`, `CAST(... AS FLOAT64)`, `JSON_EXTRACT_SCALAR`). They are not backend-portable.
- In typedata, the equivalent logic will be expressed as Ibis expressions during code generation (E003). The layer config should declare *what* to compute, not *how* to compute it in a specific SQL dialect.
- The `include_standard_computed: bool` field on `ActivityLayer` can stay -- it is a declarative flag that the Ibis code generator can read. The generator (E003) will be responsible for producing the actual Ibis window functions.

**Action:** Delete `standard_computed_columns()` and `standard_additional_columns()`. Keep `include_standard_computed` as a declarative config flag.

### Decision 3: Handle `ComputedColumn = Any` forward reference

**Recommendation: Keep the exact same pattern.**

Rationale:

- The source uses `ComputedColumn = Any` as a module-level placeholder, then resolves it later via `model_rebuild()`. This is an established Pydantic v2 pattern for circular/forward references.
- The story S004 (model-rebuild-public-api) explicitly handles the `model_rebuild()` call in `typedata/__init__.py` after all modules load.
- The epic notes confirm: "Uses TYPE_CHECKING imports for ComputedColumn, Measure, QualityConfig (resolved via model_rebuild)."

**Action:** Keep `ComputedColumn = Any` as-is. Add a comment explaining the resolution mechanism:

```python
# Forward reference: resolved to typedata.components.ComputedColumn
# via model_rebuild() in typedata/__init__.py
ComputedColumn = Any
```

### Decision 4: Generalize dbt-specific fields

Four fields need evaluation:

| Field | Class | dbt-Specific? | Decision |
|-------|-------|---------------|----------|
| `target_schema: str` | PrepLayer, DimensionLayer | Partially. dbt uses "schema" for output dataset routing. | **Keep.** Schema/dataset targeting is a universal warehouse concept, not dbt-specific. Useful for Ibis `create_table` calls. |
| `materialization: MaterializationType` | PrepLayer, DimensionLayer, SnapshotLayer | The *name* "materialization" comes from dbt, but the *concept* (table vs view vs incremental) is universal. | **Keep.** `MaterializationType` is already extracted in S002 as a clean enum. Ibis backends need to know whether to `CREATE TABLE` or `CREATE VIEW`. |
| `tags: list[str]` | PrepLayer, DimensionLayer | dbt uses tags for model selection (`dbt run --select tag:prep`). | **Keep.** Tags are useful metadata for any system (filtering, discovery, documentation). Not dbt-specific. |
| `tests: list[Any]` | PrepLayer, DimensionLayer | dbt test configs. | **Rename to `quality_checks`** to align with typedata's quality framework (E001-S005). Keep the type as `list[Any]` for now -- E002-S004 will resolve forward references to `QualityConfig`. |

**Additional field review:**

| Field | Class | Decision |
|-------|-------|---------|
| `partitioning_field`, `partitioning_type` | SnapshotLayer | **Keep.** These are warehouse-level concepts (BigQuery partitioning, DuckDB doesn't use them but ignores them). Backend-agnostic config. |
| `clustering_fields` | SnapshotLayer | **Keep.** Same reasoning -- physical optimization hints that backends can use or ignore. |
| `partition_by`, `cluster_by` | ActivityLayer | **Keep.** Same as above. |
| `unique_key` | ActivityLayer | **Keep.** Needed for incremental merge logic regardless of backend. |
| `lookback_days` | ActivityLayer | **Keep.** Incremental processing window -- backend-agnostic concept. |
| `depends_on: list[str]` | PrepLayer | **Keep.** DAG dependency declaration is universal. |

### Decision 5: What stays, what gets cleaned up -- full summary

**Kept as-is (pure configuration):**

- `PrepLayer` -- all 6 data fields, `model_post_init` (auto-adds "prep" tag)
- `DimensionLayer` -- all 6 data fields
- `SnapshotLayer` -- all 7 data fields (entirely declarative, no methods)
- `ActivityLayer` -- all 6 data fields, `include_standard_computed` flag

**Removed (dbt/SQL generation):**

- `PrepLayer.to_sql()` -- dbt Jinja SQL generation
- `DimensionLayer.to_sql()` -- dbt Jinja SQL generation
- `ActivityLayer.to_sql()` -- dbt Jinja SQL generation
- `ActivityLayer.standard_computed_columns()` -- raw BigQuery SQL strings
- `ActivityLayer.standard_additional_columns()` -- raw BigQuery SQL strings

**Renamed:**

- `tests: list[Any]` -> `quality_checks: list[Any]` on PrepLayer and DimensionLayer (aligns with typedata naming)

**Import update:**

- `from .types import MaterializationType` -- stays the same (relative import within `typedata.core`)

---

## Resulting File Structure

```python
"""Layer configuration classes for entity transformation pipelines."""

from typing import Any

from pydantic import BaseModel
from pydantic import Field as PydanticField

from .types import MaterializationType

# Forward reference: resolved to typedata.components.ComputedColumn
# via model_rebuild() in typedata/__init__.py
ComputedColumn = Any


class PrepLayer(BaseModel):
    """Prep/staging layer configuration."""

    model_name: str
    computed_columns: list[ComputedColumn] = PydanticField(default_factory=list)
    materialization: MaterializationType = MaterializationType.TABLE
    target_schema: str = "prep"
    tags: list[str] = PydanticField(default_factory=list)
    quality_checks: list[Any] = PydanticField(default_factory=list)
    depends_on: list[str] = PydanticField(default_factory=list)

    def model_post_init(self, __context) -> None:
        """Add default tags."""
        if "prep" not in self.tags:
            self.tags.append("prep")


class DimensionLayer(BaseModel):
    """Dimension layer configuration (Type 1 or Type 2 SCD)."""

    model_name: str
    computed_columns: list[ComputedColumn] = PydanticField(default_factory=list)
    materialization: MaterializationType = MaterializationType.TABLE
    target_schema: str = "business"
    tags: list[str] = PydanticField(default_factory=list)
    quality_checks: list[Any] = PydanticField(default_factory=list)


class SnapshotLayer(BaseModel):
    """Snapshot layer configuration (daily snapshots)."""

    enabled: bool = True
    date_column: str = "ds"
    deduplication_order_by: str = "updated_at DESC"
    partitioning_field: str = "ds"
    partitioning_type: str = "DAY"
    clustering_fields: list[str] = PydanticField(default_factory=list)
    materialization: MaterializationType = MaterializationType.INCREMENTAL


class ActivityLayer(BaseModel):
    """Activity schema layer configuration."""

    activity_types: list[Any] = PydanticField(default_factory=list)
    include_standard_computed: bool = True
    partition_by: str = "date_"
    cluster_by: list[str] = PydanticField(
        default_factory=lambda: ["activity", "customer", "ts"]
    )
    unique_key: str = "activity_id"
    lookback_days: int = 7
```

---

## Acceptance Criteria Mapping

| Acceptance Criterion | Addressed By |
|---------------------|--------------|
| PrepLayer importable with model_name, computed_columns, materialization, target_schema | All fields kept |
| DimensionLayer importable with model_name, computed_columns, materialization | All fields kept |
| SnapshotLayer importable with enabled flag and configuration | All fields kept (no changes) |
| ActivityLayer importable with model_name and configuration | All fields kept, methods stripped |
| PrepLayer auto-adds 'prep' tag in model_post_init | `model_post_init` kept |

---

## Risks and Open Questions

1. **`tests` -> `quality_checks` rename** -- The downstream story S002 (Entity/LayersConfig) and the quality framework (E001-S005) should use `quality_checks` consistently. Verify this does not conflict with any existing naming in those stories.

2. **ActivityLayer docstring update** -- The original docstring references "dbt incremental model". Update to: "Composes multiple ActivityType instances into a complete incremental model with standard ActivitySchema 2.0 computed columns and table configuration."

3. **`deduplication_order_by` on SnapshotLayer** -- This is a raw SQL fragment (`"updated_at DESC"`). In the future, this could be expressed as a structured sort config, but for now keep as string since it is backend-agnostic ordering syntax. Flag for E003 to handle appropriately during code generation.
