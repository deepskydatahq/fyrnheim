# Design: M001-E002-S003 -- Extract SourceMapping class

**Date:** 2026-02-17
**Story:** M001-E002-S003-source-mapping
**Status:** Plan

---

## 1. Summary

SourceMapping bridges an Entity definition to a concrete data source, with a `field_mappings` dict that maps entity field names (the contract) to source column names (the physical schema). It is a small (~49 lines) Pydantic model with one `model_validator` that enforces required-field coverage.

This document covers four design decisions: generality for the public library, TenantExtension inclusion, API cleanup opportunities, and interaction with the entity system.

---

## 2. Decision 1: Is SourceMapping generic enough for the public library?

**Decision: Yes. Extract as-is with minimal changes.**

### Analysis

SourceMapping solves a universal data engineering problem: the same logical entity (e.g. "transactions" with fields `transaction_id`, `amount_cents`, `currency`) can be backed by different physical schemas across sources and environments. The mapping layer decouples the entity contract from source column names.

The current implementation has zero timo-data-stack-specific logic:

- It references `Entity` and `Source` via TYPE_CHECKING forward references (no concrete source types baked in).
- The `field_mappings` dict is a plain `dict[str, str]` -- maximally generic.
- The validator checks `entity.required_fields` coverage -- a pattern any user with required/optional field contracts will need.

Real-world usage in timo-data-stack confirms the pattern is clean. The transactions entity file shows the typical flow:

```python
source_mapping = SourceMapping(
    entity=transactions_entity,
    source=BigQuerySource(...),
    field_mappings={
        "transaction_id": "id",   # entity field -> source column
        "amount_cents": "subtotal",
        ...
    },
)
```

This is a pattern that any typedata user defining entities against varying data sources will need.

### What stays the same

- Class name: `SourceMapping`
- Fields: `entity`, `source`, `field_mappings`
- Validator: `validate_required_fields`
- `model_config = {"arbitrary_types_allowed": True}`

### What changes

- TYPE_CHECKING imports update from `from .entity import Entity` / `from .source import Source` to `from typedata.core.entity import Entity` / `from typedata.core.source import Source` (or keep relative `.entity`, `.source` since it stays in `typedata/core/`).

**Recommendation:** Keep relative imports (`.entity`, `.source`) since SourceMapping lives in the same `core` subpackage. This matches the existing pattern and avoids coupling to the full package path.

---

## 3. Decision 2: Should TenantExtension be included or deferred?

**Decision: Defer. Do not extract TenantExtension in this story or in M001.**

### Analysis

TenantExtension is a multi-tenancy concern that wraps SourceMapping with tenant-specific computed columns and measures. Examining the code and usage:

**Arguments for deferring:**

1. **The mission explicitly says to skip it.** M001 notes state: "Skip TenantExtension for now (multi-tenant is deferred)". The epic (M001-E002) says: "Skip TenantExtension for now (multi-tenant is deferred) -- or include as opt-in". The stronger signal is to skip.

2. **It couples to `ComputedColumn` and `Measure` at the type level.** TenantExtension's `all_computed_columns` and `all_measures` properties reach through `self.entity.all_computed_columns` and `self.entity.all_measures`. This creates a dependency chain: TenantExtension -> SourceMapping -> Entity -> ComputedColumn/Measure. While the forward-reference pattern handles this, it adds complexity to model_rebuild ordering for no immediate user benefit.

3. **Only two concrete usages exist** (transactions and subscriptions in the timo-data-stack entities/ directory). This is not yet a well-proven generic pattern -- it is shaped by one project's multi-tenant architecture.

4. **It conflates two concerns.** "Which source backs this entity" (SourceMapping) and "what tenant-specific columns to add" (TenantExtension) are separable. A public library should let users compose these concepts rather than bundling them into a single class.

5. **The name is timo-specific.** "Tenant" is domain language from the timo-data-stack architecture. A public library would likely call this something like `EntityOverlay`, `SchemaExtension`, or `SourceCustomization` -- and the API would probably look different.

**Arguments for including:**

- It is small (69 lines) and could go in as an opt-in class.
- Users doing multi-project data stacks may want it.

**Recommendation:** Defer to a future story (possibly M002 or a later epic). If the pattern proves needed, re-design it with a more generic name and interface. For now, SourceMapping stands alone.

---

## 4. Decision 3: API cleanup for field_mappings interface

**Decision: Keep the current interface. Add one documentation enhancement.**

### Analysis of current interface

```python
field_mappings: dict[str, str] = PydanticField(default_factory=dict)
```

The convention is `{entity_field_name: source_column_name}`. This is documented in the docstring:

> `field_mappings: Dict mapping entity field names to source column names`
> `e.g., {"transaction_id": "id"} means source column "id" maps to entity field "transaction_id"`

### Cleanup options considered

**Option A: Reverse the mapping direction to `{source_column: entity_field}`.**
Rejected. The current direction reads naturally as "the entity's `transaction_id` comes from the source's `id` column". Entity fields are the stable contract; source columns vary. Keying by entity field means you can iterate the mapping and ask "where does each entity field come from?" which is the common query direction during code generation.

**Option B: Use a list of FieldMapping objects instead of plain dict.**

```python
class FieldMapping(BaseModel):
    entity_field: str
    source_column: str
    transform: str | None = None  # future: type cast, expression
```

Considered but rejected for now. The plain dict is simpler, matches how dbt source mappings work, and covers all current use cases. If transforms-at-mapping-time become needed, a `FieldMapping` model can be introduced later as a backward-compatible addition (accept `dict[str, str | FieldMapping]`).

**Option C: Rename to `column_mappings` for clarity.**
Rejected. `field_mappings` is consistent with the Entity model's `required_fields` / `optional_fields` naming. Fields are the typedata abstraction; columns are the source-level term. The mapping bridges entity fields to source columns, so `field_mappings` is accurate.

### What to improve

1. **Add a `description` field** (optional `str | None = None`). SourceMapping instances benefit from a human-readable note, e.g., "Maps transactions entity to Lemonsqueezy BigQuery table". This helps when multiple SourceMappings exist for different environments.

2. **Improve the docstring** to explicitly state the mapping direction convention using the format `entity_field_name -> source_column_name` with a concrete example. The current docstring is good but could be slightly more explicit.

3. **Consider adding a `source_name` or `name` field** (optional `str | None = None`) for identification when users have multiple mappings. Deferred -- can be added later without breaking changes.

**Recommendation:** Extract as-is. Optionally add a `description: str | None = None` field. Do not change `field_mappings` structure.

---

## 5. Decision 4: How does SourceMapping interact with the entity system?

### Dependency graph

```
Entity  <---- SourceMapping ----> Source (union type)
  |                |
  |                +-- field_mappings: dict[str, str]
  |
  +-- required_fields: list[Field]
  +-- optional_fields: list[Field]
  +-- layers: LayersConfig
  +-- source: Source | None  (legacy inline source)
```

### Interaction pattern

SourceMapping is the **external binding** between Entity and Source. It is used when:

1. **Entity defines the contract** (required_fields, optional_fields) without a hardcoded source.
2. **SourceMapping provides the source** and maps source columns to entity fields.
3. **Validator ensures coverage** -- all `entity.required_fields` must have mappings.

This is distinct from the legacy pattern where `Entity.source` was set directly. The two patterns coexist:

- **Legacy:** `Entity(source=BigQuerySource(...))` -- source is embedded in the entity.
- **New:** `Entity(required_fields=[...])` + `SourceMapping(entity=..., source=..., field_mappings={...})` -- source is external.

### Import resolution

SourceMapping uses TYPE_CHECKING imports for Entity and Source to avoid circular dependencies at module load time. This means:

- `SourceMapping` can be defined before Entity and Source are fully loaded.
- `SourceMapping.model_rebuild()` must be called in `typedata/__init__.py` after all modules are imported, alongside `Entity.model_rebuild()`.
- This is already planned in S004 (model_rebuild and public API).

### Where SourceMapping lives in the module tree

```
typedata/
  core/
    entity.py        # Entity, LayersConfig, Source union
    source.py         # BigQuerySource, DerivedSource, Field, etc.
    source_mapping.py # SourceMapping  <-- this story
    layer.py          # PrepLayer, DimensionLayer, etc.
    __init__.py       # re-exports
  __init__.py         # model_rebuild() + public API
```

### Usage in code generation (downstream)

The generator (M001-E003) will need to resolve the source from either:
- `entity.source` (legacy), or
- `source_mapping.source` (new pattern)

This is a generator concern, not a SourceMapping concern. SourceMapping's job is purely declarative: "this entity, backed by this source, with these column mappings." The generator reads this declaration.

### Public API surface

After extraction, SourceMapping should be importable as:

```python
from typedata import SourceMapping
from typedata.core import SourceMapping       # also works
from typedata.core.source_mapping import SourceMapping  # direct
```

---

## 6. Implementation Plan

**Status:** Complete

### Step 1: Create `src/typedata/core/source_mapping.py`

Extract as-is from timo-data-stack with minimal import path changes. No TenantExtension.

**Source:** `/home/tmo/roadtothebeach/tmo/timo-data-stack/metadata/core/source_mapping.py`
**Target:** `src/typedata/core/source_mapping.py`

#### What stays the same

- Class name: `SourceMapping`
- Fields: `entity: "Entity"`, `source: "Source"`, `field_mappings: dict[str, str]`
- `model_config = {"arbitrary_types_allowed": True}`
- `validate_required_fields` model_validator (mode="after")
- `field_mappings` defaults to `default_factory=dict`

#### What changes

- TYPE_CHECKING imports stay relative (`.entity`, `.source`) -- same as source since file lives in same `core/` subpackage. No change needed.

#### Resulting file

```python
"""Source mapping for connecting entities to data sources."""

from typing import TYPE_CHECKING

from pydantic import BaseModel, model_validator
from pydantic import Field as PydanticField

if TYPE_CHECKING:
    from .entity import Entity
    from .source import Source


class SourceMapping(BaseModel):
    """Maps an entity to a specific data source with field mappings.

    This allows the same entity definition to be used with different
    data sources by specifying how source columns map to entity fields.

    Attributes:
        entity: The entity this mapping is for
        source: The data source (BigQuery, etc.)
        field_mappings: Dict mapping entity field names to source column names
                       e.g., {"transaction_id": "id"} means source column "id"
                       maps to entity field "transaction_id"
    """

    model_config = {"arbitrary_types_allowed": True}

    entity: "Entity"
    source: "Source"
    field_mappings: dict[str, str] = PydanticField(default_factory=dict)

    @model_validator(mode="after")
    def validate_required_fields(self) -> "SourceMapping":
        """Validate that all required fields have mappings."""
        if self.entity.required_fields is None:
            return self  # Old-style entity, no validation needed

        required_field_names = {f.name for f in self.entity.required_fields}
        mapped_field_names = set(self.field_mappings.keys())

        missing = required_field_names - mapped_field_names
        if missing:
            raise ValueError(
                f"SourceMapping missing required field mappings: {missing}. "
                f"All required fields must be mapped."
            )
        return self
```

This is byte-for-byte identical to the source file. The TYPE_CHECKING imports already use relative `.entity` and `.source`, which is correct since SourceMapping lives in the same `core/` subpackage in both timo-data-stack and typedata.

### Step 2: Export from `src/typedata/core/__init__.py`

Add to the core package exports:

```python
from .source_mapping import SourceMapping
```

### Step 3: Wire `model_rebuild()` and top-level re-export (deferred to S004)

In S004 (model_rebuild + public API), add to `src/typedata/__init__.py`:

```python
from typedata.core.source_mapping import SourceMapping
# ... after all imports ...
SourceMapping.model_rebuild()
```

This step is explicitly S004's responsibility, not S003's. S003 only creates the module and the core export.

### Step 4: Write tests

File: `tests/test_source_mapping.py`

Test cases derived from acceptance criteria:

| # | Test | What it validates |
|---|------|-------------------|
| 1 | `test_source_mapping_importable` | `from typedata.core import SourceMapping` works |
| 2 | `test_source_mapping_accepts_entity_and_field_mappings` | Constructor accepts `entity`, `source`, `field_mappings` dict |
| 3 | `test_source_mapping_validates_required_field_coverage` | Raises `ValidationError` when required fields lack mappings |
| 4 | `test_source_mapping_allows_unmapped_optional_fields` | Optional fields can be omitted from `field_mappings` |
| 5 | `test_source_mapping_empty_mappings_with_no_required_fields` | Old-style entity (required_fields=None) skips validation |
| 6 | `test_source_mapping_default_empty_field_mappings` | `field_mappings` defaults to `{}` when not provided |

**Note on test setup:** Tests need Entity and Source instances. Since S003 depends on S002 (Entity extraction), tests can use the real Entity/Source classes from `typedata.core`. If S002 is not yet merged, tests should use `unittest.mock.MagicMock` objects with `.required_fields` and `.optional_fields` attributes, relying on `arbitrary_types_allowed` to accept them. The validator only accesses `self.entity.required_fields` (a list of objects with `.name` attributes), so mocks are straightforward.

### Not included (explicitly skipped)

- **TenantExtension** -- Deferred per design decision 2. Multi-tenant patterns are out of scope for M001.
- **`description` field** -- Mentioned as optional in design decision 3. Not adding now; can be added later as a non-breaking change.
- **`name` field** -- Same reasoning as description.

---

## 7. Summary of decisions

| # | Question | Decision | Rationale |
|---|----------|----------|-----------|
| 1 | Generic enough for public library? | Yes, extract as-is | Zero timo-specific logic; universal entity-to-source binding pattern |
| 2 | Include TenantExtension? | Defer | Multi-tenant is out of scope for M001; only 2 usages; name and API are timo-specific |
| 3 | API cleanup for field_mappings? | Keep dict[str, str] | Simple, correct direction, extensible later; optionally add description field |
| 4 | Entity system interaction? | External binding via TYPE_CHECKING | SourceMapping is the new-style pattern; coexists with legacy Entity.source; needs model_rebuild in S004 |
