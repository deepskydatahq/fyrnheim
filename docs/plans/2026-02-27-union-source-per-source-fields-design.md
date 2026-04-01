# UnionSource Per-Source Fields Design

## Overview
Add two optional dict fields (`field_mappings`, `literal_columns`) to `TableSource` so that UnionSource sub-sources can normalize columns before UNION ALL. This is the model-layer change that enables codegen in S002.

## Problem Statement
UnionSource combines multiple TableSources into one, but sources often have different column names for the same concept (e.g. `contact_email` vs `email_address`). They also need tagging with constant values (e.g. `source_platform='hubspot'`). Currently there's no per-source normalization — all sources must already have matching schemas.

## Expert Perspectives

### Technical
- **Direction semantics:** `{source_column: unified_column}` — at the source-normalization layer, the natural operation is "rename source column X to Y before union." This is the inverse of `SourceMapping.field_mappings` which uses `{entity_field: source_column}` at the entity contract layer.
- **Naming:** Keep `field_mappings` as specified in the story. A docstring distinguishes it from `SourceMapping.field_mappings`. The two operate at different abstraction layers.
- **Placement:** Fields go on `TableSource` (not `BaseTableSource`) since only `TableSource` is used in `UnionSource.sources`.

### Simplification Review
- Nothing to remove — design is minimal.
- Two separate fields (field_mappings + literal_columns) justified: consumed separately by codegen, clearer than a combined dict.
- No custom validators beyond Pydantic type checking (YAGNI — codegen validates at generation time).

## Proposed Solution

Add two optional fields to `TableSource` in `src/fyrnheim/core/source.py`:

```python
class TableSource(BaseTableSource):
    transforms: SourceTransforms | None = None
    fields: list[Field] | None = None
    field_mappings: dict[str, str] = PydanticField(default_factory=dict)
    literal_columns: dict[str, Any] = PydanticField(default_factory=dict)
```

- `field_mappings`: `{source_col: unified_col}` — renames before union
- `literal_columns`: `{col_name: value}` — constant column injection
- Both default to `{}` — fully backward compatible

## Design Details

### Files Changed
1. **`src/fyrnheim/core/source.py`** — Add two fields + docstring to `TableSource`
2. **`tests/test_core_source.py`** — Add 7 new tests

### Tests
- `TestTableSource`: field_mappings default, literal_columns default, with field_mappings, with literal_columns
- `TestUnionSource`: union with field_mappings, union with literal_columns, union with both

### Not Changed
- No codegen changes (S002)
- No new classes or modules
- No changes to `BaseTableSource`, `UnionSource`, or `SourceMapping`
- No exports changes (fields are on an already-exported class)

## Alternatives Considered
1. **New `UnionSourceEntry` wrapper class** — rejected: adds type complexity, changes `UnionSource.sources` type, breaks backward compat
2. **Fields on `BaseTableSource`** — rejected: `EventAggregationSource` inherits from it and doesn't need these
3. **Rename to `column_renames`** — rejected: diverges from story spec which explicitly says `field_mappings`

## Success Criteria
- All 6 acceptance criteria pass
- All existing tests pass unchanged
- `field_mappings` direction is `{source_column: unified_column}` (documented in docstring)
