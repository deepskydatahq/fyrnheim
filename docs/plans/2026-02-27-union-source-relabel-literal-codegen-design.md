# Union Source .relabel() and ibis.literal() Codegen Design

**Date:** 2026-02-27
**Story:** M005-E002-S002
**Task:** typedata-g8k
**Status:** Brainstorm

---

## Overview

Modify `_generate_union_source_functions()` in `IbisCodeGenerator` to emit `.relabel()` and `.mutate(ibis.literal(...))` calls on per-sub-source return expressions when `field_mappings` or `literal_columns` are present on the `TableSource`. Sub-sources without these fields generate identical code to today.

## Problem Statement

UnionSource combines multiple TableSources into a single table via `ibis.union()`. When sources have different column names for the same concept (e.g., `contact_email` vs `email`) or need constant columns injected (e.g., `product_type='video'`), the generated code currently has no mechanism to normalize per-source before the union. S001 adds the model fields; this story makes the code generator use them.

## Expert Perspectives

### Technical

**1. The field_mappings direction question**

This is the most important design decision. There are two competing conventions:

- **SourceMapping.field_mappings** (E001): `{entity_field: source_column}` direction. Meaning: "entity field `transaction_id` comes from source column `id`." Used at the entity-contract layer. The generated `.rename()` call currently passes this dict AS-IS to Ibis `.rename()` (which accepts `{new_name: old_name}`).

- **TableSource.field_mappings** (S001 design doc): `{source_column: unified_column}` direction. Meaning: "source column `email` becomes `contact_email` before union." Used at the source-normalization layer.

The **S002 acceptance criteria** say: `field_mappings={'email': 'contact_email'}` generates `.relabel({'contact_email': 'email'})`. Reading this: the AC says the dict stores `{source_col: unified_col}` = `{'email': 'contact_email'}`, and `.relabel()` needs `{old_name: new_name}` = ... wait. `.relabel({'contact_email': 'email'})` would rename `contact_email` to `email`, but the source column is `email` and we want it unified to `contact_email`. That means the AC is inverting.

Let me re-read: the AC says the source has column `email` and we want it to become `contact_email` in the unified schema. If `field_mappings={'email': 'contact_email'}` means `{source_col: unified_col}`, then `.relabel()` should be `.relabel({'email': 'contact_email'})` (rename `email` to `contact_email`) -- NO inversion needed since Ibis `.relabel()` takes `{old_name: new_name}`.

But the AC says it generates `.relabel({'contact_email': 'email'})` -- which would rename `contact_email` to `email`. This is wrong if we follow the S001 design direction.

The **S002 implementation hints** say: "append `.relabel({v: k for k, v in field_mappings.items()})` ... field_mappings stores `{entity_field: source_col}`, `.relabel()` needs `{source_col: entity_field}`". This hints assume `SourceMapping`-style direction `{entity_field: source_column}`, not the S001 design's `{source_column: unified_column}` direction.

**Resolution:** The S001 design doc explicitly states `{source_column: unified_column}` direction for `TableSource.field_mappings`. This was a deliberate decision to differentiate from `SourceMapping.field_mappings`. Since `.relabel()` in Ibis takes `{old_name: new_name}`, and `field_mappings` stores `{source_col: unified_col}` which IS `{old_name: new_name}`, the codegen should pass the dict directly to `.relabel()` with NO inversion.

The acceptance criteria example is inconsistent with the S001 design. The AC appears to assume `{entity_field: source_column}` direction (like SourceMapping), but S001 designed the opposite. **Follow the S001 design** -- it was a deliberate architectural choice. The AC example should be read as: `field_mappings={'email': 'contact_email'}` (source `email` becomes `contact_email`) generates `.relabel({'email': 'contact_email'})`. The tests should verify this behavior.

**2. `.relabel()` vs `.rename()` in Ibis**

The codebase currently uses `.rename()` for the `SourceMapping` case (`_build_rename_suffix`). However:

- Ibis `.rename()` accepts `{new_name: old_name}` dict or keyword `rename(new_name="old_name")` syntax. The direction is INVERSE of `.relabel()`.
- Ibis `.relabel()` accepts `{old_name: new_name}` dict. More natural for "rename source column X to Y."

Since `TableSource.field_mappings` stores `{source_col: unified_col}` = `{old_name: new_name}`, `.relabel()` is the natural fit -- pass the dict directly with no inversion. This is cleaner than using `.rename()` which would require inverting the dict.

The story explicitly requests `.relabel()`. Use it.

**3. `.mutate(col=ibis.literal(val))` generation**

For `literal_columns={'product_type': 'video'}`, generate:

```python
.mutate(product_type=ibis.literal('video'))
```

Key considerations:
- Column names must be valid Python identifiers (they will be used as keyword arguments)
- Values need proper quoting: strings get `ibis.literal('value')`, numbers get `ibis.literal(42)`, booleans get `ibis.literal(True)`
- Use `repr()` for the value inside `ibis.literal()` -- this handles string quoting and numeric/boolean serialization correctly

**4. Chain order**

```
conn.read_parquet(path).relabel({...}).mutate(col=ibis.literal(val))
```

Relabel first (fix column names), then mutate (add new columns). This order is correct because:
- Literal columns are new columns that don't exist in the source -- order doesn't matter for them
- But conceptually, normalize existing columns first, then augment

**5. Code generation approach**

Build suffix strings for each sub-source, similar to the existing `_build_rename_suffix()` pattern:

```python
# For each sub_source in source.sources:
suffix = ""
if sub_source.field_mappings:
    suffix += f".relabel({sub_source.field_mappings!r})"
if sub_source.literal_columns:
    literal_args = ", ".join(
        f"{col}=ibis.literal({val!r})" for col, val in sub_source.literal_columns.items()
    )
    suffix += f".mutate({literal_args})"
```

Then append `suffix` to both the DuckDB and BigQuery return expressions.

**6. No changes to the union aggregator**

The `source_{name}()` function that calls `ibis.union(*parts)` stays identical. All normalization happens inside the per-sub-source functions. This is the correct separation of concerns.

### Simplification Review

- **What would I remove?** Nothing. The design adds suffix strings to return expressions -- minimal code change.
- **Is every component essential?** Yes:
  - `.relabel()` suffix: essential for column renaming
  - `.mutate()` suffix: essential for literal column injection
  - Chain order (relabel then mutate): essential for correctness
  - Conditional generation (skip when empty): essential for backward compat
- **Could the two suffixes be combined?** No -- `.relabel()` takes a dict, `.mutate()` takes keyword args. They are different Ibis operations.
- **Could we reuse `_build_rename_suffix()`?** No -- that method reads from `self.source_mapping` (entity-level), while this reads from `sub_source.field_mappings` (per-source). Different data sources, different direction semantics, different Ibis method (`.rename()` vs `.relabel()`). A helper method _per sub-source_ could work but is over-abstraction for two string concatenations.
- **Should we build a generic "suffix builder" abstraction?** No. YAGNI. Two inline if-blocks in the loop are clearer than a new method for a pattern used exactly once.

## Proposed Solution

Modify `_generate_union_source_functions()` in `IbisCodeGenerator` to:

1. For each sub-source, compute a `suffix` string:
   - If `sub_source.field_mappings` is non-empty: append `.relabel({...})` using the dict AS-IS (no inversion)
   - If `sub_source.literal_columns` is non-empty: append `.mutate(col=ibis.literal(val), ...)`
2. Append `suffix` to the return expressions in both DuckDB and BigQuery branches
3. Leave the union aggregator function unchanged

## Design Details

### Files Changed

1. **`src/fyrnheim/generators/ibis_code_generator.py`** -- Modify `_generate_union_source_functions()` to build and append suffix per sub-source
2. **`tests/test_ibis_code_generator.py`** -- Add new test class for union sub-source field_mappings and literal_columns codegen

### Code Change in `_generate_union_source_functions()`

The change is localized to the inner loop where sub-source functions are generated. Currently the return expressions are:

```python
return conn.read_parquet(parquet_path)
# and
return conn.table("...", database=("...", "..."))
```

After the change, they become (when suffix is non-empty):

```python
return conn.read_parquet(parquet_path).relabel({...}).mutate(...)
# and
return conn.table("...", database=("...", "...")).relabel({...}).mutate(...)
```

The suffix is computed once per sub-source and appended to both branches.

### Suffix Building Logic

```python
suffix = ""
if sub_source.field_mappings:
    suffix += f".relabel({sub_source.field_mappings!r})"
if sub_source.literal_columns:
    literal_args = ", ".join(
        f"{col}=ibis.literal({val!r})"
        for col, val in sub_source.literal_columns.items()
    )
    suffix += f".mutate({literal_args})"
```

This uses `repr()` for both the relabel dict and literal values, ensuring correct Python syntax for strings (`'video'`), numbers (`42`), and booleans (`True`).

### Test Plan

New test class `TestUnionSourceFieldMappingsCodegen` (or similar) with these tests:

| Test | What it verifies |
|------|------------------|
| `test_relabel_generated_for_field_mappings` | Sub-source with `field_mappings={'email': 'contact_email'}` generates `.relabel({'email': 'contact_email'})` |
| `test_mutate_literal_generated_for_literal_columns` | Sub-source with `literal_columns={'product_type': 'video'}` generates `.mutate(product_type=ibis.literal('video'))` |
| `test_both_relabel_and_mutate_chained` | Sub-source with both generates `.relabel({...}).mutate(...)` |
| `test_empty_mappings_no_suffix` | Sub-source with empty dicts generates same code as before |
| `test_multiple_sources_different_mappings` | Two sub-sources with different field_mappings each get their own `.relabel()` |
| `test_generated_module_valid_python` | Full module with field_mappings/literal_columns passes `ast.parse()` |
| `test_union_aggregator_unchanged` | `source_{name}()` still calls `ibis.union()` with no changes |
| `test_existing_union_tests_still_pass` | Existing `TestUnionSourceGeneration` tests pass (backward compat) |

### Dependency

This story depends on S001 (`field_mappings` and `literal_columns` fields on `TableSource`). S001 is in-progress. The codegen changes will only work once S001 is merged. Tests can be written with the assumption that S001 fields exist.

### Direction Reconciliation Summary

| Layer | Dict | Direction | Ibis Method | Inversion |
|-------|------|-----------|-------------|-----------|
| `SourceMapping.field_mappings` | `{'transaction_id': 'id'}` | entity_field -> source_col | `.rename()` | None (`.rename()` takes `{new: old}`) |
| `TableSource.field_mappings` | `{'email': 'contact_email'}` | source_col -> unified_col | `.relabel()` | None (`.relabel()` takes `{old: new}`) |

Both pass their dicts AS-IS to their respective Ibis methods. No inversion anywhere. Clean.

**Note:** The acceptance criteria in the story TOML show `.relabel({'contact_email': 'email'})` which would be an inversion. This appears to be an error in the AC -- it was written assuming `SourceMapping`-style `{entity_field: source_column}` direction, but S001 chose the opposite direction for `TableSource`. The implementation should follow the S001 design (no inversion), and the AC test assertions should match.

## Alternatives Considered

1. **Use `.rename()` instead of `.relabel()`** -- Rejected. `.rename()` takes `{new: old}` direction, which would require inverting `TableSource.field_mappings` (which stores `{old: new}`). `.relabel()` takes `{old: new}` which matches directly. The story also explicitly requests `.relabel()`.

2. **Build a shared helper for suffix construction** -- Rejected. The suffix logic is 6 lines of code used in one place. A helper method adds indirection without value. If a third call site appears, refactor then.

3. **Change `TableSource.field_mappings` direction to match `SourceMapping`** -- Rejected. S001 design made a deliberate choice to use `{source_col: unified_col}` at the normalization layer. This is the natural "rename X to Y" direction and matches `.relabel()` semantics. Changing it now would break S001.

4. **Generate multi-line code (assign to variable, then relabel)** -- Rejected. Chaining on the return expression is simpler, matches the existing pattern for `_build_rename_suffix()`, and produces valid Python.

## Success Criteria

- Sub-source with `field_mappings` generates `.relabel()` on return expression (no inversion)
- Sub-source with `literal_columns` generates `.mutate(col=ibis.literal(val))`
- Both can be chained: `.relabel({...}).mutate(...)`
- Empty mappings produce no suffix (backward compatible)
- Multiple sub-sources get independent suffixes
- Generated module passes `ast.parse()`
- Union aggregator function unchanged
- All existing union codegen tests pass
