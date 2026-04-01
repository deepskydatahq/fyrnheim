# Design: M005-E002-S003 -- E2E Test Enhanced UnionSource with Field Normalization on DuckDB

**Story:** M005-E002-S003
**Epic:** M005-E002 -- Enhanced UnionSource with per-source field normalization
**Mission:** M005 -- Source resolution for timo-growth-stack migration
**Date:** 2026-02-27
**Status:** plan

---

## Overview

Design for an end-to-end test that proves the full pipeline for `UnionSource` with per-source `field_mappings` and `literal_columns`: define entity with heterogeneous sources, generate Ibis code, execute on DuckDB, verify unified output table.

---

## Problem Statement

S001 adds `field_mappings` and `literal_columns` to `TableSource`. S002 generates `.relabel()` and `.mutate(ibis.literal())` calls in union sub-source functions. S003 (this story) must prove the full pipeline works end-to-end: two Parquet files with different schemas unify into one table with entity field names, literal column tagging, and correct row counts. Without this E2E test, we only have unit tests for model construction and codegen string output -- we have not verified the generated code actually *executes* correctly on a real backend.

---

## Expert Perspectives

### Technical Architect

**Why this test matters:** The existing E2E tests (14 passing) cover `TableSource` with `SourceMapping.field_mappings` and single-source entities. There is zero E2E coverage for `UnionSource`. The `_generate_union_source_functions()` codegen path produces multiple sub-source functions plus a union aggregator -- this is the most complex codegen path and the highest risk for runtime failures (e.g., column mismatch in `ibis.union()`).

**Minimal API surface:** The test should use the same public API the existing E2E tests use: `Entity`, `UnionSource`, `TableSource`, `generate()`, `create_connection()`, `IbisExecutor`. No new test infrastructure needed.

**Explicit over implicit:** Each assertion should test one specific behavior. Column presence, column absence (source-native names), literal column values per source, and row counts should each be individually verifiable.

### Simplification Review

**What can be removed?**

1. Runner-level tests (`run_entity()`, `run()`) are NOT needed for this story. The existing runner E2E tests demonstrate runner-level wiring. This story should focus on the IbisExecutor-level pipeline to stay minimal. Runner integration for UnionSource can be a separate story if needed.

2. The backward-compatibility E2E test (plain UnionSource without `field_mappings`/`literal_columns`) is needed per AC but can reuse the same two Parquet files with a separate entity definition -- no new fixtures required.

3. We do NOT need to test every combination (e.g., field_mappings only, literal_columns only, both). The unit tests in S002 cover individual codegen variations. The E2E test should prove the "both together" golden path plus the "neither" backward-compat path. Two test scenarios total.

4. The test data should be minimal. Three rows per source is sufficient. Two columns per source (different names) plus one shared column (e.g., `id`) is enough to prove normalization.

**Is every component essential?** Yes -- two fixtures (Parquet files), two entity definitions (enhanced + plain), two test classes (or one class with clear method names). Nothing can be removed without losing AC coverage.

---

## Proposed Solution

### Test Data Design

Two Parquet files simulating heterogeneous sources:

**Source 1: "hubspot_contacts" (3 rows)**
| contact_id | contact_email      | contact_name |
|------------|--------------------|--------------|
| 1          | alice@example.com  | Alice        |
| 2          | bob@example.com    | Bob          |
| 3          | carol@example.com  | Carol        |

**Source 2: "stripe_customers" (2 rows)**
| customer_id | email_address     | full_name |
|-------------|-------------------|-----------|
| 101         | dave@example.com  | Dave      |
| 102         | eve@example.com   | Eve       |

Why this schema:
- Different column counts is not needed; same number of meaningful columns makes the union schema alignment clearer
- Different column *names* (the whole point of field_mappings)
- Different row counts (3 vs 2) so we can verify the sum is 5, not a duplicate or filter
- Simple string/int types -- no need for type complexity in this test

### Entity Definition

Unified entity fields: `id` (INT64), `email` (STRING), `name` (STRING)

```python
entity = Entity(
    name="contacts",
    description="Unified contacts from multiple sources",
    source=UnionSource(
        sources=[
            TableSource(
                project="test", dataset="test", table="hubspot_contacts",
                duckdb_path=str(hubspot_parquet_path),
                field_mappings={"id": "contact_id", "email": "contact_email", "name": "contact_name"},
                literal_columns={"source_platform": "hubspot"},
            ),
            TableSource(
                project="test", dataset="test", table="stripe_customers",
                duckdb_path=str(stripe_parquet_path),
                field_mappings={"id": "customer_id", "email": "email_address", "name": "full_name"},
                literal_columns={"source_platform": "stripe"},
            ),
        ]
    ),
    layers=LayersConfig(
        prep=PrepLayer(model_name="prep_contacts"),
        dimension=DimensionLayer(model_name="dim_contacts"),
    ),
)
```

Note the `field_mappings` direction: `{entity_field: source_column}` -- matching the convention from S001/S002 and E001. The codegen inverts this for `.relabel()`.

### Test Structure

One new test class in `tests/test_e2e_ibis_executor.py`:

```
class TestE2EUnionSourceFieldNormalization:
```

#### Fixtures

**`union_parquets(tmp_path)`** -- creates both Parquet files, returns `(hubspot_path, stripe_path, hubspot_row_count, stripe_row_count)`

**`union_entity_and_generated(union_parquets, tmp_path)`** -- defines entity with field_mappings + literal_columns, generates code, returns `(entity, generated_dir, total_row_count)`

**`plain_union_entity_and_generated(union_parquets, tmp_path)`** -- defines entity with NO field_mappings or literal_columns (backward compat), generates code. This requires both Parquet files to have the *same* column names, so we create a separate pair of Parquet files for this test OR (simpler) we create a second entity that reads a single Parquet source twice. Actually, the simplest approach: create two Parquet files that already share the same column names (id, email, name) and union them without mappings. This needs its own fixture to avoid coupling.

Wait -- simplification review says minimize fixtures. Better approach: the backward-compat test can just use two Parquet files with *identical* column schemas (different data). This is a separate fixture from the "different schemas" fixture. That is still only two fixtures total.

Revised plan: **one fixture for the heterogeneous-schema pair**, **one fixture for the homogeneous-schema pair**, and each scenario builds its own entity.

Even simpler: make the backward-compat test use a single Parquet file listed twice in the UnionSource (same TableSource, same path, no field_mappings). This proves `ibis.union()` works without normalization and avoids needing a second pair of files. The row count is 2x the single file.

**Final fixture design:**

1. `union_parquets(tmp_path)` -- two Parquet files with different schemas (hubspot/stripe)
2. `enhanced_union_generated(union_parquets, tmp_path)` -- entity + generate with field_mappings + literal_columns
3. No dedicated fixture for backward-compat; just a test method that builds a plain UnionSource from a single Parquet file

#### Test Methods (6 methods covering all 6 AC)

| Method | AC Covered |
|--------|-----------|
| `test_output_has_entity_field_names` | AC1: entity field names present |
| `test_source_column_names_absent` | AC1 (inverse): source-native column names absent |
| `test_literal_columns_correct_values` | AC2: literal_columns produce correct per-source values |
| `test_row_count_equals_sum` | AC3: row count = sum of both input files |
| `test_output_columns_complete` | AC4: entity fields + literal columns all present |
| `test_plain_union_backward_compatible` | AC5: UnionSource without field_mappings/literal_columns still works |

AC6 (no regressions) is verified by the existing 14 tests continuing to pass in the same test run.

### Assertions Detail

**`test_output_has_entity_field_names`:**
- Execute entity via IbisExecutor
- Read dim_contacts table to pandas
- Assert `"id" in df.columns`, `"email" in df.columns`, `"name" in df.columns`

**`test_source_column_names_absent`:**
- Assert `"contact_id" not in df.columns`, `"contact_email" not in df.columns`, etc.
- Assert `"customer_id" not in df.columns`, `"email_address" not in df.columns`, etc.

**`test_literal_columns_correct_values`:**
- Filter rows where `source_platform == "hubspot"` -- assert count == 3 (hubspot row count)
- Filter rows where `source_platform == "stripe"` -- assert count == 2 (stripe row count)
- This proves literal_columns injected the correct value per sub-source

**`test_row_count_equals_sum`:**
- Assert `result.row_count == 5` (3 + 2)

**`test_output_columns_complete`:**
- Assert exact column set: `{"id", "email", "name", "source_platform"}` is a subset of `set(df.columns)`
- (dim layer may add no computed columns, so the set should be exactly those 4)

**`test_plain_union_backward_compatible`:**
- Create a single Parquet file with columns `id, email, name` (3 rows)
- Build UnionSource with two TableSources pointing at the same file, no field_mappings, no literal_columns
- Generate, execute, verify row_count == 6 (3 * 2) and columns are `id, email, name`

---

## Design Details

### Imports Needed

The test adds these imports to the existing file (some already imported):

```python
from fyrnheim import (
    Entity, LayersConfig, PrepLayer, DimensionLayer,
    TableSource, UnionSource,
)
from fyrnheim._generate import generate
from fyrnheim.engine.connection import create_connection
from fyrnheim.engine.executor import IbisExecutor
```

`Field` and `SourceMapping` are NOT needed -- `UnionSource` uses `field_mappings` on `TableSource` directly, not `SourceMapping`.

### Generated Code (expected output from S002)

For the enhanced union source, codegen should produce something like:

```python
def source_contacts_hubspot_contacts(conn, backend):
    if backend == "duckdb":
        parquet_path = os.path.expanduser("/tmp/.../hubspot_contacts.parquet")
        return conn.read_parquet(parquet_path).relabel({"contact_id": "id", "contact_email": "email", "contact_name": "name"}).mutate(source_platform=ibis.literal("hubspot"))
    ...

def source_contacts_stripe_customers(conn, backend):
    if backend == "duckdb":
        parquet_path = os.path.expanduser("/tmp/.../stripe_customers.parquet")
        return conn.read_parquet(parquet_path).relabel({"customer_id": "id", "email_address": "email", "full_name": "name"}).mutate(source_platform=ibis.literal("stripe"))
    ...

def source_contacts(conn, backend):
    parts = [
        source_contacts_hubspot_contacts(conn, backend),
        source_contacts_stripe_customers(conn, backend),
    ]
    return ibis.union(*parts)
```

The `.relabel()` inverts `field_mappings` (`{entity: source}` -> `{source: entity}`). The `.mutate()` injects literal columns. This is what S002 implements.

### Where This Test Sits in the Pipeline

```
S001 (model) --> S002 (codegen) --> S003 (this: E2E)
```

S003 depends on both S001 and S002 being complete. It cannot be implemented until the `TableSource.field_mappings` and `TableSource.literal_columns` attributes exist and `_generate_union_source_functions()` generates the `.relabel()` and `.mutate()` calls.

---

## Alternatives Considered

1. **Use SourceMapping instead of per-TableSource field_mappings:** Rejected. The `SourceMapping` class is designed for single-source entities. For UnionSource, each sub-source needs its own mapping. The S001 design decision to put `field_mappings` on `TableSource` directly is simpler and avoids a separate mapping object per sub-source.

2. **Test at runner level (run_entity/run):** Rejected for this story. The runner layer adds complexity (auto-generate, data_dir resolution, quality checks) that is orthogonal to what we are testing. IbisExecutor-level tests are sufficient and more focused. Runner-level UnionSource testing can be a follow-up if needed.

3. **Test field_mappings-only and literal_columns-only as separate E2E tests:** Rejected. Unit tests in S002 cover these individual variations in codegen output. The E2E test should prove the full "both together" path. Adding separate E2E tests for each combination would be redundant with unit coverage.

4. **Create a shared base fixture for all union tests:** Rejected. The enhanced and backward-compat tests need different data (different vs. same schemas). Shared fixtures would add coupling. Two simple, independent fixture setups are clearer.

---

## Success Criteria

- [ ] `TestE2EUnionSourceFieldNormalization` class added to `tests/test_e2e_ibis_executor.py`
- [ ] 6 test methods passing, covering all 6 acceptance criteria
- [ ] All 14 existing E2E tests still pass (run full test file, expect 20 total)
- [ ] No new dependencies or infrastructure needed
- [ ] Test data is minimal (3 + 2 = 5 rows, 3 columns per source)
- [ ] Test follows existing patterns (fixtures, IbisExecutor context manager, create_connection("duckdb"))
