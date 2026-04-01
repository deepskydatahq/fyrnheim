# Design: M005-E003-S004 -- E2E Test Person-Style Identity Graph on DuckDB

**Story:** M005-E003-S004
**Epic:** M005-E003 -- DerivedSource identity graph codegen
**Mission:** M005 -- Source resolution for timo-growth-stack migration
**Date:** 2026-02-28
**Status:** plan

---

## Overview

Design for an end-to-end test that proves the full identity graph pipeline on DuckDB: two source entities with overlapping records join via FULL OUTER JOIN on an email match key, PriorityCoalesce resolves shared fields, and auto-generated columns (source flags, source IDs) are correct.

---

## Problem Statement

Stories S001 (IdentityGraphConfig model), S002 (codegen), and S003 (executor multi-input) implement the identity graph machinery. Without an E2E test, we only have unit tests that verify model construction, generated code strings, and mocked executor dispatch. We have not verified that the generated code actually *executes* correctly on a real DuckDB backend and produces the correct joined output with proper coalesce ordering, flag values, and ID preservation.

This test is the integration proof that the entire identity graph feature works as a coherent pipeline.

---

## Expert Perspectives

### Technical Architect

**What exactly are we proving?** Six distinct behaviors that cannot be verified by unit tests alone:

1. **Row arithmetic:** FULL OUTER JOIN with 3+2 input rows and 1 overlap produces exactly 4 output rows. This proves the join key matching works, the deduplication is correct, and no rows are lost or duplicated.

2. **PriorityCoalesce ordering:** For the overlapping record (bob@ex.com), the name field resolves to the primary source's value ("Bob H" from hubspot, not "Bob S" from stripe). This proves the `.fillna()` chain respects the priority configuration.

3. **Source flag correctness:** `is_hubspot` and `is_stripe` Boolean columns correctly reflect which sources contributed each row. The overlap record (bob) must have both flags True. Non-overlapping records have exactly one flag True, one False/NULL.

4. **Source ID preservation:** `hubspot_id` and `stripe_id` columns carry the original source-specific IDs through the join. For non-matching sources, these columns are NULL.

5. **Schema completeness:** The output table has all expected columns (match_key + coalesced fields + flags + IDs) with no extras or missing columns.

6. **Regression safety:** All 19 existing E2E tests continue to pass.

**Multi-entity execution is the key novelty.** Unlike all existing E2E tests (which execute a single entity), this test must execute three entities in dependency order within the same DuckDB connection. The source entities create dimension tables, and the derived entity reads those tables. This is the first test that exercises the executor's ability to consume previously-persisted tables as inputs.

**Explicit over implicit:** Each assertion tests one behavior. One test for row count, one for coalesce values, one for flags, one for IDs, one for schema, and regression is verified by the existing test suite running in the same pytest session.

### Simplification Reviewer

**How many test methods are truly needed?**

The 6 acceptance criteria map naturally to 5 test methods (AC6 is implicit from running the full test file). However, I propose consolidating to fewer methods because:

- AC1 (row count) and AC5 (schema) can share a single execution -- one method checks row count and column set.
- AC2 (coalesce), AC3 (flags), and AC4 (IDs) each require inspecting specific row data and test genuinely different behaviors -- keep them separate.

**Final method count: 4 test methods.**

1. `test_row_count_and_schema` -- AC1 + AC5
2. `test_priority_coalesce_resolves_name` -- AC2
3. `test_source_flags_correct` -- AC3
4. `test_source_ids_preserved` -- AC4

**Can fixtures be simpler?**

Yes. All 4 tests use the same data and entity definitions. A single class-level fixture that creates parquets, defines all 3 entities, generates code for all 3, and executes all 3 in order can be shared across all test methods. This avoids re-executing the pipeline 4 times.

But: IbisExecutor uses an in-memory DuckDB connection, and each fixture invocation creates a fresh tmp_path. If we use a session-scoped or class-scoped fixture, all 4 tests share one execution. This is the right approach -- the pipeline execution is the expensive part, and all tests are read-only queries against the output.

**Verdict: APPROVED.** Four test methods, one shared fixture that does full pipeline execution, minimal test data (5 input rows total). Nothing can be removed without losing AC coverage.

---

## Proposed Solution

### Test Data Design

Two Parquet files with minimal rows, designed so the overlap scenario is unambiguous:

**Source 1: hubspot_person (3 rows)**

| person_id | email          | full_name | signup_date |
|-----------|----------------|-----------|-------------|
| h1        | alice@ex.com   | Alice H   | 2024-01-01  |
| h2        | bob@ex.com     | Bob H     | 2024-02-01  |
| h3        | carol@ex.com   | Carol H   | 2024-03-01  |

**Source 2: stripe_person (2 rows)**

| customer_id | contact_email | name   | created_at  |
|-------------|---------------|--------|-------------|
| s1          | bob@ex.com    | Bob S  | 2024-01-15  |
| s2          | dave@ex.com   | Dave S | 2024-04-01  |

**Why this data:**

- **Overlap:** bob@ex.com appears in both, creating the one-row overlap. This is the critical record for testing coalesce priority and dual source flags.
- **Asymmetric counts (3 vs 2):** Proves the join is not simply concatenating or filtering. Expected output: 4 rows = 3 + 2 - 1.
- **Different column names:** `full_name` vs `name`, `person_id` vs `customer_id`, `email` vs `contact_email`. The IdentityGraphConfig field mappings must normalize these.
- **Different ID formats:** String IDs (`h1`/`h2`/`h3` vs `s1`/`s2`) make it easy to distinguish source origin in assertions.
- **Minimal columns:** Only the columns needed to test the acceptance criteria. No extra data.

### Entity Definitions

Three entities, defined programmatically in the test fixture:

**1. hubspot_person (TableSource)**

```python
Entity(
    name="hubspot_person",
    description="HubSpot contacts",
    source=TableSource(
        project="test", dataset="test", table="hubspot_person",
        duckdb_path=str(hubspot_parquet_path),
    ),
    layers=LayersConfig(
        prep=PrepLayer(model_name="prep_hubspot_person"),
        dimension=DimensionLayer(model_name="dim_hubspot_person"),
    ),
)
```

**2. stripe_person (TableSource)**

```python
Entity(
    name="stripe_person",
    description="Stripe customers",
    source=TableSource(
        project="test", dataset="test", table="stripe_person",
        duckdb_path=str(stripe_parquet_path),
    ),
    layers=LayersConfig(
        prep=PrepLayer(model_name="prep_stripe_person"),
        dimension=DimensionLayer(model_name="dim_stripe_person"),
    ),
)
```

**3. person (DerivedSource with IdentityGraphConfig)**

```python
Entity(
    name="person",
    description="Unified person entity",
    source=DerivedSource(
        identity_graph="person_graph",
        identity_graph_config=IdentityGraphConfig(
            match_key="email",
            sources=[
                IdentityGraphSource(
                    name="hubspot",
                    entity="hubspot_person",
                    match_key_field="email",
                    fields={"name": "full_name"},
                    id_field="person_id",
                ),
                IdentityGraphSource(
                    name="stripe",
                    entity="stripe_person",
                    match_key_field="contact_email",
                    fields={"name": "name"},
                    id_field="customer_id",
                ),
            ],
            priority=["hubspot", "stripe"],
        ),
        depends_on=["hubspot_person", "stripe_person"],
    ),
    layers=LayersConfig(
        prep=PrepLayer(model_name="prep_person"),
        dimension=DimensionLayer(model_name="dim_person"),
    ),
)
```

**Key design decisions:**

- `match_key="email"`: The unified output column name for the join key.
- `match_key_field` per source: How the match key is named in each source table (`email` in hubspot, `contact_email` in stripe). Codegen must rename these to `email` before joining.
- `fields={"name": "full_name"}` for hubspot: The unified field `name` maps to the source column `full_name`. For stripe, `name` maps to `name` (identity mapping -- the source column already has the right name).
- `priority=["hubspot", "stripe"]`: Hubspot wins in PriorityCoalesce. Bob's `name` should be "Bob H".
- `id_field="person_id"` / `id_field="customer_id"`: These generate the `hubspot_id` and `stripe_id` output columns.
- `depends_on` is explicitly set, but S001 should also auto-derive it from `identity_graph_config.sources[*].entity`. The explicit value here ensures the test works even if auto-derivation is not yet wired.

### Execution Flow

All three entities execute within a single IbisExecutor context sharing one in-memory DuckDB connection:

```
Step 1: generate(hubspot_person, output_dir=generated_dir)
Step 2: generate(stripe_person, output_dir=generated_dir)
Step 3: generate(person, output_dir=generated_dir)
Step 4: IbisExecutor.execute("hubspot_person")  -> creates dim_hubspot_person table
Step 5: IbisExecutor.execute("stripe_person")    -> creates dim_stripe_person table
Step 6: IbisExecutor.execute("person", entity=person_entity)  -> reads dim tables, joins
```

**Why pass `entity=person_entity` in step 6?** Per S003's design, the executor needs the Entity object to detect that the source is a `DerivedSource` and dispatch to the multi-input code path (calling `source_person(sources_dict)` instead of `source_person(conn, backend)`).

**Why not use the runner?** The runner adds entity discovery, auto-generate, quality checks, and data_dir resolution. These are orthogonal to what we are testing. Using IbisExecutor directly gives precise control over execution order and avoids coupling to runner internals. This follows the pattern established by all existing E2E tests.

### Expected Output

The `dim_person` table should have these 4 rows:

| email        | name    | is_hubspot | is_stripe | hubspot_id | stripe_id |
|--------------|---------|------------|-----------|------------|-----------|
| alice@ex.com | Alice H | True       | False     | h1         | NULL      |
| bob@ex.com   | Bob H   | True       | True      | h2         | s1        |
| carol@ex.com | Carol H | True       | False     | h3         | NULL      |
| dave@ex.com  | Dave S  | False      | True      | NULL       | s2        |

Notes:
- `email` is the coalesced match key (FULL OUTER JOIN, then coalesce the two match key columns into one).
- `name` is PriorityCoalesce: hubspot wins, so Bob is "Bob H" not "Bob S". For dave, only stripe has a value, so it is "Dave S".
- `is_hubspot` / `is_stripe` are derived from whether the source contributed a row (match key not null on that side of the join).
- `hubspot_id` / `stripe_id` preserve the original ID columns.

### Test Structure

One new test class in `tests/test_e2e_ibis_executor.py`:

```
class TestE2EIdentityGraphPerson:
```

#### Fixture

A single fixture `identity_graph_output` that performs the full pipeline and returns the output DataFrame plus the execution result. Scoped to the class so all 4 test methods share one execution.

```python
@pytest.fixture()
def identity_graph_output(self, tmp_path):
    """Full identity graph pipeline: 2 source entities + 1 derived person entity."""
    # 1. Create parquet files
    # 2. Define 3 entities
    # 3. Generate code for all 3
    # 4. Create IbisExecutor, execute all 3 in order
    # 5. Read dim_person to DataFrame
    # Return (result, df)
```

Note: Using instance-level fixture (not class-scoped) because `tmp_path` is function-scoped. Each test method gets its own pipeline execution. This is acceptable -- the pipeline is fast (in-memory DuckDB, 5 total rows) and avoids fixture scope complexity. If performance becomes a concern, the fixture can be refactored to class scope with a manual temp directory.

#### Test Methods

**`test_row_count_and_schema`** (AC1 + AC5)

```python
def test_row_count_and_schema(self, identity_graph_output):
    result, df = identity_graph_output
    # AC1: 4 rows from 3+2 with 1 overlap
    assert result.row_count == 4
    assert len(df) == 4
    # AC5: all expected columns present
    expected = {"email", "name", "is_hubspot", "is_stripe", "hubspot_id", "stripe_id"}
    assert expected.issubset(set(df.columns))
```

**`test_priority_coalesce_resolves_name`** (AC2)

```python
def test_priority_coalesce_resolves_name(self, identity_graph_output):
    _, df = identity_graph_output
    # Overlap record: bob's name from hubspot (primary) wins
    bob = df[df["email"] == "bob@ex.com"].iloc[0]
    assert bob["name"] == "Bob H"
    # Non-overlap: values pass through from their source
    alice = df[df["email"] == "alice@ex.com"].iloc[0]
    assert alice["name"] == "Alice H"
    dave = df[df["email"] == "dave@ex.com"].iloc[0]
    assert dave["name"] == "Dave S"
```

**`test_source_flags_correct`** (AC3)

```python
def test_source_flags_correct(self, identity_graph_output):
    _, df = identity_graph_output
    # Overlap: both flags True
    bob = df[df["email"] == "bob@ex.com"].iloc[0]
    assert bob["is_hubspot"] is True  # or == True for pandas bool
    assert bob["is_stripe"] is True
    # Hubspot-only
    alice = df[df["email"] == "alice@ex.com"].iloc[0]
    assert alice["is_hubspot"] is True
    assert alice["is_stripe"] is False  # or is None/NaN -- depends on codegen
    # Stripe-only
    dave = df[df["email"] == "dave@ex.com"].iloc[0]
    assert dave["is_hubspot"] is False
    assert dave["is_stripe"] is True
```

**`test_source_ids_preserved`** (AC4)

```python
def test_source_ids_preserved(self, identity_graph_output):
    _, df = identity_graph_output
    # Overlap: both IDs present
    bob = df[df["email"] == "bob@ex.com"].iloc[0]
    assert bob["hubspot_id"] == "h2"
    assert bob["stripe_id"] == "s1"
    # Hubspot-only: stripe_id is null
    alice = df[df["email"] == "alice@ex.com"].iloc[0]
    assert alice["hubspot_id"] == "h1"
    assert pd.isna(alice["stripe_id"])
    # Stripe-only: hubspot_id is null
    dave = df[df["email"] == "dave@ex.com"].iloc[0]
    assert pd.isna(dave["hubspot_id"])
    assert dave["stripe_id"] == "s2"
```

### Imports

The test file already imports most of what is needed. New imports required:

```python
from fyrnheim import DerivedSource, IdentityGraphConfig, IdentityGraphSource
```

These will be exported from `fyrnheim.__init__` as part of S001. The imports for `Entity`, `LayersConfig`, `PrepLayer`, `DimensionLayer`, `TableSource`, `generate`, `create_connection`, and `IbisExecutor` are already present in the test file.

---

## Design Details

### Dependency Chain

```
S001 (IdentityGraphConfig model)
  -> S002 (codegen: join, coalesce, auto-columns)
    -> S003 (executor: multi-input dispatch)
      -> S004 (this: E2E integration proof)
```

S004 cannot be implemented until S001-S003 are complete. The generated code path that S004 exercises:

1. `IbisCodeGenerator._generate_derived_source_function()` (S002) generates a `source_person(sources: dict)` function
2. `IbisExecutor._run_transform_pipeline()` (S003) detects DerivedSource, builds `sources_dict` from connection catalog, calls `source_person(sources_dict)`
3. The generated function performs FULL OUTER JOIN on match key, applies PriorityCoalesce, and creates auto-columns

### What the Generated Code Should Look Like

For the person entity, the codegen (S002) should produce something like:

```python
def source_person(sources: dict) -> ibis.Table:
    """Identity graph join for person."""
    t_hubspot = sources["hubspot"]
    t_stripe = sources["stripe"]

    # Rename match keys to unified name
    t_hubspot = t_hubspot.rename({"email": "email"})  # identity (already named email)
    t_stripe = t_stripe.rename({"email": "contact_email"})  # rename contact_email -> email

    # Rename fields
    t_hubspot = t_hubspot.rename({"name": "full_name"})
    t_stripe = t_stripe.rename({"name": "name"})  # identity

    # Track source flags before join
    t_hubspot = t_hubspot.mutate(is_hubspot=ibis.literal(True))
    t_stripe = t_stripe.mutate(is_stripe=ibis.literal(True))

    # Rename ID fields
    t_hubspot = t_hubspot.rename({"hubspot_id": "person_id"})
    t_stripe = t_stripe.rename({"stripe_id": "customer_id"})

    # FULL OUTER JOIN
    joined = t_hubspot.outer_join(t_stripe, "email")

    # Coalesce match key
    joined = joined.mutate(email=ibis.coalesce(t_hubspot.email, t_stripe.email))

    # PriorityCoalesce: hubspot first, then stripe
    joined = joined.mutate(name=ibis.coalesce(t_hubspot.name, t_stripe.name))

    # Fill source flags (NULL -> False)
    joined = joined.mutate(
        is_hubspot=joined.is_hubspot.fillna(False),
        is_stripe=joined.is_stripe.fillna(False),
    )

    # Select final columns
    return joined.select("email", "name", "is_hubspot", "is_stripe", "hubspot_id", "stripe_id")
```

The exact codegen output is S002's responsibility. This E2E test does not assert on generated code -- it only asserts on execution output.

### Flag Semantics: True/False vs True/NULL

The codegen design (S002) should produce Boolean flags that are True/False (not True/NULL). The generated code should `.fillna(False)` on the source flags after the FULL OUTER JOIN, because NULL on the non-matching side of the join is semantically False ("this source did not contribute this record").

The test assertions should use `== True` and `== False` with pandas Boolean comparison, not `is True`/`is False` (which may not work with pandas/numpy dtypes). Alternatively, convert to Python bool first.

### Regression Safety (AC6)

AC6 is verified implicitly: the new test class is added to the existing `test_e2e_ibis_executor.py` file. Running `pytest tests/test_e2e_ibis_executor.py` will execute all 19 existing tests plus the 4 new ones (23 total). If any existing test breaks, the CI run fails.

No existing entity definitions, fixtures, or test methods are modified. The new test class is entirely additive.

---

## Alternatives Considered

1. **Use the runner (`run()`) instead of IbisExecutor directly.** Rejected. The runner adds entity discovery from a directory, auto-generate, and quality checks. These are orthogonal concerns. Using IbisExecutor directly matches the existing E2E test pattern and gives precise control over multi-entity execution order. The runner does not yet support explicit execution ordering for DerivedSource (it relies on `resolve_execution_order()` which S003 wires up), so testing at the executor level avoids coupling to runner internals.

2. **One comprehensive test method instead of four.** Rejected. A single method with 15+ assertions is harder to debug when something fails. Four focused methods with clear names make it immediately obvious which behavior broke. The fixture sharing means no execution overhead.

3. **Use `date_field` for first_seen columns.** Deferred. The acceptance criteria do not mention first_seen date columns. The story TOML explicitly lists: match_key + coalesced fields + flags + IDs. Date columns are part of the codegen design (S002) but not required for this E2E test. They can be added in a follow-up test if needed.

4. **Test 3+ sources (not just 2).** Deferred. The acceptance criteria specify "two source entities." A 3-source test is valuable for proving cascading joins but is better suited to a follow-up story. This test focuses on the 2-source golden path which is the minimum for proving identity graph correctness.

5. **Class-scoped fixture to avoid re-execution.** Considered but deferred. `tmp_path` is function-scoped, and creating a class-scoped manual temp directory adds complexity. With 5 total input rows and in-memory DuckDB, the pipeline execution is sub-second per test. The simplicity of function-scoped fixtures outweighs the marginal performance cost.

---

## Success Criteria

- [ ] `TestE2EIdentityGraphPerson` class added to `tests/test_e2e_ibis_executor.py`
- [ ] 4 test methods passing, covering all 6 acceptance criteria (AC1+AC5 combined, AC6 implicit)
- [ ] All 19 existing E2E tests still pass (expect 23 total in the file)
- [ ] No new dependencies or test infrastructure needed
- [ ] Test data is minimal (3 + 2 = 5 input rows, 4 output rows)
- [ ] Multi-entity execution within a single IbisExecutor context works correctly
- [ ] Test follows existing patterns (fixtures, `IbisExecutor` context manager, `create_connection("duckdb")`)
- [ ] New imports limited to `DerivedSource`, `IdentityGraphConfig`, `IdentityGraphSource` (from S001)
