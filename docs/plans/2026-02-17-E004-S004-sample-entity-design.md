# Design: M001-E004-S004 -- Create sample entity with test data for end-to-end validation

**Story:** M001-E004-S004
**Date:** 2026-02-17
**Status:** plan

---

## 1. Summary

Create an `examples/` directory containing a sample "customers" entity with parquet test data that demonstrates the full typedata workflow: define entity with typed layers, generate Ibis transformations, execute on DuckDB, and verify quality. The example serves three purposes: (1) integration test target for S005, (2) living documentation for new users, and (3) validation that all typedata primitives compose correctly.

---

## 2. Design Decision: What sample entity to use

### Decision: `customers` with 6 source columns

The story TOML and epic notes both prescribe `customers`. This is the right choice:

- **Universal** -- every data team has customers. No domain-specific knowledge needed to understand it.
- **Simple enough to read** -- 6 source columns is enough to demonstrate all layer types without becoming a wall of code.
- **Rich enough to exercise the framework** -- includes strings (name, email), integers (id, amount_cents), timestamps (created_at), and categoricals (plan) -- enough type variety to prove type casting, hashing, domain extraction, and computed columns all work.

### Source schema: `raw_customers`

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `id` | INT64 | Customer ID | `1` |
| `email` | STRING | Customer email address | `alice@acme.com` |
| `name` | STRING | Full name | `Alice Johnson` |
| `created_at` | TIMESTAMP | Account creation timestamp | `2024-03-15T10:30:00` |
| `plan` | STRING | Subscription plan name | `pro` |
| `amount_cents` | INT64 | Monthly payment in cents | `4900` |

**Why these 6 columns:**

- `id` -- primary key, demonstrates NotNull + integer handling
- `email` -- core identity field, demonstrates hashing primitive, domain extraction primitive, NotNull + Unique quality checks
- `name` -- nullable string, demonstrates handling of optional fields
- `created_at` -- timestamp, demonstrates type casting (timestamp -> date), date truncation primitive (signup_month)
- `plan` -- categorical, demonstrates computed boolean flags (is_paying)
- `amount_cents` -- integer, demonstrates the Divide source transform (cents -> dollars), InRange quality check

**What we deliberately excluded:**

- `updated_at` -- not needed; one timestamp is enough to demonstrate date operations
- `country`, `phone`, `address` -- add columns without adding new transformation patterns
- `status` -- would be useful for LifecycleFlags component, but the story acceptance criteria do not mention it, and adding it would blur the focus between "sample entity" and "component showcase"

---

## 3. Design Decision: Exact sample data

### Decision: 12 rows, checked into `examples/data/customers.parquet`

**12 rows** because:

- Story requires "10+ rows"
- 12 is enough to have variety across plans, email domains, dates, and edge cases
- Small enough to visually inspect all rows in test output
- Round enough to verify row counts trivially (no off-by-one confusion)

### Sample data

```
id | email                    | name             | created_at              | plan       | amount_cents
---|--------------------------|------------------|-------------------------|------------|-------------
 1 | alice@acme.com           | Alice Johnson    | 2024-01-15T10:30:00     | pro        | 4900
 2 | bob@gmail.com            | Bob Smith        | 2024-02-20T14:15:00     | starter    | 1900
 3 | carol@bigcorp.io         | Carol Williams   | 2024-03-10T09:00:00     | enterprise | 19900
 4 | dave@example.com         | Dave Brown       | 2024-04-05T16:45:00     | pro        | 4900
 5 | eve@startup.dev          | Eve Davis        | 2024-05-12T11:20:00     | starter    | 1900
 6 | frank@gmail.com          | Frank Miller     | 2024-06-01T08:00:00     | free       | 0
 7 | grace@enterprise.co      | Grace Wilson     | 2024-07-20T13:30:00     | enterprise | 19900
 8 | hank@yahoo.com           | Hank Taylor      | 2024-08-15T15:00:00     | free       | 0
 9 | iris@acme.com            | Iris Anderson    | 2024-09-03T10:00:00     | pro        | 4900
10 | jack@outlook.com         | Jack Thomas      | 2024-10-18T12:00:00     | starter    | 1900
11 | karen@bigcorp.io         | Karen Martinez   | 2024-11-25T09:30:00     | enterprise | 19900
12 | leo@startup.dev          | Leo Garcia       | 2024-12-30T17:00:00     | pro        | 4900
```

**Design choices in the data:**

- **No nulls in required fields** -- email and id are always present (quality checks verify this)
- **Name is always present** -- keeps the example simple; a future example could add null names to demonstrate NotNull edge cases
- **Spread across 12 months** -- each row has a different month, making signup_month extraction visually verifiable
- **4 plan types** -- `free` (2), `starter` (3), `pro` (4), `enterprise` (3) -- tests the `is_paying` computed column on both `true` (plan != free) and `false` (plan == free) cases
- **amount_cents = 0 for free** -- tests InRange(min=0) boundary condition
- **Repeated email domains** -- `acme.com` (2), `gmail.com` (2), `bigcorp.io` (2), `startup.dev` (2) -- verifies email_domain extraction works consistently; gmail/yahoo/outlook trigger `is_personal_email` logic if we add it
- **No duplicate emails** -- keeps Unique('email_hash') check clean in the happy path
- **Timestamps vary by hour** -- proves timestamp-to-date casting handles different times correctly

---

## 4. Design Decision: How to generate/ship the parquet file

### Decision: Check in a generated parquet file + include the generation script

**Ship `examples/data/customers.parquet` as a checked-in file.** Also include `examples/data/generate_sample_data.py` as a standalone script that regenerates it.

**Rationale:**

1. **Checked-in parquet** -- The parquet file must be present for the end-to-end test (S005) to run without extra dependencies. If parquet is only generated by a fixture, then `examples/` is not self-contained -- someone cloning the repo cannot run the example without first running pytest. That defeats the purpose of an examples directory.

2. **Generation script alongside** -- The script serves as documentation ("here is exactly how this data was created") and as a regeneration tool if the schema evolves. It uses pandas (not a core typedata dependency, but commonly available for data teams) and is 20-30 lines.

3. **Not a pytest fixture** -- The S005 end-to-end test can create its own test data inline using pandas in a fixture (for isolation), but the examples/ directory should contain a pre-built file for standalone use.

**File sizes are negligible.** A 12-row parquet file with 6 columns is under 2 KB. Checking it in adds essentially zero repo bloat.

### Generation script

```python
"""Generate sample customer data for the typedata example.

Run: python examples/data/generate_sample_data.py
Output: examples/data/customers.parquet
"""

import pandas as pd
from pathlib import Path

data = {
    "id": list(range(1, 13)),
    "email": [
        "alice@acme.com", "bob@gmail.com", "carol@bigcorp.io",
        "dave@example.com", "eve@startup.dev", "frank@gmail.com",
        "grace@enterprise.co", "hank@yahoo.com", "iris@acme.com",
        "jack@outlook.com", "karen@bigcorp.io", "leo@startup.dev",
    ],
    "name": [
        "Alice Johnson", "Bob Smith", "Carol Williams", "Dave Brown",
        "Eve Davis", "Frank Miller", "Grace Wilson", "Hank Taylor",
        "Iris Anderson", "Jack Thomas", "Karen Martinez", "Leo Garcia",
    ],
    "created_at": pd.to_datetime([
        "2024-01-15T10:30:00", "2024-02-20T14:15:00", "2024-03-10T09:00:00",
        "2024-04-05T16:45:00", "2024-05-12T11:20:00", "2024-06-01T08:00:00",
        "2024-07-20T13:30:00", "2024-08-15T15:00:00", "2024-09-03T10:00:00",
        "2024-10-18T12:00:00", "2024-11-25T09:30:00", "2024-12-30T17:00:00",
    ]),
    "plan": [
        "pro", "starter", "enterprise", "pro", "starter", "free",
        "enterprise", "free", "pro", "starter", "enterprise", "pro",
    ],
    "amount_cents": [4900, 1900, 19900, 4900, 1900, 0, 19900, 0, 4900, 1900, 19900, 4900],
}

df = pd.DataFrame(data)
output_path = Path(__file__).parent / "customers.parquet"
df.to_parquet(output_path, index=False)
print(f"Written {len(df)} rows to {output_path}")
```

---

## 5. Design Decision: Layers to demonstrate

### Decision: PrepLayer + DimensionLayer + QualityConfig

The story acceptance criteria explicitly require:

1. **PrepLayer** -- type casts, email hash
2. **DimensionLayer** -- computed columns (email_domain, lifecycle segment)
3. **Quality checks** -- NotNull on email, Unique on email_hash

We do NOT include SnapshotLayer or ActivityLayer because:
- They are not mentioned in the acceptance criteria
- They add complexity without teaching new concepts
- SnapshotLayer requires date partitioning which complicates the example
- ActivityLayer is a fundamentally different pattern (event-based, not entity-based)

### 5a. PrepLayer definition

The PrepLayer applies source-level transformations before business logic:

```python
PrepLayer(
    model_name="prep_customers",
    computed_columns=[
        # Hash email for identity resolution (hashing primitive)
        ComputedColumn(
            name="email_hash",
            expression=hash_email("email"),
            description="SHA256 hash of lowercase trimmed email",
        ),
        # Cast created_at to date (type cast)
        ComputedColumn(
            name="created_date",
            expression='t.created_at.cast("date")',
            description="Account creation date (date only, no time)",
        ),
        # Convert cents to dollars (Divide pattern)
        ComputedColumn(
            name="amount_dollars",
            expression="t.amount_cents / 100.0",
            description="Monthly payment in dollars",
        ),
    ],
)
```

**What this demonstrates:**
- `hash_email` primitive -- shows how reusable Ibis expression functions work
- Type casting via Ibis `.cast()` -- the fundamental "typed transformation" value prop
- Arithmetic transformation -- simple but proves computed columns work with expressions

### 5b. DimensionLayer definition

The DimensionLayer adds business-logic computed columns on top of the prep output:

```python
DimensionLayer(
    model_name="dim_customers",
    computed_columns=[
        # Extract email domain (string primitive)
        ComputedColumn(
            name="email_domain",
            expression="t.email.split('@')[1]",
            description="Email domain extracted from email address",
        ),
        # Is paying customer (business logic flag)
        ComputedColumn(
            name="is_paying",
            expression="t.plan != 'free'",
            description="True if customer is on a paid plan",
        ),
        # Signup month for cohort analysis (date primitive)
        ComputedColumn(
            name="signup_month",
            expression=date_trunc_month("created_at"),
            description="Month of signup for cohort analysis",
        ),
    ],
)
```

**What this demonstrates:**
- String operations via Ibis (`.split('@')[1]`)
- Boolean computed columns from business rules
- `date_trunc_month` primitive -- shows how primitives compose with computed columns
- The two-layer pattern: prep handles data cleansing, dimension handles business semantics

### 5c. QualityConfig definition

```python
QualityConfig(
    primary_key="email_hash",
    checks=[
        NotNull("email"),
        NotNull("id"),
        Unique("email_hash"),
        InRange("amount_cents", min=0),
    ],
)
```

**What this demonstrates:**
- NotNull -- most common data quality check
- Unique -- identity integrity on the hashed field
- InRange -- boundary validation on numeric fields
- Checks reference both source columns (`email`, `id`, `amount_cents`) and computed columns (`email_hash`)

---

## 6. Design Decision: What primitives/components to showcase

### Decision: 3 primitives, 0 components

**Primitives used:**

| Primitive | From module | Used in |
|-----------|-------------|---------|
| `hash_email(col)` | `typedata.primitives.hashing` | PrepLayer -- `email_hash` column |
| `date_trunc_month(col)` | `typedata.primitives.dates` | DimensionLayer -- `signup_month` column |
| Inline Ibis expressions | (no import needed) | PrepLayer (`cast`, `/`), DimensionLayer (`.split`, `!=`) |

**Why no components (LifecycleFlags, TimeBasedMetrics):**

The acceptance criteria mention "computed columns (email_domain, lifecycle segment)" -- but "lifecycle segment" can be implemented as a simple `is_paying` boolean computed column. Using the full `LifecycleFlags` component would require:

1. A `status` source column (our schema does not have one; `plan` is not a status)
2. Defining `active_states` / `churned_states` / `at_risk_states` lists
3. Importing and expanding the component

This adds complexity without matching the actual data shape. The `is_paying` flag derived from `plan != 'free'` is a cleaner "lifecycle segment" for this dataset.

**TimeBasedMetrics** would add `days_since_created` and `created_month` -- useful, but `signup_month` already demonstrates date truncation, and `days_since_created` would produce different values every time the test runs (non-deterministic). Keeping it out makes the example more predictable for testing.

**The goal of S004 is not to showcase every feature.** It is to provide a minimal, complete example that proves the end-to-end pipeline works. Components can be showcased in a future "advanced examples" story.

---

## 7. Design Decision: Standalone project vs reference files

### Decision: Reference files in `examples/`, not a standalone project

The `examples/` directory should contain:

```
examples/
    entities/
        customers.py          # Entity definition
    data/
        customers.parquet     # Sample data (checked in)
        generate_sample_data.py  # Script to regenerate parquet
```

**Not a standalone project** (no separate `pyproject.toml`, no `__init__.py`, no virtualenv). Reasons:

1. **S004's scope is "sample entity with test data"** -- not "sample project with its own build system." A standalone project would be S006-level scope.
2. **The entity file imports from typedata** (`from typedata.core import Entity, ...`), so it requires typedata to be installed. It is not self-contained regardless.
3. **The E004 epic acceptance criteria say** "Sample entity + sample parquet data included in examples/ directory" and "README or examples/README.md shows the complete workflow." Reference files + a short README satisfy this.
4. **S005 (end-to-end test) depends on S004** -- the test will import the entity definition and point at the parquet file. Having examples/ be "just files" makes this import straightforward.

**What about running the example?** The S003 `run()` function is the entry point. Users run:

```python
from typedata import run

results = run(entities_dir="examples/entities", data_dir="examples/data")
```

Or from the command line (future CLI story):

```bash
typedata run --entities examples/entities --data examples/data
```

This means `examples/` is runnable _through typedata_, not as its own project. That is the correct design -- typedata is the framework, examples/ demonstrates using it.

---

## 8. Complete entity definition

File: `examples/entities/customers.py`

```python
"""Sample customers entity demonstrating the full typedata workflow.

This entity transforms raw customer records through two layers:
- PrepLayer: type casts, email hashing, unit conversion
- DimensionLayer: business logic columns (email domain, paying flag, signup cohort)

Quality checks validate the output: NotNull, Unique, InRange.
"""

from typedata.components import ComputedColumn
from typedata.core import DimensionLayer, Entity, LayersConfig, PrepLayer
from typedata.core.source import Field, TableSource
from typedata.primitives import date_trunc_month, hash_email
from typedata.quality import InRange, NotNull, QualityConfig, Unique

entity = Entity(
    name="customers",
    description="Sample customer entity for typedata demonstration",
    source=TableSource(
        project="example",
        dataset="raw",
        table="customers",
        duckdb_path="examples/data/customers.parquet",
        fields=[
            Field(name="id", type="INT64", description="Customer ID"),
            Field(name="email", type="STRING", description="Customer email address"),
            Field(name="name", type="STRING", description="Full name"),
            Field(name="created_at", type="TIMESTAMP", description="Account creation time"),
            Field(name="plan", type="STRING", description="Subscription plan"),
            Field(name="amount_cents", type="INT64", description="Monthly payment in cents"),
        ],
    ),
    layers=LayersConfig(
        prep=PrepLayer(
            model_name="prep_customers",
            computed_columns=[
                ComputedColumn(
                    name="email_hash",
                    expression=hash_email("email"),
                    description="SHA256 hash of lowercase trimmed email",
                ),
                ComputedColumn(
                    name="created_date",
                    expression='t.created_at.cast("date")',
                    description="Account creation date (date only)",
                ),
                ComputedColumn(
                    name="amount_dollars",
                    expression="t.amount_cents / 100.0",
                    description="Monthly payment in dollars",
                ),
            ],
        ),
        dimension=DimensionLayer(
            model_name="dim_customers",
            computed_columns=[
                ComputedColumn(
                    name="email_domain",
                    expression="t.email.split('@')[1]",
                    description="Email domain extracted from address",
                ),
                ComputedColumn(
                    name="is_paying",
                    expression="t.plan != 'free'",
                    description="True if customer is on a paid plan",
                ),
                ComputedColumn(
                    name="signup_month",
                    expression=date_trunc_month("created_at"),
                    description="Signup month for cohort analysis",
                ),
            ],
        ),
    ),
    quality=QualityConfig(
        primary_key="email_hash",
        checks=[
            NotNull("email"),
            NotNull("id"),
            Unique("email_hash"),
            InRange("amount_cents", min=0),
        ],
    ),
)
```

---

## 9. Expected generated output

When `typedata.generate()` processes this entity, it should produce a file like:

```python
"""
customers entity transformations.

Auto-generated from examples/entities/customers.py
"""

import ibis


def prep_customers(source: ibis.Table) -> ibis.Table:
    """Prep layer: type casts, hashing, unit conversion."""
    t = source
    return t.mutate(
        email_hash=t.email.lower().strip().hash().cast("string"),
        created_date=t.created_at.cast("date"),
        amount_dollars=t.amount_cents / 100.0,
    )


def dim_customers(source: ibis.Table) -> ibis.Table:
    """Dimension layer: business logic columns."""
    t = source
    return t.mutate(
        email_domain=t.email.split("@")[1],
        is_paying=t.plan != "free",
        signup_month=t.created_at.truncate("M"),
    )


def transform_customers(source: ibis.Table) -> ibis.Table:
    """Full transformation pipeline: prep -> dimension."""
    prepped = prep_customers(source)
    return dim_customers(prepped)
```

This shows:
- Separate functions per layer (testable independently)
- A top-level `transform_<entity>()` that chains them
- Pure Ibis expressions -- no pandas, no SQL strings

---

## 10. Expected output schema after full pipeline

After running `transform_customers()` on the sample data, the output DuckDB table should have:

| Column | Type | Source |
|--------|------|--------|
| `id` | INT64 | Original |
| `email` | STRING | Original |
| `name` | STRING | Original |
| `created_at` | TIMESTAMP | Original |
| `plan` | STRING | Original |
| `amount_cents` | INT64 | Original |
| `email_hash` | STRING | PrepLayer computed |
| `created_date` | DATE | PrepLayer computed |
| `amount_dollars` | FLOAT64 | PrepLayer computed |
| `email_domain` | STRING | DimensionLayer computed |
| `is_paying` | BOOLEAN | DimensionLayer computed |
| `signup_month` | TIMESTAMP | DimensionLayer computed |

**12 columns, 12 rows.** The 6 original columns pass through unchanged; 6 computed columns are added.

### Spot-check values for test assertions (S005)

| Row (id=1) | Column | Expected value |
|------------|--------|---------------|
| 1 | `email_domain` | `"acme.com"` |
| 1 | `is_paying` | `True` |
| 1 | `amount_dollars` | `49.0` |
| 6 | `is_paying` | `False` |
| 6 | `amount_dollars` | `0.0` |
| 2 | `email_domain` | `"gmail.com"` |

---

## 11. Directory structure

```
typedata/
    examples/
        entities/
            customers.py               # Entity definition (section 8)
        data/
            customers.parquet          # 12-row sample dataset (checked in)
            generate_sample_data.py    # Script to regenerate parquet
```

No `__init__.py` in examples/ -- it is not a Python package. No `pyproject.toml` -- it is not a standalone project.

---

## 12. Acceptance criteria verification

| Criterion | Satisfied by |
|-----------|-------------|
| `examples/entities/customers.py` defines Entity with PrepLayer + DimensionLayer | Section 8 -- full entity definition with both layers |
| `examples/data/customers.parquet` contains 10+ sample customer records | Section 3 -- 12 rows, checked-in parquet |
| PrepLayer includes: email hash, type casts for dates | Section 5a -- `email_hash` (hash_email primitive), `created_date` (cast to date) |
| DimensionLayer includes: computed columns (email_domain, lifecycle segment) | Section 5b -- `email_domain` (split), `is_paying` (lifecycle segment), `signup_month` |
| Quality checks: NotNull on email, Unique on email_hash | Section 5c -- `NotNull("email")`, `NotNull("id")`, `Unique("email_hash")`, `InRange("amount_cents", min=0)` |

---

## 13. Implementation checklist

1. Create `examples/entities/` directory
2. Create `examples/data/` directory
3. Write `examples/data/generate_sample_data.py` (section 4)
4. Run the generation script to produce `examples/data/customers.parquet`
5. Write `examples/entities/customers.py` (section 8)
6. Verify entity definition is valid Python: `python -c "import examples.entities.customers"` (requires typedata installed)
7. Verify parquet has 12 rows and 6 columns: `python -c "import pandas; df = pandas.read_parquet('examples/data/customers.parquet'); print(df.shape); print(df.dtypes)"`

---

## 14. Open questions (resolved)

**Q: Should we include edge case data (nulls, duplicates, malformed emails)?**
A: No. The sample data should be the happy path. Edge cases belong in test fixtures (S005), not in the example that new users see first. A clean dataset that passes all quality checks is more instructive than one designed to trigger failures.

**Q: Should the entity file import from `typedata.primitives` or use inline Ibis expressions?**
A: Both. Use `hash_email` and `date_trunc_month` as imported primitives (shows the primitives feature), and use inline expressions like `t.email.split('@')[1]` and `t.plan != 'free'` (shows that raw Ibis works too). This demonstrates that primitives are a convenience, not a requirement.

**Q: Should `duckdb_path` in the TableSource be a relative or absolute path?**
A: Relative (`examples/data/customers.parquet`). The execution engine (S002) resolves paths relative to the project root. Absolute paths would break portability across machines.

**Q: Should we add `__all__` to customers.py to export the entity?**
A: No. The entity discovery mechanism (S001) scans for `Entity` instances in module-level variables. It finds `entity = Entity(...)` by type inspection, not by `__all__`. Keeping the variable named `entity` (lowercase) follows the pattern from timo-data-stack.

**Q: How much of the generated output format (section 9) does S004 need to nail down?**
A: S004 does not implement generation -- that is E003 (code generators). S004 only needs the entity definition and sample data. Section 9 is included here to show what the expected end-to-end flow looks like, so S005 (end-to-end test) has a reference for what to assert.
