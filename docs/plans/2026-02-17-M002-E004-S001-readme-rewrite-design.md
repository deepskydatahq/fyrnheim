# Design: Rewrite README with Project Overview and Core Concepts

**Story:** M002-E004-S001
**Date:** 2026-02-17

---

## Goal

Replace the current 5-line README with a compelling, under-200-line README that serves as the primary landing page for the Fyrnheim project on GitHub. It must communicate what Fyrnheim is, why it exists, how to install and use it, and how it compares to dbt -- all without overwhelming a first-time visitor.

---

## Structure (Proposed Section Order)

```
1. Title + tagline                          ~  2 lines
2. Description paragraph                    ~  3 lines
3. Quickstart (install + minimal example)   ~ 25 lines
4. Core Concepts                            ~ 60 lines
5. Why Fyrnheim? (dbt comparison)           ~ 25 lines
6. Status                                   ~  8 lines
7. License                                  ~  2 lines
```

**Estimated total: ~125 lines** (well under the 200-line cap).

---

## Section Details

### 1. Title + Tagline (~2 lines)

**Format:** H1 heading with one-liner beneath it.

**Tagline candidate:**
> Define typed Python entities, generate transformations, run anywhere.

This tagline already exists in pyproject.toml and the current README. It is accurate and concise -- keep it.

**Sub-tagline (optional, 1 sentence):**
> A dbt alternative built on Pydantic + Ibis.

### 2. Description Paragraph (~3 lines)

2-3 sentences expanding on the tagline. Must hit three points:
- **What it is:** A Python library for defining data transformation entities
- **How it works:** Pydantic models define entities; Ibis generates backend-portable code
- **Key benefit:** One definition runs locally on DuckDB and in production on BigQuery/Snowflake

**Draft:**
> Fyrnheim lets data teams define business entities as typed Pydantic models and automatically generates Ibis transformation code from those definitions. The same entity runs on DuckDB for instant local development and deploys to BigQuery, Snowflake, or Postgres in production with zero changes. No SQL, no Jinja, no vendor lock-in.

### 3. Quickstart (~25 lines)

Two sub-sections:

#### Install

```bash
pip install fyrnheim[duckdb]
```

One line. The `[duckdb]` extra is the primary local-dev path and the only fully-supported backend today. No need to mention bigquery here (covered in Status section).

#### Minimal Example

Show the smallest useful entity definition + run command. Pull from the customers example but strip it down to ~15 lines. Must demonstrate:
- Importing from `fyrnheim`
- Creating an `Entity` with a `TableSource`
- One `PrepLayer` with one `ComputedColumn`
- One quality check

Then show `generate()` + `run()` in a 3-line script. This gives the reader a complete mental model: define -> generate -> run.

**Snippet plan:**

```python
from fyrnheim import Entity, TableSource, LayersConfig, PrepLayer, ComputedColumn, QualityConfig, NotNull

entity = Entity(
    name="customers",
    source=TableSource(
        project="myproject", dataset="raw", table="customers",
        duckdb_path="customers.parquet",
    ),
    layers=LayersConfig(
        prep=PrepLayer(
            model_name="prep_customers",
            computed_columns=[
                ComputedColumn(name="amount_dollars", expression="t.amount_cents / 100.0"),
            ],
        ),
    ),
    quality=QualityConfig(checks=[NotNull("email")]),
)
```

Then:

```python
from fyrnheim import generate, run
generate(entity)
result = run("entities/", "data/", backend="duckdb")
```

### 4. Core Concepts (~60 lines)

Five sub-sections, each with a 1-3 sentence explanation and a small code snippet (2-5 lines). These map directly to the acceptance criteria.

#### Entities

Explain: A Pydantic model describing a business object (customers, orders, products) -- its source, transformations, and quality rules. The central building block.

Snippet: The `Entity(...)` constructor call (abbreviated, 4 lines showing name/source/layers/quality fields).

#### Layers

Explain: Composable transformation stages that an entity flows through. PrepLayer cleans raw data; DimensionLayer adds business logic columns; SnapshotLayer tracks changes over time.

Snippet: Show `LayersConfig(prep=..., dimension=...)` with one ComputedColumn each, to illustrate the progression.

#### Primitives

Explain: Reusable Python functions that replace SQL snippets. Hashing, date operations, categorization -- import and compose them instead of copy-pasting SQL.

Snippet:
```python
from fyrnheim.primitives import hash_email, date_trunc_month
ComputedColumn(name="email_hash", expression=hash_email("email"))
```

#### Components

Explain: Multi-column patterns that generate related fields from a single config. For example, LifecycleFlags produces `is_active`, `is_churned`, `is_at_risk` from a status column.

Snippet:
```python
from fyrnheim import LifecycleFlags
flags = LifecycleFlags(status_column="status", active_states=["active"], churned_states=["cancelled"])
```

#### Quality Checks

Explain: Declarative data quality rules that run after transformations. Built-in checks include NotNull, Unique, InRange, InSet, MatchesPattern, ForeignKey.

Snippet:
```python
quality=QualityConfig(
    primary_key="email_hash",
    checks=[NotNull("email"), Unique("email_hash"), InRange("amount_cents", min=0)],
)
```

### 5. Why Fyrnheim? (~25 lines)

A factual comparison table (not opinion). Two columns: "dbt" and "Fyrnheim". Rows covering 5-6 dimensions:

| Dimension | dbt | Fyrnheim |
|-----------|-----|----------|
| Language | SQL + Jinja | Python |
| Type safety | None (runtime errors) | Pydantic validation at definition time |
| Local dev | Requires warehouse connection | DuckDB on local parquet files |
| Backend portability | Dialect-specific SQL | Ibis compiles to 15+ backends |
| Testing | Custom schema tests | pytest + quality checks |
| Boilerplate | Jinja macros, YAML configs | Python functions, Pydantic models |

Follow the table with a 2-sentence "Fyrnheim is not..." paragraph to set expectations. Pull from VISION.md's "What We Don't Do" section: not an orchestrator, not an extraction tool, not a BI layer.

### 6. Status (~8 lines)

Bullet list format:

- **Alpha** -- API may change before 1.0
- **DuckDB backend** -- fully supported
- **BigQuery backend** -- planned
- **Python 3.11+** required
- **452 tests passing** (concrete credibility signal)

### 7. License (~2 lines)

Single line: "MIT" with link to LICENSE file.

---

## Key Messaging Decisions

1. **Lead with the developer experience, not the architecture.** The first thing a reader sees should be "here's what you write" (the entity definition), not "here's how it works internally."

2. **Use the customers example throughout.** One consistent entity across all snippets reduces cognitive load. The reader builds understanding incrementally.

3. **Keep dbt comparison factual.** No "dbt is bad" framing. State what each tool does and let the reader decide. The comparison table format forces factual parallel structure.

4. **No API reference in the README.** The README is a landing page, not docs. Core concepts give just enough to understand the model; full API docs come later (M002-E004-S002+).

5. **Install line uses `fyrnheim[duckdb]`, not `pip install fyrnheim`.** The bare install is useless without a backend. Guide the user to the happy path immediately.

6. **No badges for now.** The project is alpha with no CI badges, PyPI badge, or docs badge yet. Adding empty/broken badges hurts credibility. Add them when the infrastructure exists.

---

## Line Budget Breakdown

| Section | Est. Lines |
|---------|-----------|
| Title + tagline | 3 |
| Description | 5 |
| Quickstart (install + example) | 30 |
| Core Concepts (5 sub-sections) | 65 |
| Why Fyrnheim? (comparison table) | 25 |
| Status | 10 |
| License | 3 |
| Blank lines / headers | 15 |
| **Total** | **~156** |

Comfortably under the 200-line ceiling with room for minor adjustments.

---

## Out of Scope

- Full API reference (separate docs story)
- Contributing guide (separate story)
- Changelog (not yet needed)
- CI/CD badges (no infrastructure yet)
- Tutorial / walkthrough (separate story)
