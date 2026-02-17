# Fyrnheim Vision

## The Transformation

**We help data teams become confident owners of their transformation logic by letting them define entities once in Python and run them anywhere.**

### Before

Data teams write SQL transformations in dbt that are:
- **Locked to one warehouse** — switching from BigQuery to Snowflake means rewriting SQL dialects, macros, and tests
- **Untyped and fragile** — a column rename breaks downstream models silently; you find out when the dashboard is wrong
- **Impossible to test locally** — every `dbt run` hits your cloud warehouse, so iteration is slow and expensive
- **Full of boilerplate** — the same staging patterns, type casts, and naming conventions copy-pasted across hundreds of SQL files with Jinja templating that's hard to read and harder to debug
- **Disconnected from the rest of the stack** — Python teams maintain SQL as a separate language with separate tooling, separate testing, separate CI

### After

Data teams define typed Python entities and get:
- **One definition, any backend** — the same Pydantic entity runs on DuckDB locally and BigQuery/Snowflake/Postgres in production via Ibis
- **Type safety at definition time** — Pydantic validates your entity schema, field types, and relationships before anything runs
- **Instant local development** — test against parquet files on DuckDB, zero cloud costs, sub-second iteration
- **Generated transformations** — define the entity and its layers (prep, dimension, snapshot, activity, analytics), get Ibis code generated automatically
- **Python-native everything** — define, test, compose, and deploy transformations using tools you already know (pytest, mypy, your IDE)

## Who We Serve

**Data engineers and analytics engineers** on Python-first teams who:
- Build and maintain transformation layers (the "T" in ELT)
- Are frustrated by dbt's SQL + Jinja complexity, vendor lock-in, and slow local dev
- Want type safety and testability that modern Python provides
- Need to support multiple backends or want the freedom to switch

**Secondary:** Solo founders and small teams who need a data stack that works locally before scaling to production.

## Core Mechanism

**Entity-as-code**: A Pydantic model defines what a business entity is (fields, types, sources, layers, quality checks). Fyrnheim generates Ibis transformation code from that definition and executes it on any supported backend.

```
Entity (Pydantic) → Layers (prep, dim, snapshot, activity, analytics) → Ibis expressions → Any backend
```

### Key Concepts

- **Entities** — Pydantic models that describe business objects (customers, transactions, products) with their source, transformations, and quality rules
- **Layers** — Composable transformation stages (PrepLayer → DimensionLayer → SnapshotLayer → ActivityLayer → AnalyticsLayer) that each entity flows through
- **Primitives** — Reusable Python functions that replace SQL snippets (`hash_email()`, `categorize()`, `days_since()`)
- **Components** — Multi-column patterns that generate related fields from a single config (`LifecycleFlags`, `TimeBasedMetrics`)
- **Backend portability** — Ibis compiles the same Python expressions to DuckDB, BigQuery, Snowflake, Postgres, and 15+ other backends

## What We Don't Do

- **Not an orchestrator** — we don't schedule or monitor pipelines (use Dagster, Airflow, or cron)
- **Not an extraction tool** — we don't pull data from APIs (use DLT, Airbyte, or Fivetran)
- **Not a BI tool** — we don't render dashboards (use Evidence, Metabase, or Superset)
- **Not a semantic layer** — we define entities for transformation, not metric queries at runtime
- **Not a dbt wrapper** — we replace dbt's approach entirely with Python-native definitions

## Success Looks Like

- A data engineer defines a new entity in 20 lines of Python and gets prep, dimension, and snapshot layers generated — without writing SQL
- The same entity definition runs `pytest` locally on DuckDB, then deploys to BigQuery in production with zero changes
- When a source column is renamed, Pydantic validation catches it at definition time — before any pipeline runs
- A team migrates from Snowflake to BigQuery by changing one config line, not rewriting 200 SQL files
- Contributors on GitHub submit new primitives and components that the community reuses across projects

## Last Updated

2026-02-16
