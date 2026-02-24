# Fyrnheim Value Ladder

**Vision:** We help data teams become confident owners of their transformation logic by letting them define entities once in Python and run them anywhere.

**Last Updated:** 2026-02-24

---

## Status Summary

| Level | Value | Status |
|-------|-------|--------|
| 1 | Define & Run Locally | shipped |
| 2 | 5-Minute Onboarding | shipped |
| 3 | Production Backend | planned |
| 4 | Publishable Package | planned |
| 5 | Multi-Entity Projects | future |
| 6 | Orchestration Ready | future |

**Next level to build:** Level 3 — Production Backend

---

## Level 1: Define & Run Locally

**Status:** shipped

Define a typed entity in Python using Pydantic, generate all 5 transformation layers (prep, dimension, snapshot, activity, analytics), and run on DuckDB locally with sub-second iteration.

**What the user gets:**
- Entity-as-code: one Pydantic model defines fields, types, sources, layers, quality checks
- Code generation: Ibis expressions generated automatically from entity definition
- Local execution: run against parquet files on DuckDB, zero cloud costs
- Type safety: Pydantic validates schema before anything runs
- Quality checks: NotNull, Unique, InRange, ForeignKey — run with `fyr check`

**Missions:** M001 (core framework), M003 (complete layer stack)

---

## Level 2: 5-Minute Onboarding

**Status:** shipped

A data engineer installs fyrnheim, scaffolds a project, defines an entity, and sees transformation results — all in under 5 minutes via the CLI.

**What the user gets:**
- `fyr init` scaffolds project with sample entity and data
- `fyr generate` discovers entities and generates transform modules
- `fyr run` executes transforms, printing entity names, layers, row counts, timings
- `fyr check` runs quality checks with pass/fail output
- `fyr list` shows discovered entities with layers and status
- Clear error messages that guide toward the fix

**Missions:** M002 (CLI and developer experience)

---

## Level 3: Production Backend

**Status:** planned

The same entity definition that runs locally on DuckDB also runs on BigQuery, Snowflake, or Postgres in production — with zero code changes. Switch backends by changing one config line.

**What the user gets:**
- Backend portability: `fyrnheim.yaml` backend setting switches between DuckDB, BigQuery, Snowflake, Postgres
- Same entity, same tests, different target — no SQL dialect rewrites
- Production-grade execution on cloud warehouses
- The core promise of "define once, run anywhere" fulfilled

**Why this is next:**
- Levels 1-2 prove the model works locally. Level 3 proves it works in production.
- Without this, fyrnheim is a local dev tool. With it, it's a production data stack.
- This is the "aha moment" for teams evaluating a dbt alternative.

---

## Level 4: Publishable Package

**Status:** planned

`pip install fyrnheim` from PyPI with stable API, proper documentation, changelog, and versioning. The package is ready for external users and contributors.

**What the user gets:**
- Install from PyPI (not just git clone)
- Stable public API with semantic versioning
- Documentation: API reference, tutorials, migration guide from dbt
- Changelog tracking what changed between versions

**Why after Level 3:**
- Publishing before production backends work would invite users to a tool that can't do what it promises
- Level 3 completes the core value proposition; Level 4 makes it accessible

---

## Level 5: Multi-Entity Projects

**Status:** future

Define entity graphs with cross-entity references and dependency-ordered execution. A full project with customers, orders, and products entities runs in the correct order with shared dimensions.

**What the user gets:**
- Entity dependency resolution: orders depends on customers, run in correct order
- Cross-entity references: foreign keys validated across entities
- Project-level execution: `fyr run` runs all entities in dependency order
- Shared dimensions and metrics across entity boundaries

---

## Level 6: Orchestration Ready

**Status:** future

Drop fyrnheim into Dagster, Airflow, or any orchestrator for scheduled production pipelines. Each entity becomes a task in the DAG.

**What the user gets:**
- Dagster integration: each entity as a Dagster asset
- Airflow integration: each entity as an Airflow task
- Incremental runs: only re-process entities whose sources changed
- Monitoring: pipeline health, row counts, quality check results in orchestrator UI
