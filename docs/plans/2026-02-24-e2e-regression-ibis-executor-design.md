# E2E Regression Test via IbisExecutor on DuckDB — Design

## Overview

A single focused E2E test that proves the full generate → executor.execute() → verify pipeline works through the multi-backend source function dispatch path on DuckDB. This is the mission regression gate for M004.

## Problem Statement

Existing E2E tests all use `register_parquet()` which bypasses the generated `source_fn(conn, backend)` path entirely. This means the multi-backend dispatch mechanism — the core M004 architectural change — has zero E2E coverage. We need one test that exercises the actual generated source function dispatch.

## Expert Perspectives

### Technical
- The real value isn't "prove E2E works on DuckDB" (already proven). It's proving the **multi-backend dispatch mechanism** works: source functions receive the `backend` parameter and the generated code branches on it correctly.
- Write against current `DuckDBExecutor` — the rename to `IbisExecutor` is a trivial two-line import change when M004-E001 lands.

### Simplification Review
- Removed `backends.py` entirely — premature infrastructure for a test story
- Reduced from 5 tests to 1 focused E2E test
- Deferred BigQuery extras guard to connection factory story (M004-E002-S001)
- The "missing extras" acceptance criterion should be addressed where the import actually happens (the connection factory), not in the test story

## Proposed Solution

**One E2E test** in `tests/test_e2e_pipeline.py` that:

1. Defines an entity with source, prep layer, and dimension layer
2. Generates code (producing source functions with backend branching)
3. Executes via `DuckDBExecutor` **without** `register_parquet()` — forcing the `source_fn(conn, "duckdb")` dispatch path
4. Verifies output row count, target name, and that transforms actually ran

This single test covers the acceptance criterion "entity defined → code generated → executor executes on DuckDB → output verified" while adding genuinely new coverage of the source function dispatch path.

### File Changes

| File | Action | What Changes |
|------|--------|-------------|
| `tests/test_e2e_pipeline.py` | Extend | Add `TestMultiBackendE2E` class with 1 test |

### What This Does NOT Do

- Does not rename `DuckDBExecutor` (that's M004-E001)
- Does not create new production modules
- Does not add BigQuery extras guard (belongs in M004-E002-S001 connection factory)
- Does not modify existing tests

### Rename Preparedness

When M004-E001 lands: change import from `DuckDBExecutor` to `IbisExecutor` — a two-line change.

## Alternatives Considered

1. **5 tests + new backends.py module** — Over-engineered. The extras guard is premature and the additional tests duplicate coverage already present in unit tests.
2. **No new tests** — Insufficient. Existing E2E tests bypass source_fn dispatch via `register_parquet()`.

## Success Criteria

- New E2E test passes: entity → generate → execute on DuckDB via source_fn dispatch → verify output
- All ~596 existing tests still pass
- No new production code needed (test-only change)

## Note on "Missing Extras" Criterion

The acceptance criterion "Test for missing extras: importing bigquery backend without ibis[bigquery] gives helpful error" requires production code (the guard itself) to exist first. That production code belongs in the connection factory (M004-E002-S001). The test for it should live alongside that code. This story should note this dependency and defer the extras test.
