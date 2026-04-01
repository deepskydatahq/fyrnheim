# BigQuery Mock Tests Design

## Overview

Four mock-based test cases across two existing test files that serve as executable specs for the BigQuery execution path through IbisExecutor and the connection factory.

## Problem Statement

The M004 mission introduces a generic IbisExecutor with BigQuery support. Before shipping, we need tests that prove the BigQuery path works without requiring a live BigQuery instance. These tests validate the contract that prior stories (E001-E003) must satisfy.

## Expert Perspectives

### Technical
- Write tests as executable specs against the future API surface
- Mock only ibis-level dependencies (connections, `ibis.bigquery.connect`), not internal interfaces
- Import real `IbisExecutor` and `create_connection` — tests fail with `ImportError` until prior stories ship, acting as a forcing function

### Simplification Review
- No `pytest.skip` or conditional imports — fail outright to surface missing implementations
- Merge AC1 + AC2 into a single executor test (both verify `_run_transform_pipeline` behavior)
- Rename test classes to reflect the generic abstraction, not BigQuery-specific naming
- Keep tests self-contained with no shared fixtures

## Proposed Solution

Three test functions across two files, each using `unittest.mock` to mock ibis objects:

### File 1: `tests/test_engine_executor.py` — append `TestIbisExecutorBackendPath`

**test_executor_passes_backend_to_source_and_source_calls_conn_table** (covers AC1 + AC2):
- Construct `IbisExecutor(conn=mock_conn, backend="bigquery")`
- Call `_run_transform_pipeline` with a fake module whose source function captures args
- Assert source function receives `backend="bigquery"` and calls `conn.table(name, database=(project, dataset))`

### File 2: `tests/test_engine_runner.py` — append `TestConnectionFactory` and `TestRunnerBackendParam`

**test_create_connection_bigquery** (covers AC3):
- Patch `ibis` at module level inside `fyrnheim.engine.connection`
- Call `create_connection("bigquery", project_id="x", dataset_id="y")`
- Assert `ibis.bigquery.connect` called with correct kwargs

**test_runner_creates_ibis_executor_for_bigquery** (covers AC4):
- Patch `create_connection` and `IbisExecutor` in the runner module
- Call `run()` with `backend="bigquery"` and empty entities dir
- Assert runner called `create_connection("bigquery")` and `IbisExecutor(conn=..., backend="bigquery")`

## Design Details

| AC | Test | File |
|----|------|------|
| IbisExecutor executes _run_transform_pipeline with backend='bigquery' | test_executor_passes_backend_to_source_and_source_calls_conn_table | test_engine_executor.py |
| Source function receives backend='bigquery' and calls conn.table() | (merged into above) | test_engine_executor.py |
| Connection factory calls ibis.bigquery.connect with correct params | test_create_connection_bigquery | test_engine_runner.py |
| Runner with backend='bigquery' creates IbisExecutor | test_runner_creates_ibis_executor_for_bigquery | test_engine_runner.py |

### Key decisions

1. **No new files** — append to existing test files as new classes
2. **Direct imports, fail outright** — no pytest.skip, no conditional imports
3. **Mock at ibis boundary** — internal interfaces imported and called for real
4. **Self-contained tests** — no shared fixtures, minimal mock setup per test

## Alternatives Considered

- **Skip-on-import-failure**: Would silently pass when dependencies aren't implemented. Rejected — defeats the forcing function purpose.
- **Separate test file `test_bigquery_mock.py`**: Would fragment test organization. Rejected — appending to existing files follows project convention.
- **Four separate tests (one per AC)**: AC1 and AC2 test the same code path (`_run_transform_pipeline`). Merged into one test per simplification review.

## Success Criteria

- All 3 test functions fail with clear `ImportError` before E001-E003 are implemented
- All 3 test functions pass once E001-E003 are complete
- No production code changes required in this story
- Existing ~596 tests unaffected
