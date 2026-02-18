# Snapshot E2E Test Design

## Overview
E2E test proving full snapshot flow: entity with SnapshotLayer -> generate -> execute on DuckDB -> verify dedup, surrogate keys, ds column. Uses duplicate input rows to prove deduplication works.

## Test Data
5 rows with duplicate ids:
- id=1 appears twice (different updated_at, names, plans)
- id=2 appears twice
- id=3 appears once

Expected: dim has 5 rows, snapshot has 3 rows (deduped by id).

## Test Class: TestSnapshotE2E
- test_dim_table_created: 5 rows preserved
- test_snapshot_table_created: snapshot_{name} exists
- test_snapshot_has_ds_column: ds column with today's date
- test_snapshot_has_surrogate_key: unique snapshot_key per row
- test_snapshot_deduplicates_rows: 5 -> 3 rows
- test_snapshot_keeps_latest_version: DESC ordering picks newest

## Files
- Add to tests/test_e2e_pipeline.py (follow existing fixture pattern)
