# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.6.2] - 2026-04-17

### Fixed

- `project_analytics_entity` now only emits rows for ids that have at
  least one event from a source referenced by the entity's
  `state_fields` (including `coalesce` strategy `priority` lists) OR an
  event whose `event_type` matches one of the entity's `measures`
  activity names. Previously, every AnalyticsEntity emitted one row per
  unique id in the global enriched-events stream — most rows entirely
  NULL for the entity in question. On the timo-data-stack pipeline this
  reduced per-entity row counts from 76,762 (global unique-id count) to
  the correct per-entity subset, and materially cut full-pipeline
  runtime by shrinking the outer loop's iteration space.

## [0.6.1] - 2026-04-09

### Changed (internal)

- State-table I/O (`_write_state_row`, `_load_state`, `fyr drop`) now uses
  parameterized queries via a new `IbisExecutor.execute_parameterized` method
  instead of f-string interpolation with a backend-aware `_escape` helper.
  The `_escape` function has been deleted. This is a purely internal refactor
  — no user-facing API changes — that structurally eliminates the class of
  SQL escape bugs (e.g. #98) that required manual dialect patches in v0.4.1
  and v0.5.1. BigQuery uses `ScalarQueryParameter` via `QueryJobConfig`;
  DuckDB uses native `$name` placeholders (the executor translates `@name`
  to `$name` transparently, so callers write BigQuery-style templates
  everywhere).

## [0.6.0] - 2026-04-09

### Added

- `AnalyticsEntity` and `MetricsModel` now support `materialization="table"`
  to write outputs directly to the active backend (BigQuery or DuckDB)
  instead of local parquet. The new `project`, `dataset`, and `table`
  fields (the latter defaulting to the entity's `name`) specify the
  warehouse destination. Default `materialization="parquet"` preserves
  existing behavior — projects without the new fields are unaffected.
  Mirrors the `StagingView` primitive's shape, so entity outputs become
  first-class warehouse artifacts (#99).

- `IbisExecutor.write_table(project, dataset, name, df)` — new backend-
  agnostic method for writing a pandas DataFrame to a warehouse table.
  BigQuery implementation uses `load_table_from_dataframe` with
  `WRITE_TRUNCATE`; DuckDB implementation uses `CREATE OR REPLACE TABLE`
  with automatic schema creation. ClickHouse and Postgres raise
  `NotImplementedError` — add them when a real project needs them.

- Run summary now reports each output's destination (parquet path or
  warehouse FQN) so you can see at a glance where your entities landed.

## [0.5.1] - 2026-04-09

### Fixed

- `staging_runner._escape` now uses backslash escape for single quotes on
  BigQuery, which rejects SQL-standard quote doubling. A StagingView whose
  rendered SQL contained a single quote in the first 500 characters would
  previously abort Phase 0 with a cryptic "concatenated string literals"
  error from the state row write, even though the view itself had already
  materialized (#98).

## [0.5.0] - 2026-04-08

### Changed (BREAKING)

- `apply_activity_definitions` now preserves events from sources whose raw
  event no `ActivityDefinition` matched. Previously these events were
  silently dropped, causing state fields on those sources to return stale
  values. See ADR-0001
  (`docs/decisions/0001-activity-definitions-drop-vs-preserve.md`) for the
  full rationale.

#### Migration

- If your project only uses activities (no state fields): no action needed.
- If you use state fields on sources that also have activity definitions:
  remove any no-op passthrough activities you added as a workaround.
- If you relied on drop semantics for downstream correctness: filter
  explicitly via `events.filter(event_type.isin([...activity_names...]))`.

### Fixed

- `examples/entities/customers.py`: corrected `EventOccurred(event_types=...)`
  to `EventOccurred(event_type=...)` (#100).

## [0.3.0] - 2026-03-09

### Added

- `fyr docs generate` command — generates a self-contained HTML documentation site with interactive entity lineage DAG (dagre-d3), entity detail pages, and sidebar navigation
- `fyr docs serve` command — serves generated docs on a local HTTP server and opens the browser
- JSON catalog builder (`fyrnheim.docs.catalog`) for extracting entity metadata
- Inline identity graph sources — `IdentityGraphSource` now accepts an inline `TableSource` instead of requiring a named entity reference
- Optional `prep_columns` on `IdentityGraphSource` for lightweight transforms before identity graph joins
- Code generator and executor support for inline identity graph sources

### Changed

- `extract_dependencies` promoted from private to public API in `fyrnheim.engine.resolution`
- `IdentityGraphSource.entity` is now optional (exactly one of `entity` or `source` must be set)

## [0.2.0] - 2026-03-07

### Added

- Entity unit testing framework (`fyrnheim.testing`) with `EntityTest` base class
- `fyr test` CLI command with test discovery, pass/fail output, and `--entity` filter
- Postgres backend support via `create_connection('postgres', ...)`
- `[postgres]` optional dependency group in pyproject.toml
- Example test file in `fyr init` scaffold (`tests/test_customers.py`)
- CONTRIBUTING.md for open source contributors
- Documentation website (Astro Starlight)

### Changed

- README updated with testing workflow and `fyr test` documentation
- README Status section lists Postgres as supported backend

### Fixed

- Ruff import sorting in test_public_api.py

## [0.1.0] - 2026-03-04

### Added

- Entity definition system with Pydantic models and typed fields
- Source types: SourceMapping, DerivedSource, UnionSource
- Ibis-based executor for backend-agnostic transformations
- DuckDB and BigQuery backend support
- CLI (`fyr`) with `init`, `run`, `validate`, and `inspect` commands
- Scaffold project template with sample entity and data
- Field-level lineage tracking
- Connection factory for multi-backend configuration
- YAML-based project configuration (`fyrnheim.yaml`)
