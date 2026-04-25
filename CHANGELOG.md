# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.10.1] - 2026-04-25

### Removed

- `StateSource.snapshot_grain: Literal["hourly", "daily", "weekly"] = "daily"`
  — declared in M041 with the intent that the engine would orchestrate
  loads at the configured grain (hourly/daily/weekly), but the engine
  never read the field. `SnapshotStore` always wrote per-date parquet
  regardless of the declared grain, so setting `snapshot_grain="hourly"`
  silently produced daily snapshots with no warning. This was a silent
  lie in the API surface for ~30 versions (v0.4.x → v0.10.0).
  Discovered during /retro on 2026-04-24; user confirmed removal on
  2026-04-25 (Option B: remove now, re-introduce only when the engine
  is ready to honor it).
- The four corresponding reader sites in
  `src/fyrnheim/visualization/dag.py`: the source tooltip line
  ("Snapshot Grain: …"), the `node_details` dict entry, the per-node
  `node-detail` HTML span, and the JS detail-panel field. The grain
  was decorative-only (display, never executed against), so removing
  the readers is a pure cleanup with no behavior change.
- `TestStateSourceSnapshotGrain` (5 tests) in
  `tests/test_state_event_source.py` — the only tests that pinned the
  field's existence. Test suite drops from 609 → 604.

### Migration

- **Soft upgrade — no user code changes required.** Pydantic v2 defaults
  to `extra="ignore"` and `BaseTableSource` / `StateSource` do not
  override the model config. Existing
  `StateSource(..., snapshot_grain="daily")` declarations continue to
  parse without error after this upgrade — the kwarg is silently
  dropped. Verified by inspection: no `model_config = ConfigDict(extra=...)`
  exists on either class. Users may remove the kwarg at their leisure
  to reflect that it never had effect; there is no urgency.
- If you had read `StateSource.snapshot_grain` in custom code, that
  attribute access now raises `AttributeError`. The field was never
  honored by the engine, so any code that branched on it was
  branching on a dead value.

### Notes

- **Why patch (not major).** Per Pydantic v2 `extra="ignore"` default,
  removing a never-honored field is a non-breaking change at the API
  parsing surface — existing TOML/code declarations parse identically
  to before, just with the kwarg silently dropped. The semantic
  contract was already broken (the field claimed to control grain but
  never did), so removing it aligns the surface with documented intent.
  Patch-shaped per release-discipline memory: "fix aligns with intent".
- **Future enhancement:** when grain-aware orchestration is actually
  implemented (engine reads the field, snapshot store writes
  hour-bucketed parquet for `hourly`, week-bucketed for `weekly`),
  the field can be re-introduced as part of that mission with the
  semantics it always claimed to have. File a follow-up mission at
  that time.

## [0.10.0] - 2026-04-24

### Added

- `BaseTableSource.duckdb_fixture_is_transformed: bool = False` —
  when True AND the source reads from its `duckdb_path` parquet AND
  the backend is DuckDB, the engine SKIPS `transforms`, `fields`
  (json_path extraction), and `filter` at source read time. Use this
  when your parquet fixture is a post-transform snapshot of the
  BigQuery output — the fixture IS the final shape, and re-applying
  transforms would either fail (pre-rename columns missing) or
  double-transform. Unblocks migrating SQL staging views to
  declarative pydantic sources when the DuckDB fixture is
  post-transform. Applies to `StateSource`, `EventSource`,
  `TableSource`, and `DerivedSource` via inheritance. Gate condition
  (all three must hold): flag=True AND `config.backend == "duckdb"`
  AND `source.duckdb_path` is truthy (non-empty string). Mirrors
  the parquet-read branch of `BaseTableSource.read_table`, which
  rejects empty strings via `if not self.duckdb_path`.
- Diagnostic INFO log line on the skip path:
  `StateSource/EventSource %s: duckdb_fixture_is_transformed=True,
  skipping transforms/fields/filter (reading duckdb_path fixture)`.

### Notes

- **`computed_columns` still apply on the skip path.** Computed
  expressions are backend-independent — they evaluate the same way
  against the post-transform shape on DuckDB as on BigQuery. No
  reason to skip them; applying them keeps backend-parity on the
  derived-column layer. The skip is all-or-nothing for the
  transforms / json_path / filter stages; `computed_columns` is a
  separate stage with different semantics and remains active.
- **Backend-parity warning**: if you set
  `duckdb_fixture_is_transformed=True` but your fixture is NOT
  actually in post-transform shape, DuckDB-only runs will produce
  wrong results with no automatic error. Catch this with
  cross-backend tests. The flag is explicit opt-in, not
  auto-detected, so the mistake is visible at the source
  declaration site.
- **Default `False` preserves v0.9.1 behavior.** Users who do not
  opt in see identical uniform-transform application on both
  backends.
- **FR-5 (source-level joins) deferred to M070.** With this flag,
  the two Pardot sources that need joins can keep their
  `StagingView` SQL for the join layer and migrate everything else
  to pydantic — no urgent need for a source-level `joins` API.

## [0.9.1] - 2026-04-24

### Fixed

- `SnapshotDiffPipeline` emitted `entity_id` as `"1.0"` (float-shaped
  string) instead of `"1"` for `StateSource`s whose `id_field` was an
  integer column **and** whose DataFrame contained any float-typed
  sibling column. This silently broke identity resolution: downstream
  joins and mappings that expected `"1" == "1"` saw `"1.0" != "1"`.
  **Root cause (M071 Phase 1):** `pd.DataFrame.iterrows()` packs each
  row into a homogeneous-dtype `Series`; when the row mixes int64 and
  float64 columns, the int is promoted to `np.float64`, so
  `str(row[id_field])` produced `"1.0"`. The promotion is _not_ in
  `ibis.memtable.execute()` or the SnapshotStore parquet round-trip
  (both preserve `int64`) — it is specifically in the per-row
  `iterrows()` Series materialization inside
  `_make_appeared_events` / `_make_disappeared_events`. **Fix:** new
  `_stringify_id(v)` helper in `src/fyrnheim/engine/diff_engine.py`
  casts integral-valued floats (`v.is_integer()`) back to `int` before
  stringifying; applied at both `str(row[id_field])` call sites.
  Regression tests cover cold-start, M066 empty-diff replay, and
  `row_disappeared` paths (`TestM071StringifyId` in
  `tests/test_diff_engine.py`, `TestM071Int64EntityIdIntegration` in
  `tests/test_snapshot_diff.py`).

## [0.9.0] - 2026-04-24

### Added

- `BaseTableSource.filter: str | None = None` — declarative source-level
  row filter. When set, applied after transforms and computed_columns
  via the `eval()` pattern (same as `ComputedColumn.expression`), with
  `{'ibis': ibis, 't': table}` context. Applies to `StateSource`,
  `EventSource`, and `DerivedSource` via inheritance. Unblocks migrating
  SQL `WHERE`-clause-only staging views to declarative pydantic
  sources.
- `Field.source_column: str | None = None` — when `Field.json_path` is
  set, the engine extracts the JSON value from this column. Defaults
  to `Field.name` for the common case where the JSON source column is
  named the same as the field being extracted.
- `Field.json_path` is now honored by the engine — previously declared
  on the `Field` model but never applied. Top-level paths (`$.<key>`)
  are supported; nested paths (`$.a.b`) raise a `ValueError` at
  pipeline setup with a pointer to file a future-enhancement request.
  The extracted JSON scalar is typed via `.unwrap_as(<ibis_type>)`
  based on `Field.type`, where supported types are `STRING`, `INT64`,
  `FLOAT64`, `BOOLEAN`, `DATE`, `TIMESTAMP`, `BYTES`.

### Changed

- `source.fields` is now engine-consumed for `json_path` extraction.
  Previously it was read-only metadata — engines never iterated it.
  Users who declared `Field(name=X, json_path=Y)` expecting the field
  to be silently ignored will see extraction happen on upgrade. This
  matches the documented model intent; if you had a non-conforming
  setup (declared `json_path` + expected it ignored), remove the
  `json_path` attribute.
- Pipeline-stage order at source load time is now (load-bearing):
  `read_table → transforms → json_path → computed_columns → filter`.
  All five stages apply before the `full_refresh` / snapshot-diff
  branch in `StateSource`, so both paths and the snapshot store save
  see the same post-filter table.

### Notes

- **Filter NULL-gotcha**: SQL three-valued logic means `t.col != True`
  drops NULL rows silently. If you want NULLs to be treated as `False`
  (kept in the filtered output), write `t.col.fillna(False) != True`
  instead. This is SQL semantics, not a framework-level issue — the
  `BaseTableSource.filter` docstring documents the escape hatch.
- **FR-5 (source-level joins) deferred to M070.** Use `StagingView`
  for joins in the meantime; the M070 mission will revisit
  source-level joins after real usage data from v0.9.0 informs the
  API shape.

## [0.8.2] - 2026-04-24

### Fixed

- `SourceTransforms` (`renames`, `type_casts`, `divides`, `multiplies`)
  were declared on `StateSource` and `EventSource` pydantic models but
  never applied by the engine. They are now honored at source read
  time, in the order: `type_casts → divides → multiplies → renames`.
- `StateSource.computed_columns` were similarly declared but ignored
  by `_load_state_source`. They now apply at read time matching the
  existing `EventSource` behavior. Computed columns are applied
  **after** transforms, so expressions can reference renamed or
  cast columns.
- Migration note: if you set `transforms` or `computed_columns` on a
  `StateSource` today expecting them to be silently ignored, this is
  a behavior change — but that setup contradicts the documented model
  intent, so it should be rare in practice. Unblocks the first phase
  of migrating SQL staging views to declarative pydantic sources.

## [0.8.1] - 2026-04-23

### Fixed

- Pipelines with StateSources whose upstream view returns 0 rows
  (empty placeholders — e.g. `SELECT ... FROM UNNEST([1]) LIMIT 0`
  for Salesforce placeholders) crashed with `Invalid Input Error:
  Provided table/dataframe must have at least one column` on every
  run after the first. The v0.8.0 replay-on-empty-diff branch fired
  unconditionally when events were empty and a previous snapshot
  existed, attempting to replay `row_appeared` events from a 0-row
  DataFrame. Three production StateSources reproduced:
  `salesforce_accounts`, `salesforce_contacts`,
  `salesforce_opportunities`. `SnapshotDiffPipeline.run()` now
  checks the current table row count before attempting the replay;
  when current is empty, the replay is skipped (nothing to replay)
  and the pipeline returns an empty events table — the pre-v0.8.0
  behavior for this specific case.

## [0.8.0] - 2026-04-23

### Fixed

- `SnapshotDiffPipeline` no longer silently produces 0 events when
  upstream StateSource data is unchanged since the last saved
  snapshot. Previously, any pipeline run against stable or
  slowly-changing StateSources after the first run would emit an
  empty event stream, and downstream `AnalyticsEntity` materialization
  would produce 0 rows — silent pipeline failure. A production
  pipeline running against two GA4 daily-session StateSources hit
  this: `sessions` entity dropped from 3,000 rows to 0 rows on the
  second run, `anon_users` entity dropped from 2,241 to 1,348 rows,
  with no error signal anywhere. Fix: when the diff returns 0 events
  AND a previous snapshot exists, replay every current row as a
  synthetic `row_appeared` event so downstream state-field
  materialization continues to produce correct output.

### Added

- `StateSource.full_refresh: bool = False` — when True, skip the
  snapshot-diff machinery entirely and emit `row_appeared` for every
  current row on every run. Useful for state-only sources where
  CDC-style `field_changed` events are not needed, or to force
  deterministic state-reflects-current behavior regardless of
  snapshot-store state.

### Changed

- Default `SnapshotDiffPipeline` behavior is now "replay row_appeared
  on empty diff" (see Fixed). Users whose pipelines were previously
  producing 0 rows from stable StateSources will see entity row
  counts jump to correct values on the next run after upgrading —
  this is the fix, not a regression. No action required unless
  downstream consumers had cached or aggregated the (incorrect)
  empty output; in that case, invalidate those caches after the
  first 0.8.0 run.

## [0.7.3] - 2026-04-19

### Fixed

- `_coerce_to_arrow_friendly_dtype` now attempts to coerce mixed
  numeric-and-string columns to a nullable numeric dtype when every
  string is a valid JSON number. Previously the column fell through
  to object dtype, breaking the BigQuery PyArrow registration path
  for any state field whose source payload stored the value
  inconsistently as JSON numbers in some rows and JSON-quoted numbers
  in others (e.g. `{"score": 11}` vs `{"score": "-5"}` across the
  same source).

### Changed

- State fields and `latest` measures over payloads with intentionally
  mixed numeric-and-numeric-string values now produce a unified
  nullable numeric column (`Int64` if all values parse to int,
  `Float64` if any parse to float). Columns containing any
  non-numeric string are unaffected and continue to produce
  object-dtype output — which still fails PyArrow registration
  loudly, as it should (genuinely inconsistent data needs upstream
  cleanup, not silent type coercion).

## [0.7.2] - 2026-04-19

### Fixed

- `_coerce_to_arrow_friendly_dtype` now recognises numpy scalar types
  (`np.int8`/`int16`/`int32`/`int64`, unsigned variants,
  `np.float16`/`float32`/`float64`, `np.bool_`) alongside their Python
  equivalents. v0.7.1 fixed the Python-scalar case, but BigQuery's
  client returns numpy scalars — the helper's strict `type()` match
  rejected them and the output column fell through to object dtype,
  reintroducing the same PyArrow registration failure v0.7.1 was meant
  to eliminate. DuckDB was unaffected because it returns plain Python
  scalars.

## [0.7.1] - 2026-04-19

### Fixed

- `project_analytics_entity` output now uses pandas nullable dtypes
  (`Int64` / `Float64` / `boolean`) for numeric and boolean state fields
  instead of object dtype with Python-scalar-and-None values. v0.7.0
  produced object-dtype columns that PyArrow could not convert, breaking
  the BigQuery memtable-registration path for any entity with a
  nullable numeric or boolean state field. DuckDB was unaffected.

## [0.7.0] - 2026-04-19

Performance bundle: push-down analytics-entity projection (M060), parallel
source/entity I/O (M059), benchmark harness (M058), and post-retro cleanup
(M061).

### Performance

- `project_analytics_entity` now compiles to a single backend
  `filter → group_by → aggregate` Ibis expression instead of materializing
  the full enriched-events stream to pandas and looping in Python. JSON
  payload extraction stays backend-portable (DuckDB `JSON_TYPE` / BigQuery
  `SAFE.*` casts). Measured on a synthetic DuckDB fixture (1000 entities,
  mixed state fields + measures, median of 3):

  | rows | legacy | new | speedup |
  | ---- | ------ | ----- | ------- |
  | 1k   | 72.5ms | 19.2ms | 3.8×   |
  | 10k  | 832ms  | 31.8ms | 26×    |
  | 50k  | 7607ms | 91.3ms | 83×    |

  The new shape scales essentially flat with event volume on the client;
  the old Python loop was `O(n_ids × n_fields)`. On BigQuery the win is
  one aggregation query per entity instead of a full event-table export.

- Source loads (phase 1) and entity/metrics writes (phases 4–5) now fan
  out to a `ThreadPoolExecutor` with a configurable worker count
  (`max_parallel_io`, default `4`). Biggest wins are on BigQuery-bound
  pipelines where each load/write blocks on a server round-trip; small
  local DuckDB projects are unaffected at the default. Source-load order
  is preserved — `event_tables[i]` still corresponds to
  `config.sources[i]` regardless of completion order.

### Added

- `fyr bench` CLI subcommand — runs the configured pipeline end-to-end
  and prints a per-phase, per-source, per-entity timing report. Human-
  readable table by default, `--json` for machine consumption. Exit
  contract matches `fyr run`.
- `PipelineTimings` is now populated on every `PipelineResult`,
  recording phase 0–5 durations plus per-source, per-identity-graph,
  per-entity (`project_s` / `write_s`), and per-metrics-model timings.
  Total `elapsed_seconds` is approximately the sum of the phase
  timings.
- `ResolvedConfig.max_parallel_io: int = 4` — bounds the I/O thread
  pool. Configurable via `fyrnheim.yaml` or CLI overrides.
- `fyr run --max-parallel-io N` and `fyr bench --max-parallel-io N` —
  override the configured pool size for a single invocation.

### Changed

- Phase 1 (source loading) is now fail-fast: if any source loader
  raises, the exception propagates out of `run_pipeline` via
  `future.result()` rather than being accumulated into
  `PipelineResult.errors` and reported at phase end. This matches the
  serial pre-M059 contract where the first failure aborted the run.

  **Migration:** if you were inspecting `result.errors` for source-load
  failures, wrap `run_pipeline(...)` in `try/except` instead. No public
  API signature changed.

### Fixed

- `IbisExecutor` connection leaks in four CLI commands closed:
  `fyr run`, `fyr materialize`, `fyr drop`, and `fyr list-staging` now
  wrap the executor in `try/finally` with `executor.close()` on both
  success and error paths, matching the `fyr bench` pattern introduced
  in M058.
- `IbisExecutor.register_parquet` now holds `self._conn_lock` around
  the connection mutation, for symmetry with every other
  `self._conn`-touching method. Removes a latent thread-safety gap
  under M059's parallel source loads.

### Known limitations

- No BigQuery credential-gated integration test covers the push-down
  path yet — BQ portability is verified via `con.compile(expr)` against
  a dummy connection, not a live job. Tracked as typedata-tidw.
- `project_analytics_entity` still materializes aggregation results to
  a pandas DataFrame and round-trips JSON scalars client-side for mixed-
  type preservation, even when no `computed_fields` are declared. A
  lazy-return path for the no-`computed_fields` case may land in a
  future release. Tracked as typedata-vmok.

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
