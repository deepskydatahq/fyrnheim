# M089 — Parallel staging view materialization

Fyrnheim materializes staging views by dependency level. Views in the same level do not depend on each other and can be submitted concurrently; later levels wait until all materializations in earlier levels succeed.

## Concurrency control

`materialize_staging_views(..., max_parallel_io=N)` bounds the number of concurrent staging view jobs. The pipeline passes `ResolvedConfig.max_parallel_io`, so the same `max_parallel_io` setting that controls source and output I/O now also controls staging materialization.

- `max_parallel_io = 1` preserves the previous serial execution shape.
- `max_parallel_io > 1` runs independent staging views in a `ThreadPoolExecutor` per dependency level.
- Summary ordering remains deterministic: materialized views are reported in topological submission order, not completion order.

## Dependency and failure behavior

- Dependency levels are derived from explicit `StagingView.depends_on` declarations.
- External dependency names are ignored, matching prior topological-sort behavior.
- If any materialization in a level fails, the exception propagates and later dependent levels are not started.
- Content-hash state skipping still runs before materialization; unchanged views are reported as skipped and are not submitted to the worker pool.
- Fixture-shadowed views are filtered out before state tracking and materialization, preserving DuckDB fixture behavior.

## Benchmark note

The unit benchmark shape used in tests submits three independent staging views with each job sleeping for approximately 50 ms. Serial execution would take roughly 150 ms plus overhead. With `max_parallel_io=3`, the same level completes in about one job duration plus overhead, demonstrating the intended wall-time shape: independent staging work is bounded by the slowest view in the level rather than the sum of all views.

Real BigQuery gains depend on project quotas, slot availability, and view SQL cost. The implementation intentionally reuses `max_parallel_io` as the safety knob instead of adding backend-specific quota management.
