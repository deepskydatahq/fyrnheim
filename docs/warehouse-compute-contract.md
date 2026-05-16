# Warehouse compute-only contract

Fyrnheim supports two execution modes with different materialization rules:

- **DuckDB local development** may read local parquet fixtures and use local Python/Pandas compatibility paths where the engine still needs them.
- **Warehouse backends** (starting with BigQuery, and conservatively any backend other than DuckDB) must keep transformation compute in Ibis/SQL. Python may orchestrate expressions, submit writes, and fetch final outputs, but it must not download source or intermediate warehouse data to compute transformations in pandas.

If a warehouse run reaches a feature that still requires local pandas transformation compute, Fyrnheim raises `UnsupportedWarehouseComputeError` before source loading instead of silently extracting data from the warehouse.

## Allowed materialization boundaries

Warehouse-backed runs may materialize data only at explicit boundaries:

1. **Final outputs** — the pipeline may execute a final expression to write a parquet output or warehouse table.
2. **Explicit small metadata checks** — documented count/schema probes are allowed when they do not transform source rows locally.
3. **User-requested previews/debugging** — CLI or interactive preview commands may fetch data because the user explicitly asked to inspect it.
4. **Warehouse DDL/DML execution** — view/table materialization through backend SQL clients is allowed.

Everything else should remain an Ibis expression until one of those boundaries.

## Forbidden fallbacks

Warehouse-backed runs must not:

- call `.execute()` on source, event, identity, activity, metrics, or entity intermediates in order to transform rows in Python;
- build transformation outputs through `pd.DataFrame`, `pd.concat`, row iteration, or `ibis.memtable` registration of downloaded warehouse data;
- evaluate Python-only computed fields over warehouse intermediate results;
- use BigQuery as a remote file source while pandas performs the actual transformation.

## Current audit dispositions

| Engine path | Disposition for warehouse backends | Reason |
| --- | --- | --- |
| `EventSource` loading and event-shape conversion | Allowed | v0.14 composes backend Ibis expressions and backend `UNION ALL`. |
| Activity derivation | Allowed | v0.14 composes backend predicates/projections/unions. |
| Identity resolution and enrichment | Allowed | v0.14 uses backend JSON extraction, hashing, joins, and coalesce fallback. |
| `MetricsModel` aggregation | Allowed | v0.14 uses backend filtering, grouping, and aggregate expressions. |
| Staging view materialization | Allowed | Executes backend SQL/view creation; no pandas row transformation. |
| Final output write/materialization | Allowed | This is an explicit output boundary. |
| `StateSource` snapshot diff and `full_refresh` | Blocked by `UnsupportedWarehouseComputeError` | Current diff/replay code materializes rows into pandas and constructs row events locally. Use an `EventSource` or staging-derived event stream for warehouse runs until StateSource diff is rewritten in SQL/Ibis. |
| `AnalyticsEntity` projection | Blocked by `UnsupportedWarehouseComputeError` | Current projection executes the post-aggregation result in Python for JSON scalar parsing/computed fields and re-registers a memtable. Use `MetricsModel` outputs or wait for a fully Ibis-native entity projection. |
| Private empty-result helpers using `ibis.memtable(pd.DataFrame(...))` | Allowed only for DuckDB/local compatibility or non-row-transforming empty schemas | These helpers must not become a warehouse transformation fallback. |

## Fixing a blocked warehouse run

When `UnsupportedWarehouseComputeError` names an asset:

1. Prefer an existing warehouse-native model (`EventSource`, activity definitions, identity graphs, `MetricsModel`, or staging views).
2. Move source-specific SQL joins/normalization into a `StagingView` if that keeps the work in the warehouse.
3. For state-like data, emit a warehouse-native event stream instead of using `StateSource` snapshot diff until SQL/Ibis diff support exists.
4. Do not work around the guard by calling `.execute()` manually in engine code; add a backend-native expression path or keep the feature DuckDB-only.

DuckDB remains the supported local development backend for parquet fixtures and local iteration.
