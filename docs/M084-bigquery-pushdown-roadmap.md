# BigQuery pushdown performance roadmap

Source: root `speed_improvements.md` performance note.

## Problem

Fyrnheim still has several pandas-first execution paths that call `.execute()` against BigQuery, download large intermediate event streams into Python, and then perform row-wise loops or pandas groupbys locally. This is architecturally wrong for warehouse-scale projects: BigQuery should perform filtering, unioning, identity resolution, activity derivation, and aggregation; Python should orchestrate expressions and fetch/write only final materialized outputs.

## Observed bottlenecks

| Area | Current shape | Planned mission |
| --- | --- | --- |
| Source concat / event stream assembly | Multiple `.execute()` calls and `pd.concat()` in `pipeline.py` | M084 |
| Identity resolution | `identity_engine.py` downloads events and loops with JSON parsing + SHA hashing | M085 |
| Activity filtering | `activity_engine.py` downloads events and loops over matches | M086 |
| Metrics aggregation | `metrics_engine.py` downloads events and uses pandas groupby | M087 |
| Column pushdown | Source reads carry more columns than downstream phases need | M088 |
| Staging materialization | Independent staging views materialize sequentially | M089 |

Existing related missions:

- M058: timing harness to measure impact.
- M059: bounded parallel I/O for source loads and writes.
- M060: analytics entity projection pushdown, covering one major local-compute path not repeated here.
- M078: shared source-stage runner, useful foundation for consistent source expression construction.

## Sequencing

1. M084 establishes a warehouse-native event-stream expression contract and removes the source concat/pandas materialization path.
2. M085 and M086 push identity and activity phases onto that event-stream contract.
3. M087 pushes metrics aggregation over the warehouse-native enriched/activity event expressions.
4. M088 prunes source columns once downstream needs are expressed declaratively.
5. M089 parallelizes staging view materialization as an independent quick win.

Each mission should preserve DuckDB behavior and include BigQuery-gated integration or mocked SQL/compile coverage where real credentials are unavailable.
