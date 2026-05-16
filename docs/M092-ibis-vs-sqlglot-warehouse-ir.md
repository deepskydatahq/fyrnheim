# M092 — Ibis vs SQLGlot for Fyrnheim's warehouse IR

Date: 2026-05-16
Status: accepted recommendation
Mission: `M092`

## Decision

Keep **Ibis as Fyrnheim's default relational expression layer** for the warehouse engine, but introduce a clearer Fyrnheim-owned internal contract around transformation phases and allow **targeted SQLGlot use at SQL boundaries**.

Do **not** migrate the engine wholesale from Ibis to SQLGlot now.

Recommended shape:

1. Fyrnheim product models (`Source`, `ActivityDefinition`, `IdentityGraph`, `AnalyticsEntity`, `MetricsModel`) remain the public API.
2. Engine phases continue to build composable Ibis expressions where the operation is naturally relational.
3. Fyrnheim adds a small internal IR/policy layer for phase capabilities and materialization boundaries so the engine can answer: "is this phase expression-native for this backend?"
4. SQLGlot is introduced, if needed, as a targeted SQL AST/rendering/validation tool for:
   - backend-native SQL patterns Ibis cannot express cleanly;
   - generated SQL lint/parse/normalization checks;
   - BigQuery-focused debugging and golden SQL tests;
   - future SQL-source/staging-view analysis.

In short: **Ibis for expression composition; SQLGlot for SQL ownership where Fyrnheim needs exact dialect control.**

## Why this question matters now

M091 made warehouse execution a hard contract: BigQuery/warehouse pipelines must not download source or intermediate data and compute transformations in pandas. Before M091, Ibis could be used opportunistically: if a transformation became awkward, the engine could `.execute()`, finish in pandas, and wrap results in `ibis.memtable`. That escape hatch is now forbidden.

The question is therefore not "Ibis vs SQL strings" in the abstract. It is whether Fyrnheim can express every core transformation as backend-native compute with enough confidence, debugging visibility, and testability.

## Workload comparison

| Fyrnheim workload | Current/desired shape | Ibis fit | SQLGlot fit | Recommendation |
| --- | --- | --- | --- | --- |
| Source read, projection, type casts, renames | Relational projection/mutation | Strong. Existing `source_stage.py` uses table ops and preserves composition. | Possible but would require owning schemas/aliases manually. | Keep Ibis. |
| Source joins | Left joins over Ibis tables with dependency ordering | Strong for current single-key left joins. | Strong for generating exact SQL joins, but more manual. | Keep Ibis; consider SQLGlot only for SQL inspection. |
| JSON path extraction | Ibis JSON expressions with backend compilation | Adequate but dialect edge cases are visible (`unwrap_as`, casts). | Strong when exact BigQuery `JSON_VALUE`/`JSON_QUERY` shape matters. | Keep Ibis, add compile tests; use SQLGlot validation/golden SQL for hard cases. |
| EventSource normalization | Build canonical event rows and payload JSON | Strong. Existing tests compile to BigQuery SQL without `.execute()`. | Possible, but lower-level and more verbose. | Keep Ibis. |
| Activity derivation | Predicates, projections, `UNION ALL` | Strong. Natural relational expression. | Possible but unnecessary. | Keep Ibis. |
| Identity resolution | Filter, JSON extraction, deterministic hash, group, join/coalesce | Mostly strong. Backend-specific hash return type already handled. | Useful if hash dialects become brittle. | Keep Ibis with focused backend compile tests. |
| MetricsModel aggregation | Filter, date grain, dimensions, aggregate expressions | Strong. Existing BigQuery compile test covers `GROUP BY`, JSON, count distinct. | Possible but duplicative. | Keep Ibis. |
| AnalyticsEntity projection | State/measure projection is Ibis-native; Python computed fields remain local-only | Mostly strong. Relational pieces fit; Python computed fields do not. | Useful for owned SQL projection, but Python expression semantics still need a translation layer. | Keep Ibis-native projection; block computed fields on warehouses until a supported expression subset exists. |
| StateSource snapshot diff | Current code is pandas diff; desired shape is full outer/left anti joins + JSON event construction | Possible in Ibis, but complex around event row union and payload JSON. | Strong conceptual fit for exact SQL diff query, especially BigQuery. | Prototype both; likely Ibis for portable expression, SQLGlot if BigQuery SQL needs exact control. |
| Final materialization | `.execute()` to parquet or `write_table` output boundary | Ibis connection/write integration is useful. | SQLGlot does not execute; still needs backend client. | Keep Ibis/executor. |

## Evidence from current code

### Ibis is already carrying the pushed-down paths

- `src/fyrnheim/engine/source_stage.py` runs the shared source chain as Ibis operations: read → transforms → joins → JSON path → computed columns → filter.
- `src/fyrnheim/engine/event_source_loader.py` builds event payloads with Ibis `struct(...).cast("json").cast("string")` and does not execute.
- `src/fyrnheim/engine/activity_engine.py` builds predicates/projections/unions with Ibis.
- `src/fyrnheim/engine/identity_engine.py` builds mapping and enrichment expressions with JSON extraction, hashing, grouping, and joins.
- `src/fyrnheim/engine/metrics_engine.py` builds backend `GROUP BY` aggregations.
- BigQuery compile tests already exist for EventSource and MetricsModel shapes.

This is strong evidence that Ibis is not merely incidental. It is the current compositional backbone.

### The blocked M091 paths are not solved by SQLGlot alone

`AnalyticsEntity` still has one Python-only semantic surface:

- `computed_fields` use Python expression evaluation over rows.

State/measure projection now returns warehouse-native Ibis expressions for supported shapes. SQLGlot can generate SQL, but it does not automatically translate Fyrnheim's Python computed-field semantics into backend expressions. Fyrnheim still needs an expression/capability layer either way.

`StateSource` snapshot diff is more SQL-shaped:

- row appeared = current left anti join previous;
- row disappeared = previous left anti join current;
- field changed = join current/previous and compare each non-id field;
- final result = `UNION ALL` canonical event rows with JSON payloads.

SQLGlot may help here, especially for BigQuery JSON object construction and explicit full/anti join SQL. But a direct SQLGlot implementation would have to own schema, quoting, aliasing, field iteration, backend execution, and DuckDB parity. Ibis can express most of the same plan while preserving the existing backend abstraction.

## Conceptual StateSource diff prototype

The blocked M091 path with the clearest SQL shape is StateSource diff. A warehouse-native plan does not require local pandas:

```text
current(id, fields...)      previous(id, fields...)

appeared:
  current LEFT JOIN previous USING(id)
  WHERE previous.id IS NULL
  SELECT source, CAST(current.id AS STRING), snapshot_ts,
         'row_appeared', TO_JSON_STRING(STRUCT(current.fields...))

disappeared:
  previous LEFT JOIN current USING(id)
  WHERE current.id IS NULL
  SELECT source, CAST(previous.id AS STRING), snapshot_ts,
         'row_disappeared', TO_JSON_STRING(STRUCT(previous.fields...))

changed per field F:
  current JOIN previous USING(id)
  WHERE current.F IS DISTINCT FROM previous.F
  SELECT source, CAST(current.id AS STRING), snapshot_ts,
         'field_changed', TO_JSON_STRING(STRUCT('F' AS field_name,
                                                previous.F AS old_value,
                                                current.F AS new_value))

final:
  appeared UNION ALL disappeared UNION ALL changed_field_1 ... changed_field_n
```

Ibis can express this as joins, filters, `ibis.struct`, casts, and `ibis.union`, then compile to BigQuery/DuckDB. SQLGlot can express the same query directly as an AST. The prototype conclusion is:

- **SQLGlot is more direct for the final SQL shape** and likely easier to compare against hand-written SQL.
- **Ibis is safer for integrating with the rest of Fyrnheim's expression pipeline**, because `current` and `previous` are already Ibis tables and downstream phases consume Ibis tables.
- If Ibis cannot produce correct BigQuery JSON payload SQL or null-safe comparisons, use SQLGlot for that subquery or for SQL golden-test validation rather than replacing the whole engine.

## Tradeoffs

### Backend portability

- **Ibis:** Stronger. A single expression can target DuckDB, BigQuery, ClickHouse, and Postgres where supported.
- **SQLGlot:** Strong SQL transpilation, but Fyrnheim would own more backend capability checks and execution integration.

Winner: Ibis.

### BigQuery dialect fidelity

- **Ibis:** Good enough for many paths, but generated SQL can be opaque and some dialect-specific functions require custom UDF/workarounds.
- **SQLGlot:** Better for exact SQL rendering, parse/format/normalize, and reasoning about BigQuery syntax.

Winner: SQLGlot.

### Static validation and capability checks

- **Ibis:** Type-ish expression objects and backend compile failures provide some validation, but not a Fyrnheim-level contract.
- **SQLGlot:** AST can be inspected, but Fyrnheim would need to build or infer semantic types.

Winner: neither alone. Fyrnheim needs an internal capability layer.

### Composability

- **Ibis:** Strong. Engine phases compose table expressions naturally.
- **SQLGlot:** Strong at SQL AST composition, but integrating with Ibis tables/backends would require boundaries and conventions.

Winner: Ibis for the current architecture.

### Debuggability

- **Ibis:** `ibis.to_sql(..., dialect="bigquery")` helps, but generated SQL may not be the SQL humans would write.
- **SQLGlot:** Excellent for formatting, normalizing, parsing, and golden SQL diffs.

Winner: SQLGlot.

### Testing strategy

- **Ibis:** Existing tests can execute on DuckDB and compile to BigQuery. This is already working.
- **SQLGlot:** Would enable SQL AST/golden SQL tests, but behavior still needs execution tests.

Winner: hybrid.

### Migration cost

- **Ibis:** Low; already implemented across pushed-down paths.
- **SQLGlot:** High for wholesale migration. Every phase would need schemas, aliasing, execution bridges, and parity tests.

Winner: Ibis.

## Risks

1. **Ibis hides SQL shape until compile time.** Mitigation: add BigQuery compile/golden SQL tests for every warehouse-native phase.
2. **SQLGlot can tempt Fyrnheim into hand-owned SQL too early.** Mitigation: only use SQLGlot behind narrow internal interfaces.
3. **A hybrid can become two IRs with inconsistent semantics.** Mitigation: Fyrnheim owns the phase contract; Ibis/SQLGlot are compilers/helpers, not public APIs.
4. **Python expression features remain hard either way.** Mitigation: explicitly classify Python-only computed expressions as DuckDB/local-only until translated to a backend expression subset.

## Recommendation details

### Short term

- Keep Ibis in the engine.
- Add BigQuery compile coverage for every warehouse-native path that currently lacks it.
- Implement StateSource diff as an Ibis expression first, because it must compose with existing source-stage Ibis tables and downstream event streams.
- Use SQLGlot in the StateSource diff spike to validate or compare generated BigQuery SQL shape if Ibis SQL becomes hard to reason about.

### Medium term

- Define a Fyrnheim internal transformation capability contract:
  - phase name;
  - input/output schema;
  - whether the phase is warehouse-native for a backend;
  - allowed materialization boundaries;
  - known dialect requirements.
- Keep SQL inspection tests that compile Ibis to SQL and parse with SQLGlot for BigQuery where useful.
- Consider SQLGlot-backed rendering only for isolated subqueries that Ibis cannot express correctly.

### Do not do now

- Do not replace Ibis wholesale with SQLGlot.
- Do not expose SQLGlot ASTs as Fyrnheim's public model API.
- Do not reintroduce local pandas fallback while evaluating either path.

## Follow-up work filed

- `M093` — Warehouse-native StateSource snapshot diff spike (completed: implemented first in Ibis, with BigQuery compile coverage).
- `M094` — Internal warehouse transformation capability contract (completed: phase capabilities now record backend, schema, materialization policy, tooling, and support status; M091 guardrails use those capabilities).
- `M096` — SQLGlot SQL validation harness (completed: tests compile Ibis expressions to BigQuery SQL, parse/normalize with SQLGlot, and assert structural AST properties for warehouse-native phases).

## Final answer to the original question

When Fyrnheim moves away from locally owned compute, **Ibis is still the right middle layer for most of the engine today** because Fyrnheim's phases are relational and already compose as Ibis expressions. **SQLGlot would be more direct for SQL authoring and BigQuery dialect control**, but directness comes with higher ownership cost: schemas, aliases, backend capability checks, execution integration, and parity testing.

The best next architecture is not "Ibis or SQLGlot". It is **Fyrnheim-owned semantics with Ibis as the default expression compiler and SQLGlot as a targeted SQL boundary tool**.
