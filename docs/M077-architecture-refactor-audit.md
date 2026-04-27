# M077 Architecture Refactor Audit

Date: 2026-04-27

## Context

M068-M076 added a dense sequence of source-pipeline extensions:

- read-time transforms and computed columns
- json_path extraction and source-level filtering
- backend-aware fixture shadowing
- StateSource joins and source topological sorting
- computed_column skip-if-output-exists semantics for transformed fixtures
- EventSource joins and EventSource join-target guards

The features are covered by regression tests and are valuable. The audit question is whether the architecture now has seams that should be refactored before more source-pipeline features land.

## Findings

| # | Area | Evidence | Impact | Risk | Disposition |
|---|------|----------|--------|------|-------------|
| 1 | Source pipeline stage runner | `_build_state_source_table` in `src/fyrnheim/engine/pipeline.py` and `_build_event_source_table` in `src/fyrnheim/engine/event_source_loader.py` both encode `read → transforms → joins → json_path → computed_columns → filter`, including fixture-shadow and M075 computed-column skip behavior. | High: every future source-stage extension must preserve two mirrored paths. | Medium: careless extraction could alter pipeline semantics. | Create M078. |
| 2 | Source/staging graph sorting | `_topo_sort_sources` in `src/fyrnheim/engine/pipeline.py` is a 100+ line source-specific graph sorter; `src/fyrnheim/engine/staging_runner.py` has related staging DAG logic. Source sorting returns dependency levels and has EventSource-target guards, while staging sorting has different output needs. | Medium: duplicated graph mechanics may drift; shared utility could improve diagnostics and tests. | Medium: source and staging semantics differ enough that over-generalization could hurt clarity. | Create M079. |
| 3 | Registry discovery duplication | `activity_registry.py`, `analytics_entity_registry.py`, `identity_registry.py`, and `metrics_model_registry.py` repeat dynamic import, sys.path insertion, file filtering, duplicate detection, and list/single variable extraction patterns. Identity registry also writes to `sys.modules` while the others do not. | Medium: publishable package polish benefits from one tested discovery primitive and consistent import behavior. | Low/medium: mechanical if behavior is characterized first. | Create M080. |
| 4 | CLI command growth | `src/fyrnheim/cli.py` is ~716 lines. Several command handlers are 50+ lines (`_discover_assets`, `run`, `bench`, `materialize`, `drop`, `list_staging`). | Medium: CLI behavior is user-facing; command orchestration mixed with implementation logic makes future packaging/docs work harder. | Medium: tests must pin behavior and output before moving code. | Create M081. |
| 5 | Regression test organization | M068-M076 regression coverage is strong but spread across large files: `tests/test_source_transforms.py` (~700 lines), `tests/test_e2e_joins.py` (~656), `tests/test_m072_fixture_shadowing.py` (~746), `tests/test_state_event_source.py` (~622). Similar lifecycle-stage fixtures and stage-order assertions recur. | Medium: future extension work needs invariant tests that are easy to find and reuse. | Low/medium: test-only refactor, but fixture changes can accidentally weaken coverage. | Create M082. |
| 6 | Public source model concentration | `src/fyrnheim/core/source.py` is ~366 lines and holds Field, Join, BaseTableSource, TableSource, StateSource, and EventSource semantics. | Low/medium: currently acceptable; splitting public models may increase import churn before package readiness. | Medium: public imports and docs may be affected. | Defer; revisit after M078. |
| 7 | Visualization DAG size | `src/fyrnheim/visualization/dag.py` is ~902 lines and `generate_dag_html` is ~642 lines. | Medium, but outside the M068-M076 source-extension architecture streak. | Medium. | Fold into separate visualization mission if/when DAG work resumes. |

## Recommended follow-up missions

1. **M078 — Shared source stage runner extraction**
   - Highest leverage because it reduces duplicate source-pipeline semantics.
   - Must be behavior-preserving and guarded by invariant tests.

2. **M079 — Graph sorting utility evaluation**
   - Evaluate whether source and staging DAG logic should share a utility or remain intentionally separate.
   - Audit-first because level-vs-linear output and EventSource-target guards may justify separate implementations.

3. **M080 — Registry discovery consolidation**
   - Mechanical consolidation candidate once behavior is characterized.
   - Helps Level 4 publishable-package polish.

4. **M081 — CLI service decomposition**
   - Prepare the CLI for package/public-user readiness by moving command bodies behind service helpers while preserving Click behavior.

5. **M082 — Extension regression test architecture**
   - Consolidate fixtures/helpers and add stage-order invariant tests so future source extensions are safer.

## Deferred / rejected work

- Do not change Join semantics, fixture-shadow semantics, json_path grammar, EventSource join-target behavior, or release/versioning policy as part of the audit.
- Do not split `core/source.py` until M078 clarifies the best source-pipeline seam.
- Do not refactor `visualization/dag.py` under this mission; it belongs to a DAG/visualization mission.

## Validation

This audit is complete when M078-M082 exist as draft mission TOMLs with scoped outcomes, validator context, and explicit non-goals.
