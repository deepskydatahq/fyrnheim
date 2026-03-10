# Zeffy Attribution Pipeline — Spike Analysis & Blueprint (M019-spike)

## Goal

Investigate the Zeffy dbt attribution pipeline (client-zeffy) to identify what Fyrnheim needs to support similar marketing attribution workloads. Produce a gap analysis, a Fyrnheim-native blueprint, and draft missions for follow-up work.

## Gap Analysis

| Zeffy Technique | Fyrnheim Status | Gap |
|---|---|---|
| Identity resolution (amplitude_id → account) | Covered — IdentityGraphConfig + DerivedSource | None |
| Union sources (multiple event streams) | Covered — UnionSource with per-source mappings | None |
| Channel classification (cascading CASE WHEN) | Partially covered — CaseColumn handles it, chaining needed for priority tiers | Low |
| Signal selection (filter to acquisition events) | Covered — PrepLayer + filter expressions | None |
| Window function dedup (ROW_NUMBER + QUALIFY) | Partially covered — expressible as strings, no helper pattern | Medium |
| Incremental materialization | Gap — enum defined, codegen not implemented | High |
| Config-driven model variants (6 models from 1 config) | Gap — no parameterized entities | High (but workaround: separate entities) |
| JSON field extraction | Covered — json_extract_scalar(), json_value() | None |
| GROUP BY aggregation | Covered — AggregationSource + AnalyticsLayer | None |
| Wide table pivoting | Gap — no pivot support | Medium |
| Date arithmetic | Covered — date_diff_days(), days_since(), etc. | None |
| Source freshness | Covered — MaxAge quality check | None |

### Summary

Fyrnheim handles ~70% of the Zeffy pipeline natively. Three significant gaps:

1. **Incremental materialization** — critical for production event pipelines that can't reprocess everything daily
2. **Parameterized entity variants** — needed for multi-model attribution, but workaround exists (separate entities per model)
3. **Window function dedup pattern** — common enough to warrant a helper

## Fyrnheim-Native Blueprint

Redesigned for Fyrnheim's strengths rather than direct-translating dbt patterns.

### Data Flow

```
Sources
  amplitude_events (TableSource)
  amplitude_merges (TableSource)
  organizations (TableSource)
  survey_responses (TableSource)
       │
       ▼
Entity: amplitude_event
  PrepLayer:
    - JSON extraction (gclid, fbclid, utm_*, org_id)
    - Page intent classification (CaseColumn)
    - Channel classification (chained CaseColumn)
    - Filter to acquisition-relevant events
       │
       ▼
Entity: account
  Source: IdentityGraphConfig
    - organizations (inline TableSource, match on org_id)
    - amplitude_events (inline, prep_columns for dedup)
    - amplitude_merges (inline, cross-device resolution)
  DimensionLayer:
    - account_name, created_at, referral_source
       │
       ▼
Entity: acquisition_signal
  Source: DerivedSource (depends_on: account)
    - Joins resolved account_id onto filtered events
    - Dedup: one signal per (account, type, timestamp)
  PrepLayer:
    - signal_type (CaseColumn: session/signup/survey)
    - Channel enrichment (UA heuristic reclassification)
  ActivityLayer:
    - first_touch (row_appears)
    - signup (status_becomes: has_signup = true)
       │
       ▼
Entity: attribution_first_touch
  Source: AggregationSource (from: acquisition_signal)
    - Filter: signal_timestamp < signup_timestamp
    - Group by: account_id
    - Agg: FIRST(channel, order_by=signal_timestamp)

Entity: attribution_paid_priority
  Source: AggregationSource (from: acquisition_signal)
    - Filter: signal_timestamp < signup_timestamp
    - Group by: account_id
    - Computed: priority_channel via cascading rules
      (gclid → fbclid → utm paid → first_touch fallback)
       │
       ▼
Entity: account_attributed (final wide table)
  Source: DerivedSource
    - Joins account + first_touch + paid_priority
    - One row per account, all attribution columns
```

### Key Decisions

**IdentityGraphConfig replaces manual merge SQL.** Fyrnheim's graph handles direct + transitive resolution with priority-based field selection. The amplitude merge table feeds in as a second inline source.

**ActivityLayer replaces manual window functions** for detecting first_touch and signup events. `row_appears` and `status_becomes` are already native triggers.

**Each attribution model is a separate entity.** Simplest Fyrnheim-native approach — no new features needed. Some repetition across models, but each is small (source + 2-3 computed columns). Other models (last_touch, survey_override, etc.) follow the same pattern.

**Channel classification uses chained CaseColumns.** One for primary classification (click IDs + UTM), one for enrichment (UA heuristic reclassification of direct_or_unknown).

## What Doesn't Change

- Codegen: existing IbisCodeGenerator handles all entity types in the blueprint
- Executor: existing IbisExecutor + runner handle dependency resolution
- Quality checks: MaxAge, NotNull, Unique cover Zeffy's test patterns
- Expression helpers: CaseColumn, contains_any, isin_literal (M018) cover channel classification

## Proposed Follow-Up Missions

### M020: Incremental Materialization
Highest-impact gap. Without this, event pipelines reprocess all data daily.
- Epic 1: Codegen for APPEND strategy (new rows only, date-filtered)
- Epic 2: Codegen for MERGE strategy (upsert on key)
- Epic 3: Executor support (detect existing table, apply strategy)

### M021: Window Function Helpers
Common dedup and ordering patterns deserve first-class helpers.
- `dedup_by(partition, order_by)` — ROW_NUMBER + filter pattern
- `first_value_by(column, partition, order_by)` / `last_value_by(...)` — aggregation without GROUP BY

### M022: Zeffy Attribution PoC
Hands-on proof of concept using the blueprint above.
- Epic 1: Model the pipeline (amplitude_event → account → acquisition_signal → attribution models)
- Epic 2: Run against sample Zeffy data on DuckDB
- Epic 3: Document learnings, surface gaps the spike missed
