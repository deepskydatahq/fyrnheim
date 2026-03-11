# Zeffy Test Setup Design (M022)

## Goal

Create a local Fyrnheim implementation of the Zeffy marketing attribution pipeline, extracting data from Snowflake via dlt and modeling it as Fyrnheim entities running on DuckDB.

## Project Location

`propel/client-zeffy-fyrnheim/` — separate from both the Fyrnheim open-source repo and the existing dbt project.

## Structure

```
propel/client-zeffy-fyrnheim/
├── pyproject.toml              # deps: dlt[duckdb,filesystem,parquet,sql_database], fyrnheim
├── .env                        # Snowflake credentials (gitignored)
├── src/zeffy/pipelines/
│   ├── amplitude.py            # EVENTS_165674 (30 days), MERGE_IDS_165674
│   └── organizations.py        # sql_organization, sql_organization_qualification_questions
├── entities/                   # Fyrnheim entity definitions
├── data/                       # dlt parquet output (gitignored)
├── generated/                  # Fyrnheim codegen output (gitignored)
└── fyrnheim.yaml               # backend: duckdb, data_dir: ./data
```

## dlt Pipelines

### amplitude.py

- Source: Snowflake `database_production.amplitude` via `dlt.sources.sql_database`
- Tables: `EVENTS_165674` (last 30 days by `event_time`), `MERGE_IDS_165674` (full)
- Auth: Snowflake key-pair (unencrypted `.p8` key from client-zeffy)
- Destination: `dlt.destinations.filesystem(bucket_url="./data")`, parquet
- Write disposition: `replace`

### organizations.py

- Source: Snowflake `database_production.production_all` via `dlt.sources.sql_database`
- Tables: `sql_organization`, `sql_organization_qualification_questions`
- Filter: `__HEVO__MARKED_DELETED = false`
- Same destination pattern

## Fyrnheim Entities

### touchpoints

- Source: TableSource from `data/amplitude/events_165674/`
- PrepLayer:
  - JSON extraction: gclid, fbclid, utm_source, utm_medium, utm_campaign, org_id (from event_properties/user_properties)
  - Page intent classification (CaseColumn: acquisition / post_signup / utility)
  - Channel classification (chained CaseColumn: click_id > UTM > referrer > UA heuristics)
  - Filter to acquisition-relevant events (session_start, signup, survey_referral)

### account

- Source: IdentityGraphConfig
  - organizations: inline TableSource from `data/organizations/sql_organization/`, match on org_id
  - amplitude_events: resolved from touchpoints entity
  - amplitude_merges: inline TableSource from `data/amplitude/merge_ids_165674/`
- DimensionLayer: account_name, created_at, referral_source

### attribution

- Source: AggregationSource (from: touchpoints), grouped by account_id
- Computed columns:
  - first_touch_channel: first pre-signup channel by timestamp (first_value_by helper)
  - paid_priority_channel: cascading priority rules via CaseColumn (gclid → fbclid → utm paid → first_touch fallback)
  - first_touch_at, signup_at, days_to_signup

## What doesn't change

- No Fyrnheim framework changes needed — uses existing features (TableSource, IdentityGraphConfig, AggregationSource, CaseColumn, first_value_by, dedup_by)
- M020 incremental materialization and M021 window helpers are available from recent PRs
