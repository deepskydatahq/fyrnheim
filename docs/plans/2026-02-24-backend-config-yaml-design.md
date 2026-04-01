# Backend Config YAML Design

## Overview

Add a `backends` section to `fyrnheim.yaml` and extract the active backend's connection params into a `backend_config: dict` field on both `ProjectConfig` and `ResolvedConfig`. Extract at load time so downstream code gets immediately-usable values.

## Problem Statement

Fyrnheim needs to support per-backend connection parameters (e.g., BigQuery's `project_id` and `dataset_id`) in `fyrnheim.yaml`. The runner will call `create_connection(backend, **backend_config)`, so the config layer must provide a flat dict of the active backend's params.

## Expert Perspectives

### Technical
- Extract at load time (approach A) — `ProjectConfig` holds the active backend's sub-dict, not the full `backends` map. This makes the mental model clean: load config, get immediately-usable values, no surprise lookups downstream. Carrying the full map would be YAGNI. If cross-backend comparison is ever needed, re-read the YAML or add a separate method.
- Error messages should clearly indicate config typos (e.g., backend name mismatch), not generic "missing key" errors.

### Simplification Review
- Verdict: **APPROVED** — design is minimal.
- Extraction at load time is the only shape that makes sense for the downstream `**backend_config` unpacking pattern.
- "Don't validate schema" decision is correctly principled — each layer validates its own responsibility (config validates container is a dict, connection factory validates specific keys).

## Proposed Solution

### YAML Schema

```yaml
backend: bigquery
backends:
  bigquery:
    project_id: deepskydata
    dataset_id: timodata_model_dev
```

- `backend` selects the active backend (default: `"duckdb"`)
- `backends` is an optional mapping of backend name to connection params
- Only the active backend's params are extracted; the rest are ignored

### Changes to `src/fyrnheim/config.py`

1. **Add `backend_config: dict` to `ProjectConfig`** — default `{}`
2. **Extract in `load_config()`** — read `backends` mapping, look up `backends[active_backend]`, validate both are dicts, store as `backend_config`
3. **Add `backend_config: dict` to `ResolvedConfig`**
4. **Wire through in `resolve_config()`** — pass `backend_config` with `{}` default when no config file

### Key Behaviors

| Scenario | Result |
|----------|--------|
| Missing `backends` key | `backend_config = {}` |
| `backends` present, no entry for active backend | `backend_config = {}` |
| `backends` is not a dict | `ConfigError` |
| `backends.bigquery` is not a dict | `ConfigError` |

### Changes to `tests/test_config.py`

8 new test methods:
- `test_backends_bigquery_parsed` — extracts project_id and dataset_id
- `test_missing_backends_defaults_to_empty_dict` — no backends section
- `test_backends_present_but_active_backend_absent` — active backend not in map
- `test_backends_not_a_dict_raises_config_error` — validation
- `test_backend_entry_not_a_dict_raises_config_error` — validation
- `test_all_defaults_includes_empty_backend_config` — existing defaults test
- `test_backend_config_passed_through` — resolve_config wiring
- `test_no_config_backend_config_empty` — no config file

## Alternatives Considered

**B) Store full backends map, extract later:** ProjectConfig stores the full `backends` mapping, extraction happens in `resolve_config()` or downstream. Rejected because it carries unused data and defers extraction, violating YAGNI and making the downstream API less clean.

## Design Decisions

- No CLI override for `backend_config` (YAGNI — config file only)
- No validation of backend-specific keys (belongs in connection factory layer)
- No storage of full `backends` map (extract at load time)

## Success Criteria

- All 4 acceptance criteria met
- Existing ~596 tests continue to pass
- New tests cover parsing, defaults, and error cases
