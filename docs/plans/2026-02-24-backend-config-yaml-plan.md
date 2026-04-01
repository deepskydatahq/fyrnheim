# Implementation Plan: Support BigQuery connection params in fyrnheim.yaml

**Task:** typedata-jdn (M004-E002-S002)
**Status:** plan → ready

## Prerequisites

- None for implementation — config layer changes are self-contained
- Logically blocked by typedata-hto (connection factory), but no code dependency

## Step-by-step Changes

### Step 1: Add `backend_config` field to `ProjectConfig`

**File:** `src/fyrnheim/config.py`, line 25

Add after `backend: str`:
```python
backend_config: dict
```

### Step 2: Extract `backend_config` in `load_config()`

**File:** `src/fyrnheim/config.py`, lines 68–74

Replace the return block with extraction logic + return:

```python
    # Extract active backend's connection params
    backend_name = raw.get("backend", "duckdb")
    backends_raw = raw.get("backends")
    backend_config: dict = {}

    if backends_raw is not None:
        if not isinstance(backends_raw, dict):
            raise ConfigError(
                f"'backends' must be a mapping in {config_path}, "
                f"got {type(backends_raw).__name__}"
            )
        entry = backends_raw.get(backend_name)
        if entry is not None:
            if not isinstance(entry, dict):
                raise ConfigError(
                    f"'backends.{backend_name}' must be a mapping in {config_path}, "
                    f"got {type(entry).__name__}"
                )
            backend_config = entry

    return ProjectConfig(
        project_root=project_root,
        entities_dir=_resolve_dir(project_root, raw.get("entities_dir", "entities")),
        data_dir=_resolve_dir(project_root, raw.get("data_dir", "data")),
        output_dir=_resolve_dir(project_root, raw.get("output_dir", "generated")),
        backend=backend_name,
        backend_config=backend_config,
    )
```

| Scenario | Result |
|----------|--------|
| Missing `backends` key | `backend_config = {}` |
| `backends` present, no entry for active backend | `backend_config = {}` |
| `backends` is not a dict | `ConfigError` |
| `backends.bigquery` is not a dict | `ConfigError` |

### Step 3: Add `backend_config` field to `ResolvedConfig`

**File:** `src/fyrnheim/config.py`, line 84

Add after `backend: str`:
```python
backend_config: dict
```

### Step 4: Wire `backend_config` through `resolve_config()`

**File:** `src/fyrnheim/config.py`, line 101–107

Add to `return ResolvedConfig(...)`:
```python
backend_config=config.backend_config if config else {},
```

### Step 5: Add tests in `tests/test_config.py`

**In `TestLoadConfig`** — 5 new tests:
1. `test_backends_bigquery_parsed` — extracts project_id and dataset_id
2. `test_missing_backends_defaults_to_empty_dict` — no backends section
3. `test_backends_present_but_active_backend_absent` — active backend not in map
4. `test_backends_not_a_dict_raises_config_error` — validation
5. `test_backend_entry_not_a_dict_raises_config_error` — validation

**Update existing** `test_all_defaults` — add `assert cfg.backend_config == {}`

**In `TestResolveConfig`** — 2 new tests:
1. `test_backend_config_passed_through` — resolve_config wiring
2. `test_no_config_backend_config_empty` — no config file

## Files Modified

| File | Action | ~Lines Changed |
|------|--------|----------------|
| `src/fyrnheim/config.py` | Edit | +20 lines |
| `tests/test_config.py` | Edit | +50 lines |

## Verification

```bash
uv run pytest tests/test_config.py -v   # New tests pass
uv run pytest -x                          # Full suite regression
uv run ruff check src/fyrnheim/config.py  # Lint clean
uv run mypy src/fyrnheim/config.py        # Type check clean
```

## Risk Assessment

- **Very low risk**: Single source file changed, extraction logic is straightforward
- **No behavioral change for existing configs**: Missing `backends` defaults to `{}`
- **No dependency on other stories**: Config layer changes are self-contained
