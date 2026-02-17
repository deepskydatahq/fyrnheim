# Config Loading Design (M002-E001-S002)

## Overview
New `src/fyrnheim/config.py` module with `ProjectConfig` dataclass and `load_config()` function that finds and parses `fyrnheim.yaml`, resolving paths relative to config location.

## Problem Statement
CLI commands need project config (entities_dir, data_dir, output_dir, backend). Config should be loaded from `fyrnheim.yaml` with walk-up directory search, defaults for missing keys, and explicit error handling for malformed files.

## Expert Perspectives

### Technical
- Config module has single responsibility: find and parse. No CLI awareness.
- CLI layer owns override merging (flags take precedence over config).
- Use `from fyrnheim import __version__` pattern — simple, unified.

### Simplification Review
- Verdict: SIMPLIFY (minor) — make `find_config` non-private, clarify path resolution, be explicit about unknown keys being ignored.

## Proposed Solution

**`src/fyrnheim/config.py`** (~60 lines):
- `ProjectConfig` frozen dataclass: `project_root`, `entities_dir`, `data_dir`, `output_dir`, `backend`
- `ConfigError` exception for malformed YAML
- `load_config(start_dir) -> ProjectConfig | None` — returns None if no file, raises ConfigError if malformed
- All paths resolved as absolute relative to config file location
- Unknown YAML keys silently ignored (forward-compatible)
- Defaults: entities/, data/, generated/, duckdb

**CLI override merging** lives in `cli.py`, not config module.

**`tests/test_config.py`** (~10 tests):
- Find in cwd, walk-up parents, returns None when missing
- All defaults applied, all keys specified
- Paths resolved relative to config location (not cwd)
- Malformed YAML raises ConfigError
- Non-dict YAML raises ConfigError
- Empty config file uses defaults
- project_root equals config file's parent dir

## Key Decisions
- `load_config()` returns None for missing config, raises ConfigError for malformed
- Config module has zero knowledge of CLI flags
- `find_config()` kept as module-level function (not private) for testability
- Frozen dataclass — immutable after creation
- Unknown YAML keys ignored

## Success Criteria
- load_config() finds and parses fyrnheim.yaml correctly
- Walk-up directory search works
- Missing keys get sensible defaults
- All paths absolute, resolved relative to config location
- Malformed YAML produces clear error
