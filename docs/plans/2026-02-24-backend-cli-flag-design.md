# Add --backend CLI Option to fyr run — Design

## Overview

Add a `--backend` CLI option to `fyr run` that overrides the backend specified in `fyrnheim.yaml`. Follow the existing pattern where CLI overrides flow through `resolve_config()`.

## Problem Statement

Users need to run transformations against different backends (e.g. BigQuery) without editing `fyrnheim.yaml` each time. A CLI flag provides quick, composable override — especially useful for testing against a different backend or running one-off commands.

## Expert Perspectives

### Technical
- **Approach A wins:** Add `backend: str | None = None` to `resolve_config()`, consistent with how `entities_dir`, `data_dir`, `output_dir` overrides already work. Single composable mental model: "CLI overrides flow through `resolve_config()`."
- **No validation at CLI layer:** Backend names are an open set. Validation belongs in the factory layer downstream. Keeps the CLI thin.
- **No output format change:** Existing `Running on {cfg.backend}` already communicates the backend name.

### Simplification Review
- Nothing to remove — design is minimal.
- Nothing to simplify — already follows established patterns.
- The decision to not validate backend names at CLI layer is correct separation of concerns.

## Proposed Solution

Three surgical changes following the existing override pattern:

### 1. `src/fyrnheim/config.py` — Add backend param to `resolve_config()`

```python
def resolve_config(
    ...,
    backend: str | None = None,
) -> FyrnheimConfig:
    ...
    if backend is not None:
        cfg.backend = backend
    return cfg
```

### 2. `src/fyrnheim/cli.py` — Add --backend Click option to `run` command

```python
@click.option("--backend", default=None, help="Override backend engine (e.g. duckdb, bigquery)")
def run_cmd(..., backend: str | None = None):
    cfg = resolve_config(..., backend=backend)
    # rest unchanged — cfg.backend already flows to engine_run()
```

### 3. `tests/test_cli_run.py` — Add tests

- `fyr run --backend bigquery` passes backend='bigquery' to runner
- `fyr run` without `--backend` uses config file default
- `--backend` appears in `fyr run --help` output
- Output includes the backend name when `--backend` is specified

## Components

| File | Change |
|------|--------|
| `src/fyrnheim/config.py` | Add `backend: str \| None = None` param to `resolve_config()`, apply override when not None |
| `src/fyrnheim/cli.py` | Add `--backend` Click option to `run` command, pass to `resolve_config()` |
| `tests/test_cli_run.py` | Add 3-4 tests covering the new option |

## Alternatives Considered

**Approach B: Override at CLI level** — Keep `resolve_config()` unchanged, do `effective_backend = backend or cfg.backend` in the run command itself. Rejected because it breaks the established pattern where all CLI overrides flow through `resolve_config()`.

## Risks / Notes

- **Dependency on M004-E002-S003 (runner uses factory):** The `--backend` CLI plumbing can be wired up and tested independently. Multi-backend execution only works once the factory story is complete.
- **No enum/validation of backend names at CLI level:** Intentional. Validation belongs in the factory layer.

## Success Criteria

- `fyr run --backend bigquery` passes `backend='bigquery'` to runner
- `fyr run` without `--backend` uses `fyrnheim.yaml` value (default: duckdb)
- Output displays active backend name
- `fyr run --help` shows `--backend` option
- All existing tests pass
