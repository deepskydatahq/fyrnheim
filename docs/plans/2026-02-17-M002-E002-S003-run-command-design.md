# Run Command Design (M002-E002-S003)

## Overview
Replace the `fyr run` stub in `cli.py` with the most complex CLI command: a full pipeline executor that calls `fyrnheim.engine.runner.run()` (or `run_entity()` for single-entity mode), prints structured per-entity progress with row counts, timing, and status, and exits with distinct codes for success, runtime errors, and quality failures.

## Problem Statement
Users need a single command to execute the entire fyrnheim pipeline -- discover entities, generate transforms, execute on DuckDB, run quality checks. The output must be readable at a glance: which entities ran, how many rows, how long, did quality pass. This is the command users run most often.

## Dependencies
- M002-E001-S001 (CLI skeleton): provides `cli.py` with Click group and `run` stub
- M002-E001-S002 (config loading): provides `load_config()` and `ProjectConfig`
- M001-E004-S001 (entity discovery): provides `EntityRegistry`, `EntityInfo`
- M001-E004-S003 (run function): provides `run()`, `run_entity()`, `RunResult`, `EntityRunResult`

## Output Format Design

This is the most important part of this story. The output must be scannable in a terminal without being noisy. Three phases: header, per-entity table, summary.

### Full Pipeline Output

```
Discovering entities... 3 found
Running on duckdb

  customers      prep -> dim       12 rows    0.3s  ok
  orders         prep -> snapshot  847 rows   1.2s  ok
  products       prep -> dim        0 rows    0.1s  ERROR

Done: 2 success, 1 error (1.6s)
```

### Single Entity Output (`--entity`)

```
Running customers on duckdb

  customers      prep -> dim       12 rows    0.3s  ok

Done (0.3s)
```

### With Quality Check Failures

```
Discovering entities... 2 found
Running on duckdb

  customers      prep -> dim       12 rows    0.3s  ok
    checks: 3 passed, 1 failed
      unique(email_hash)                          FAIL (2 failures)
  orders         prep -> snapshot  847 rows   1.2s  ok

Done: 2 success, 0 errors (1.5s)
Quality: 1 check failed
```

### Error Cases

Missing entities directory:
```
Error: Entities directory not found: entities/
```
Exit code 1.

No entities found:
```
Discovering entities... 0 found
Nothing to run.
```
Exit code 0.

Unknown `--entity` name:
```
Discovering entities... 3 found
Error: Entity 'bogus' not found. Available: customers, orders, products
```
Exit code 1.

### Format Decisions

1. **Layer chain display**: Use short layer names joined with ` -> `. Map from `EntityInfo.layers` using: `prep` -> `prep`, `dimension` -> `dim`, `snapshot` -> `snapshot`, `activity` -> `activity`, `analytics` -> `analytics`. This matches the generated function naming convention.

2. **Column alignment**: Use fixed-width columns with `f-string` formatting:
   - Name: `{name:<16s}` (left-aligned, 16 chars)
   - Layers: `{layers:<18s}` (left-aligned, 18 chars)
   - Rows: `{rows:>6s}` (right-aligned, 6 chars) + ` rows`
   - Duration: `{duration:>6s}` (right-aligned)
   - Status: `ok`, `ERROR`, `skipped`

3. **Two-space indent** for entity rows (consistent with `fyr list`).

4. **Quality details indented under entity** at 4-space indent. Only shown when a quality check fails. Passing quality checks are silent (keep output clean).

5. **Status markers**: `ok` (lowercase, calm), `ERROR` (uppercase, urgent), `skipped` (lowercase, informational). No emoji.

6. **Timing**: Show one decimal place for per-entity, one decimal for total. Use `s` suffix.

## Exit Code Logic

Three distinct exit codes encode the severity of what happened:

| Code | Meaning | Condition |
|------|---------|-----------|
| 0 | Success | `RunResult.ok is True` AND no quality failures |
| 1 | Runtime error | Any entity has `status == "error"` |
| 2 | Quality failure | All entities executed OK, but at least one quality check has `passed == False` |

Precedence: runtime errors (1) take priority over quality failures (2). If both runtime errors and quality failures exist, exit with 1.

Implementation:

```python
def _compute_exit_code(result: RunResult) -> int:
    if result.error_count > 0:
        return 1
    if _has_quality_failures(result):
        return 2
    return 0

def _has_quality_failures(result: RunResult) -> bool:
    for er in result.entities:
        if er.quality_results:
            for qr in er.quality_results:
                if not qr.passed:
                    return True
    return False
```

## Command Implementation

### Click Signature

```python
@main.command()
@click.option("--entity", default=None, help="Run a single entity by name.")
@click.option("--entities-dir", type=click.Path(), default=None,
              help="Override entities directory.")
@click.option("--data-dir", type=click.Path(), default=None,
              help="Override data directory.")
@click.option("--output-dir", type=click.Path(), default=None,
              help="Override generated output directory.")
def run(entity: str | None, entities_dir: str | None,
        data_dir: str | None, output_dir: str | None) -> None:
    """Execute the pipeline: discover, generate, transform, check."""
```

Note: The Click command is named `run` but calls `fyrnheim.engine.runner.run` internally. Import the runner function with an alias to avoid shadowing: `from fyrnheim.engine.runner import run as engine_run`.

### Config Resolution

Config and flag merging follows the pattern established in the list command design:

```python
config = load_config()

ent_dir = Path(entities_dir) if entities_dir else (config.entities_dir if config else Path("entities"))
dat_dir = Path(data_dir) if data_dir else (config.data_dir if config else Path("data"))
out_dir = Path(output_dir) if output_dir else (config.output_dir if config else Path("generated"))
backend = config.backend if config else "duckdb"
```

Flags always win. Config is next. Hardcoded defaults are last resort.

### Full Pipeline Path

```python
# 1. Validate
if not ent_dir.exists():
    click.echo(f"Error: Entities directory not found: {ent_dir}", err=True)
    raise SystemExit(1)

# 2. Discover (for layer info display)
registry = EntityRegistry()
registry.discover(ent_dir)
click.echo(f"Discovering entities... {len(registry)} found")

if len(registry) == 0:
    click.echo("Nothing to run.")
    return

click.echo(f"Running on {backend}")
click.echo()

# 3. Run the pipeline
result = engine_run(
    ent_dir, dat_dir,
    backend=backend,
    generated_dir=out_dir,
)

# 4. Print per-entity results
for er in result.entities:
    _print_entity_result(er, registry)

# 5. Print summary
click.echo()
_print_summary(result)

# 6. Exit
raise SystemExit(_compute_exit_code(result))
```

### Single Entity Path (`--entity`)

When `--entity` is provided, the flow changes:

```python
# 1. Discover to find the entity + validate name
registry = EntityRegistry()
registry.discover(ent_dir)
click.echo(f"Discovering entities... {len(registry)} found")

info = registry.get(entity)
if info is None:
    available = ", ".join(sorted(registry))
    click.echo(f"Error: Entity '{entity}' not found. Available: {available}", err=True)
    raise SystemExit(1)

click.echo(f"Running {entity} on {backend}")
click.echo()

# 2. Run single entity
from fyrnheim.engine.runner import run_entity as engine_run_entity

er = engine_run_entity(
    info.entity, dat_dir,
    backend=backend,
    generated_dir=out_dir,
)

# 3. Print result
_print_entity_result(er, registry)

# 4. Print summary
click.echo()
click.echo(f"Done ({er.duration_seconds:.1f}s)")

# 5. Exit
if er.status == "error":
    raise SystemExit(1)
if er.quality_results and any(not qr.passed for qr in er.quality_results):
    raise SystemExit(2)
```

### Output Formatting Helpers

```python
_LAYER_SHORT_NAMES = {
    "prep": "prep",
    "dimension": "dim",
    "snapshot": "snapshot",
    "activity": "activity",
    "analytics": "analytics",
}

def _format_layers(info: EntityInfo) -> str:
    """Format layer chain as 'prep -> dim'."""
    short = [_LAYER_SHORT_NAMES.get(l, l) for l in info.layers]
    return " -> ".join(short)

def _print_entity_result(er: EntityRunResult, registry: EntityRegistry) -> None:
    """Print a single entity result line."""
    info = registry.get(er.entity_name)
    layers = _format_layers(info) if info else ""
    rows = f"{er.row_count}" if er.row_count is not None else "-"
    status = "ok" if er.status == "success" else er.status.upper()

    click.echo(
        f"  {er.entity_name:<16s} {layers:<18s} {rows:>6s} rows  {er.duration_seconds:5.1f}s  {status}"
    )

    # Quality failures (only show when checks fail)
    if er.quality_results:
        passed = sum(1 for qr in er.quality_results if qr.passed)
        failed = sum(1 for qr in er.quality_results if not qr.passed)
        if failed > 0:
            click.echo(f"    checks: {passed} passed, {failed} failed")
            for qr in er.quality_results:
                if not qr.passed:
                    click.echo(f"      {qr.check_name:<42s} FAIL ({qr.failure_count} failures)")

def _print_summary(result: RunResult) -> None:
    """Print the pipeline summary line."""
    parts = [f"{result.success_count} success"]
    if result.error_count > 0:
        parts.append(f"{result.error_count} {'error' if result.error_count == 1 else 'errors'}")
    else:
        parts.append("0 errors")
    if result.skipped_count > 0:
        parts.append(f"{result.skipped_count} skipped")

    click.echo(f"Done: {', '.join(parts)} ({result.total_duration_seconds:.1f}s)")

    # Quality summary (only if any failures)
    if _has_quality_failures(result):
        total_failed = 0
        for er in result.entities:
            if er.quality_results:
                total_failed += sum(1 for qr in er.quality_results if not qr.passed)
        check_word = "check" if total_failed == 1 else "checks"
        click.echo(f"Quality: {total_failed} {check_word} failed")
```

## Key Decisions

1. **Discover twice for layer info**: The `run()` function in `runner.py` already discovers entities internally. But the CLI also needs `EntityInfo.layers` for display formatting. Rather than modifying `run()` to return layer info (coupling the library API to CLI concerns), the CLI discovers entities separately for display purposes. This costs one extra directory scan (negligible) and keeps the library API clean.

2. **No progress indicators**: For v0.1, entity results print as they complete. No progress bars, no spinners. The `run()` function blocks until complete, so results appear after the full pipeline finishes. A future enhancement could add a callback protocol for real-time progress, but that is out of scope.

3. **Quality details only on failure**: Passing quality checks produce no output. This keeps the happy path clean. Users who want to see all checks can use `fyr check` (S004).

4. **Error display**: Entity errors show `ERROR` status in the table row. The full error message is not shown inline (it would break alignment). If users need error details, they can increase log verbosity or check the returned `EntityRunResult.error` programmatically. The summary line's exit code signals whether errors occurred.

5. **`SystemExit` vs `sys.exit()`**: Use `raise SystemExit(code)` as Click's CliRunner catches it cleanly for testing. Calling `sys.exit()` also works but `SystemExit` is more explicit.

6. **Import aliasing**: `from fyrnheim.engine.runner import run as engine_run` avoids shadowing the Click command function named `run`.

## Error Handling

| Scenario | Behavior | Exit Code |
|----------|----------|-----------|
| `entities_dir` does not exist | Print error to stderr, exit | 1 |
| `--entity` name not found | Print error with available names, exit | 1 |
| `load_config()` raises `ConfigError` | Print error, exit | 1 |
| Entity runtime error (transform fails) | Print `ERROR` status, continue | 1 |
| Quality check fails (data issue) | Print failure details under entity | 2 |
| Quality runner crashes (code bug) | Quality results are None, status stays `ok` | 0 |
| `run()` raises (e.g., circular dep) | Catch, print error, exit | 1 |
| Zero entities found | Print "Nothing to run.", exit | 0 |

## Tests

**File:** `tests/test_cli_run.py` (~200 lines)

All tests use `click.testing.CliRunner`. Tests that need the real pipeline use `tmp_path` with a minimal entity and parquet file. Tests that focus on output formatting mock the runner functions.

### Minimal entity fixture

```python
MINIMAL_ENTITY = '''\
from fyrnheim import Entity, LayersConfig, PrepLayer, TableSource

entity = Entity(
    name="{name}",
    description="Test entity",
    source=TableSource(project="test", dataset="raw", table="{name}", duckdb_path="{name}.parquet"),
    layers=LayersConfig(prep=PrepLayer(model_name="prep_{name}")),
)
'''
```

### Test Cases

| Test | Description | Verifies |
|------|-------------|----------|
| `test_run_full_pipeline` | Create entity + parquet in tmp, write fyrnheim.yaml, invoke `fyr run`. Assert exit 0, output contains entity name, row count, "ok", "Done" summary. | AC: calls run(), per-entity output |
| `test_run_discovery_count` | Mock `run()`. Assert output contains "Discovering entities... N found". | AC: discovery count |
| `test_run_shows_backend` | Mock `run()`. Assert output contains "Running on duckdb". | AC: backend display |
| `test_run_per_entity_format` | Mock `run()` to return 2 entities. Assert each entity line has name, layer chain, row count, timing, status. | AC: per-entity format |
| `test_run_layer_chain_display` | Entity with prep + dimension shows "prep -> dim". | AC: layer chain |
| `test_run_summary_line` | Mock `run()` with 2 success, 1 error. Assert "Done: 2 success, 1 error (X.Xs)". | AC: summary line |
| `test_run_single_entity` | Use `--entity customers`. Mock `run_entity()`. Assert it was called (not `run()`). Assert output shows single entity result. | AC: --entity flag |
| `test_run_unknown_entity` | Use `--entity bogus`. Assert error message lists available entities, exit code 1. | AC: unknown entity |
| `test_run_reads_config` | Write fyrnheim.yaml with custom paths. Assert those paths are used (mock run to capture args). | AC: config loading |
| `test_run_flag_overrides_config` | Write fyrnheim.yaml, pass --entities-dir flag. Assert flag wins. | AC: flag overrides |
| `test_run_exit_code_0_on_success` | Mock `run()` returning all success. Assert exit code 0. | AC: exit codes |
| `test_run_exit_code_1_on_error` | Mock `run()` returning an entity with status="error". Assert exit code 1. | AC: exit codes |
| `test_run_exit_code_2_on_quality_failure` | Mock `run()` returning success entities with failing quality checks. Assert exit code 2. | AC: exit codes |
| `test_run_exit_code_1_overrides_2` | Mock `run()` with both errors and quality failures. Assert exit code 1 (errors take priority). | AC: exit codes |
| `test_run_quality_failure_output` | Mock `run()` with quality failures. Assert output shows check names and failure counts. | AC: quality display |
| `test_run_no_entities` | Empty entities dir. Assert "Nothing to run.", exit 0. | Edge case |
| `test_run_missing_entities_dir` | No --entities-dir, no config, no "entities/" dir. Assert error, exit 1. | Edge case |

### Mocking Strategy

For unit tests of output formatting and exit codes, mock at the runner boundary:

```python
@patch("fyrnheim.cli.engine_run")
def test_run_exit_code_1_on_error(mock_run, tmp_path):
    # Setup config or --entities-dir so discovery works
    mock_run.return_value = RunResult(
        entities=[
            EntityRunResult(entity_name="bad", status="error", error="boom"),
        ],
        total_duration_seconds=0.1,
        backend="duckdb",
    )
    runner = CliRunner()
    result = runner.invoke(main, ["run", "--entities-dir", str(tmp_path)])
    assert result.exit_code == 1
```

For integration tests, use real entities + parquet data (similar to the E004-S005 e2e tests).

## Lines of Code Estimate

| File | Estimated Lines |
|------|----------------|
| `cli.py` changes (replace stub, add helpers) | ~100 lines |
| `tests/test_cli_run.py` | ~200 lines |
| **Total** | **~300 lines** |

## Success Criteria
- `fyr run` executes full pipeline, prints per-entity status table with row counts and timing
- `fyr run --entity customers` runs single entity
- Discovery count and backend shown in header
- Summary line with counts and total time
- Exit code 0/1/2 for success/error/quality failure
- Config from fyrnheim.yaml, overridable via flags
- All tests pass via CliRunner
