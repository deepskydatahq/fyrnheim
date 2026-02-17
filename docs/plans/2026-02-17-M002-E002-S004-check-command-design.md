# Check Command Design (M002-E002-S004)

## Overview
Replace the `fyr check` stub in `cli.py` with a standalone quality-check runner that connects to DuckDB, discovers entities, and runs `QualityRunner.run_entity_checks()` against previously-persisted tables -- without re-executing the pipeline.

## Problem Statement
After running `fyr run`, users need a fast way to re-validate quality checks without re-executing transformations. This is useful when debugging check definitions, iterating on quality rules, or running checks as a CI gate against a persistent DuckDB file.

## Dependencies
- M002-E001-S001 (CLI skeleton): provides `cli.py` with Click group and `check` stub
- M002-E001-S002 (config loading): provides `load_config()` and `ProjectConfig`
- M001-E004-S001 (entity discovery): provides `EntityRegistry` and `EntityInfo`
- M001-E001-S005 (quality framework): provides `QualityRunner`, `QualityConfig`, `CheckResult`, `EntityResult`

## Proposed Solution

### Command Signature

```python
@main.command()
@click.option("--entity", default=None, help="Check a single entity by name.")
@click.option("--entities-dir", type=click.Path(), default=None,
              help="Override entities directory.")
@click.option("--data-dir", type=click.Path(), default=None,
              help="Override data directory.")
@click.option("--output-dir", type=click.Path(), default=None,
              help="Override generated output directory (locates DuckDB state).")
@click.option("--db-path", type=click.Path(), default=None,
              help="Override DuckDB database path.")
def check(entity: str | None, entities_dir: str | None, data_dir: str | None,
          output_dir: str | None, db_path: str | None) -> None:
    """Run quality checks against previously-executed entities."""
```

### Core Logic

The command does four things in sequence: resolve config, discover entities, connect to DuckDB, and run checks.

```python
def check(entity, entities_dir, data_dir, output_dir, db_path):
    # 1. Resolve config
    config = load_config()
    entities_dir = Path(entities_dir or (config.entities_dir if config else "entities"))
    output_dir = Path(output_dir or (config.output_dir if config else "generated"))
    db_path = db_path or ":memory:"  # See "DuckDB Connection" section

    # 2. Discover entities
    registry = EntityRegistry()
    try:
        registry.discover(entities_dir)
    except FileNotFoundError:
        click.echo(f"Error: Entities directory not found: {entities_dir}", err=True)
        raise SystemExit(1)

    # 3. Filter to target entity (if --entity flag)
    if entity:
        info = registry.get(entity)
        if info is None:
            click.echo(f"Error: Entity '{entity}' not found.", err=True)
            raise SystemExit(1)
        targets = [(entity, info)]
    else:
        targets = list(registry.items())

    # 4. Connect + run checks
    executor = DuckDBExecutor(db_path=db_path, generated_dir=output_dir)
    try:
        total_pass, total_fail, total_error = _run_all_checks(executor, targets)
    finally:
        executor.close()

    # 5. Summary + exit code
    _print_summary(total_pass, total_fail, total_error, entity_count)
    if total_error > 0:
        raise SystemExit(1)
    if total_fail > 0:
        raise SystemExit(2)
```

### DuckDB Connection Strategy

The `fyr check` command runs against **already-persisted** tables. This means:

1. **File-based DuckDB (primary use case)**: If the user ran `fyr run` with a persistent DuckDB file (e.g., via config or `--db-path`), `fyr check` connects to that same file and finds the `dim_*` / `snapshot_*` tables.

2. **In-memory fallback**: If no `db_path` is configured and the user has not specified `--db-path`, the connection is in-memory and all tables will be missing. The command handles this gracefully by printing "table not found" per entity and exiting with code 1.

**Config resolution for db_path**: The `ProjectConfig` from M002-E001-S002 does not currently include a `db_path` field. Two options:
- **Option A (recommended)**: Add an optional `db_path` field to `ProjectConfig` / `fyrnheim.yaml` (default: `None`, meaning in-memory). The `fyr run` command can also write to this path.
- **Option B**: Convention-based: look for `{output_dir}/fyrnheim.duckdb` as the default persistent path.

For this story, we use **Option A** with a fallback. If `db_path` is not in config and not passed as a flag, we try `{project_root}/fyrnheim.duckdb` as a convention, and fall back to `:memory:` if that file does not exist.

### Target Table Resolution

The table name for quality checks depends on the entity's layer configuration. The check command must resolve which table to check against:

```python
def _resolve_target_table(entity_name: str, info: EntityInfo) -> str:
    """Determine the DuckDB table name to run checks against.

    Priority: snapshot > dimension > prep (the last/outermost layer).
    """
    if "snapshot" in info.layers:
        return f"snapshot_{entity_name}"
    if "dimension" in info.layers:
        return f"dim_{entity_name}"
    if "prep" in info.layers:
        return f"prep_{entity_name}"
    return f"dim_{entity_name}"  # fallback
```

This matches the `run_entity()` convention where the executor persists to `dim_{name}` by default (the last function output in the pipeline).

### Handling Missing Tables

When an entity's target table does not exist in DuckDB (entity was never run, or was run in-memory and results are gone), the `QualityRunner.run_check()` will raise an exception from DuckDB. We catch this at the entity level:

```python
def _run_all_checks(executor, targets):
    qr = QualityRunner(executor.connection)
    total_pass = total_fail = total_error = 0
    entities_checked = 0

    for name, info in targets:
        entity = info.entity

        # Skip entities without quality config
        if not entity.quality or not entity.quality.checks:
            click.echo(f"\n{name}: skipped (no quality checks defined)")
            continue

        table_name = _resolve_target_table(name, info)

        # Check if table exists
        try:
            existing_tables = executor.connection.list_tables()
        except Exception:
            existing_tables = []

        if table_name not in existing_tables:
            click.echo(f"\n{name}: error (table '{table_name}' not found -- run 'fyr run' first)")
            total_error += 1
            continue

        # Run checks
        try:
            result = qr.run_entity_checks(
                entity_name=name,
                quality_config=entity.quality,
                primary_key=entity.quality.primary_key,
                table_name=table_name,
            )
        except Exception as e:
            click.echo(f"\n{name}: error ({e})")
            total_error += 1
            continue

        # Print per-check results
        entities_checked += 1
        click.echo(f"\n{name}:")
        for cr in result.results:
            if cr.passed:
                status = "pass"
            elif cr.error:
                status = f"ERROR ({cr.error})"
                total_error += 1
            else:
                status = f"FAIL ({cr.failure_count} failures)"
            click.echo(f"  {cr.check_name:<40s} {status}")

            if cr.passed:
                total_pass += 1
            else:
                total_fail += 1

    return total_pass, total_fail, total_error
```

### Output Format

Normal run (all pass):
```
customers:
  NotNull: email                           pass
  NotNull: id                              pass
  Unique: email_hash                       pass
  InRange: amount_cents >= 0               pass

Checks: 4 passed, 0 failed across 1 entity
```

Mixed results:
```
customers:
  NotNull: email                           pass
  Unique: email_hash                       FAIL (3 failures)
  InRange: amount_cents >= 0               pass

orders:
  NotNull: order_id                        pass

Checks: 3 passed, 1 failed across 2 entities
```

Missing table:
```
customers: error (table 'dim_customers' not found -- run 'fyr run' first)

Checks: 0 passed, 0 failed across 0 entities (1 error)
```

No quality config:
```
customers: skipped (no quality checks defined)

Checks: 0 passed, 0 failed across 0 entities
```

### Summary Line

```python
def _print_summary(total_pass, total_fail, total_error, entities_checked):
    parts = [f"{total_pass} passed", f"{total_fail} failed"]
    msg = f"\nChecks: {', '.join(parts)} across {entities_checked} {'entity' if entities_checked == 1 else 'entities'}"
    if total_error > 0:
        msg += f" ({total_error} {'error' if total_error == 1 else 'errors'})"
    click.echo(msg)
```

### Exit Codes

| Code | Condition | Description |
|------|-----------|-------------|
| 0 | All checks pass, no errors | Success |
| 1 | Runtime errors (missing table, connection failure, import error) | Runtime error |
| 2 | At least one check failed (but no runtime errors) | Check failure |

Priority: runtime errors (1) take precedence over check failures (2). If there are both runtime errors and check failures, exit with 1.

```python
if total_error > 0:
    raise SystemExit(1)
if total_fail > 0:
    raise SystemExit(2)
# else: implicit exit 0
```

### Key Decisions

1. **No pipeline re-execution**: `fyr check` is purely read-only against existing DuckDB state. This is the key differentiator from `fyr run --quality-only` (which does not exist). If tables are missing, the user is told to run `fyr run` first.

2. **Table existence check before running queries**: We call `executor.connection.list_tables()` to verify the target table exists before passing it to `QualityRunner`. This produces a clean error message instead of a cryptic DuckDB/Ibis traceback.

3. **ForeignKey checks may reference other tables**: `ForeignKey` checks reference other entity tables (e.g., `dim_orders` referencing `dim_customers`). If the referenced table is also missing, `QualityRunner.run_check()` returns a `CheckResult` with `error` set. We display this as `ERROR (...)` and count it toward `total_error`.

4. **Check results with `error` field**: `CheckResult` has an `error: str | None` field. When set, the check encountered a runtime error (not a data quality failure). We distinguish these in output: `FAIL` for data issues, `ERROR` for runtime issues.

5. **Entity name filter**: `--entity` performs exact match against `EntityRegistry`. Typos produce a clear "not found" message with exit code 1.

6. **No `--verbose` flag in this story**: Sample failure rows from `CheckResult.sample_failures` are not printed. A `--verbose` or `--show-failures` flag can be added in a later story (M002-E003 error handling epic).

### Implementation in cli.py

Changes to `src/fyrnheim/cli.py` (~60 lines of logic, replacing the stub):

```python
def _resolve_target_table(entity_name: str, info: EntityInfo) -> str:
    if "snapshot" in info.layers:
        return f"snapshot_{entity_name}"
    if "dimension" in info.layers:
        return f"dim_{entity_name}"
    if "prep" in info.layers:
        return f"prep_{entity_name}"
    return f"dim_{entity_name}"


@main.command()
@click.option("--entity", default=None, help="Check a single entity by name.")
@click.option("--entities-dir", type=click.Path(), default=None)
@click.option("--data-dir", type=click.Path(), default=None)
@click.option("--output-dir", type=click.Path(), default=None)
@click.option("--db-path", type=click.Path(), default=None)
def check(entity, entities_dir, data_dir, output_dir, db_path):
    """Run quality checks against previously-executed entities."""
    config = load_config()

    entities_path = Path(entities_dir or (config.entities_dir if config else "entities"))
    output_path = Path(output_dir or (config.output_dir if config else "generated"))

    # Resolve db_path: flag > config > convention > memory
    if db_path is None:
        if config and hasattr(config, "db_path") and config.db_path:
            db_path = str(config.db_path)
        else:
            convention_path = Path(config.project_root if config else ".") / "fyrnheim.duckdb"
            db_path = str(convention_path) if convention_path.exists() else ":memory:"

    # Discover
    registry = EntityRegistry()
    try:
        registry.discover(entities_path)
    except FileNotFoundError:
        click.echo(f"Error: Entities directory not found: {entities_path}", err=True)
        raise SystemExit(1)

    # Filter
    if entity:
        info = registry.get(entity)
        if info is None:
            click.echo(f"Error: Entity '{entity}' not found.", err=True)
            raise SystemExit(1)
        targets = [(entity, info)]
    else:
        targets = list(registry.items())

    if not targets:
        click.echo("No entities found.")
        return

    # Run checks
    executor = DuckDBExecutor(db_path=db_path, generated_dir=output_path)
    try:
        total_pass = total_fail = total_error = 0
        entities_checked = 0
        qr = QualityRunner(executor.connection)

        existing_tables = []
        try:
            existing_tables = executor.connection.list_tables()
        except Exception:
            pass

        for name, info in targets:
            e = info.entity
            if not e.quality or not e.quality.checks:
                click.echo(f"\n{name}: skipped (no quality checks defined)")
                continue

            table_name = _resolve_target_table(name, info)
            if table_name not in existing_tables:
                click.echo(f"\n{name}: error (table '{table_name}' not found -- run 'fyr run' first)")
                total_error += 1
                continue

            try:
                result = qr.run_entity_checks(
                    entity_name=name,
                    quality_config=e.quality,
                    primary_key=e.quality.primary_key,
                    table_name=table_name,
                )
            except Exception as exc:
                click.echo(f"\n{name}: error ({exc})")
                total_error += 1
                continue

            entities_checked += 1
            click.echo(f"\n{name}:")
            for cr in result.results:
                if cr.passed:
                    click.echo(f"  {cr.check_name:<40s} pass")
                    total_pass += 1
                elif cr.error:
                    click.echo(f"  {cr.check_name:<40s} ERROR ({cr.error})")
                    total_error += 1
                else:
                    click.echo(f"  {cr.check_name:<40s} FAIL ({cr.failure_count} failures)")
                    total_fail += 1

        # Summary
        entity_word = "entity" if entities_checked == 1 else "entities"
        summary = f"\nChecks: {total_pass} passed, {total_fail} failed across {entities_checked} {entity_word}"
        if total_error > 0:
            error_word = "error" if total_error == 1 else "errors"
            summary += f" ({total_error} {error_word})"
        click.echo(summary)

    finally:
        executor.close()

    # Exit codes
    if total_error > 0:
        raise SystemExit(1)
    if total_fail > 0:
        raise SystemExit(2)
```

### Tests

**File:** `tests/test_cli_check.py` (~120 lines)

All tests use `click.testing.CliRunner` and `tmp_path` for isolation.

#### Fixtures

A helper that creates a minimal entity with quality checks and a pre-populated DuckDB file:

```python
def _setup_check_env(tmp_path, *, with_quality=True, with_table=True):
    """Create entities dir, fyrnheim.yaml, and optionally a DuckDB with data."""
    entities_dir = tmp_path / "entities"
    entities_dir.mkdir()

    entity_code = ENTITY_WITH_QUALITY if with_quality else ENTITY_WITHOUT_QUALITY
    (entities_dir / "customers.py").write_text(entity_code)

    db_path = tmp_path / "fyrnheim.duckdb"
    if with_table:
        import ibis
        conn = ibis.duckdb.connect(str(db_path))
        # Create dim_customers with sample data
        conn.raw_sql("""
            CREATE TABLE dim_customers AS
            SELECT 1 as id, 'alice@example.com' as email,
                   'abc123' as email_hash, 500 as amount_cents
            UNION ALL
            SELECT 2, 'bob@example.com', 'def456', 1000
        """)
        conn.disconnect()

    config_yaml = f"entities_dir: entities\noutput_dir: generated\ndb_path: {db_path}\n"
    (tmp_path / "fyrnheim.yaml").write_text(config_yaml)

    return tmp_path
```

#### Test Cases

| Test | Description | Exit Code |
|------|-------------|-----------|
| `test_check_all_pass` | Pre-populate DuckDB with valid data, all checks pass | 0 |
| `test_check_failures` | Pre-populate with data that violates NotNull (null email) | 2 |
| `test_check_entity_flag` | `--entity customers` runs only that entity | 0 |
| `test_check_entity_not_found` | `--entity nonexistent` prints error | 1 |
| `test_check_missing_table` | No DuckDB table for entity, prints "run 'fyr run' first" | 1 |
| `test_check_no_quality_config` | Entity without quality config prints "skipped" | 0 |
| `test_check_missing_entities_dir` | `--entities-dir /nonexistent` prints error | 1 |
| `test_check_summary_line` | Output contains "Checks: N passed, M failed across K entities" | -- |
| `test_check_per_check_output` | Each check name appears in output with pass/FAIL status | -- |
| `test_check_entities_dir_override` | `--entities-dir` overrides config value | 0 |
| `test_check_db_path_override` | `--db-path` overrides config value | 0 |
| `test_check_mixed_entities` | Two entities: one passes, one has missing table. Exit code 1. | 1 |

## Lines of Code Estimate
- `cli.py` changes: ~70 lines (replacing stub + helper function)
- `tests/test_cli_check.py`: ~120 lines
- Total: ~190 lines

## What This Does NOT Include
- **Verbose failure output**: Sample failure rows are not printed. Deferred to a `--verbose` / `--show-failures` flag in M002-E003.
- **JSON output**: No `--json` flag. Can be added later.
- **Parallel check execution**: Checks run sequentially. Not a bottleneck for typical entity counts.
- **BigQuery support**: Only DuckDB connection. The `QualityRunner` is backend-agnostic, but the CLI only wires up DuckDB for now.

## Success Criteria
- `fyr check` discovers entities and runs quality checks against existing DuckDB tables
- Per-check output with check name and pass/FAIL status
- Per-entity header and skipped/error messages for edge cases
- Summary line with pass/fail counts and entity count
- `--entity` flag filters to a single entity
- Missing tables produce clear "run 'fyr run' first" message
- Exit code 0 (all pass), 1 (runtime errors), 2 (check failures)
- Config loaded from `fyrnheim.yaml`, overridable via CLI flags
- All tests pass via CliRunner
