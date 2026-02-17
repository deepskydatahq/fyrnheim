# Generate Command Design (M002-E002-S002)

## Overview
Replace the `fyr generate` stub in `src/fyrnheim/cli.py` with a real implementation that discovers entities via `EntityRegistry`, calls `generate()` for each, and prints per-entity status with a summary line. Supports `--dry-run`, `--entities-dir`, and `--output-dir` flags.

## Problem Statement
Users can call `fyrnheim.generate()` from Python, but there is no CLI way to generate transform modules. The `fyr generate` command wraps the existing `generate()` function and `EntityRegistry.discover()` into a single terminal-friendly command with clear output and proper exit codes.

## Dependencies
- **M002-E001-S001** (CLI skeleton): provides `cli.py` with Click group and `generate` stub
- **M002-E001-S002** (config loading): provides `load_config()` returning `ProjectConfig` and the CLI override-merging pattern

## Existing API Surface

**`generate(entity, output_dir, dry_run) -> GenerateResult`** (`src/fyrnheim/_generate.py`):
- `GenerateResult.entity_name: str`
- `GenerateResult.code: str`
- `GenerateResult.output_path: Path`
- `GenerateResult.written: bool` -- True if file was written, False if dry_run or content unchanged

**`EntityRegistry`** (`src/fyrnheim/engine/registry.py`):
- `discover(entities_dir: Path)` -- raises `FileNotFoundError` if dir missing, `ImportError` on bad files, `ValueError` on duplicates
- `items() -> ItemsView[str, EntityInfo]`
- `EntityInfo.entity: Entity` -- the Entity instance to pass to `generate()`

## Proposed Solution

### Command Signature

```python
@main.command()
@click.option("--dry-run", is_flag=True, help="Preview generation without writing files.")
@click.option("--entities-dir", type=click.Path(exists=True, file_okay=False), default=None,
              help="Entity definitions directory (overrides config).")
@click.option("--output-dir", type=click.Path(file_okay=False), default=None,
              help="Output directory for generated modules (overrides config).")
def generate(dry_run, entities_dir, output_dir):
```

### Core Logic (~30 lines)

```python
def generate(dry_run, entities_dir, output_dir):
    config = load_config()

    entities_path = Path(entities_dir) if entities_dir else (config.entities_dir if config else Path("entities"))
    output_path = Path(output_dir) if output_dir else (config.output_dir if config else Path("generated"))

    registry = EntityRegistry()
    try:
        registry.discover(entities_path)
    except FileNotFoundError:
        click.echo(f"Error: entities directory not found: {entities_path}", err=True)
        sys.exit(1)

    if len(registry) == 0:
        click.echo(f"No entities found in {entities_path}")
        sys.exit(0)

    click.echo(f"Generating transforms from {entities_path}")
    if dry_run:
        click.echo("Dry run -- no files will be written\n")

    written = 0
    unchanged = 0
    errors = 0

    for name, info in registry.items():
        try:
            result = _generate_fn(info.entity, output_dir=output_path, dry_run=dry_run)
            if result.written:
                status = "written"
                written += 1
            else:
                status = "unchanged" if not dry_run else "dry-run"
                unchanged += 1
            click.echo(f"  {name:<20s} {result.output_path}   {status}")
        except Exception as e:
            click.echo(f"  {name:<20s} ERROR: {e}", err=True)
            errors += 1

    # Summary
    click.echo()
    if dry_run:
        click.echo(f"Dry run: {unchanged} would be generated")
    else:
        click.echo(f"Generated: {written} written, {unchanged} unchanged")

    if errors:
        click.echo(f"Errors: {errors}", err=True)

    sys.exit(1 if errors else 0)
```

### Import Naming

The Click command name `generate` shadows the library function. Use an aliased import:

```python
from fyrnheim._generate import generate as _generate_fn
```

This mirrors the existing pattern where `_generate.py` uses an underscore prefix to avoid module/function name collision.

### Output Format

Normal run:
```
Generating transforms from entities/
  customers            generated/customers_transforms.py   written
  products             generated/products_transforms.py    unchanged

Generated: 1 written, 1 unchanged
```

Dry run:
```
Generating transforms from entities/
Dry run -- no files will be written

  customers            generated/customers_transforms.py   dry-run
  products             generated/products_transforms.py    dry-run

Dry run: 2 would be generated
```

Error case:
```
Generating transforms from entities/
  customers            generated/customers_transforms.py   written
  bad_entity           ERROR: invalid source configuration

Generated: 1 written, 0 unchanged
Errors: 1
```

### Exit Codes
- **0**: all entities generated successfully (or no entities found)
- **1**: one or more entities failed to generate, or entities_dir not found

### Error Handling

| Error | Behavior |
|-------|----------|
| `entities_dir` does not exist | Print error to stderr, exit 1 |
| No entities found | Print info message, exit 0 |
| Entity import fails (in `discover()`) | Let exception propagate -- prints traceback, exit 1 |
| Single entity generation fails | Print ERROR line to stderr, continue to next, exit 1 at end |
| Duplicate entity name | Let `ValueError` from registry propagate, exit 1 |

Note: `discover()` raises immediately on import errors and duplicate names. These are fatal -- they indicate broken entity definitions. Per-entity try/except only wraps the `generate()` call itself.

## File Changes

| File | Change |
|------|--------|
| `src/fyrnheim/cli.py` | Replace `generate` stub with real implementation |
| `tests/test_cli_generate.py` | New test file for generate command tests |

## Test Plan

All tests use `click.testing.CliRunner` with `tmp_path` fixtures.

### Fixture Helper

```python
def make_project(tmp_path, entity_code, config_yaml=None):
    """Create a minimal fyrnheim project in tmp_path."""
    entities_dir = tmp_path / "entities"
    entities_dir.mkdir()
    (entities_dir / "test_entity.py").write_text(entity_code)
    if config_yaml:
        (tmp_path / "fyrnheim.yaml").write_text(config_yaml)
    return tmp_path
```

Minimal entity code for tests:
```python
MINIMAL_ENTITY = '''
from fyrnheim import Entity, LayersConfig, PrepLayer, TableSource
entity = Entity(
    name="test_entity",
    source=TableSource(project="p", dataset="d", table="t"),
    layers=LayersConfig(prep=PrepLayer(model_name="prep_test")),
)
'''
```

### Test Cases (~10 tests)

| Test | Assertion |
|------|-----------|
| `test_generate_writes_file` | Exit 0, output file exists, stdout contains "written" |
| `test_generate_unchanged_on_rerun` | Run twice, second run shows "unchanged" |
| `test_generate_dry_run_no_write` | `--dry-run`, exit 0, no output file, stdout contains "dry-run" |
| `test_generate_dry_run_label` | `--dry-run`, stdout contains "Dry run" |
| `test_generate_summary_counts` | Multiple entities, summary shows correct written/unchanged counts |
| `test_generate_entities_dir_flag` | `--entities-dir` overrides config |
| `test_generate_output_dir_flag` | `--output-dir` changes output location |
| `test_generate_config_respected` | fyrnheim.yaml entities_dir/output_dir are used |
| `test_generate_missing_entities_dir` | Exit 1, stderr contains error |
| `test_generate_no_entities` | Exit 0, stdout contains "No entities found" |
| `test_generate_entity_error_continues` | One bad + one good entity, error on stderr, good entity still generated, exit 1 |

### Running Tests

```bash
uv run pytest tests/test_cli_generate.py -q
```

## Key Decisions
- **Single file**: generate command stays in `cli.py` (no subpackage) per CLI skeleton design
- **Alias import**: `from fyrnheim._generate import generate as _generate_fn` to avoid shadowing the Click command name
- **Plain text output**: no colors, no rich dependency, aligned columns
- **Continue on error**: individual entity failures do not stop other entities from being processed
- **Config is optional**: if no `fyrnheim.yaml` exists, fall back to defaults (`entities/`, `generated/`)
- **discover() errors are fatal**: import errors and duplicate names halt the entire command (these indicate broken entity definitions that need immediate attention)

## Success Criteria
- `fyr generate` discovers entities, generates transform modules, prints per-entity status
- `--dry-run` previews without writing, clearly labeled in output
- `--entities-dir` and `--output-dir` override config values
- Summary line shows written/unchanged counts
- Exit 0 on success, exit 1 on any generation failure
- All tests pass via CliRunner
