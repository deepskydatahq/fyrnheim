# List Command Design (M002-E002-S001)

## Overview
Replace the `fyr list` stub in `cli.py` with a real implementation that discovers entities via `EntityRegistry.discover()` and displays them in aligned plain text.

## Problem Statement
Users need a quick way to see what entities exist in their project. `fyr list` is the simplest core command -- pure read-only discovery with no side effects.

## Dependencies
- M002-E001-S001 (CLI skeleton): provides `cli.py` with Click group and `list_cmd` stub
- M002-E001-S002 (config loading): provides `load_config()` and `ProjectConfig`
- M001-E004-S001 (entity discovery): provides `EntityRegistry` and `EntityInfo`

## Proposed Solution

### Output Format

```
  customers    prep, dimension    entities/customers.py
  orders       prep, snapshot     entities/orders.py

2 entities found
```

Three columns, left-aligned with fixed-width padding:
- **Name**: `{name:<20s}`
- **Layers**: `{', '.join(info.layers):<30s}`
- **Path**: relative to project root (or absolute if no config)

No header row. Two-space indent. Summary line after a blank line.

Empty directory:
```
No entities found in entities/
```

Missing directory (exit code 1):
```
Error: Entities directory not found: entities/
```

### Implementation

Modify the existing `list_cmd` function in `src/fyrnheim/cli.py` (~20 lines of logic):

```python
@main.command(name="list")
@click.option("--entities-dir", type=click.Path(), default=None,
              help="Override entities directory.")
def list_cmd(entities_dir: str | None) -> None:
    """List discovered entities."""
    config = load_config()
    if entities_dir is None:
        entities_dir = str(config.entities_dir) if config else "entities"

    path = Path(entities_dir)
    if not path.exists():
        click.echo(f"Error: Entities directory not found: {entities_dir}", err=True)
        raise SystemExit(1)

    registry = EntityRegistry()
    registry.discover(path)

    if len(registry) == 0:
        click.echo(f"No entities found in {entities_dir}")
        return

    for _name, info in registry.items():
        layers_str = ", ".join(info.layers)
        click.echo(f"  {info.name:<20s} {layers_str:<30s} {info.path}")

    click.echo(f"\n{len(registry)} entities found")
```

### Key Decisions

1. **Path display**: Show the path as-is from `EntityInfo.path` (which is whatever `discover()` produces from the glob). If the user passes a relative `--entities-dir`, paths will be relative. If config resolves to absolute, paths will be absolute. No forced transformation -- keep it simple.

2. **Error handling**: Catch the missing directory case *before* calling `discover()` so the error message is clean (no traceback). Import errors during discovery propagate as-is -- Click will format the traceback.

3. **Exit codes**: 0 for success (including empty results), 1 only for actual errors (missing directory, import failures).

4. **No header row**: For a simple list, headers add visual noise without value. The format is self-evident.

5. **Config fallback**: If no `fyrnheim.yaml` exists (`load_config()` returns None), default to `"entities"` relative to cwd. The `--entities-dir` flag always wins.

### Tests

**File:** `tests/test_cli_list.py` (~60 lines)

All tests use `click.testing.CliRunner` and `tmp_path` for isolation.

1. **test_list_discovers_entities** -- Create a tmp dir with a minimal entity `.py` file and a `fyrnheim.yaml` pointing at it. Invoke `fyr list`. Assert output contains entity name, layers, file path, and summary line. Exit code 0.

2. **test_list_empty_directory** -- Create an empty entities dir. Invoke `fyr list --entities-dir <empty_dir>`. Assert output contains "No entities found". Exit code 0.

3. **test_list_missing_directory** -- Invoke `fyr list --entities-dir /nonexistent`. Assert stderr contains "Entities directory not found". Exit code 1.

4. **test_list_entities_dir_flag_overrides_config** -- Create a `fyrnheim.yaml` with one entities dir, but pass `--entities-dir` pointing to a different dir. Assert the flag's directory is used.

5. **test_list_shows_layers** -- Create entity with prep + dimension. Assert output contains "prep, dimension".

6. **test_list_summary_count** -- Two entities. Assert output contains "2 entities found".

### Minimal entity fixture helper

```python
MINIMAL_ENTITY = '''\
from fyrnheim import Entity, LayersConfig, PrepLayer, TableSource

entity = Entity(
    name="{name}",
    description="Test entity",
    source=TableSource(project="test", dataset="raw", table="{name}"),
    layers=LayersConfig(prep=PrepLayer(model_name="prep_{name}")),
)
'''
```

## Lines of Code Estimate
- `cli.py` changes: ~20 lines (replacing stub)
- `tests/test_cli_list.py`: ~60 lines
- Total: ~80 lines

## Success Criteria
- `fyr list` discovers and displays entities with name, layers, path
- Summary line with count
- Empty directory prints message, exits 0
- Missing directory prints error, exits 1
- `--entities-dir` overrides config
- All tests pass via CliRunner
