# Init Command Design (M002-E001-S003)

## Overview
Implement `fyr init [project_name]` that creates a ready-to-use project directory with config, subdirectories, a sample entity, and sample data. After init, `fyr generate && fyr run` works immediately.

## Problem Statement
New users need a zero-friction onboarding path. Today they must manually create directories, write config, find sample data, and author an entity from scratch. `fyr init` eliminates that by scaffolding everything in one command.

## Key Design Decisions

### 1. Sample Parquet: Bundle as Package Data
**Decision**: Bundle `examples/data/customers.parquet` inside the installed package and copy it during init.

**Why not generate inline?**
- Generating parquet inline requires pandas or pyarrow as a runtime dependency. Fyrnheim does not depend on pandas. Adding it just for init is wrong.
- pyarrow alone could work but adds a heavy transitive dep for a 12-row file.

**How it works:**
- Add `src/fyrnheim/_scaffold/` directory containing `customers.parquet` (copied from `examples/data/customers.parquet`)
- Add `src/fyrnheim/_scaffold/customers_entity.py` (the sample entity template)
- Use `importlib.resources` (stdlib, Python 3.11+) to read these at runtime
- Hatchling already packages everything under `src/fyrnheim/` -- the `_scaffold/` subdir is included automatically

**pyproject.toml change**: None needed. Hatchling's `packages = ["src/fyrnheim"]` already includes all subdirectories. However, a `py.typed` marker or explicit glob is not needed -- hatchling includes non-`.py` files in packages by default.

### 2. Sample Entity: Simplified Version
**Decision**: Use a simplified version of `examples/entities/customers.py` -- keep it approachable for first-time users.

Simplifications:
- Keep PrepLayer with 2 computed columns (email_hash, amount_dollars) -- drop created_date
- Keep DimensionLayer with 2 computed columns (email_domain, is_paying) -- drop signup_month
- Keep 3 quality checks (NotNull email, Unique email_hash, InRange amount_cents) -- drop NotNull id

This keeps the entity readable while still demonstrating all three concepts (prep, dimension, quality).

### 3. Template Storage: Files in `_scaffold/`, Not Inline Strings
**Decision**: Store template files on disk inside `src/fyrnheim/_scaffold/`.

Files in `_scaffold/`:
- `customers.parquet` -- binary, must be a file
- `customers_entity.py` -- the sample entity (a valid `.py` file, can be tested independently)
- `fyrnheim.yaml` -- default config template

**Why not inline strings in cli.py?**
- The parquet file cannot be an inline string.
- Keeping the entity as a real `.py` file means we can import-test it in CI to ensure it stays valid.
- The YAML template is 4 lines -- could be inline, but keeping it alongside the other scaffold files is cleaner.

## Proposed Solution

### New Files

**`src/fyrnheim/_scaffold/__init__.py`** (empty, makes it a package for importlib.resources)

**`src/fyrnheim/_scaffold/fyrnheim.yaml`**:
```yaml
# Fyrnheim project config
entities_dir: entities
data_dir: data
output_dir: generated
backend: duckdb
```

**`src/fyrnheim/_scaffold/customers_entity.py`**:
```python
"""Sample customers entity -- edit or replace this with your own."""

from fyrnheim import (
    ComputedColumn,
    DimensionLayer,
    Entity,
    InRange,
    LayersConfig,
    NotNull,
    PrepLayer,
    QualityConfig,
    TableSource,
    Unique,
)
from fyrnheim.primitives import hash_email

entity = Entity(
    name="customers",
    description="Sample customer entity",
    source=TableSource(
        project="example",
        dataset="raw",
        table="customers",
        duckdb_path="customers.parquet",
    ),
    layers=LayersConfig(
        prep=PrepLayer(
            model_name="prep_customers",
            computed_columns=[
                ComputedColumn(
                    name="email_hash",
                    expression=hash_email("email"),
                    description="SHA256 hash of lowercase trimmed email",
                ),
                ComputedColumn(
                    name="amount_dollars",
                    expression="t.amount_cents / 100.0",
                    description="Monthly payment in dollars",
                ),
            ],
        ),
        dimension=DimensionLayer(
            model_name="dim_customers",
            computed_columns=[
                ComputedColumn(
                    name="email_domain",
                    expression="t.email.split('@')[1]",
                    description="Email domain extracted from address",
                ),
                ComputedColumn(
                    name="is_paying",
                    expression="t.plan != 'free'",
                    description="True if customer is on a paid plan",
                ),
            ],
        ),
    ),
    quality=QualityConfig(
        primary_key="email_hash",
        checks=[
            NotNull("email"),
            Unique("email_hash"),
            InRange("amount_cents", min=0),
        ],
    ),
)
```

**`src/fyrnheim/_scaffold/customers.parquet`**: Copy of `examples/data/customers.parquet` (12 rows, ~2KB).

### Changes to `src/fyrnheim/cli.py`

Replace the `init` stub with a real implementation (~60 lines):

```python
@main.command()
@click.argument("project_name", required=False, default=None)
def init(project_name: str | None) -> None:
    """Create a new fyrnheim project with sample entity and data."""
    target = Path(project_name) if project_name else Path.cwd()
    _scaffold_project(target, project_name is not None)
```

**`_scaffold_project(target_dir, create_root)`** logic:

1. If `create_root` is True, create `target_dir/` (the named project directory).
2. Check if `fyrnheim.yaml` already exists in target. If yes, warn and skip config write. Continue with other files (to allow re-running init to add missing pieces).
3. Create subdirectories: `entities/`, `data/`, `generated/`.
4. Copy scaffold files using `importlib.resources`:
   - `fyrnheim.yaml` -> `target/fyrnheim.yaml` (skip if exists)
   - `customers_entity.py` -> `target/entities/customers.py` (skip if exists)
   - `customers.parquet` -> `target/data/customers.parquet` (skip if exists)
5. Print summary of what was created vs skipped.
6. Print next-steps hint.

### File-Level Safety

Every file write checks existence first:
- **Exists**: print `  skipped  entities/customers.py (already exists)` and move on
- **New**: print `  created  entities/customers.py` and write

This satisfies both "does not overwrite" and "preserves existing entity files".

### Output Format

```
Created myproject/
  created  fyrnheim.yaml
  created  entities/customers.py
  created  data/customers.parquet
  created  generated/

Next steps:
  cd myproject
  fyr generate
  fyr run
```

When some files exist:
```
Initializing in current directory...
  skipped  fyrnheim.yaml (already exists)
  created  entities/customers.py
  skipped  data/customers.parquet (already exists)
  created  generated/

Next steps:
  fyr generate
  fyr run
```

### Tests

**`tests/test_cli_init.py`** (~12 tests):

| Test | Type | Description |
|------|------|-------------|
| `test_init_named_project` | integration | `fyr init myproject` creates dir with all files |
| `test_init_cwd` | integration | `fyr init` in empty dir creates config + subdirs |
| `test_init_config_exists_warns` | unit | Existing fyrnheim.yaml triggers warning, no overwrite |
| `test_init_preserves_entities` | unit | Existing entity files not overwritten |
| `test_init_preserves_data` | unit | Existing parquet not overwritten |
| `test_init_creates_subdirs` | integration | entities/, data/, generated/ all created |
| `test_init_sample_entity_valid` | integration | Scaffold entity file imports and has `entity` attribute |
| `test_init_sample_data_has_rows` | integration | Scaffold parquet has >= 5 rows (check with duckdb or pyarrow) |
| `test_init_generate_succeeds` | integration | After init, calling `generate()` on sample entity produces valid code |
| `test_init_output_lists_files` | unit | CLI output includes file names |
| `test_init_output_next_steps` | unit | CLI output includes "Next steps" hint |
| `test_init_idempotent` | unit | Running init twice does not error, second run skips existing files |

### Implementation Order

1. Create `src/fyrnheim/_scaffold/` with `__init__.py`, `fyrnheim.yaml`, `customers_entity.py`
2. Copy `examples/data/customers.parquet` to `src/fyrnheim/_scaffold/customers.parquet`
3. Implement `_scaffold_project()` helper and wire into `init` command in `cli.py`
4. Write tests

## Dependencies

- **M002-E001-S001** (CLI skeleton): `cli.py` with Click group and `init` stub must exist
- **M002-E001-S002** (config loading): `config.py` with `ProjectConfig` and `load_config()` must exist (init writes the config that load_config reads)

## What This Does NOT Include

- No `fyr run` integration test (that is M002-E001-S004+ territory)
- No BigQuery config in scaffold YAML (only duckdb)
- No interactive prompts or questionnaire (YAGNI)
- No `.gitignore` generation (YAGNI)
