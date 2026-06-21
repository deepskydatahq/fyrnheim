# dbt Project Inventory

Fyrnheim's dbt inventory is the first artifact in the data model context layer. It scans a dbt project without executing dbt and emits a versioned JSON document that humans, scripts, Pi, and Claude Code can use as project context.

## Command

From a dbt project root:

```bash
fyr dbt scan --output .fyrnheim/dbt-inventory.json
```

This writes the machine-readable inventory and prints a concise summary:

```text
dbt project: jaffle_shop
source: manifest
resources: models=12, sources=4, tests=30, exposures=2, metrics=1, macros=5
inventory written: .fyrnheim/dbt-inventory.json
```

To print JSON to stdout instead:

```bash
fyr dbt scan --format json
```

Useful options:

```bash
fyr dbt scan --project-path path/to/dbt/project
fyr dbt scan --manifest path/to/target/manifest.json
fyr dbt scan --output path/to/inventory.json
```

## Manifest-first behavior

Fyrnheim prefers dbt's generated `manifest.json` because it is dbt's best source of truth for resources, configuration, and lineage.

Discovery order:

1. `--manifest <path>` when provided.
2. `<project>/target/manifest.json`.
3. `<project>/manifest.json`.
4. Fallback project-file scan when no manifest is found.

When a manifest is available, the inventory includes:

- models
- sources
- tests
- exposures
- metrics
- macros
- model materializations
- tags, descriptions, meta, and owner fields where present
- upstream dependencies from `depends_on.nodes`
- downstream dependencies computed by Fyrnheim
- tests attached to each model by reversing test dependencies

## Fallback project-file scan

If no manifest exists, Fyrnheim scans project files directly:

- `dbt_project.yml`
- `models/**/*.sql`
- `models/**/*.yml`
- `models/**/*.yaml`

The fallback scanner discovers SQL models, YAML model/source metadata, simple `ref('model')` dependencies, and simple `source('source', 'table')` dependencies.

Fallback limitations are intentionally explicit. Fyrnheim does not execute dbt, compile Jinja, resolve packages, or fully interpret dbt configuration in this mode. The generated artifact includes warnings such as:

```text
manifest.json was not found; inventory was generated from project files without executing dbt.
Fallback scanning uses simple SQL ref/source pattern matching and does not compile Jinja.
```

Use `dbt parse` or `dbt compile` to generate `target/manifest.json` when you need dbt-accurate lineage and configuration.

## Artifact shape

The inventory is JSON with schema version:

```json
{
  "schema_version": "fyrnheim.dbt_inventory.v1",
  "generation_source": "manifest",
  "project": {
    "name": "jaffle_shop",
    "path": "/path/to/project",
    "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json"
  },
  "resources": {
    "models": [],
    "sources": [],
    "tests": [],
    "exposures": [],
    "metrics": [],
    "macros": []
  },
  "warnings": []
}
```

A model record includes fields such as:

- `unique_id`
- `name`
- `package_name`
- `path`
- `original_file_path`
- `materialized`
- `tags`
- `description`
- `owner`
- `meta`
- `depends_on`
- `downstream`
- `attached_tests`
- `columns`

This is an MVP schema. Later missions will add classification, principle findings, and stabilized context artifacts on top of it.
