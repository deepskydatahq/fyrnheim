# Fyrnheim project manifest

Fyrnheim can export a machine-readable project manifest for tools that need to inspect a pipeline without executing it.

The manifest is intended for meta tools, catalogs, lineage viewers, and orchestration integrations that can access a Fyrnheim project repository and want the semantic graph behind the pipeline.

## Source of truth

Use the Fyrnheim project repository as the semantic source of truth:

1. Check out the same Git commit that your orchestrator or deployment runs.
2. Install the project's Fyrnheim dependencies in an isolated environment.
3. Run manifest discovery against the configured `entities/` directory.
4. Combine the manifest with runtime metadata from Dagster or another orchestrator.

Dagster should be treated as runtime context: run status, schedules, materialization timestamps, asset events, and freshness. Fyrnheim's manifest should be treated as the semantic model: sources, activities, identity resolution, analytics entities, metrics models, staging views, and graph relationships.

## CLI

```bash
fyr manifest --entities-dir entities --format json
```

Useful options:

```bash
fyr manifest --entities-dir entities --project-path /repo/checkout
fyr manifest --entities-dir entities --no-git
```

The command imports entity files and prints deterministic JSON. It does not run the pipeline or connect to a warehouse.

## Python API

```python
from fyrnheim.inspect import build_manifest, manifest_json

manifest = build_manifest("entities")
json_payload = manifest_json(manifest)
```

`build_manifest()` returns a JSON-serializable `dict` with schema version `fyrnheim.manifest.v1`.

## Manifest contents

Top-level fields:

- `schema_version`
- `project_path`
- `entities_dir`
- `git_commit`
- `sources`
- `activities`
- `identity_graphs`
- `analytics_entities`
- `metrics_models`
- `staging_views`
- `edges`

Each asset record has:

```json
{
  "type": "event_source",
  "name": "web_events",
  "config": {
    "name": "web_events"
  }
}
```

The `config` field is the asset's Pydantic model dump, so consumers can inspect the exact Fyrnheim declaration without depending on Python objects.

## Relationship edges

Edges are directed and use stable typed identifiers:

```json
{
  "from": "source:web_events",
  "to": "activity:signup",
  "relationship": "activity_source"
}
```

Current relationship types include:

- `depends_on` — staging view dependency
- `upstream` — source reads from a staging view
- `join` — source declaratively joins another source
- `activity_source` — activity derives from a source
- `identity_source` — identity graph observes a source
- `identity_graph` — analytics entity uses an identity graph
- `state_field` — analytics entity state field reads from a source
- `measure` — analytics entity measure derives from an activity
- `metrics_source` — metrics model reads events from a source

## GitHub + Dagster meta-tool pattern

A meta tool that has both GitHub and Dagster access should:

1. Use Dagster deployment/run metadata to identify the repo and commit.
2. Check out that commit from GitHub.
3. Run `fyr manifest` in a sandboxed environment.
4. Use Dagster APIs to fetch runtime state for Dagster assets/runs.
5. Join Dagster runtime state to Fyrnheim semantic nodes by naming convention or explicit metadata emitted by the Dagster job.

This keeps responsibilities clean: GitHub/Fyrnheim provides the versioned semantic graph, while Dagster provides the operational state of that graph.

## Attaching Fyrnheim metadata in Dagster

Fyrnheim provides a small Dagster-compatible helper that does not import or depend on Dagster. It returns plain JSON-compatible values that your Dagster job can attach to runs, observations, or materializations.

```python
from fyrnheim.integrations.dagster import build_manifest_metadata

metadata = build_manifest_metadata(
    entities_dir="entities",
    project_path=".",
)
```

Example inside Dagster code:

```python
from dagster import MetadataValue, asset
from fyrnheim.integrations.dagster import build_manifest_metadata

@asset
def fyrnheim_pipeline(context):
    metadata = build_manifest_metadata("entities", project_path=".")
    context.add_output_metadata(
        {
            "fyrnheim_manifest_hash": metadata["fyrnheim_manifest_hash"],
            "fyrnheim_git_commit": metadata["fyrnheim_git_commit"],
            "fyrnheim_schema_version": metadata["fyrnheim_schema_version"],
            "fyrnheim_asset_counts": MetadataValue.json(metadata["fyrnheim_asset_counts"]),
        }
    )
    # Run Fyrnheim pipeline here.
```

The key field for correlation is `fyrnheim_manifest_hash`. A meta tool can compare that hash with a manifest exported from the GitHub checkout for the same commit. `fyrnheim_git_commit`, `fyrnheim_project_path`, `fyrnheim_entities_dir`, and the asset counts are included to make debugging mismatches easier.

## Security note

Manifest discovery imports project Python files. When inspecting third-party or user-submitted repositories, run discovery in a restricted container or sandbox without production credentials.
