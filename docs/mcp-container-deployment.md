# Container deployment for Fyrnheim MCP analytics

Fyrnheim can run its analytics catalog and read-only insight tools as an MCP server with the `fyr-mcp-analytics` entrypoint. Local `uv run fyr-mcp-analytics ...` remains the fastest development path, but a container is the recommended shared/team deployment shape.

Use the reference files in [`examples/mcp-container/`](../examples/mcp-container/) as a starting point.

## Why containerize

A containerized MCP server gives project teams:

- reproducible Fyrnheim and backend dependency versions;
- one reviewed runtime image instead of per-laptop Python setup;
- centralized secret injection for ClickHouse/warehouse credentials;
- a clean boundary between agent clients and project code;
- simpler upgrades and rollbacks through image tags.

## Transport model

The current Fyrnheim MCP server runs over stdio. That means the MCP client starts the process and communicates over stdin/stdout.

For Docker, use an interactive process:

```bash
docker run --rm -i ... fyrnheim-mcp-analytics:local ...
```

For Compose, use a one-shot run command rather than a detached service:

```bash
docker compose run --rm -T fyrnheim-mcp-analytics
```

Do not deploy this reference as a detached HTTP service unless a future Fyrnheim mission adds an HTTP/SSE transport.

## Reference image

Build the reference image from the Fyrnheim repository root:

```bash
docker build -f examples/mcp-container/Dockerfile -t fyrnheim-mcp-analytics:local .
```

The image installs Fyrnheim with the `mcp`, `duckdb`, and `clickhouse` extras. Downstream projects can copy the Dockerfile and pin Fyrnheim to a package version, tag, or commit SHA in their own dependency files.

## Project mounting

Mount the downstream Fyrnheim project at `/project`:

```bash
docker run --rm -i \
  --mount type=bind,src=/absolute/path/to/project,dst=/project,readonly \
  fyrnheim-mcp-analytics:local \
  --entities-dir /project/entities \
  --project-path /project \
  --config /project/fyrnheim.yaml
```

The mounted project should contain:

- `entities/` with Fyrnheim definitions;
- `fyrnheim.yaml` with backend configuration;
- optional `insights.recipes` for read-only query tools.

## Secrets and warehouse credentials

Pass warehouse credentials at runtime through environment variables or deployment secrets. Example ClickHouse variables:

```bash
docker run --rm -i \
  --mount type=bind,src=/absolute/path/to/project,dst=/project,readonly \
  --env CLICKHOUSE_HOST \
  --env CLICKHOUSE_PORT \
  --env CLICKHOUSE_USER \
  --env CLICKHOUSE_PASSWORD \
  --env CLICKHOUSE_DATABASE \
  fyrnheim-mcp-analytics:local \
  --entities-dir /project/entities \
  --project-path /project \
  --config /project/fyrnheim.yaml
```

Do not bake credentials, `.env` files, or project-private secrets into the image.

## MCP client configuration

A client can launch Docker directly. Adapt paths and image tags for your environment:

```json
{
  "mcpServers": {
    "fyrnheim-analytics-container": {
      "command": "docker",
      "args": [
        "run",
        "--rm",
        "-i",
        "--mount",
        "type=bind,src=/absolute/path/to/project,dst=/project,readonly",
        "--env",
        "CLICKHOUSE_HOST",
        "--env",
        "CLICKHOUSE_PORT",
        "--env",
        "CLICKHOUSE_USER",
        "--env",
        "CLICKHOUSE_PASSWORD",
        "--env",
        "CLICKHOUSE_DATABASE",
        "fyrnheim-mcp-analytics:local",
        "--entities-dir",
        "/project/entities",
        "--project-path",
        "/project",
        "--config",
        "/project/fyrnheim.yaml"
      ]
    }
  }
}
```

See [`examples/mcp-container/mcp-client-config.json`](../examples/mcp-container/mcp-client-config.json).

## Smoke verification

The reference includes a DuckDB-backed smoke project with one analytics catalog model and one content insight recipe.

Create the local fixture database:

```bash
uv run python examples/mcp-container/smoke-project/scripts/create_smoke_db.py
```

Verify recipe loading without Docker:

```bash
uv run python - <<'PY'
from fyrnheim.mcp.insight_tools import list_insight_recipes, top_content_items

config = "examples/mcp-container/smoke-project/fyrnheim.yaml"
print(list_insight_recipes(config))
print(top_content_items(config_path=config, metric="total_engagement", limit=2))
PY
```

Then build and run the container:

```bash
docker build -f examples/mcp-container/Dockerfile -t fyrnheim-mcp-analytics:local .
docker run --rm -i \
  --mount type=bind,src="$PWD/examples/mcp-container/smoke-project",dst=/project,readonly \
  fyrnheim-mcp-analytics:local \
  --entities-dir /project/entities \
  --project-path /project \
  --config /project/fyrnheim.yaml
```

In an MCP client, call:

- `list_analytics_models`
- `list_insight_recipes`
- `top_content_items`

## Propel Data rollout

For Propel Data, use this as the recommended team deployment path:

1. Keep `uv run fyr-mcp-analytics ...` for local recipe development.
2. Build a Propel-owned image from the reference Dockerfile.
3. Pin Fyrnheim to a release or commit that includes M099 and M100.
4. Mount `/home/tmo/roadtothebeach/propel/propel-data` or bake a reviewed copy of the project into the image, depending on deployment policy.
5. Inject ClickHouse credentials via environment/secrets.
6. Start with a `top_linkedin_posts` recipe in Propel's `fyrnheim.yaml`.
7. Defer `promising_leads` until `accounts.email_domain`, `accounts.lead_level`, and `companies.is_icp_fit` are warehouse-native and materialized.

The reference deliberately does not hardcode Propel fields or credentials into Fyrnheim core. Propel-specific recipes belong in Propel Data configuration.
