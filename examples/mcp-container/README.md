# Containerized Fyrnheim MCP analytics server

This directory is a reference deployment for running `fyr-mcp-analytics` in a container. It is intentionally generic: copy or adapt it in a downstream project and pin Fyrnheim to a released version, tag, or commit SHA.

The current MCP server uses stdio transport, so MCP clients should launch the container with an interactive stdin (`docker run -i` or `docker compose run -T/-i`). Do not run it as a detached HTTP service unless Fyrnheim adds an HTTP transport later.

## Build

From the Fyrnheim repository root:

```bash
docker build -f examples/mcp-container/Dockerfile -t fyrnheim-mcp-analytics:local .
```

## Smoke project

Create the DuckDB fixture used by the included smoke project:

```bash
uv run python examples/mcp-container/smoke-project/scripts/create_smoke_db.py
```

Then run the MCP server over stdio with the smoke project mounted at `/project`:

```bash
docker run --rm -i \
  --mount type=bind,src="$PWD/examples/mcp-container/smoke-project",dst=/project,readonly \
  fyrnheim-mcp-analytics:local \
  --entities-dir /project/entities \
  --project-path /project \
  --config /project/fyrnheim.yaml
```

For Compose-based local testing:

```bash
cd examples/mcp-container
docker compose run --rm -T fyrnheim-mcp-analytics
```

## Downstream project usage

For a real project, mount that project instead of `smoke-project`:

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

Warehouse credentials should come from environment variables, Docker/Compose secrets, or the deployment platform's secret manager. Never bake real credentials into the image.

See `mcp-client-config.json` for a Claude Desktop / MCP-client style command that launches the container directly.
