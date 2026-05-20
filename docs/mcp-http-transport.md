# Streamable HTTP MCP transport

Fyrnheim supports two MCP transports for analytics catalog and read-only insight tools:

- **stdio** via `fyr-mcp-analytics` for local clients that can launch a process, including Docker-launched desktop workflows.
- **Streamable HTTP** via `fyr-mcp-analytics-http` for hosted clients such as Claude that connect to an MCP server URL.

Both entrypoints expose the same tool surface. The HTTP entrypoint does not add arbitrary SQL, writes, materializations, or pipeline execution.

## Local stdio transport

Use stdio when the MCP client can launch a local command:

```bash
fyr-mcp-analytics \
  --entities-dir /path/to/project/entities \
  --project-path /path/to/project \
  --config /path/to/project/fyrnheim.yaml
```

This remains the recommended path for local debugging and Docker `docker run -i` style desktop setups.

## Hosted HTTP transport

Use HTTP when the MCP client needs a URL:

```bash
fyr-mcp-analytics-http \
  --host 0.0.0.0 \
  --port 8000 \
  --path /mcp \
  --entities-dir /app/entities \
  --project-path /app \
  --config /app/fyrnheim.yaml
```

The MCP endpoint is then available at:

```text
http://<host>:8000/mcp
```

For deterministic testing or some proxy setups, the entrypoint also supports:

```bash
fyr-mcp-analytics-http \
  --json-response \
  --stateless-http \
  --entities-dir /app/entities \
  --project-path /app \
  --config /app/fyrnheim.yaml
```

## Claude / hosted client setup

For a hosted Claude MCP integration, deploy the HTTP server behind an access-controlled HTTPS URL and configure Claude with that URL:

```text
https://mcp.example.com/mcp
```

Operational recommendations:

1. Run the server in a container or VM that can import the project entities.
2. Mount or bake the project files at a fixed path such as `/app`.
3. Pass ClickHouse or warehouse credentials via environment variables/secrets.
4. Put the endpoint behind TLS and an authenticated gateway until Fyrnheim adds first-class MCP auth.
5. Start with catalog and bounded insight recipes only.

Do not expose an unauthenticated production MCP endpoint to the public internet.

## Container sketch

A hosted container command looks like:

```bash
docker run --rm \
  -p 8000:8000 \
  --env CLICKHOUSE_HOST \
  --env CLICKHOUSE_PORT \
  --env CLICKHOUSE_USER \
  --env CLICKHOUSE_PASSWORD \
  --env CLICKHOUSE_DATABASE \
  propel-data-mcp:latest \
  fyr-mcp-analytics-http \
  --host 0.0.0.0 \
  --port 8000 \
  --path /mcp \
  --entities-dir /app/entities \
  --project-path /app \
  --config /app/fyrnheim.yaml
```

## Supabase BYO MCP compatibility

Supabase's BYO MCP guide targets **Supabase Edge Functions**, the **Deno/TypeScript Edge Runtime**, and the TypeScript SDK's `WebStandardStreamableHTTPServerTransport`.

Fyrnheim's HTTP MCP server is a **Python** FastMCP application. It can speak Streamable HTTP, but it cannot run directly inside a Supabase Edge Function because Edge Functions do not run Python containers.

Recommended Supabase options:

1. **Proxy pattern** — deploy Fyrnheim's Python HTTP MCP server on a container host, then use a Supabase Edge Function only as a lightweight authenticated proxy.
2. **TypeScript adapter** — build a separate Edge Function that exposes a subset of Fyrnheim MCP tools from exported JSON manifests/recipes or by querying the warehouse directly.
3. **Use a container host directly** — skip Supabase Edge for the MCP runtime and connect Claude to the Python HTTP endpoint.

For Propel Data, the most direct path is a Python container-hosted HTTP MCP endpoint. Supabase Edge is useful only as a proxy or future TypeScript adapter layer.

## Tool surface

The HTTP transport exposes the same tools as stdio:

- `list_analytics_models`
- `describe_analytics_model`
- `list_metrics`
- `list_dimensions`
- `describe_metric`
- `describe_dimension`
- `query_analytics_model`
- `preview_analytics_query_sql`
- `list_insight_recipes`
- `run_insight_recipe`
- `top_content_items`
- `find_promising_records`
