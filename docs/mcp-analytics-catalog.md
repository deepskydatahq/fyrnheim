# Fyrnheim MCP analytics catalog

Fyrnheim exposes a read-only analytics catalog that is suitable for MCP tools. The first MCP slice is intentionally narrow: it answers what analytics models, metrics, and dimensions are available in a project. It does **not** execute queries or connect to a warehouse.

## What the catalog covers

The catalog is derived from the Fyrnheim project manifest:

- `AnalyticsEntity`
  - metrics: `measures`
  - dimensions: `canonical_id`, `state_fields`, and `computed_fields`
- `MetricsModel`
  - metrics: `metric_fields`
  - dimensions: declared `dimensions` plus the `_date` time-grain column

Each metric and dimension record includes model name, model type, kind/source metadata, and identifiers that are stable enough for agents to reference.

## Python usage

```python
from fyrnheim.inspect import build_manifest
from fyrnheim.analytics_catalog import (
    build_analytics_catalog,
    list_analytics_models,
    list_metrics,
    list_dimensions,
    describe_metric,
    describe_dimension,
)

manifest = build_manifest("entities")
catalog = build_analytics_catalog(manifest)

models = list_analytics_models(catalog)
metrics = list_metrics(catalog)
dimensions = list_dimensions(catalog)
workshop_count = describe_metric(catalog, "workshop_count")
lead_level = describe_dimension(catalog, "lead_level")
```

## MCP-ready tool functions

The tool functions in `fyrnheim.mcp.analytics_tools` load the manifest/catalog from an entities directory and return JSON-compatible dictionaries:

```python
from fyrnheim.mcp.analytics_tools import (
    list_analytics_models,
    list_metrics,
    list_dimensions,
    describe_metric,
    describe_dimension,
)

list_analytics_models("entities")
list_metrics("entities", model="accounts")
list_dimensions("entities", model="accounts")
describe_metric("entities", "workshop_count")
describe_dimension("entities", "lead_level")
```

These functions are useful even outside MCP because they form a stable agent-facing surface over Fyrnheim analytics definitions.

## Optional MCP server

Install the optional MCP extra when you want to run Fyrnheim as an MCP server:

```bash
pip install 'fyrnheim[mcp]'
```

Then run:

```bash
fyr-mcp-analytics --entities-dir /path/to/project/entities --project-path /path/to/project
```

The server registers these tools:

- `list_analytics_models`
- `list_metrics`
- `list_dimensions`
- `describe_metric`
- `describe_dimension`

Example Claude Desktop-style configuration:

```json
{
  "mcpServers": {
    "fyrnheim-analytics": {
      "command": "fyr-mcp-analytics",
      "args": [
        "--entities-dir",
        "/path/to/project/entities",
        "--project-path",
        "/path/to/project"
      ]
    }
  }
}
```

## Ambiguity handling

Metric and dimension names can repeat across models. `describe_metric()` and `describe_dimension()` do not guess when a name is ambiguous. They return all matches and set `ambiguous: true`. Pass the `model` argument, or use a full `metric_id` / `dimension_id`, to select one record.

## No query execution yet

This mission intentionally stops at metadata. Future work may add safe query planning or query execution, but that requires a filter grammar, backend credentials, limits, permissions, and materialization-location rules. For now, these tools help agents discover the analytical surface before proposing changes or asking a human which metric to use.
