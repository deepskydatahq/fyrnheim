# Fyrnheim MCP analytics catalog

Fyrnheim exposes a read-only analytics catalog and safe model-query surface that is suitable for agents. Agents should reason from declared analytics models, their grain, metrics, dimensions, and limitations instead of guessing physical source tables.

The MCP tools do **not** accept arbitrary SQL and do not write, materialize, or run pipelines.

## What the catalog covers

The catalog is derived from the Fyrnheim project manifest:

- `AnalyticsEntity`
  - metrics: `measures`
  - dimensions: `canonical_id`, `state_fields`, and `computed_fields`
- `MetricsModel`
  - metrics: `metric_fields`
  - dimensions: declared `dimensions` plus the `_date` time-grain column

Each model includes concise agent-facing context:

- model type and grain;
- defining entity / aggregate level;
- metrics and dimensions;
- recommended question shapes;
- limitations, especially where the model grain cannot answer lower-grain questions.

Each metric and dimension includes usage hints for safe selection, grouping, filtering, and sorting.

## Python usage

```python
from fyrnheim.inspect import build_manifest
from fyrnheim.analytics_catalog import (
    build_analytics_catalog,
    describe_analytics_model,
    list_analytics_models,
    list_metrics,
    list_dimensions,
    describe_metric,
    describe_dimension,
)

manifest = build_manifest("entities")
catalog = build_analytics_catalog(manifest)

models = list_analytics_models(catalog)
content = describe_analytics_model(catalog, "content_metrics_daily")
metrics = list_metrics(catalog)
dimensions = list_dimensions(catalog)
workshop_count = describe_metric(catalog, "workshop_count")
lead_level = describe_dimension(catalog, "lead_level")
```

## Generic analytics model queries

Use `query_analytics_model` to query declared model metrics and dimensions only. Fyrnheim builds the Ibis expression and applies a mandatory capped limit.

```python
from fyrnheim.mcp.analytics_tools import query_analytics_model, preview_analytics_query_sql

result = query_analytics_model(
    "fyrnheim.yaml",
    "content_metrics_daily",
    ["impressions", "reactions"],
    dimensions=["source"],
    filters={"_date": {"gte": "2026-01-01"}},
    order_by=[{"field": "impressions", "direction": "desc"}],
    limit=25,
)

preview = preview_analytics_query_sql(
    "fyrnheim.yaml",
    "content_metrics_daily",
    ["impressions"],
    dimensions=["source"],
)
```

`preview_analytics_query_sql` compiles the generated query. It does not accept agent-written SQL.

## MCP-ready tool functions

The tool functions in `fyrnheim.mcp.analytics_tools` load the manifest/catalog from an entities directory and return JSON-compatible dictionaries:

```python
from fyrnheim.mcp.analytics_tools import (
    describe_analytics_model,
    list_analytics_models,
    list_metrics,
    list_dimensions,
    describe_metric,
    describe_dimension,
    query_analytics_model,
    preview_analytics_query_sql,
)

list_analytics_models("entities")
describe_analytics_model("entities", "content_metrics_daily")
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

Then run locally over stdio:

```bash
fyr-mcp-analytics --entities-dir /path/to/project/entities --project-path /path/to/project --config /path/to/project/fyrnheim.yaml
```

The server registers these catalog/query tools:

- `list_analytics_models`
- `describe_analytics_model`
- `list_metrics`
- `list_dimensions`
- `describe_metric`
- `describe_dimension`
- `query_analytics_model`
- `preview_analytics_query_sql`

It also keeps the recipe-oriented insight tools:

- `list_insight_recipes`
- `run_insight_recipe`
- `top_content_items`
- `find_promising_records`

For hosted clients that need an MCP URL instead of a launched process, see [Streamable HTTP MCP transport](mcp-http-transport.md).

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
        "/path/to/project",
        "--config",
        "/path/to/project/fyrnheim.yaml"
      ]
    }
  }
}
```

## Ambiguity handling

Metric and dimension names can repeat across models. `describe_metric()` and `describe_dimension()` do not guess when a name is ambiguous. They return all matches and set `ambiguous: true`. Pass the `model` argument, or use a full `metric_id` / `dimension_id`, to select one record.

## Agent guidance for model grain

Agents should inspect `describe_analytics_model` before querying. If a model is daily/channel-grain, it can rank channels or days but cannot rank individual source records unless the record identifier is declared as a dimension.

For Propel Data, `content_metrics_daily` is useful for questions like:

- Which content source/channel generated the most impressions this week?
- How did LinkedIn compare to website content by day?
- Which day had the highest aggregate engagement?

It is **not** sufficient for:

- Which individual LinkedIn post performed best?
- Which AuthoredUp post had the highest engagement?

Those require a per-post model or a reliable per-post source table exposed as a declared analytics model. If only `content_metrics_daily` is available, the agent should answer at channel/day grain and explain the limitation instead of guessing physical source table names such as `authoredup_posts`.
