# MCP insight recipe tools

Fyrnheim no longer registers recipe-oriented insight tools on the MCP server.

The former MCP tools were:

- `list_insight_recipes`
- `run_insight_recipe`
- `top_content_items`
- `find_promising_records`

They were removed from the agent-facing MCP surface because their recipe and tag selection model encouraged brittle, business-specific behavior. Agents should use the semantic analytics model tools instead:

- `describe_analytics_model`
- `query_analytics_model`
- `preview_analytics_query_sql`

Those tools force the agent to reason from declared model grain, metrics, dimensions, limitations, and generated read-only Ibis queries. They do not accept arbitrary SQL, write data, materialize assets, or execute pipelines.

## Migration guidance

Replace recipe calls with semantic model flow:

1. Call `list_analytics_models` to discover available models.
2. Call `describe_analytics_model` for the target model.
3. If the model grain can answer the question, call `query_analytics_model` with declared metrics and dimensions only.
4. If the model grain cannot answer the question, explain the limitation instead of guessing physical table names.

For example, a daily/channel-grain model such as `content_metrics_daily` can answer channel/day rollups, but it cannot rank individual posts unless a post identifier is declared as a dimension or a separate per-post model exists.

The underlying Python recipe parsing/execution helpers remain in the package for compatibility, but they are not exposed as MCP tools.
