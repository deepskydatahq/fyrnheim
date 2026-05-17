# MCP read-only insight query tools

Fyrnheim's MCP analytics tools can move beyond catalog discovery when a project defines explicit insight recipes. Recipes are bounded, read-only table queries that agents may run to answer business questions such as "what content performed best?" or "which records look promising?".

This layer is intentionally conservative:

- no arbitrary SQL from agents;
- no writes or materializations;
- no pipeline execution;
- only configured recipes can run;
- every recipe has a required limit with a maximum cap;
- filters and sort columns must be declared in the recipe.

## Recipe configuration

Add recipes under `insights.recipes` in `fyrnheim.yaml`.

### `table_top`

Use `table_top` for a single declared table or source/model.

```yaml
insights:
  recipes:
    top_linkedin_posts:
      type: table_top
      source: linkedin_posts
      tags: [content, linkedin]
      columns:
        - post_id
        - published_date
        - text
        - impressions
        - reactions
        - comments
        - shares
        - total_engagement
        - engagement_rate_pct
        - performance_tier
      order_by:
        default: total_engagement
        allowed:
          - impressions
          - total_engagement
          - engagement_rate_pct
      filters:
        allowed:
          - performance_tier
      limit:
        default: 20
        max: 100
```

`source` references a Fyrnheim manifest asset. For fixture tests or custom physical names, use `table: table_name` instead.

### `joined_top`

Use `joined_top` for a bounded left join between two configured tables/assets.

```yaml
insights:
  recipes:
    promising_leads:
      type: joined_top
      primary: accounts
      tags: [leads]
      join:
        right: companies
        left_on: email_domain
        right_on: domain
      columns:
        - email
        - company
        - lead_level
        - workshop_count
        - newsletter_subscriptions
        - industry
        - is_icp_fit
      order_by:
        default: workshop_count
        allowed:
          - lead_level
          - workshop_count
          - newsletter_subscriptions
      filters:
        allowed:
          - lead_level
          - is_icp_fit
        defaults:
          is_icp_fit: true
      limit:
        default: 50
        max: 200
```

## MCP tools

The MCP server now exposes these read-only insight tools in addition to the analytics catalog tools:

- `list_insight_recipes`
- `run_insight_recipe`
- `top_content_items`
- `find_promising_records`

Example local stdio MCP server command:

```bash
fyr-mcp-analytics \
  --entities-dir /path/to/project/entities \
  --project-path /path/to/project \
  --config /path/to/project/fyrnheim.yaml
```

For hosted clients that need an MCP URL instead of a launched process, see [Streamable HTTP MCP transport](mcp-http-transport.md).

`top_content_items` chooses a recipe tagged `content` unless you pass a specific recipe. `find_promising_records` chooses a recipe tagged `leads` or `promising_records` unless you pass a specific recipe.

## Python usage

```python
from fyrnheim.mcp.insight_tools import list_insight_recipes, top_content_items

recipes = list_insight_recipes("fyrnheim.yaml")
result = top_content_items(
    config_path="fyrnheim.yaml",
    recipe="top_linkedin_posts",
    metric="engagement_rate_pct",
    limit=10,
)
```

Results are JSON-compatible dictionaries with recipe name, order field, filters, row count, and rows.

## Backend notes

Recipes execute through Ibis using the backend in `fyrnheim.yaml`.

- DuckDB is useful for local tests and fixtures.
- ClickHouse and BigQuery can be used when their backend configuration is present and credentials are available.
- Fyrnheim resolves source assets to physical backend table names using the same source conventions as the pipeline where possible. Explicit `table` / `right_table` can be used when a project wants total control.

## Propel Data handoff context

For Propel Data, start with content performance because `linkedin_posts` already has source-level computed columns for engagement and performance tier.

Suggested first recipe:

```yaml
insights:
  recipes:
    top_linkedin_posts:
      type: table_top
      source: linkedin_posts
      tags: [content, linkedin]
      columns:
        - post_id
        - published_date
        - text
        - impressions
        - reactions
        - comments
        - shares
        - total_engagement
        - engagement_rate_pct
        - performance_tier
      order_by:
        default: total_engagement
        allowed:
          - impressions
          - total_engagement
          - engagement_rate_pct
      filters:
        allowed:
          - performance_tier
      limit:
        default: 20
        max: 100
```

Promising leads should wait until the project has warehouse-native versions of fields such as `accounts.email_domain`, `accounts.lead_level`, and `companies.is_icp_fit`. Once those fields are warehouse-safe and materialized, add a `joined_top` recipe similar to the example above.
