# Fyrnheim

Define typed Python entities, generate transformations, run anywhere.

A dbt alternative built on Pydantic + Ibis.

Fyrnheim lets data teams define business entities as typed Pydantic models and automatically generates Ibis transformation code from those definitions. The same entity runs on DuckDB for instant local development and deploys to BigQuery, ClickHouse, or Postgres in production with zero changes. No SQL, no Jinja, no vendor lock-in.

## Install

```bash
pip install fyrnheim[duckdb]
```

## Quick Start

**1. Create a project:**

```bash
fyr init myproject && cd myproject
```

```
Created myproject/
  created  entities/
  created  data/
  created  generated/
  created  fyrnheim.yaml
  created  entities/customers.py
  created  data/customers.parquet
```

**2. Look at the sample entity** in `entities/customers.py`:

```python
entity = Entity(
    name="customers",
    source=TableSource(..., duckdb_path="customers.parquet"),
    layers=LayersConfig(
        prep=PrepLayer(model_name="prep_customers", computed_columns=[
            ComputedColumn(name="email_hash", expression=hash_email("email")),
            ComputedColumn(name="amount_dollars", expression="t.amount_cents / 100.0"),
        ]),
        dimension=DimensionLayer(model_name="dim_customers", computed_columns=[
            ComputedColumn(name="is_paying", expression="t.plan != 'free'"),
        ]),
    ),
    quality=QualityConfig(checks=[NotNull("email"), Unique("email_hash")]),
)
```

**3. Generate transformation code:**

```bash
fyr generate
```

```
Generating transforms from entities
  customers            generated/customers_transforms.py   written

Generated: 1 written, 0 unchanged
```

**4. Run the pipeline:**

```bash
fyr run
```

```
Discovering entities... 1 found
Running on duckdb

  customers        prep -> dim            12 rows    0.1s  ok

Done: 1 success, 0 errors (0.2s)
```

Add your own entities to `entities/` and data to `data/`. See `examples/` for more.

## Core Concepts

### Entities

An entity is a Pydantic model describing a business object -- customers, orders, products. It declares its source, transformation layers, and quality rules in one place.

```python
entity = Entity(
    name="customers",
    description="...",
    source=TableSource(...),
    layers=LayersConfig(prep=..., dimension=...),
    quality=QualityConfig(checks=[...]),
)
```

### Layers

Composable transformation stages that an entity flows through:

| Layer | Purpose |
|-------|---------|
| **PrepLayer** | Clean raw data: type casts, renames, computed columns |
| **DimensionLayer** | Add business logic columns (is_paying, account_type) |
| **SnapshotLayer** | Track changes over time (daily snapshots, SCD) |
| **ActivityConfig** | Detect events from state changes (row_appears, status_becomes, field_changes) |
| **AnalyticsLayer** | Date-grain metric aggregation (snapshot and event metrics) |

```python
layers=LayersConfig(
    prep=PrepLayer(
        model_name="prep_customers",
        computed_columns=[ComputedColumn(name="amount_dollars", expression="t.amount_cents / 100.0")],
    ),
    dimension=DimensionLayer(
        model_name="dim_customers",
        computed_columns=[ComputedColumn(name="is_paying", expression="t.plan != 'free'")],
    ),
    activity=ActivityConfig(
        model_name="activity_customers",
        entity_id_field="customer_id",
        types=[ActivityType(name="signed_up", trigger="row_appears", timestamp_field="created_at")],
    ),
    analytics=AnalyticsLayer(
        model_name="analytics_customers",
        date_expression="t.created_at.date()",
        metrics=[AnalyticsMetric(name="new_customers", expression="t.count()", metric_type="event")],
    ),
)
```

### Source Types

Fyrnheim supports multiple source types for different data patterns:

**TableSource** -- read from a single warehouse table or local parquet file:

```python
source=TableSource(
    project="myproject", dataset="raw", table="customers",
    duckdb_path="data/customers.parquet",  # local dev
)
```

**UnionSource** -- combine multiple sources into a common schema. Each sub-source can remap columns with `field_mappings` and inject constants with `literal_columns`:

```python
source=UnionSource(
    sources=[
        TableSource(
            project="myproject", dataset="raw", table="youtube_videos",
            duckdb_path="youtube_videos/*.parquet",
            field_mappings={"video_id": "product_id"},
            literal_columns={"product_type": "video", "source_platform": "youtube"},
        ),
        TableSource(
            project="myproject", dataset="raw", table="linkedin_posts",
            duckdb_path="linkedin_posts/*.parquet",
            field_mappings={"post_id": "product_id", "text": "title"},
            literal_columns={"product_type": "post", "source_platform": "linkedin"},
        ),
    ],
)
```

**DerivedSource** -- build identity graphs by joining multiple entities on a shared key. Cascading FULL OUTER JOIN with priority-based field resolution:

```python
source=DerivedSource(
    identity_graph="person_graph",
    identity_graph_config=IdentityGraphConfig(
        match_key="email_hash",
        sources=[
            IdentityGraphSource(name="crm", entity="crm_contacts", match_key_field="email_hash",
                                fields={"email": "email", "name": "full_name"}),
            IdentityGraphSource(name="billing", entity="transactions", match_key_field="customer_email_hash",
                                fields={"email": "customer_email", "name": "customer_name"}),
        ],
        priority=["crm", "billing"],  # CRM wins when both have a value
    ),
)
```

Auto-generated columns: `is_{source}` flags, `{source}_id`, `first_seen_{source}` dates.

**AggregationSource** -- aggregate from another entity with GROUP BY and Ibis expressions:

```python
source=AggregationSource(
    source_entity="person",
    group_by_column="account_id",
    filter_expression="t.account_id.notnull()",
    aggregations=[
        ComputedColumn(name="num_persons", expression="t.person_id.nunique()"),
        ComputedColumn(name="first_seen", expression="t.created_at.min()"),
    ],
)
```

**EventAggregationSource** -- aggregate raw event streams (reads from a table, groups by a key):

```python
source=EventAggregationSource(
    project="myproject", dataset="raw", table="page_views",
    duckdb_path="page_views/*.parquet",
    group_by_column="user_id",
)
```

### SourceMapping

Decouple entity field names from source column names. Define a contract of `required_fields` on the entity, then map source columns to those fields:

```python
entity = Entity(
    name="transactions",
    description="Customer transactions",
    required_fields=[
        Field(name="transaction_id", type="STRING"),
        Field(name="amount_cents", type="INT64"),
    ],
    source=TableSource(project="p", dataset="d", table="orders", duckdb_path="orders/*.parquet"),
    layers=LayersConfig(prep=PrepLayer(model_name="prep_transactions")),
)

source_mapping = SourceMapping(
    entity=entity,
    source=entity.source,
    field_mappings={"transaction_id": "id", "amount_cents": "subtotal"},
)
```

Validates that all required fields have mappings at definition time.

### Multi-Entity Dependency Resolution

When entities depend on each other (DerivedSource, AggregationSource), `fyr run` automatically resolves the execution order using topological sort. Dependencies run first:

```
transactions, subscriptions   (TableSource -- no dependencies)
         |           |
         v           v
        person               (DerivedSource -- identity graph joins transactions + subscriptions)
           |
           v
        account              (AggregationSource -- groups person by account_id)
```

No manual ordering needed. Define your entities and Fyrnheim figures out the DAG.

### Primitives

Reusable Python functions that replace SQL snippets. Hashing, date operations, categorization -- import and compose them instead of copy-pasting SQL.

```python
from fyrnheim.primitives import hash_email, date_trunc_month

ComputedColumn(name="email_hash", expression=hash_email("email"))
ComputedColumn(name="signup_month", expression=date_trunc_month("created_at"))
```

### Components

Multi-column patterns that generate related fields from a single config. LifecycleFlags produces `is_active`, `is_churned`, `is_at_risk` from a status column. TimeBasedMetrics computes tenure and recency.

```python
from fyrnheim import LifecycleFlags

flags = LifecycleFlags(
    status_column="status",
    active_states=["active"],
    churned_states=["cancelled"],
)
```

### Quality Checks

Declarative data quality rules that run after transformations. Built-in checks include NotNull, Unique, InRange, InSet, MatchesPattern, and ForeignKey.

```python
quality=QualityConfig(
    primary_key="email_hash",
    checks=[
        NotNull("email"),
        Unique("email_hash"),
        InRange("amount_cents", min=0),
    ],
)
```

## Project Configuration

Configure your project with `fyrnheim.yaml` at the project root:

```yaml
entities_dir: entities
data_dir: data
output_dir: generated
backend: duckdb
backend_config:
  db_path: my_project.duckdb

# Push results to a separate output backend after fyr run
output_backend: clickhouse
output_config:
  host: localhost
  port: "8123"
  database: default
  user: default
  password: ""
```

All settings can be overridden via CLI flags. `fyr run --backend bigquery` runs on BigQuery regardless of what `fyrnheim.yaml` says.

## Production Deployment

A typical production pattern:

1. **Extract** raw data with DLT (or any EL tool) into parquet files or a warehouse
2. **Transform** with Fyrnheim: `fyr run --backend bigquery` (or duckdb for local)
3. **Push** results to an output backend for serving (ClickHouse, Postgres, etc.)

Configure the output backend in `fyrnheim.yaml`:

```yaml
backend: duckdb           # transform backend
output_backend: clickhouse # push dim/analytics tables here after run
output_config:
  host: ch.example.com
  port: "8123"
  database: analytics
```

This separation lets you develop locally on DuckDB while pushing production results to a fast query engine.

## Why Fyrnheim?

| | dbt | Fyrnheim |
|---|---|---|
| Language | SQL + Jinja | Python |
| Type safety | Runtime errors | Pydantic validation at definition time |
| Local dev | Requires warehouse connection | DuckDB on local parquet files |
| Backend portability | Dialect-specific SQL | Ibis compiles to 15+ backends |
| Testing | Custom schema tests | pytest + quality checks |
| Boilerplate | Jinja macros, YAML configs | Python functions, Pydantic models |
| Identity resolution | Manual SQL joins | Built-in identity graph (DerivedSource) |
| Multi-source union | Manual UNION ALL | UnionSource with field mapping |

Fyrnheim is not an orchestrator, not an extraction tool, and not a BI layer. It handles the transformation step: raw data in, clean business entities out.

## Status

- **Alpha** -- API may change before 1.0
- **DuckDB backend** -- fully supported
- **BigQuery backend** -- supported
- **ClickHouse output** -- supported as output sink
- **Python 3.11+** required

## License

MIT
