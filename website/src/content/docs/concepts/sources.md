---
title: Sources
description: Connect entities to data using TableSource, UnionSource, DerivedSource, and more.
---

Fyrnheim supports multiple source types for different data patterns. Each entity declares a source that tells Fyrnheim where to read data from.

## TableSource

Read from a single warehouse table or local parquet file:

```python
from fyrnheim import TableSource

source = TableSource(
    project="myproject",
    dataset="raw",
    table="customers",
    duckdb_path="data/customers.parquet",  # local dev
)
```

The `duckdb_path` field is used for local development with DuckDB. The `project`, `dataset`, and `table` fields are used for warehouse backends like BigQuery.

## UnionSource

Combine multiple sources into a common schema. Each sub-source can remap columns with `field_mappings` and inject constants with `literal_columns`:

```python
from fyrnheim import UnionSource, TableSource

source = UnionSource(
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

## DerivedSource

Build identity graphs by joining multiple entities on a shared key. Uses cascading FULL OUTER JOIN with priority-based field resolution:

```python
from fyrnheim import DerivedSource, IdentityGraphConfig, IdentityGraphSource

source = DerivedSource(
    identity_graph="person_graph",
    identity_graph_config=IdentityGraphConfig(
        match_key="email_hash",
        sources=[
            IdentityGraphSource(
                name="crm",
                entity="crm_contacts",
                match_key_field="email_hash",
                fields={"email": "email", "name": "full_name"},
            ),
            IdentityGraphSource(
                name="billing",
                entity="transactions",
                match_key_field="customer_email_hash",
                fields={"email": "customer_email", "name": "customer_name"},
            ),
        ],
        priority=["crm", "billing"],  # CRM wins when both have a value
    ),
)
```

Auto-generated columns: `is_{source}` flags, `{source}_id`, `first_seen_{source}` dates.

## AggregationSource

Aggregate from another entity with GROUP BY and Ibis expressions:

```python
from fyrnheim import AggregationSource, ComputedColumn

source = AggregationSource(
    source_entity="person",
    group_by_column="account_id",
    filter_expression="t.account_id.notnull()",
    aggregations=[
        ComputedColumn(name="num_persons", expression="t.person_id.nunique()"),
        ComputedColumn(name="first_seen", expression="t.created_at.min()"),
    ],
)
```

## EventAggregationSource

Aggregate raw event streams. Reads from a table and groups by a key:

```python
from fyrnheim import EventAggregationSource

source = EventAggregationSource(
    project="myproject",
    dataset="raw",
    table="page_views",
    duckdb_path="page_views/*.parquet",
    group_by_column="user_id",
)
```
