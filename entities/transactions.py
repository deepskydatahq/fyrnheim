"""Transactions entity with SourceMapping for Lemonsqueezy data.

Maps Lemonsqueezy source columns (id, subtotal) to entity field names
(transaction_id, amount_cents). Feeds the person identity graph via
customer_email match key.
"""

from fyrnheim import (
    ComputedColumn,
    DimensionLayer,
    Entity,
    Field,
    LayersConfig,
    PrepLayer,
    SourceMapping,
    TableSource,
)
from fyrnheim.primitives import hash_email

entity = Entity(
    name="transactions",
    description="Customer transactions from Lemonsqueezy",
    required_fields=[
        Field(name="transaction_id", type="STRING", description="Primary key"),
        Field(
            name="customer_email",
            type="STRING",
            description="Customer email address",
        ),
        Field(
            name="amount_cents",
            type="INT64",
            description="Transaction amount in cents",
        ),
        Field(name="currency", type="STRING", description="Currency code"),
        Field(name="status", type="STRING", description="Transaction status"),
        Field(
            name="created_at",
            type="TIMESTAMP",
            description="Transaction creation time",
        ),
    ],
    optional_fields=[
        Field(name="store_id", type="INT64", description="Store/merchant ID"),
        Field(
            name="identifier",
            type="STRING",
            description="External order identifier",
        ),
        Field(name="order_number", type="INT64", description="Order number"),
        Field(name="customer_id", type="INT64", description="Customer ID"),
        Field(name="customer_name", type="STRING", description="Customer name"),
        Field(name="total", type="INT64", description="Total amount in cents"),
        Field(
            name="refunded",
            type="BOOLEAN",
            description="Whether transaction was refunded",
        ),
        Field(
            name="updated_at",
            type="TIMESTAMP",
            description="Last update time",
        ),
    ],
    core_computed=[
        ComputedColumn(
            name="customer_email_hash",
            expression=hash_email("customer_email"),
            description="Hashed email for identity resolution",
        ),
    ],
    source=TableSource(
        project="deepskydata",
        dataset="timodata_sources",
        table="transactions",
        duckdb_path="transactions/*.parquet",
        fields=[
            Field(name="id", type="STRING", description="Transaction ID"),
            Field(name="store_id", type="INT64", description="Lemonsqueezy store ID"),
            Field(name="identifier", type="STRING", description="Order identifier"),
            Field(name="order_number", type="INT64", description="Order number"),
            Field(name="status", type="STRING", description="Transaction status"),
            Field(name="customer_id", type="INT64", description="Customer ID"),
            Field(name="customer_name", type="STRING", description="Customer name"),
            Field(
                name="customer_email",
                type="STRING",
                description="Customer email address",
            ),
            Field(name="total", type="INT64", description="Total amount in cents"),
            Field(name="subtotal", type="INT64", description="Subtotal in cents"),
            Field(name="currency", type="STRING", description="Currency code"),
            Field(
                name="refunded",
                type="BOOLEAN",
                description="Whether transaction was refunded",
            ),
            Field(
                name="created_at",
                type="TIMESTAMP",
                description="Transaction creation time",
            ),
            Field(
                name="updated_at",
                type="TIMESTAMP",
                description="Last update time",
            ),
        ],
    ),
    layers=LayersConfig(
        prep=PrepLayer(model_name="prep_transactions"),
        dimension=DimensionLayer(model_name="dim_transactions"),
    ),
)

source_mapping = SourceMapping(
    entity=entity,
    source=entity.source,
    field_mappings={
        # Required fields
        "transaction_id": "id",
        "customer_email": "customer_email",
        "amount_cents": "subtotal",
        "currency": "currency",
        "status": "status",
        "created_at": "created_at",
        # Optional fields
        "store_id": "store_id",
        "identifier": "identifier",
        "order_number": "order_number",
        "customer_id": "customer_id",
        "customer_name": "customer_name",
        "total": "total",
        "refunded": "refunded",
        "updated_at": "updated_at",
    },
)
