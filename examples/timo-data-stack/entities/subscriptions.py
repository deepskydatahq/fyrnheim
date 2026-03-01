"""Subscriptions entity with SourceMapping for Lemonsqueezy data.

Maps Lemonsqueezy source columns (id) to entity field names (subscription_id).
Includes lifecycle flags (is_active, is_churned). Feeds the person identity
graph via user_email match key.
"""

from fyrnheim import (
    ComputedColumn,
    DimensionLayer,
    Entity,
    Field,
    LayersConfig,
    LifecycleFlags,
    PrepLayer,
    SourceMapping,
    TableSource,
)
from fyrnheim.primitives import hash_email

entity = Entity(
    name="subscriptions",
    description="Customer subscriptions from Lemonsqueezy",
    required_fields=[
        Field(
            name="subscription_id", type="STRING", description="Primary key"
        ),
        Field(
            name="user_email",
            type="STRING",
            description="Subscriber email address",
        ),
        Field(name="status", type="STRING", description="Subscription status"),
        Field(
            name="created_at",
            type="TIMESTAMP",
            description="Subscription creation time",
        ),
    ],
    optional_fields=[
        Field(name="store_id", type="INT64", description="Store/merchant ID"),
        Field(name="product_id", type="INT64", description="Product ID"),
        Field(name="variant_id", type="INT64", description="Variant ID"),
        Field(name="user_name", type="STRING", description="Subscriber name"),
        Field(
            name="renews_at",
            type="TIMESTAMP",
            description="Next renewal date",
        ),
        Field(
            name="ends_at",
            type="TIMESTAMP",
            description="Subscription end date",
        ),
        Field(
            name="cancelled", type="BOOLEAN", description="Whether cancelled"
        ),
        Field(
            name="billing_anchor",
            type="INT64",
            description="Day of month for billing",
        ),
        Field(
            name="card_brand", type="STRING", description="Payment card brand"
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
            expression=hash_email("user_email"),
            description="Hashed email for identity resolution",
        ),
        *LifecycleFlags(
            status_column="status",
            active_states=["active", "on_trial"],
            churned_states=["cancelled", "expired", "unpaid"],
        ).to_computed_columns(),
    ],
    source=TableSource(
        project="deepskydata",
        dataset="timodata_sources",
        table="subscriptions",
        duckdb_path="subscriptions/*.parquet",
        fields=[
            Field(name="id", type="STRING", description="Subscription ID"),
            Field(
                name="store_id",
                type="INT64",
                description="Lemonsqueezy store ID",
            ),
            Field(name="product_id", type="INT64", description="Product ID"),
            Field(name="variant_id", type="INT64", description="Variant ID"),
            Field(
                name="status",
                type="STRING",
                description="Subscription status",
            ),
            Field(
                name="user_email",
                type="STRING",
                description="Subscriber email",
            ),
            Field(
                name="user_name",
                type="STRING",
                description="Subscriber name",
            ),
            Field(
                name="renews_at",
                type="TIMESTAMP",
                description="Next renewal",
            ),
            Field(name="ends_at", type="TIMESTAMP", description="End date"),
            Field(
                name="cancelled",
                type="BOOLEAN",
                description="Cancelled flag",
            ),
            Field(
                name="billing_anchor",
                type="INT64",
                description="Billing day",
            ),
            Field(
                name="card_brand", type="STRING", description="Card brand"
            ),
            Field(
                name="created_at",
                type="TIMESTAMP",
                description="Created time",
            ),
            Field(
                name="updated_at",
                type="TIMESTAMP",
                description="Updated time",
            ),
        ],
    ),
    layers=LayersConfig(
        prep=PrepLayer(model_name="prep_subscriptions"),
        dimension=DimensionLayer(model_name="dim_subscriptions"),
    ),
)

source_mapping = SourceMapping(
    entity=entity,
    source=entity.source,
    field_mappings={
        # Required fields
        "subscription_id": "id",
        "user_email": "user_email",
        "status": "status",
        "created_at": "created_at",
        # Optional fields
        "store_id": "store_id",
        "product_id": "product_id",
        "variant_id": "variant_id",
        "user_name": "user_name",
        "renews_at": "renews_at",
        "ends_at": "ends_at",
        "cancelled": "cancelled",
        "billing_anchor": "billing_anchor",
        "card_brand": "card_brand",
        "updated_at": "updated_at",
    },
)
