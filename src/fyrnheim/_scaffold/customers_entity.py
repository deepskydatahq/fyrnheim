"""Sample customers entity -- edit or replace this with your own.

Demonstrates: prep, dimension, snapshot, and activity layers.
"""

from fyrnheim import (
    ActivityConfig,
    ActivityType,
    ComputedColumn,
    DimensionLayer,
    Entity,
    InRange,
    LayersConfig,
    NotNull,
    PrepLayer,
    QualityConfig,
    SnapshotLayer,
    TableSource,
    Unique,
)
from fyrnheim.primitives import hash_email

entity = Entity(
    name="customers",
    description="Sample customer entity",
    source=TableSource(
        project="example",
        dataset="raw",
        table="customers",
        duckdb_path="customers.parquet",
    ),
    layers=LayersConfig(
        prep=PrepLayer(
            model_name="prep_customers",
            computed_columns=[
                ComputedColumn(
                    name="email_hash",
                    expression=hash_email("email"),
                    description="SHA256 hash of lowercase trimmed email",
                ),
                ComputedColumn(
                    name="amount_dollars",
                    expression="t.amount_cents / 100.0",
                    description="Monthly payment in dollars",
                ),
            ],
        ),
        dimension=DimensionLayer(
            model_name="dim_customers",
            computed_columns=[
                ComputedColumn(
                    name="email_domain",
                    expression="t.email.split('@')[1]",
                    description="Email domain extracted from address",
                ),
                ComputedColumn(
                    name="is_paying",
                    expression="t.plan != 'free'",
                    description="True if customer is on a paid plan",
                ),
            ],
        ),
        snapshot=SnapshotLayer(
            natural_key="email_hash",
            deduplication_order_by="created_at DESC",
        ),
        activity=ActivityConfig(
            model_name="activity_customers",
            types=[
                ActivityType(
                    name="signup",
                    trigger="row_appears",
                    timestamp_field="created_at",
                ),
                ActivityType(
                    name="became_paying",
                    trigger="status_becomes",
                    timestamp_field="created_at",
                    field="plan",
                    values=["pro", "starter", "enterprise"],
                ),
            ],
            entity_id_field="id",
            person_id_field="email_hash",
        ),
    ),
    quality=QualityConfig(
        primary_key="email_hash",
        checks=[
            NotNull("email"),
            Unique("email_hash"),
            InRange("amount_cents", min=0),
        ],
    ),
)
