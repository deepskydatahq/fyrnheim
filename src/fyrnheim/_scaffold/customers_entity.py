"""Sample customers entity -- edit or replace this with your own."""

from fyrnheim import (
    ComputedColumn,
    DimensionLayer,
    Entity,
    InRange,
    LayersConfig,
    NotNull,
    PrepLayer,
    QualityConfig,
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
