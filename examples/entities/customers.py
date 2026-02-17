"""Sample customers entity demonstrating the full fyrnheim workflow.

This entity transforms raw customer records through two layers:
- PrepLayer: email hashing, date casting, unit conversion
- DimensionLayer: business logic columns (email domain, paying flag, signup cohort)

Quality checks validate the output: NotNull, Unique, InRange.
"""

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
from fyrnheim.primitives import date_trunc_month, hash_email

entity = Entity(
    name="customers",
    description="Sample customer entity for fyrnheim demonstration",
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
                    name="created_date",
                    expression='t.created_at.cast("date")',
                    description="Account creation date (date only)",
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
                ComputedColumn(
                    name="signup_month",
                    expression=date_trunc_month("created_at"),
                    description="Signup month for cohort analysis",
                ),
            ],
        ),
    ),
    quality=QualityConfig(
        primary_key="email_hash",
        checks=[
            NotNull("email"),
            NotNull("id"),
            Unique("email_hash"),
            InRange("amount_cents", min=0),
        ],
    ),
)
