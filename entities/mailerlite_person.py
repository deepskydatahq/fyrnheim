"""Internal entity for MailerLite subscribers.

Feeds the person identity graph. The dim table provides email (match key)
and email_hash for downstream identity resolution.
"""

from fyrnheim import (
    ComputedColumn,
    DimensionLayer,
    Entity,
    Field,
    LayersConfig,
    PrepLayer,
    TableSource,
)
from fyrnheim.primitives import hash_email

entity = Entity(
    name="mailerlite_person",
    description="Internal: MailerLite subscribers mapped to person schema",
    is_internal=True,
    source=TableSource(
        project="deepskydata",
        dataset="timodata_sources",
        table="subscribers",
        duckdb_path="mailerlite_subscribers/*.parquet",
        fields=[
            Field(name="id", type="STRING", description="Subscriber ID"),
            Field(name="email", type="STRING", description="Email address"),
            Field(
                name="status", type="STRING", description="Subscription status"
            ),
            Field(
                name="subscribed_at",
                type="TIMESTAMP",
                description="Subscription date",
            ),
            Field(
                name="source", type="STRING", description="Lead source/channel"
            ),
        ],
    ),
    layers=LayersConfig(
        prep=PrepLayer(
            model_name="prep_mailerlite_person",
        ),
        dimension=DimensionLayer(
            model_name="dim_mailerlite_person",
            computed_columns=[
                ComputedColumn(
                    name="email_hash",
                    expression=hash_email("email"),
                    description="SHA256 hash of normalized email for identity matching",
                ),
                ComputedColumn(
                    name="created_at",
                    expression="t.subscribed_at",
                    description="When person first appeared in MailerLite",
                ),
            ],
        ),
    ),
)
