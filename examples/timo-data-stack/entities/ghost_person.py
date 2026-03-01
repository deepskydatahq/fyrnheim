"""Internal entity for Ghost members.

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
    name="ghost_person",
    description="Internal: Ghost members mapped to person schema",
    is_internal=True,
    source=TableSource(
        project="deepskydata",
        dataset="timodata_sources",
        table="members",
        duckdb_path="ghost_members/*.parquet",
        fields=[
            Field(name="id", type="STRING", description="Member ID"),
            Field(name="email", type="STRING", description="Email address"),
            Field(
                name="status",
                type="STRING",
                description="Member status (free, paid, comped)",
            ),
            Field(name="name", type="STRING", description="Member name"),
            Field(
                name="created_at",
                type="TIMESTAMP",
                description="Member creation date",
            ),
            Field(
                name="email_disabled",
                type="BOOLEAN",
                description="Email disabled flag",
            ),
        ],
    ),
    layers=LayersConfig(
        prep=PrepLayer(
            model_name="prep_ghost_person",
        ),
        dimension=DimensionLayer(
            model_name="dim_ghost_person",
            computed_columns=[
                ComputedColumn(
                    name="email_hash",
                    expression=hash_email("email"),
                    description="SHA256 hash of normalized email for identity matching",
                ),
            ],
        ),
    ),
)
