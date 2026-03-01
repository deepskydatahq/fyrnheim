"""Signals entity: unified engagement signals (walker + shortio + ghost).

Uses UnionSource to combine three engagement signal sources. Each sub-source
normalizes timestamps to signal_timestamp and injects source/signal_type tags
via literal_columns. Walker uses event_name as signal_type (not literal).
"""

from fyrnheim import (
    ComputedColumn,
    DimensionLayer,
    Entity,
    LayersConfig,
    PrepLayer,
    TableSource,
    UnionSource,
)
from fyrnheim.primitives import concat_hash, hash_email

entity = Entity(
    name="signals",
    description="Unified engagement signals from walker, shortio, and ghost",
    source=UnionSource(
        sources=[
            TableSource(
                project="deepskydata",
                dataset="timodata_sources",
                table="walker_events",
                duckdb_path="walker_events/*.parquet",
                field_mappings={
                    "timestamp": "signal_timestamp",
                },
                literal_columns={
                    "source": "walker",
                },
            ),
            TableSource(
                project="deepskydata",
                dataset="timodata_sources",
                table="shortio_clicks",
                duckdb_path="shortio_clicks/*.parquet",
                field_mappings={
                    "clicked_at": "signal_timestamp",
                    "utm_source": "channel_source",
                    "utm_medium": "channel_medium",
                    "utm_campaign": "campaign",
                },
                literal_columns={
                    "source": "shortio",
                    "signal_type": "link_clicked",
                },
            ),
            TableSource(
                project="deepskydata",
                dataset="timodata_sources",
                table="ghost_members",
                duckdb_path="ghost_members/*.parquet",
                field_mappings={
                    "created_at": "signal_timestamp",
                },
                literal_columns={
                    "source": "ghost",
                    "signal_type": "newsletter_subscribed",
                },
            ),
        ],
    ),
    layers=LayersConfig(
        prep=PrepLayer(model_name="prep_signals"),
        dimension=DimensionLayer(
            model_name="dim_signals",
            computed_columns=[
                ComputedColumn(
                    name="person_id",
                    expression=hash_email("email"),
                    description="Person identity key (null for anonymous walker/shortio signals)",
                ),
                ComputedColumn(
                    name="signal_id",
                    expression=concat_hash(
                        "email", "session_id", "signal_timestamp", "signal_type", "source"
                    ),
                    description="Unique signal identifier",
                ),
            ],
        ),
    ),
)
