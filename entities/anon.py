"""Anon entity: anonymous visitor sessions from Walker.

Single TableSource (NOT UnionSource). Tracks anonymous visitor sessions
with computed columns for anon_id (hash), source literal, and
channel_category (categorize_contains on referrer).
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
from fyrnheim.primitives import categorize_contains

entity = Entity(
    name="anon",
    description="Anonymous visitor sessions from Walker web analytics",
    source=TableSource(
        project="deepskydata",
        dataset="timodata_sources",
        table="walker_events",
        duckdb_path="walker_events/*.parquet",
        fields=[
            Field(name="session_id", type="STRING", description="Session identifier"),
            Field(name="timestamp", type="TIMESTAMP", description="Event timestamp"),
            Field(name="referrer", type="STRING", description="HTTP referrer URL"),
            Field(name="event_name", type="STRING", description="Event name"),
            Field(name="page_path", type="STRING", description="Page path visited"),
        ],
    ),
    layers=LayersConfig(
        prep=PrepLayer(model_name="prep_anon"),
        dimension=DimensionLayer(
            model_name="dim_anon",
            computed_columns=[
                ComputedColumn(
                    name="anon_id",
                    expression='(ibis.literal("walker") + t.session_id).hash().cast("string")',
                    description="Anonymous visitor identity hash",
                ),
                ComputedColumn(
                    name="source",
                    expression='ibis.literal("walker")',
                    description="Source platform tag",
                ),
                ComputedColumn(
                    name="channel_category",
                    expression=categorize_contains(
                        "referrer",
                        {
                            "social_linkedin": ["linkedin.com", "lnkd.in"],
                            "social_youtube": ["youtube.com"],
                            "newsletter": [
                                "mail.google",
                                "ghost.io",
                                "substack.com",
                                "mailerlite.com",
                                "convertkit.com",
                                "beehiiv.com",
                            ],
                            "seo": ["google.com", "bing.com", "duckduckgo.com", "ecosia.org"],
                            "ai": ["chatgpt.com", "perplexity.ai", "claude.ai"],
                        },
                        default="direct",
                    ),
                    description="Channel category based on referrer URL patterns",
                ),
            ],
        ),
    ),
)
