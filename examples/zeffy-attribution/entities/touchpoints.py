"""Touchpoints entity: amplitude events with channel classification.

Reads amplitude events parquet, extracts JSON fields (gclid, fbclid, utm_*,
referring_domain, organization_id), and classifies each event into a marketing
channel using CaseColumn.
"""

from fyrnheim import (
    ComputedColumn,
    DimensionLayer,
    Entity,
    LayersConfig,
    PrepLayer,
    TableSource,
)
from fyrnheim.components.expressions import CaseColumn, isin_literal
from fyrnheim.primitives import json_extract_scalar

entity = Entity(
    name="touchpoints",
    description="Amplitude events enriched with channel classification for attribution",
    source=TableSource(
        project="zeffy",
        dataset="amplitude",
        table="events",
        duckdb_path="examples/zeffy-attribution/data/amplitude/events/*.parquet",
    ),
    layers=LayersConfig(
        prep=PrepLayer(
            model_name="prep_touchpoints",
            computed_columns=[
                ComputedColumn(
                    name="gclid",
                    expression=json_extract_scalar("event_properties", "gclid"),
                    description="Google Click ID from event properties",
                ),
                ComputedColumn(
                    name="fbclid",
                    expression=json_extract_scalar("event_properties", "fbclid"),
                    description="Facebook Click ID from event properties",
                ),
                ComputedColumn(
                    name="utm_source",
                    expression=json_extract_scalar("event_properties", "utm_source"),
                    description="UTM source from event properties",
                ),
                ComputedColumn(
                    name="utm_medium",
                    expression=json_extract_scalar("event_properties", "utm_medium"),
                    description="UTM medium from event properties",
                ),
                ComputedColumn(
                    name="utm_campaign",
                    expression=json_extract_scalar("event_properties", "utm_campaign"),
                    description="UTM campaign from event properties",
                ),
                ComputedColumn(
                    name="referring_domain",
                    expression=json_extract_scalar("event_properties", "referring_domain"),
                    description="Referring domain from event properties",
                ),
                ComputedColumn(
                    name="form_type",
                    expression=json_extract_scalar("event_properties", "form_type"),
                    description="Form type from event properties",
                ),
                ComputedColumn(
                    name="organization_id",
                    expression=json_extract_scalar("user_properties", "Organization"),
                    description="Organization ID from user properties",
                ),
            ],
        ),
        dimension=DimensionLayer(
            model_name="dim_touchpoints",
            computed_columns=[
                CaseColumn(
                    name="channel",
                    cases=[
                        ("t.gclid.notnull()", "paid_search_google"),
                        ("t.fbclid.notnull()", "paid_social_meta"),
                        (
                            isin_literal("t.utm_medium", ["cpc", "ppc", "paid", "paid_social"]),
                            "paid_other",
                        ),
                        ("t.utm_source.notnull()", "organic_campaign"),
                        ("t.referring_domain.notnull()", "organic_referral"),
                    ],
                    default="direct_or_unknown",
                    description="Marketing channel classification based on attribution signals",
                ),
            ],
        ),
    ),
)
