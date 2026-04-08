"""Acquisition signal entity: deduplicated touchpoints per organization.

Depends on touchpoints entity. Uses dedup_by() to keep only the first
event per (organization_id, channel) combination, filtering out
direct/unknown traffic.
"""

from fyrnheim import (
    ComputedColumn,
    DimensionLayer,
    Entity,
    LayersConfig,
    PrepLayer,
)
from fyrnheim.components.expressions import dedup_by
from fyrnheim.core.source import AggregationSource

entity = Entity(
    name="acquisition_signal",
    description="Deduplicated acquisition signals per organization from touchpoints",
    source=AggregationSource(
        source_entity="touchpoints",
        group_by_column="organization_id",
        filter_expression="t.channel != 'direct_or_unknown'",
        aggregations=[
            ComputedColumn(
                name="first_event_time",
                expression="t.event_time.min()",
                description="Earliest event time for this org",
            ),
            ComputedColumn(
                name="last_event_time",
                expression="t.event_time.max()",
                description="Latest event time for this org",
            ),
            ComputedColumn(
                name="touchpoint_count",
                expression="t.amplitude_id.count()",
                description="Number of attributed touchpoints",
            ),
            ComputedColumn(
                name="distinct_channels",
                expression="t.channel.nunique()",
                description="Number of distinct channels seen",
            ),
        ],
    ),
    layers=LayersConfig(
        prep=PrepLayer(
            model_name="prep_acquisition_signal",
            computed_columns=[
                ComputedColumn(
                    name="rn",
                    expression=dedup_by("organization_id", "first_event_time"),
                    description="Row number for dedup (keep earliest signal per org)",
                ),
            ],
        ),
        dimension=DimensionLayer(model_name="dim_acquisition_signal"),
    ),
)
