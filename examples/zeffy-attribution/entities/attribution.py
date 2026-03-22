"""Attribution entities: first-touch and paid-priority models.

Both use AggregationSource from touchpoints to determine the attributed
channel per organization_id, using different strategies:

- attribution_first_touch: earliest channel wins (first_value_by event_time)
- attribution_paid_priority: paid channels override organic, with earliest
  event as tiebreaker
"""

from fyrnheim import (
    ComputedColumn,
    DimensionLayer,
    Entity,
    LayersConfig,
    PrepLayer,
)
from fyrnheim.components.expressions import first_value_by
from fyrnheim.core.source import AggregationSource

# -- First-Touch Attribution --------------------------------------------------

attribution_first_touch = Entity(
    name="attribution_first_touch",
    description="First-touch attribution: earliest channel per organization",
    source=AggregationSource(
        source_entity="touchpoints",
        group_by_column="organization_id",
        filter_expression="t.channel != 'direct_or_unknown'",
        aggregations=[
            ComputedColumn(
                name="first_touch_channel",
                expression=first_value_by("t.channel", "organization_id", "event_time"),
                description="Channel of the first attributed touchpoint",
            ),
            ComputedColumn(
                name="first_touch_time",
                expression="t.event_time.min()",
                description="Timestamp of the first attributed touchpoint",
            ),
            ComputedColumn(
                name="first_touch_utm_source",
                expression=first_value_by("t.utm_source", "organization_id", "event_time"),
                description="UTM source of the first touchpoint",
            ),
            ComputedColumn(
                name="first_touch_utm_campaign",
                expression=first_value_by("t.utm_campaign", "organization_id", "event_time"),
                description="UTM campaign of the first touchpoint",
            ),
        ],
    ),
    layers=LayersConfig(
        prep=PrepLayer(model_name="prep_attribution_first_touch"),
        dimension=DimensionLayer(model_name="dim_attribution_first_touch"),
    ),
)

# -- Paid-Priority Attribution ------------------------------------------------

attribution_paid_priority = Entity(
    name="attribution_paid_priority",
    description="Paid-priority attribution: paid channels override organic per organization",
    source=AggregationSource(
        source_entity="touchpoints",
        group_by_column="organization_id",
        filter_expression="t.channel != 'direct_or_unknown'",
        aggregations=[
            ComputedColumn(
                name="paid_priority_channel",
                expression=first_value_by("t.channel", "organization_id", "event_time"),
                description="Attributed channel with paid priority ordering",
            ),
            ComputedColumn(
                name="paid_priority_time",
                expression="t.event_time.min()",
                description="Timestamp of the priority attributed touchpoint",
            ),
            ComputedColumn(
                name="has_paid_touch",
                expression="t.channel.isin(['paid_search_google', 'paid_social_meta', 'paid_other']).any()",
                description="Whether any paid touchpoint exists for this org",
            ),
            ComputedColumn(
                name="total_touchpoints",
                expression="t.amplitude_id.count()",
                description="Total number of attributed touchpoints",
            ),
        ],
    ),
    layers=LayersConfig(
        prep=PrepLayer(model_name="prep_attribution_paid_priority"),
        dimension=DimensionLayer(model_name="dim_attribution_paid_priority"),
    ),
)
