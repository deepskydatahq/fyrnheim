"""Account entity: aggregation from person by business email domain.

Uses AggregationSource to GROUP BY account_id from dim_person.
Filters out personal email domains (account_id IS NULL).
Aggregations: person count, source presence flags, first-seen date.
"""

from fyrnheim import (
    ComputedColumn,
    DimensionLayer,
    Entity,
    LayersConfig,
    PrepLayer,
)
from fyrnheim.core.source import AggregationSource

entity = Entity(
    name="account",
    description="Business account aggregated from person identity graph",
    source=AggregationSource(
        source_entity="person",
        group_by_column="account_id",
        filter_expression="t.account_id.notnull()",
        aggregations=[
            ComputedColumn(
                name="email_domain",
                expression="t.email_domain.arbitrary()",
                description="Representative email domain for the account",
            ),
            ComputedColumn(
                name="num_persons",
                expression="t.person_id.nunique()",
                description="Count of distinct persons in this account",
            ),
            ComputedColumn(
                name="has_ghost_person",
                expression="t.is_ghost_person.any()",
                description="True if any person sourced from Ghost",
            ),
            ComputedColumn(
                name="has_mailerlite_person",
                expression="t.is_mailerlite_person.any()",
                description="True if any person sourced from MailerLite",
            ),
            ComputedColumn(
                name="has_transactions",
                expression="t.is_transactions.any()",
                description="True if any person has transactions",
            ),
            ComputedColumn(
                name="has_subscriptions",
                expression="t.is_subscriptions.any()",
                description="True if any person has subscriptions",
            ),
            ComputedColumn(
                name="first_seen_date",
                expression="t.created_at.min()",
                description="Earliest person creation date in this account",
            ),
        ],
    ),
    layers=LayersConfig(
        prep=PrepLayer(model_name="prep_account"),
        dimension=DimensionLayer(model_name="dim_account"),
    ),
)
