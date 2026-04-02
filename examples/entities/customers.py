"""Example: Activities-first customer pipeline.

Demonstrates: StateSource -> SnapshotDiff -> ActivityDefinition -> IdentityGraph -> EntityModel
"""

from fyrnheim.core import (
    ActivityDefinition,
    ComputedColumn,
    EntityModel,
    EventOccurred,
    EventSource,
    FieldChanged,
    IdentityGraph,
    IdentitySource,
    RowAppeared,
    StateField,
    StateSource,
    StreamAnalyticsModel,
    StreamMetric,
)

# 1. Sources
crm_source = StateSource(
    name="crm_contacts",
    project="my_project",
    dataset="crm",
    table="contacts",
    id_field="contact_id",
)

billing_source = EventSource(
    name="billing_events",
    project="my_project",
    dataset="billing",
    table="transactions",
    entity_id_field="customer_id",
    timestamp_field="created_at",
    event_type_field="event_type",
)

# 2. Activity Definitions
signup = ActivityDefinition(
    name="signup",
    source="crm_contacts",
    trigger=RowAppeared(),
)

became_paying = ActivityDefinition(
    name="became_paying",
    source="crm_contacts",
    trigger=FieldChanged(field="plan", to_values=["pro", "enterprise"]),
)

purchase = ActivityDefinition(
    name="purchase",
    source="billing_events",
    trigger=EventOccurred(event_types=["purchase"]),
)

# 3. Identity Graph
customer_identity = IdentityGraph(
    name="customer_identity",
    canonical_id="customer_id",
    sources=[
        IdentitySource(
            source="crm_contacts",
            id_field="contact_id",
            match_key_field="email_hash",
        ),
        IdentitySource(
            source="billing_events",
            id_field="customer_id",
            match_key_field="email_hash",
        ),
    ],
)

# 4. Entity Model
customers = EntityModel(
    name="customers",
    identity_graph="customer_identity",
    state_fields=[
        StateField(name="email", source="crm_contacts", field="email", strategy="latest"),
        StateField(name="name", source="crm_contacts", field="full_name", strategy="latest"),
        StateField(name="plan", source="crm_contacts", field="plan", strategy="latest"),
        StateField(
            name="first_seen", source="crm_contacts", field="created_at", strategy="first"
        ),
    ],
    computed_fields=[
        ComputedColumn(name="is_paying", expression="plan != 'free'"),
    ],
)

# 5. Analytics Model
daily_metrics = StreamAnalyticsModel(
    name="customer_metrics_daily",
    identity_graph="customer_identity",
    date_grain="daily",
    metrics=[
        StreamMetric(
            name="new_signups",
            expression="count()",
            event_filter="signup",
            metric_type="count",
        ),
        StreamMetric(
            name="total_customers",
            expression="count()",
            metric_type="snapshot",
        ),
    ],
)
