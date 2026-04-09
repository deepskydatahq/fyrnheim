"""Example: Activities-first customer pipeline.

Demonstrates: StateSource -> SnapshotDiff -> ActivityDefinition -> IdentityGraph -> AnalyticsEntity
"""

from fyrnheim.core import (
    ActivityDefinition,
    AnalyticsEntity,
    ComputedColumn,
    EventOccurred,
    EventSource,
    FieldChanged,
    IdentityGraph,
    IdentitySource,
    Measure,
    RowAppeared,
    StateField,
    StateSource,
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
    trigger=EventOccurred(event_type="purchase"),
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

# 4. Analytics Entity
customers = AnalyticsEntity(
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
    measures=[
        Measure(name="purchase_count", activity="purchase", aggregation="count"),
        Measure(name="total_spent", activity="purchase", aggregation="sum", field="amount"),
    ],
    computed_fields=[
        ComputedColumn(name="is_paying", expression="plan != 'free'"),
    ],
)
