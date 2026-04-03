"""Sample customer pipeline -- edit or replace this with your own.

Demonstrates: StateSource, ActivityDefinition, IdentityGraph, EntityModel.
"""

from fyrnheim import (
    ActivityDefinition,
    ComputedColumn,
    EntityModel,
    FieldChanged,
    IdentityGraph,
    IdentitySource,
    RowAppeared,
    StateField,
    StateSource,
)

# 1. Source -- a slowly-changing state table
crm_source = StateSource(
    name="crm_contacts",
    project="example",
    dataset="raw",
    table="customers",
    id_field="id",
)

# 2. Activity Definitions -- named business events detected from state changes
signup = ActivityDefinition(
    name="signup",
    source="crm_contacts",
    trigger=RowAppeared(),
    entity_id_field="id",
)

became_paying = ActivityDefinition(
    name="became_paying",
    source="crm_contacts",
    trigger=FieldChanged(field="plan", to_values=["pro", "starter", "enterprise"]),
    entity_id_field="id",
)

# 3. Identity Graph -- cross-source identity resolution
customer_identity = IdentityGraph(
    name="customer_identity",
    canonical_id="customer_id",
    sources=[
        IdentitySource(
            source="crm_contacts",
            id_field="id",
            match_key_field="email",
        ),
    ],
)

# 4. Entity Model -- derived current state
customers = EntityModel(
    name="customers",
    identity_graph="customer_identity",
    state_fields=[
        StateField(name="email", source="crm_contacts", field="email", strategy="latest"),
        StateField(name="name", source="crm_contacts", field="name", strategy="latest"),
        StateField(name="plan", source="crm_contacts", field="plan", strategy="latest"),
    ],
    computed_fields=[
        ComputedColumn(name="is_paying", expression="plan != 'free'"),
    ],
)
