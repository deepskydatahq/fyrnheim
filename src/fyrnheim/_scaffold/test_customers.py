"""Example test -- validates the customer pipeline definitions.

Run with: pytest tests/
"""

from entities.customers import (
    became_paying,
    crm_source,
    customer_identity,
    customers,
    signup,
)


def test_source_defined() -> None:
    """Verify that the source is configured."""
    assert crm_source.name == "crm_contacts"
    assert crm_source.id_field == "id"


def test_activities_defined() -> None:
    """Verify that activity definitions exist."""
    assert signup.name == "signup"
    assert became_paying.name == "became_paying"


def test_identity_graph_defined() -> None:
    """Verify that the identity graph is configured."""
    assert customer_identity.name == "customer_identity"
    assert len(customer_identity.sources) == 1


def test_analytics_entity_defined() -> None:
    """Verify that the analytics entity is configured."""
    assert customers.name == "customers"
    assert len(customers.state_fields) >= 3
    assert len(customers.computed_fields) >= 1
