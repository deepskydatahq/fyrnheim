"""Tests for M028 ActivityDefinition and trigger models."""

import pytest
from pydantic import ValidationError

from fyrnheim.core.activity import (  # noqa: I001
    ActivityDefinition,
    EventOccurred,
    FieldChanged,
    RowAppeared,
    RowDisappeared,
)

# ---------------------------------------------------------------------------
# Story 1: Trigger type models
# ---------------------------------------------------------------------------


class TestRowAppeared:
    def test_validates_with_no_required_fields(self):
        trigger = RowAppeared()
        assert trigger.trigger_type == "row_appeared"

    def test_trigger_type_literal(self):
        trigger = RowAppeared()
        assert trigger.trigger_type == "row_appeared"


class TestFieldChanged:
    def test_validates_with_field(self):
        trigger = FieldChanged(field="plan")
        assert trigger.field == "plan"
        assert trigger.trigger_type == "field_changed"
        assert trigger.from_values is None
        assert trigger.to_values is None

    def test_with_from_and_to_values(self):
        trigger = FieldChanged(field="plan", from_values=["free"], to_values=["pro"])
        assert trigger.from_values == ["free"]
        assert trigger.to_values == ["pro"]

    def test_without_field_raises_validation_error(self):
        with pytest.raises(ValidationError):
            FieldChanged()  # type: ignore[call-arg]


class TestRowDisappeared:
    def test_validates_with_no_required_fields(self):
        trigger = RowDisappeared()
        assert trigger.trigger_type == "row_disappeared"


class TestEventOccurred:
    def test_validates_with_no_required_fields(self):
        trigger = EventOccurred()
        assert trigger.trigger_type == "event_occurred"
        assert trigger.event_type is None

    def test_with_event_type_filter(self):
        trigger = EventOccurred(event_type="page_view")
        assert trigger.event_type == "page_view"


# ---------------------------------------------------------------------------
# Story 2: ActivityDefinition model
# ---------------------------------------------------------------------------


class TestActivityDefinition:
    def test_with_row_appeared(self):
        ad = ActivityDefinition(
            name="signup", source="crm", trigger=RowAppeared(), entity_id_field="email"
        )
        assert ad.name == "signup"
        assert ad.source == "crm"
        assert isinstance(ad.trigger, RowAppeared)

    def test_with_field_changed(self):
        ad = ActivityDefinition(
            name="upgrade",
            source="crm",
            trigger=FieldChanged(field="plan", to_values=["pro"]),
            entity_id_field="user_id",
        )
        assert ad.name == "upgrade"
        assert isinstance(ad.trigger, FieldChanged)
        assert ad.trigger.field == "plan"

    def test_without_name_raises_validation_error(self):
        with pytest.raises(ValidationError):
            ActivityDefinition(source="crm", trigger=RowAppeared(), entity_id_field="email")  # type: ignore[call-arg]

    def test_empty_name_raises_validation_error(self):
        with pytest.raises(ValidationError):
            ActivityDefinition(
                name="", source="crm", trigger=RowAppeared(), entity_id_field="email"
            )

    def test_without_source_raises_validation_error(self):
        with pytest.raises(ValidationError):
            ActivityDefinition(name="signup", trigger=RowAppeared(), entity_id_field="email")  # type: ignore[call-arg]

    def test_empty_source_raises_validation_error(self):
        with pytest.raises(ValidationError):
            ActivityDefinition(
                name="signup", source="", trigger=RowAppeared(), entity_id_field="email"
            )

    def test_accepts_optional_include_fields(self):
        ad = ActivityDefinition(
            name="signup",
            source="crm",
            trigger=RowAppeared(),
            entity_id_field="email",
            include_fields=["email", "plan"],
        )
        assert ad.include_fields == ["email", "plan"]

    def test_include_fields_defaults_to_empty(self):
        ad = ActivityDefinition(
            name="signup", source="crm", trigger=RowAppeared(), entity_id_field="email"
        )
        assert ad.include_fields == []

    def test_requires_entity_id_field(self):
        with pytest.raises(ValidationError):
            ActivityDefinition(name="signup", source="crm", trigger=RowAppeared())  # type: ignore[call-arg]

    def test_accepts_person_id_field(self):
        ad = ActivityDefinition(
            name="signup",
            source="crm",
            trigger=RowAppeared(),
            entity_id_field="email",
            person_id_field="email_hash",
        )
        assert ad.person_id_field == "email_hash"

    def test_entity_id_field_must_not_be_empty(self):
        with pytest.raises(ValidationError):
            ActivityDefinition(
                name="signup",
                source="crm",
                trigger=RowAppeared(),
                entity_id_field="",
            )


class TestExports:
    def test_core_exports(self):
        from fyrnheim.core import (
            ActivityDefinition,
            EventOccurred,
            FieldChanged,
            RowAppeared,
            RowDisappeared,
        )

        assert ActivityDefinition is not None
        assert RowAppeared is not None
        assert FieldChanged is not None
        assert RowDisappeared is not None
        assert EventOccurred is not None

    def test_top_level_exports(self):
        from fyrnheim import (
            ActivityDefinition,
            EventOccurred,
            FieldChanged,
            RowAppeared,
            RowDisappeared,
        )

        assert ActivityDefinition is not None
        assert RowAppeared is not None
        assert FieldChanged is not None
        assert RowDisappeared is not None
        assert EventOccurred is not None
