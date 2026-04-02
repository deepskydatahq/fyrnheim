"""Tests for EntityModel and StateField Pydantic models."""

import pytest
from pydantic import ValidationError

from fyrnheim.components.computed_column import ComputedColumn
from fyrnheim.core import EntityModel, StateField


class TestStateField:
    def test_latest_strategy_validates(self):
        sf = StateField(name="email", source="crm", field="email", strategy="latest")
        assert sf.strategy == "latest"
        assert sf.priority is None

    def test_first_strategy_validates(self):
        sf = StateField(name="email", source="crm", field="email", strategy="first")
        assert sf.strategy == "first"

    def test_coalesce_with_priority_validates(self):
        sf = StateField(
            name="email",
            source="crm",
            field="email",
            strategy="coalesce",
            priority=["crm", "billing"],
        )
        assert sf.strategy == "coalesce"
        assert sf.priority == ["crm", "billing"]

    def test_coalesce_without_priority_raises(self):
        with pytest.raises(ValidationError, match="coalesce strategy requires a priority list"):
            StateField(
                name="email", source="crm", field="email", strategy="coalesce"
            )

    def test_coalesce_with_empty_priority_raises(self):
        with pytest.raises(ValidationError, match="coalesce strategy requires a priority list"):
            StateField(
                name="email",
                source="crm",
                field="email",
                strategy="coalesce",
                priority=[],
            )

    def test_latest_ignores_priority(self):
        sf = StateField(
            name="email",
            source="crm",
            field="email",
            strategy="latest",
            priority=["crm"],
        )
        assert sf.priority == ["crm"]  # no error raised

    def test_invalid_strategy_raises(self):
        with pytest.raises(ValidationError):
            StateField(name="email", source="crm", field="email", strategy="invalid")

    def test_empty_name_raises(self):
        with pytest.raises(ValidationError):
            StateField(name="", source="crm", field="email", strategy="latest")


class TestEntityModel:
    def _make_state_field(self, **overrides):
        defaults = {"name": "email", "source": "crm", "field": "email", "strategy": "latest"}
        defaults.update(overrides)
        return StateField(**defaults)

    def test_valid_entity_model(self):
        em = EntityModel(
            name="customers",
            identity_graph="customer_identity",
            state_fields=[self._make_state_field()],
        )
        assert em.name == "customers"
        assert em.identity_graph == "customer_identity"
        assert len(em.state_fields) == 1
        assert em.computed_fields == []

    def test_missing_name_raises(self):
        with pytest.raises(ValidationError):
            EntityModel(
                identity_graph="customer_identity",
                state_fields=[self._make_state_field()],
            )

    def test_empty_name_raises(self):
        with pytest.raises(ValidationError):
            EntityModel(
                name="",
                identity_graph="customer_identity",
                state_fields=[self._make_state_field()],
            )

    def test_empty_state_fields_raises(self):
        with pytest.raises(ValidationError):
            EntityModel(
                name="customers",
                identity_graph="customer_identity",
                state_fields=[],
            )

    def test_accepts_computed_fields(self):
        cc = ComputedColumn(name="full_name", expression="first_name || ' ' || last_name")
        em = EntityModel(
            name="customers",
            identity_graph="customer_identity",
            state_fields=[self._make_state_field()],
            computed_fields=[cc],
        )
        assert len(em.computed_fields) == 1
        assert em.computed_fields[0].name == "full_name"


class TestCoreImport:
    def test_import_from_core(self):
        from fyrnheim.core import EntityModel as EM, StateField as SF

        assert EM is EntityModel
        assert SF is StateField
