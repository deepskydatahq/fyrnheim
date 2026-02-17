"""Tests for SourceMapping class."""

import pytest
from pydantic import ValidationError

from fyrnheim import Entity, Field, LayersConfig, PrepLayer, SourceMapping, TableSource


class TestSourceMappingImport:
    """Verify SourceMapping is importable from expected paths."""

    def test_importable_from_top_level(self):
        from fyrnheim import SourceMapping as SM

        assert SM is not None

    def test_importable_from_core(self):
        from fyrnheim.core import SourceMapping as SM

        assert SM is not None

    def test_importable_from_module(self):
        from fyrnheim.core.source_mapping import SourceMapping as SM

        assert SM is not None


class TestSourceMappingConstruction:
    """Tests for SourceMapping construction and validation."""

    def _make_entity(self, **kwargs):
        defaults = {
            "name": "transactions",
            "description": "Transaction records",
            "layers": LayersConfig(prep=PrepLayer(model_name="prep_transactions")),
            "required_fields": [
                Field(name="transaction_id", type="STRING"),
                Field(name="amount_cents", type="INT64"),
            ],
        }
        defaults.update(kwargs)
        return Entity(**defaults)

    def test_accepts_entity_source_and_mappings(self):
        entity = self._make_entity()
        source = TableSource(project="p", dataset="d", table="orders")
        mapping = SourceMapping(
            entity=entity,
            source=source,
            field_mappings={
                "transaction_id": "id",
                "amount_cents": "subtotal",
            },
        )
        assert mapping.entity is entity
        assert mapping.source is source
        assert mapping.field_mappings["transaction_id"] == "id"

    def test_validates_required_field_coverage(self):
        entity = self._make_entity()
        source = TableSource(project="p", dataset="d", table="orders")
        with pytest.raises(ValidationError, match="missing required field mappings"):
            SourceMapping(
                entity=entity,
                source=source,
                field_mappings={"transaction_id": "id"},
                # missing amount_cents mapping
            )

    def test_allows_unmapped_optional_fields(self):
        entity = self._make_entity(
            optional_fields=[Field(name="currency", type="STRING")],
        )
        source = TableSource(project="p", dataset="d", table="orders")
        mapping = SourceMapping(
            entity=entity,
            source=source,
            field_mappings={
                "transaction_id": "id",
                "amount_cents": "subtotal",
                # currency not mapped -- that's fine, it's optional
            },
        )
        assert "currency" not in mapping.field_mappings

    def test_skips_validation_when_no_required_fields(self):
        entity = Entity(
            name="events",
            description="Raw events",
            layers=LayersConfig(prep=PrepLayer(model_name="prep_events")),
            source=TableSource(project="p", dataset="d", table="events"),
        )
        source = TableSource(project="p", dataset="d", table="events_v2")
        # No required_fields -> no validation, empty mappings OK
        mapping = SourceMapping(
            entity=entity,
            source=source,
        )
        assert mapping.field_mappings == {}

    def test_default_empty_field_mappings(self):
        entity = Entity(
            name="test",
            description="test",
            layers=LayersConfig(prep=PrepLayer(model_name="prep")),
            source=TableSource(project="p", dataset="d", table="t"),
        )
        mapping = SourceMapping(
            entity=entity,
            source=TableSource(project="p2", dataset="d2", table="t2"),
        )
        assert mapping.field_mappings == {}

    def test_extra_mappings_allowed(self):
        entity = self._make_entity()
        source = TableSource(project="p", dataset="d", table="orders")
        mapping = SourceMapping(
            entity=entity,
            source=source,
            field_mappings={
                "transaction_id": "id",
                "amount_cents": "subtotal",
                "extra_field": "bonus_col",  # extra mapping is fine
            },
        )
        assert "extra_field" in mapping.field_mappings
