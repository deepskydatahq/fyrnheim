"""Tests for StateSource and EventSource models."""

import pytest
from pydantic import ValidationError

from fyrnheim.core.entity import Entity, LayersConfig
from fyrnheim.core.layer import PrepLayer
from fyrnheim.core.source import (
    EventSource,
    Field,
    SourceTransforms,
    StateSource,
    TypeCast,
)


class TestStateSource:
    """Tests for the StateSource model."""

    def test_minimal_creation(self):
        s = StateSource(
            name="crm",
            project="p",
            dataset="d",
            table="t",
            id_field="id",
        )
        assert s.name == "crm"
        assert s.project == "p"
        assert s.dataset == "d"
        assert s.table == "t"
        assert s.id_field == "id"
        assert s.transforms is None
        assert s.fields is None

    def test_without_id_field_raises(self):
        with pytest.raises(ValidationError):
            StateSource(
                name="crm",
                project="p",
                dataset="d",
                table="t",
            )

    def test_without_name_raises(self):
        with pytest.raises(ValidationError):
            StateSource(
                project="p",
                dataset="d",
                table="t",
                id_field="id",
            )

    def test_accepts_optional_transforms(self):
        transforms = SourceTransforms(
            type_casts=[TypeCast(field="age", target_type="INT64")]
        )
        s = StateSource(
            name="crm",
            project="p",
            dataset="d",
            table="t",
            id_field="id",
            transforms=transforms,
        )
        assert s.transforms is not None
        assert len(s.transforms.type_casts) == 1

    def test_accepts_optional_fields(self):
        fields = [Field(name="email", type="STRING")]
        s = StateSource(
            name="crm",
            project="p",
            dataset="d",
            table="t",
            id_field="id",
            fields=fields,
        )
        assert s.fields is not None
        assert len(s.fields) == 1
        assert s.fields[0].name == "email"

    def test_inherits_base_table_source_validation(self):
        """Empty project/dataset/table should fail."""
        with pytest.raises(ValidationError):
            StateSource(
                name="crm",
                project="",
                dataset="d",
                table="t",
                id_field="id",
            )

    def test_empty_name_raises(self):
        with pytest.raises(ValidationError):
            StateSource(
                name="",
                project="p",
                dataset="d",
                table="t",
                id_field="id",
            )

    def test_empty_id_field_raises(self):
        with pytest.raises(ValidationError):
            StateSource(
                name="crm",
                project="p",
                dataset="d",
                table="t",
                id_field="",
            )


class TestEventSource:
    """Tests for the EventSource model."""

    def test_minimal_creation(self):
        e = EventSource(
            name="views",
            project="p",
            dataset="d",
            table="t",
            entity_id_field="user_id",
            timestamp_field="ts",
        )
        assert e.name == "views"
        assert e.entity_id_field == "user_id"
        assert e.timestamp_field == "ts"
        assert e.event_type is None
        assert e.event_type_field is None
        assert e.transforms is None
        assert e.fields is None

    def test_without_entity_id_field_raises(self):
        with pytest.raises(ValidationError):
            EventSource(
                name="views",
                project="p",
                dataset="d",
                table="t",
                timestamp_field="ts",
            )

    def test_without_timestamp_field_raises(self):
        with pytest.raises(ValidationError):
            EventSource(
                name="views",
                project="p",
                dataset="d",
                table="t",
                entity_id_field="user_id",
            )

    def test_accepts_optional_event_type(self):
        e = EventSource(
            name="views",
            project="p",
            dataset="d",
            table="t",
            entity_id_field="user_id",
            timestamp_field="ts",
            event_type="page_view",
        )
        assert e.event_type == "page_view"
        assert e.event_type_field is None

    def test_accepts_optional_event_type_field(self):
        e = EventSource(
            name="events",
            project="p",
            dataset="d",
            table="t",
            entity_id_field="user_id",
            timestamp_field="ts",
            event_type_field="event_name",
        )
        assert e.event_type is None
        assert e.event_type_field == "event_name"

    def test_both_event_type_and_field_raises(self):
        with pytest.raises(ValidationError, match="cannot have both"):
            EventSource(
                name="events",
                project="p",
                dataset="d",
                table="t",
                entity_id_field="user_id",
                timestamp_field="ts",
                event_type="page_view",
                event_type_field="event_name",
            )

    def test_accepts_optional_transforms_and_fields(self):
        e = EventSource(
            name="events",
            project="p",
            dataset="d",
            table="t",
            entity_id_field="user_id",
            timestamp_field="ts",
            transforms=SourceTransforms(),
            fields=[Field(name="url", type="STRING")],
        )
        assert e.transforms is not None
        assert e.fields is not None
        assert len(e.fields) == 1

    def test_without_name_raises(self):
        with pytest.raises(ValidationError):
            EventSource(
                project="p",
                dataset="d",
                table="t",
                entity_id_field="user_id",
                timestamp_field="ts",
            )

    def test_inherits_base_table_source_validation(self):
        with pytest.raises(ValidationError):
            EventSource(
                name="views",
                project="",
                dataset="d",
                table="t",
                entity_id_field="user_id",
                timestamp_field="ts",
            )


class TestSourceUnionAndExports:
    """Tests for Story 3: Source union type and public exports."""

    def test_entity_with_state_source(self):
        entity = Entity(
            name="customer",
            description="A customer entity",
            source=StateSource(
                name="crm",
                project="p",
                dataset="d",
                table="t",
                id_field="customer_id",
            ),
            layers=LayersConfig(prep=PrepLayer(model_name="prep_customer")),
        )
        assert isinstance(entity.source, StateSource)

    def test_entity_with_event_source(self):
        entity = Entity(
            name="page_views",
            description="Page view events",
            source=EventSource(
                name="views",
                project="p",
                dataset="d",
                table="t",
                entity_id_field="user_id",
                timestamp_field="ts",
            ),
            layers=LayersConfig(prep=PrepLayer(model_name="prep_views")),
        )
        assert isinstance(entity.source, EventSource)

    def test_import_from_core(self):
        """Verify public API exports work."""
        from fyrnheim.core import EventSource as ES, StateSource as SS

        assert SS is StateSource
        assert ES is EventSource
