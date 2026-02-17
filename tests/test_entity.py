"""Tests for Entity, LayersConfig, and Source union type."""

import pytest
from pydantic import ValidationError

from typedata.core.entity import Entity, LayersConfig
from typedata.core.layer import DimensionLayer, PrepLayer, SnapshotLayer
from typedata.core.source import (
    AggregationSource,
    DerivedSource,
    EventAggregationSource,
    Field,
    TableSource,
    UnionSource,
)

# ---------------------------------------------------------------------------
# LayersConfig tests
# ---------------------------------------------------------------------------


class TestLayersConfig:
    """Tests for LayersConfig validation."""

    def test_requires_at_least_one_layer(self):
        with pytest.raises(ValidationError, match="At least one layer"):
            LayersConfig()

    def test_with_prep_only(self):
        config = LayersConfig(prep=PrepLayer(model_name="prep_test"))
        assert config.prep is not None
        assert config.dimension is None

    def test_with_dimension_only(self):
        config = LayersConfig(dimension=DimensionLayer(model_name="dim_test"))
        assert config.dimension is not None

    def test_with_snapshot_only(self):
        config = LayersConfig(snapshot=SnapshotLayer())
        assert config.snapshot is not None

    def test_with_multiple_layers(self):
        config = LayersConfig(
            prep=PrepLayer(model_name="prep_test"),
            dimension=DimensionLayer(model_name="dim_test"),
            snapshot=SnapshotLayer(),
        )
        assert config.prep is not None
        assert config.dimension is not None
        assert config.snapshot is not None


# ---------------------------------------------------------------------------
# Entity tests
# ---------------------------------------------------------------------------


class TestEntityBasic:
    """Basic Entity construction and validation tests."""

    def _make_layers(self):
        return LayersConfig(prep=PrepLayer(model_name="prep_test"))

    def test_entity_with_source(self):
        entity = Entity(
            name="transactions",
            description="Transaction records",
            layers=self._make_layers(),
            source=TableSource(project="p", dataset="d", table="t"),
        )
        assert entity.name == "transactions"
        assert entity.source is not None

    def test_entity_with_required_fields(self):
        entity = Entity(
            name="transactions",
            description="Transaction records",
            layers=self._make_layers(),
            required_fields=[
                Field(name="id", type="STRING"),
                Field(name="amount", type="FLOAT64"),
            ],
        )
        assert len(entity.required_fields) == 2

    def test_entity_requires_fields_or_source(self):
        with pytest.raises(ValidationError, match="required_fields or source"):
            Entity(
                name="test",
                description="test",
                layers=self._make_layers(),
            )

    def test_entity_name_pattern_valid(self):
        entity = Entity(
            name="my_entity_v2",
            description="test",
            layers=self._make_layers(),
            source=TableSource(project="p", dataset="d", table="t"),
        )
        assert entity.name == "my_entity_v2"

    def test_entity_name_pattern_rejects_uppercase(self):
        with pytest.raises(ValidationError):
            Entity(
                name="BadName",
                description="test",
                layers=self._make_layers(),
                source=TableSource(project="p", dataset="d", table="t"),
            )

    def test_entity_name_pattern_rejects_dash(self):
        with pytest.raises(ValidationError):
            Entity(
                name="my-entity",
                description="test",
                layers=self._make_layers(),
                source=TableSource(project="p", dataset="d", table="t"),
            )

    def test_entity_name_rejects_leading_digit(self):
        with pytest.raises(ValidationError):
            Entity(
                name="2things",
                description="test",
                layers=self._make_layers(),
                source=TableSource(project="p", dataset="d", table="t"),
            )

    def test_is_internal_defaults_false(self):
        entity = Entity(
            name="test",
            description="test",
            layers=self._make_layers(),
            source=TableSource(project="p", dataset="d", table="t"),
        )
        assert entity.is_internal is False

    def test_is_internal_set_true(self):
        entity = Entity(
            name="test",
            description="test",
            layers=self._make_layers(),
            source=TableSource(project="p", dataset="d", table="t"),
            is_internal=True,
        )
        assert entity.is_internal is True


# ---------------------------------------------------------------------------
# Entity properties tests
# ---------------------------------------------------------------------------


class TestEntityProperties:
    """Tests for Entity computed properties."""

    def _make_layers(self):
        return LayersConfig(prep=PrepLayer(model_name="prep_test"))

    def test_all_fields_from_required(self):
        entity = Entity(
            name="test",
            description="test",
            layers=self._make_layers(),
            required_fields=[
                Field(name="id", type="STRING"),
            ],
            optional_fields=[
                Field(name="email", type="STRING"),
            ],
        )
        assert len(entity.all_fields) == 2
        assert entity.all_fields[0].name == "id"
        assert entity.all_fields[1].name == "email"

    def test_all_fields_from_source(self):
        entity = Entity(
            name="test",
            description="test",
            layers=self._make_layers(),
            source=TableSource(
                project="p",
                dataset="d",
                table="t",
                fields=[
                    Field(name="col1", type="STRING"),
                    Field(name="col2", type="INT64"),
                ],
            ),
        )
        assert len(entity.all_fields) == 2

    def test_all_fields_raises_when_no_fields(self):
        entity = Entity(
            name="test",
            description="test",
            layers=self._make_layers(),
            source=DerivedSource(identity_graph="person_graph"),
            required_fields=[Field(name="id", type="STRING")],
        )
        # When required_fields is set, it takes precedence
        assert len(entity.all_fields) == 1

    def test_all_computed_columns_merges(self):
        entity = Entity(
            name="test",
            description="test",
            layers=LayersConfig(
                dimension=DimensionLayer(
                    model_name="dim_test",
                    computed_columns=[{"name": "col1", "expression": "..."}],
                ),
            ),
            core_computed=[{"name": "col0", "expression": "..."}],
            source=TableSource(project="p", dataset="d", table="t"),
        )
        assert len(entity.all_computed_columns) == 2

    def test_all_computed_columns_empty(self):
        entity = Entity(
            name="test",
            description="test",
            layers=self._make_layers(),
            source=TableSource(project="p", dataset="d", table="t"),
        )
        assert entity.all_computed_columns == []

    def test_all_measures_empty(self):
        entity = Entity(
            name="test",
            description="test",
            layers=self._make_layers(),
            source=TableSource(project="p", dataset="d", table="t"),
        )
        assert entity.all_measures == []

    def test_get_layer(self):
        layers = self._make_layers()
        entity = Entity(
            name="test",
            description="test",
            layers=layers,
            source=TableSource(project="p", dataset="d", table="t"),
        )
        assert entity.get_layer("prep") is not None
        assert entity.get_layer("dimension") is None

    def test_has_layer(self):
        entity = Entity(
            name="test",
            description="test",
            layers=self._make_layers(),
            source=TableSource(project="p", dataset="d", table="t"),
        )
        assert entity.has_layer("prep") is True
        assert entity.has_layer("snapshot") is False


# ---------------------------------------------------------------------------
# Source union tests
# ---------------------------------------------------------------------------


class TestSourceUnion:
    """Tests for Source union type accepting all expected types."""

    def _make_entity(self, source):
        return Entity(
            name="test",
            description="test",
            layers=LayersConfig(prep=PrepLayer(model_name="prep")),
            source=source,
            required_fields=[Field(name="id", type="STRING")],
        )

    def test_accepts_table_source(self):
        entity = self._make_entity(
            TableSource(project="p", dataset="d", table="t")
        )
        assert isinstance(entity.source, TableSource)

    def test_accepts_derived_source(self):
        entity = self._make_entity(
            DerivedSource(identity_graph="person_graph")
        )
        assert isinstance(entity.source, DerivedSource)

    def test_accepts_aggregation_source(self):
        entity = self._make_entity(
            AggregationSource(source_entity="person", group_by_column="account_id")
        )
        assert isinstance(entity.source, AggregationSource)

    def test_accepts_event_aggregation_source(self):
        entity = self._make_entity(
            EventAggregationSource(
                project="p", dataset="d", table="t", group_by_column="user_id"
            )
        )
        assert isinstance(entity.source, EventAggregationSource)

    def test_accepts_union_source(self):
        entity = self._make_entity(
            UnionSource(
                sources=[
                    TableSource(project="p", dataset="d", table="t1"),
                    TableSource(project="p", dataset="d", table="t2"),
                ]
            )
        )
        assert isinstance(entity.source, UnionSource)
