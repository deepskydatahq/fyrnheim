"""Tests for the fyrnheim public API and model_rebuild resolution."""

import fyrnheim


class TestFlatImports:
    """Verify flat imports from top-level package work."""

    def test_core_entity_types(self):
        from fyrnheim import Entity, Field, LayersConfig, Source

        assert Entity is not None
        assert LayersConfig is not None
        assert Source is not None
        assert Field is not None

    def test_layer_types(self):
        from fyrnheim import DimensionLayer, PrepLayer, SnapshotLayer

        assert PrepLayer is not None
        assert DimensionLayer is not None
        assert SnapshotLayer is not None

    def test_source_types(self):
        from fyrnheim import (
            AggregationSource,
            DerivedSource,
            EventAggregationSource,
            TableSource,
            UnionSource,
        )

        assert TableSource is not None
        assert DerivedSource is not None
        assert AggregationSource is not None
        assert EventAggregationSource is not None
        assert UnionSource is not None

    def test_component_types(self):
        from fyrnheim import ComputedColumn, Measure

        assert ComputedColumn is not None
        assert Measure is not None

    def test_quality_types(self):
        from fyrnheim import NotNull, QualityConfig, Unique

        assert QualityConfig is not None
        assert NotNull is not None
        assert Unique is not None

    def test_primitive_functions(self):
        from fyrnheim import categorize, hash_email, sum_

        assert callable(hash_email)
        assert callable(categorize)
        assert callable(sum_)

    def test_source_mapping(self):
        from fyrnheim import SourceMapping

        assert SourceMapping is not None


class TestNestedImports:
    """Verify nested imports work and return same objects as flat imports."""

    def test_entity_same_object(self):
        from fyrnheim import Entity as E1
        from fyrnheim.core import Entity as E2

        assert E1 is E2

    def test_computed_column_same_object(self):
        from fyrnheim import ComputedColumn as C1
        from fyrnheim.components import ComputedColumn as C2

        assert C1 is C2

    def test_not_null_same_object(self):
        from fyrnheim import NotNull as N1
        from fyrnheim.quality import NotNull as N2

        assert N1 is N2

    def test_hash_email_same_object(self):
        from fyrnheim import hash_email as H1
        from fyrnheim.primitives import hash_email as H2

        assert H1 is H2


class TestModelRebuild:
    """Tests for forward reference resolution via model_rebuild."""

    def test_entity_forward_refs_resolved(self):
        """Entity.model_rebuild() resolves ComputedColumn, Measure, QualityConfig."""
        from fyrnheim import Entity

        field_info = Entity.model_fields
        # If model_rebuild hadn't been called, these would still be string refs
        assert "core_computed" in field_info
        assert "core_measures" in field_info
        assert "quality" in field_info

    def test_entity_with_computed_columns_validates(self):
        """Creating Entity with ComputedColumn in dimension layer validates correctly."""
        from fyrnheim import (
            ComputedColumn,
            DimensionLayer,
            Entity,
            Field,
            LayersConfig,
            PrepLayer,
            TableSource,
        )

        entity = Entity(
            name="test_entity",
            description="Test entity with computed columns",
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_test"),
                dimension=DimensionLayer(
                    model_name="dim_test",
                    computed_columns=[
                        ComputedColumn(
                            name="full_name",
                            expression="first_name || ' ' || last_name",
                        ),
                    ],
                ),
            ),
            required_fields=[
                Field(name="id", type="STRING"),
                Field(name="email", type="STRING"),
            ],
            core_computed=[
                ComputedColumn(name="email_hash", expression="MD5(email)"),
            ],
            source=TableSource(project="p", dataset="d", table="users"),
        )
        assert entity.name == "test_entity"
        assert len(entity.all_computed_columns) == 2

    def test_entity_with_quality_config_validates(self):
        """Creating Entity with quality=QualityConfig validates."""
        from fyrnheim import (
            Entity,
            Field,
            LayersConfig,
            NotNull,
            PrepLayer,
            QualityConfig,
            TableSource,
        )

        entity = Entity(
            name="test_entity",
            description="Test entity with quality",
            layers=LayersConfig(prep=PrepLayer(model_name="prep_test")),
            required_fields=[Field(name="id", type="STRING")],
            source=TableSource(project="p", dataset="d", table="users"),
            quality=QualityConfig(checks=[NotNull("id")]),
        )
        assert entity.quality is not None
        assert len(entity.quality.checks) == 1

    def test_source_mapping_model_rebuild(self):
        """SourceMapping with Entity and Source validates correctly."""
        from fyrnheim import (
            Entity,
            Field,
            LayersConfig,
            PrepLayer,
            SourceMapping,
            TableSource,
        )

        entity = Entity(
            name="transactions",
            description="Transactions",
            layers=LayersConfig(prep=PrepLayer(model_name="prep_tx")),
            required_fields=[
                Field(name="tx_id", type="STRING"),
                Field(name="amount", type="INT64"),
            ],
        )
        mapping = SourceMapping(
            entity=entity,
            source=TableSource(project="p", dataset="d", table="orders"),
            field_mappings={"tx_id": "id", "amount": "subtotal"},
        )
        assert mapping.entity.name == "transactions"


class TestAllExports:
    """Verify __all__ is accurate."""

    def test_all_exports_are_accessible(self):
        for name in fyrnheim.__all__:
            assert hasattr(fyrnheim, name), f"{name} in __all__ but not accessible"

    def test_version(self):
        assert fyrnheim.__version__ == "0.1.0"
