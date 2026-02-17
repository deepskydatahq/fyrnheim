"""Tests for layer configuration classes."""


from fyrnheim.core.layer import DimensionLayer, PrepLayer, SnapshotLayer
from fyrnheim.core.types import MaterializationType


class TestPrepLayer:
    """Tests for PrepLayer configuration."""

    def test_basic_creation(self):
        layer = PrepLayer(model_name="prep_transactions")
        assert layer.model_name == "prep_transactions"
        assert layer.materialization == MaterializationType.TABLE
        assert layer.target_schema == "prep"

    def test_auto_adds_prep_tag(self):
        layer = PrepLayer(model_name="prep_test")
        assert "prep" in layer.tags

    def test_does_not_duplicate_prep_tag(self):
        layer = PrepLayer(model_name="prep_test", tags=["prep", "other"])
        assert layer.tags.count("prep") == 1

    def test_default_empty_computed_columns(self):
        layer = PrepLayer(model_name="prep_test")
        assert layer.computed_columns == []

    def test_default_empty_quality_checks(self):
        layer = PrepLayer(model_name="prep_test")
        assert layer.quality_checks == []

    def test_default_empty_depends_on(self):
        layer = PrepLayer(model_name="prep_test")
        assert layer.depends_on == []

    def test_custom_materialization(self):
        layer = PrepLayer(
            model_name="prep_test",
            materialization=MaterializationType.VIEW,
        )
        assert layer.materialization == MaterializationType.VIEW

    def test_custom_target_schema(self):
        layer = PrepLayer(model_name="prep_test", target_schema="staging")
        assert layer.target_schema == "staging"

    def test_depends_on(self):
        layer = PrepLayer(model_name="prep_test", depends_on=["prep_users"])
        assert layer.depends_on == ["prep_users"]


class TestDimensionLayer:
    """Tests for DimensionLayer configuration."""

    def test_basic_creation(self):
        layer = DimensionLayer(model_name="dim_users")
        assert layer.model_name == "dim_users"
        assert layer.materialization == MaterializationType.TABLE
        assert layer.target_schema == "business"

    def test_default_empty_computed_columns(self):
        layer = DimensionLayer(model_name="dim_test")
        assert layer.computed_columns == []

    def test_default_empty_quality_checks(self):
        layer = DimensionLayer(model_name="dim_test")
        assert layer.quality_checks == []

    def test_no_prep_tag_auto_added(self):
        layer = DimensionLayer(model_name="dim_test")
        assert "prep" not in layer.tags

    def test_with_computed_columns(self):
        layer = DimensionLayer(
            model_name="dim_test",
            computed_columns=[{"name": "full_name", "expression": "first || last"}],
        )
        assert len(layer.computed_columns) == 1


class TestSnapshotLayer:
    """Tests for SnapshotLayer configuration."""

    def test_defaults(self):
        layer = SnapshotLayer()
        assert layer.enabled is True
        assert layer.date_column == "ds"
        assert layer.deduplication_order_by == "updated_at DESC"
        assert layer.partitioning_field == "ds"
        assert layer.partitioning_type == "DAY"
        assert layer.clustering_fields == []
        assert layer.materialization == MaterializationType.INCREMENTAL

    def test_disabled(self):
        layer = SnapshotLayer(enabled=False)
        assert layer.enabled is False

    def test_custom_date_column(self):
        layer = SnapshotLayer(date_column="snapshot_date")
        assert layer.date_column == "snapshot_date"

    def test_custom_clustering(self):
        layer = SnapshotLayer(clustering_fields=["user_id", "ds"])
        assert layer.clustering_fields == ["user_id", "ds"]
