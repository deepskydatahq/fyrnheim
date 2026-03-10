"""Tests for layer configuration classes."""

import pytest
from pydantic import ValidationError

from fyrnheim.core.layer import DimensionLayer, PrepLayer, SnapshotLayer
from fyrnheim.core.types import IncrementalStrategy, MaterializationType


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


class TestPrepLayerIncremental:
    """Tests for PrepLayer incremental configuration fields and validation."""

    def test_incremental_fields_default_to_none(self):
        layer = PrepLayer(model_name="prep_events")
        assert layer.incremental_strategy is None
        assert layer.unique_key is None
        assert layer.incremental_key is None

    def test_existing_entities_without_incremental_still_validate(self):
        layer = PrepLayer(model_name="prep_events", materialization=MaterializationType.TABLE)
        assert layer.materialization == MaterializationType.TABLE

    def test_valid_append_config(self):
        layer = PrepLayer(
            model_name="prep_events",
            materialization=MaterializationType.INCREMENTAL,
            incremental_strategy=IncrementalStrategy.APPEND,
            incremental_key="created_at",
        )
        assert layer.incremental_strategy == IncrementalStrategy.APPEND
        assert layer.incremental_key == "created_at"

    def test_valid_merge_config(self):
        layer = PrepLayer(
            model_name="prep_events",
            materialization=MaterializationType.INCREMENTAL,
            incremental_strategy=IncrementalStrategy.MERGE,
            unique_key="event_id",
        )
        assert layer.incremental_strategy == IncrementalStrategy.MERGE
        assert layer.unique_key == "event_id"

    def test_incremental_without_strategy_raises(self):
        with pytest.raises(ValidationError, match="incremental_strategy"):
            PrepLayer(
                model_name="prep_events",
                materialization=MaterializationType.INCREMENTAL,
            )

    def test_merge_without_unique_key_raises(self):
        with pytest.raises(ValidationError, match="unique_key"):
            PrepLayer(
                model_name="prep_events",
                materialization=MaterializationType.INCREMENTAL,
                incremental_strategy=IncrementalStrategy.MERGE,
            )

    def test_append_without_incremental_key_raises(self):
        with pytest.raises(ValidationError, match="incremental_key"):
            PrepLayer(
                model_name="prep_events",
                materialization=MaterializationType.INCREMENTAL,
                incremental_strategy=IncrementalStrategy.APPEND,
            )

    def test_valid_delete_insert_config(self):
        layer = PrepLayer(
            model_name="prep_events",
            materialization=MaterializationType.INCREMENTAL,
            incremental_strategy=IncrementalStrategy.DELETE_INSERT,
            unique_key="event_id",
        )
        assert layer.incremental_strategy == IncrementalStrategy.DELETE_INSERT
        assert layer.unique_key == "event_id"

    def test_table_materialization_ignores_incremental_fields(self):
        """Backward compat: TABLE materialization with no incremental fields is fine."""
        layer = PrepLayer(model_name="prep_events", materialization=MaterializationType.TABLE)
        assert layer.incremental_strategy is None


class TestDimensionLayerIncremental:
    """Tests for DimensionLayer incremental configuration fields and validation."""

    def test_incremental_fields_default_to_none(self):
        layer = DimensionLayer(model_name="dim_users")
        assert layer.incremental_strategy is None
        assert layer.unique_key is None
        assert layer.incremental_key is None

    def test_existing_entities_without_incremental_still_validate(self):
        layer = DimensionLayer(model_name="dim_users", materialization=MaterializationType.TABLE)
        assert layer.materialization == MaterializationType.TABLE

    def test_valid_append_config(self):
        layer = DimensionLayer(
            model_name="dim_users",
            materialization=MaterializationType.INCREMENTAL,
            incremental_strategy=IncrementalStrategy.APPEND,
            incremental_key="updated_at",
        )
        assert layer.incremental_strategy == IncrementalStrategy.APPEND
        assert layer.incremental_key == "updated_at"

    def test_valid_merge_config(self):
        layer = DimensionLayer(
            model_name="dim_users",
            materialization=MaterializationType.INCREMENTAL,
            incremental_strategy=IncrementalStrategy.MERGE,
            unique_key="user_id",
        )
        assert layer.incremental_strategy == IncrementalStrategy.MERGE
        assert layer.unique_key == "user_id"

    def test_incremental_without_strategy_raises(self):
        with pytest.raises(ValidationError, match="incremental_strategy"):
            DimensionLayer(
                model_name="dim_users",
                materialization=MaterializationType.INCREMENTAL,
            )

    def test_merge_without_unique_key_raises(self):
        with pytest.raises(ValidationError, match="unique_key"):
            DimensionLayer(
                model_name="dim_users",
                materialization=MaterializationType.INCREMENTAL,
                incremental_strategy=IncrementalStrategy.MERGE,
            )

    def test_append_without_incremental_key_raises(self):
        with pytest.raises(ValidationError, match="incremental_key"):
            DimensionLayer(
                model_name="dim_users",
                materialization=MaterializationType.INCREMENTAL,
                incremental_strategy=IncrementalStrategy.APPEND,
            )

    def test_valid_delete_insert_config(self):
        layer = DimensionLayer(
            model_name="dim_users",
            materialization=MaterializationType.INCREMENTAL,
            incremental_strategy=IncrementalStrategy.DELETE_INSERT,
            unique_key="user_id",
        )
        assert layer.incremental_strategy == IncrementalStrategy.DELETE_INSERT
        assert layer.unique_key == "user_id"

    def test_table_materialization_ignores_incremental_fields(self):
        layer = DimensionLayer(model_name="dim_users", materialization=MaterializationType.TABLE)
        assert layer.incremental_strategy is None
