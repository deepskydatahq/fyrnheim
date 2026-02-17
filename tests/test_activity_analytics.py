"""Tests for activity and analytics layer configuration classes."""

import pytest
from pydantic import ValidationError

from typedata.core.activity import ActivityConfig, ActivityType
from typedata.core.analytics import (
    AnalyticsLayer,
    AnalyticsMetric,
    AnalyticsModel,
    AnalyticsSource,
    ComputedMetric,
)

# ---------------------------------------------------------------------------
# Activity tests
# ---------------------------------------------------------------------------


class TestActivityImports:
    """Verify activity classes are importable."""

    def test_activity_config_importable(self):
        assert ActivityConfig is not None

    def test_activity_type_importable(self):
        assert ActivityType is not None


class TestActivityConfig:
    """Tests for ActivityConfig validation and behaviour."""

    def test_valid_config_with_person_id(self):
        at = ActivityType(
            name="signup",
            trigger="row_appears",
            timestamp_field="created_at",
        )
        config = ActivityConfig(
            model_name="activity_users",
            types=[at],
            entity_id_field="user_id",
            person_id_field="person_id",
        )
        assert config.model_name == "activity_users"
        assert len(config.types) == 1
        assert config.types[0].trigger == "row_appears"

    def test_empty_types_raises_validation_error(self):
        with pytest.raises(ValidationError):
            ActivityConfig(
                model_name="activity_users",
                types=[],
                entity_id_field="user_id",
                person_id_field="person_id",
            )

    def test_missing_both_id_fields_raises_value_error(self):
        at = ActivityType(
            name="signup",
            trigger="row_appears",
            timestamp_field="created_at",
        )
        with pytest.raises(ValueError, match="person_id_field or anon_id_field"):
            ActivityConfig(
                model_name="activity_users",
                types=[at],
                entity_id_field="user_id",
            )

    def test_identity_field_returns_person_id_when_set(self):
        at = ActivityType(
            name="signup",
            trigger="row_appears",
            timestamp_field="created_at",
        )
        config = ActivityConfig(
            model_name="activity_users",
            types=[at],
            entity_id_field="user_id",
            person_id_field="person_id",
        )
        assert config.identity_field == "person_id"

    def test_identity_field_returns_anon_id_when_person_id_none(self):
        at = ActivityType(
            name="page_view",
            trigger="row_appears",
            timestamp_field="ts",
        )
        config = ActivityConfig(
            model_name="activity_pages",
            types=[at],
            entity_id_field="page_id",
            anon_id_field="anonymous_id",
        )
        assert config.identity_field == "anonymous_id"


# ---------------------------------------------------------------------------
# Analytics tests
# ---------------------------------------------------------------------------


class TestAnalyticsImports:
    """Verify analytics classes are importable."""

    def test_analytics_metric_importable(self):
        assert AnalyticsMetric is not None

    def test_analytics_layer_importable(self):
        assert AnalyticsLayer is not None

    def test_analytics_source_importable(self):
        assert AnalyticsSource is not None

    def test_computed_metric_importable(self):
        assert ComputedMetric is not None

    def test_analytics_model_importable(self):
        assert AnalyticsModel is not None


class TestAnalyticsLayer:
    """Tests for AnalyticsLayer validation."""

    def test_valid_layer_with_one_metric(self):
        metric = AnalyticsMetric(
            name="total_revenue",
            expression="_.revenue.sum()",
            metric_type="event",
        )
        layer = AnalyticsLayer(
            model_name="analytics_orders",
            date_expression="_.created_at.date()",
            metrics=[metric],
        )
        assert layer.model_name == "analytics_orders"
        assert len(layer.metrics) == 1
        assert layer.dimensions == []

    def test_empty_metrics_raises_validation_error(self):
        with pytest.raises(ValidationError):
            AnalyticsLayer(
                model_name="analytics_orders",
                date_expression="_.created_at.date()",
                metrics=[],
            )

    def test_metric_empty_name_raises_validation_error(self):
        with pytest.raises(ValidationError):
            AnalyticsMetric(
                name="",
                expression="_.revenue.sum()",
                metric_type="event",
            )


class TestAnalyticsModel:
    """Tests for AnalyticsModel composition."""

    def test_model_with_source_and_computed_metric(self):
        source = AnalyticsSource(entity="product")
        computed = ComputedMetric(
            name="revenue_per_user",
            expression="total_revenue / active_users",
            description="Revenue divided by active users",
        )
        model = AnalyticsModel(
            name="analytics_daily",
            description="Daily analytics model",
            grain="date",
            sources=[source],
            computed_metrics=[computed],
        )
        assert model.name == "analytics_daily"
        assert len(model.sources) == 1
        assert len(model.computed_metrics) == 1
        assert model.computed_metrics[0].name == "revenue_per_user"

    def test_analytics_source_default_layer(self):
        source = AnalyticsSource(entity="orders")
        assert source.layer == "analytics"
