"""Tests for StreamAnalyticsModel and StreamMetric."""

import pytest
from pydantic import ValidationError

from fyrnheim.core.analytics_model import StreamAnalyticsModel, StreamMetric


class TestStreamMetric:
    def test_valid_metric(self):
        m = StreamMetric(name="signups", expression="count()", metric_type="count")
        assert m.name == "signups"
        assert m.expression == "count()"
        assert m.metric_type == "count"
        assert m.event_filter is None

    def test_metric_with_event_filter(self):
        m = StreamMetric(
            name="revenue",
            expression="sum(amount)",
            metric_type="sum",
            event_filter="activity_type == 'purchase'",
        )
        assert m.event_filter == "activity_type == 'purchase'"

    def test_metric_type_accepts_count_sum_snapshot(self):
        for mt in ("count", "sum", "snapshot"):
            m = StreamMetric(name="x", expression="expr()", metric_type=mt)
            assert m.metric_type == mt

    def test_metric_type_rejects_invalid(self):
        with pytest.raises(ValidationError):
            StreamMetric(name="x", expression="expr()", metric_type="average")

    def test_metric_name_required(self):
        with pytest.raises(ValidationError):
            StreamMetric(name="", expression="count()", metric_type="count")

    def test_metric_expression_required(self):
        with pytest.raises(ValidationError):
            StreamMetric(name="x", expression="", metric_type="count")


class TestStreamAnalyticsModel:
    def _metric(self, **kwargs):
        defaults = {"name": "signups", "expression": "count()", "metric_type": "count"}
        defaults.update(kwargs)
        return StreamMetric(**defaults)

    def test_valid_model(self):
        model = StreamAnalyticsModel(
            name="daily_metrics",
            identity_graph="customer",
            date_grain="daily",
            metrics=[self._metric()],
        )
        assert model.name == "daily_metrics"
        assert model.identity_graph == "customer"
        assert model.date_grain == "daily"
        assert len(model.metrics) == 1
        assert model.dimensions == []

    def test_valid_model_without_identity_graph(self):
        model = StreamAnalyticsModel(
            name="daily_metrics",
            date_grain="daily",
            metrics=[self._metric()],
        )
        assert model.name == "daily_metrics"
        assert model.identity_graph is None
        assert model.date_grain == "daily"

    def test_valid_model_with_identity_graph(self):
        model = StreamAnalyticsModel(
            name="daily_metrics",
            identity_graph="customer",
            date_grain="daily",
            metrics=[self._metric()],
        )
        assert model.identity_graph == "customer"

    def test_model_without_name_raises(self):
        with pytest.raises(ValidationError):
            StreamAnalyticsModel(
                name="",
                identity_graph="customer",
                date_grain="daily",
                metrics=[self._metric()],
            )

    def test_date_grain_accepts_daily_weekly_monthly(self):
        for grain in ("daily", "weekly", "monthly"):
            model = StreamAnalyticsModel(
                name="m",
                identity_graph="ig",
                date_grain=grain,
                metrics=[self._metric()],
            )
            assert model.date_grain == grain

    def test_date_grain_rejects_invalid(self):
        with pytest.raises(ValidationError):
            StreamAnalyticsModel(
                name="m",
                identity_graph="ig",
                date_grain="yearly",
                metrics=[self._metric()],
            )

    def test_metrics_required(self):
        with pytest.raises(ValidationError):
            StreamAnalyticsModel(
                name="m",
                identity_graph="ig",
                date_grain="daily",
                metrics=[],
            )

    def test_optional_dimensions(self):
        model = StreamAnalyticsModel(
            name="m",
            identity_graph="ig",
            date_grain="daily",
            metrics=[self._metric()],
            dimensions=["country", "plan"],
        )
        assert model.dimensions == ["country", "plan"]

    def test_import_from_core(self):
        from fyrnheim.core import StreamAnalyticsModel as SAM, StreamMetric as SM

        assert SAM is StreamAnalyticsModel
        assert SM is StreamMetric

    def test_import_from_fyrnheim(self):
        from fyrnheim import StreamAnalyticsModel as SAM, StreamMetric as SM

        assert SAM is StreamAnalyticsModel
        assert SM is StreamMetric
