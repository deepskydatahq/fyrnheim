"""Tests for MetricsModel and MetricField types."""

import pytest
from pydantic import ValidationError

from fyrnheim.core import MetricField, MetricsModel


class TestMetricField:
    def test_valid_sum_delta(self):
        mf = MetricField(field_name="view_count", aggregation="sum_delta")
        assert mf.field_name == "view_count"
        assert mf.aggregation == "sum_delta"

    def test_valid_last_value(self):
        mf = MetricField(field_name="subscriber_count", aggregation="last_value")
        assert mf.aggregation == "last_value"

    def test_valid_max_value(self):
        mf = MetricField(field_name="peak_viewers", aggregation="max_value")
        assert mf.aggregation == "max_value"

    def test_invalid_aggregation(self):
        with pytest.raises(ValidationError):
            MetricField(field_name="x", aggregation="average")

    def test_empty_field_name(self):
        with pytest.raises(ValidationError):
            MetricField(field_name="", aggregation="sum_delta")


class TestMetricsModel:
    def test_valid_model(self):
        mm = MetricsModel(
            name="yt_daily",
            sources=["youtube"],
            grain="daily",
            metric_fields=[
                MetricField(field_name="view_count", aggregation="sum_delta"),
            ],
        )
        assert mm.name == "yt_daily"
        assert mm.sources == ["youtube"]
        assert mm.grain == "daily"
        assert len(mm.metric_fields) == 1
        assert mm.dimensions == []

    def test_all_grains(self):
        for grain in ("hourly", "daily", "weekly", "monthly"):
            mm = MetricsModel(
                name="test",
                sources=["src"],
                grain=grain,
                metric_fields=[
                    MetricField(field_name="x", aggregation="sum_delta"),
                ],
            )
            assert mm.grain == grain

    def test_invalid_grain(self):
        with pytest.raises(ValidationError):
            MetricsModel(
                name="test",
                sources=["src"],
                grain="yearly",
                metric_fields=[
                    MetricField(field_name="x", aggregation="sum_delta"),
                ],
            )

    def test_requires_at_least_one_metric_field(self):
        with pytest.raises(ValidationError):
            MetricsModel(
                name="test",
                sources=["src"],
                grain="daily",
                metric_fields=[],
            )

    def test_optional_dimensions(self):
        mm = MetricsModel(
            name="test",
            sources=["src"],
            grain="daily",
            metric_fields=[
                MetricField(field_name="x", aggregation="sum_delta"),
            ],
            dimensions=["entity_id"],
        )
        assert mm.dimensions == ["entity_id"]

    def test_empty_name_rejected(self):
        with pytest.raises(ValidationError):
            MetricsModel(
                name="",
                sources=["src"],
                grain="daily",
                metric_fields=[
                    MetricField(field_name="x", aggregation="sum_delta"),
                ],
            )

    def test_empty_source_rejected(self):
        with pytest.raises(ValidationError):
            MetricsModel(
                name="test",
                sources=[],
                grain="daily",
                metric_fields=[
                    MetricField(field_name="x", aggregation="sum_delta"),
                ],
            )

    def test_multiple_metric_fields(self):
        mm = MetricsModel(
            name="yt_daily",
            sources=["youtube"],
            grain="daily",
            metric_fields=[
                MetricField(field_name="view_count", aggregation="sum_delta"),
                MetricField(field_name="subscriber_count", aggregation="last_value"),
                MetricField(field_name="peak_viewers", aggregation="max_value"),
            ],
        )
        assert len(mm.metric_fields) == 3


class TestMetricsModelImport:
    def test_import_from_core(self):
        from fyrnheim.core import MetricField, MetricsModel

        assert MetricField is not None
        assert MetricsModel is not None

    def test_import_from_fyrnheim(self):
        from fyrnheim import MetricField, MetricsModel

        assert MetricField is not None
        assert MetricsModel is not None
