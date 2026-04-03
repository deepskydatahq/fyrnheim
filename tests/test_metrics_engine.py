"""Tests for the metrics engine."""

import json

import ibis
import pandas as pd

from fyrnheim.core.metrics_model import MetricField, MetricsModel
from fyrnheim.engine.metrics_engine import aggregate_metrics


def _make_events(rows: list[dict]) -> ibis.expr.types.Table:
    """Helper to create an events memtable from row dicts."""
    df = pd.DataFrame(rows)
    return ibis.memtable(df)


def _payload(field_name: str, old_value: str, new_value: str) -> str:
    return json.dumps(
        {"field_name": field_name, "old_value": old_value, "new_value": new_value}
    )


class TestSumDelta:
    def test_basic_sum_delta_daily(self):
        events = _make_events(
            [
                {
                    "source": "youtube",
                    "entity_id": "video_a",
                    "ts": "2024-01-01T10:00:00",
                    "event_type": "field_changed",
                    "payload": _payload("view_count", "100", "150"),
                },
                {
                    "source": "youtube",
                    "entity_id": "video_b",
                    "ts": "2024-01-01T12:00:00",
                    "event_type": "field_changed",
                    "payload": _payload("view_count", "50", "80"),
                },
                {
                    "source": "youtube",
                    "entity_id": "video_a",
                    "ts": "2024-01-02T10:00:00",
                    "event_type": "field_changed",
                    "payload": _payload("view_count", "150", "200"),
                },
            ]
        )
        model = MetricsModel(
            name="yt_daily",
            source="youtube",
            grain="daily",
            metric_fields=[
                MetricField(field_name="view_count", aggregation="sum_delta"),
            ],
        )
        result = aggregate_metrics(events, model).execute()
        assert len(result) == 2
        day1 = result[result["_date"] == "2024-01-01"]
        day2 = result[result["_date"] == "2024-01-02"]
        assert day1["view_count_sum_delta"].iloc[0] == 80.0  # 50 + 30
        assert day2["view_count_sum_delta"].iloc[0] == 50.0

    def test_sum_delta_filters_source(self):
        events = _make_events(
            [
                {
                    "source": "youtube",
                    "entity_id": "a",
                    "ts": "2024-01-01T10:00:00",
                    "event_type": "field_changed",
                    "payload": _payload("view_count", "100", "200"),
                },
                {
                    "source": "tiktok",
                    "entity_id": "b",
                    "ts": "2024-01-01T10:00:00",
                    "event_type": "field_changed",
                    "payload": _payload("view_count", "500", "600"),
                },
            ]
        )
        model = MetricsModel(
            name="yt_daily",
            source="youtube",
            grain="daily",
            metric_fields=[
                MetricField(field_name="view_count", aggregation="sum_delta"),
            ],
        )
        result = aggregate_metrics(events, model).execute()
        assert len(result) == 1
        assert result["view_count_sum_delta"].iloc[0] == 100.0


class TestLastValue:
    def test_last_value_daily(self):
        events = _make_events(
            [
                {
                    "source": "youtube",
                    "entity_id": "video_a",
                    "ts": "2024-01-01T08:00:00",
                    "event_type": "field_changed",
                    "payload": _payload("subscriber_count", "100", "110"),
                },
                {
                    "source": "youtube",
                    "entity_id": "video_a",
                    "ts": "2024-01-01T20:00:00",
                    "event_type": "field_changed",
                    "payload": _payload("subscriber_count", "110", "130"),
                },
            ]
        )
        model = MetricsModel(
            name="yt_daily",
            source="youtube",
            grain="daily",
            metric_fields=[
                MetricField(field_name="subscriber_count", aggregation="last_value"),
            ],
        )
        result = aggregate_metrics(events, model).execute()
        assert len(result) == 1
        assert result["subscriber_count_last_value"].iloc[0] == 130.0


class TestMaxValue:
    def test_max_value_daily(self):
        events = _make_events(
            [
                {
                    "source": "youtube",
                    "entity_id": "video_a",
                    "ts": "2024-01-01T08:00:00",
                    "event_type": "field_changed",
                    "payload": _payload("peak_viewers", "0", "500"),
                },
                {
                    "source": "youtube",
                    "entity_id": "video_a",
                    "ts": "2024-01-01T12:00:00",
                    "event_type": "field_changed",
                    "payload": _payload("peak_viewers", "500", "300"),
                },
                {
                    "source": "youtube",
                    "entity_id": "video_a",
                    "ts": "2024-01-01T18:00:00",
                    "event_type": "field_changed",
                    "payload": _payload("peak_viewers", "300", "800"),
                },
            ]
        )
        model = MetricsModel(
            name="yt_daily",
            source="youtube",
            grain="daily",
            metric_fields=[
                MetricField(field_name="peak_viewers", aggregation="max_value"),
            ],
        )
        result = aggregate_metrics(events, model).execute()
        assert len(result) == 1
        assert result["peak_viewers_max_value"].iloc[0] == 800.0


class TestDimensions:
    def test_entity_id_dimension(self):
        events = _make_events(
            [
                {
                    "source": "youtube",
                    "entity_id": "video_a",
                    "ts": "2024-01-01T10:00:00",
                    "event_type": "field_changed",
                    "payload": _payload("view_count", "100", "150"),
                },
                {
                    "source": "youtube",
                    "entity_id": "video_b",
                    "ts": "2024-01-01T12:00:00",
                    "event_type": "field_changed",
                    "payload": _payload("view_count", "50", "80"),
                },
            ]
        )
        model = MetricsModel(
            name="yt_daily",
            source="youtube",
            grain="daily",
            metric_fields=[
                MetricField(field_name="view_count", aggregation="sum_delta"),
            ],
            dimensions=["entity_id"],
        )
        result = aggregate_metrics(events, model).execute()
        assert len(result) == 2
        va = result[result["entity_id"] == "video_a"]
        vb = result[result["entity_id"] == "video_b"]
        assert va["view_count_sum_delta"].iloc[0] == 50.0
        assert vb["view_count_sum_delta"].iloc[0] == 30.0

    def test_no_dimensions_sums_all(self):
        events = _make_events(
            [
                {
                    "source": "youtube",
                    "entity_id": "video_a",
                    "ts": "2024-01-01T10:00:00",
                    "event_type": "field_changed",
                    "payload": _payload("view_count", "100", "150"),
                },
                {
                    "source": "youtube",
                    "entity_id": "video_b",
                    "ts": "2024-01-01T12:00:00",
                    "event_type": "field_changed",
                    "payload": _payload("view_count", "50", "80"),
                },
            ]
        )
        model = MetricsModel(
            name="yt_daily",
            source="youtube",
            grain="daily",
            metric_fields=[
                MetricField(field_name="view_count", aggregation="sum_delta"),
            ],
        )
        result = aggregate_metrics(events, model).execute()
        assert len(result) == 1
        assert result["view_count_sum_delta"].iloc[0] == 80.0


class TestGrains:
    def test_hourly_grain(self):
        events = _make_events(
            [
                {
                    "source": "src",
                    "entity_id": "a",
                    "ts": "2024-01-01T10:15:00",
                    "event_type": "field_changed",
                    "payload": _payload("x", "0", "10"),
                },
                {
                    "source": "src",
                    "entity_id": "a",
                    "ts": "2024-01-01T10:45:00",
                    "event_type": "field_changed",
                    "payload": _payload("x", "10", "25"),
                },
                {
                    "source": "src",
                    "entity_id": "a",
                    "ts": "2024-01-01T11:15:00",
                    "event_type": "field_changed",
                    "payload": _payload("x", "25", "30"),
                },
            ]
        )
        model = MetricsModel(
            name="test",
            source="src",
            grain="hourly",
            metric_fields=[MetricField(field_name="x", aggregation="sum_delta")],
        )
        result = aggregate_metrics(events, model).execute()
        assert len(result) == 2
        h10 = result[result["_date"] == "2024-01-01T10"]
        h11 = result[result["_date"] == "2024-01-01T11"]
        assert h10["x_sum_delta"].iloc[0] == 25.0  # 10 + 15
        assert h11["x_sum_delta"].iloc[0] == 5.0

    def test_weekly_grain(self):
        # 2024-01-03 is a Wednesday, Monday is 2024-01-01
        # 2024-01-08 is a Monday
        events = _make_events(
            [
                {
                    "source": "src",
                    "entity_id": "a",
                    "ts": "2024-01-03T10:00:00",
                    "event_type": "field_changed",
                    "payload": _payload("x", "0", "10"),
                },
                {
                    "source": "src",
                    "entity_id": "a",
                    "ts": "2024-01-08T10:00:00",
                    "event_type": "field_changed",
                    "payload": _payload("x", "10", "30"),
                },
            ]
        )
        model = MetricsModel(
            name="test",
            source="src",
            grain="weekly",
            metric_fields=[MetricField(field_name="x", aggregation="sum_delta")],
        )
        result = aggregate_metrics(events, model).execute()
        assert len(result) == 2
        w1 = result[result["_date"] == "2024-01-01"]
        w2 = result[result["_date"] == "2024-01-08"]
        assert w1["x_sum_delta"].iloc[0] == 10.0
        assert w2["x_sum_delta"].iloc[0] == 20.0

    def test_monthly_grain(self):
        events = _make_events(
            [
                {
                    "source": "src",
                    "entity_id": "a",
                    "ts": "2024-01-15T10:00:00",
                    "event_type": "field_changed",
                    "payload": _payload("x", "0", "10"),
                },
                {
                    "source": "src",
                    "entity_id": "a",
                    "ts": "2024-02-20T10:00:00",
                    "event_type": "field_changed",
                    "payload": _payload("x", "10", "50"),
                },
            ]
        )
        model = MetricsModel(
            name="test",
            source="src",
            grain="monthly",
            metric_fields=[MetricField(field_name="x", aggregation="sum_delta")],
        )
        result = aggregate_metrics(events, model).execute()
        assert len(result) == 2
        jan = result[result["_date"] == "2024-01-01"]
        feb = result[result["_date"] == "2024-02-01"]
        assert jan["x_sum_delta"].iloc[0] == 10.0
        assert feb["x_sum_delta"].iloc[0] == 40.0


class TestMultipleMetrics:
    def test_multiple_metrics_merged(self):
        events = _make_events(
            [
                {
                    "source": "youtube",
                    "entity_id": "a",
                    "ts": "2024-01-01T10:00:00",
                    "event_type": "field_changed",
                    "payload": _payload("view_count", "100", "200"),
                },
                {
                    "source": "youtube",
                    "entity_id": "a",
                    "ts": "2024-01-01T12:00:00",
                    "event_type": "field_changed",
                    "payload": _payload("subscriber_count", "50", "60"),
                },
            ]
        )
        model = MetricsModel(
            name="yt_daily",
            source="youtube",
            grain="daily",
            metric_fields=[
                MetricField(field_name="view_count", aggregation="sum_delta"),
                MetricField(field_name="subscriber_count", aggregation="last_value"),
            ],
        )
        result = aggregate_metrics(events, model).execute()
        assert len(result) == 1
        assert result["view_count_sum_delta"].iloc[0] == 100.0
        assert result["subscriber_count_last_value"].iloc[0] == 60.0


class TestEdgeCases:
    def test_filters_non_field_changed_events(self):
        events = _make_events(
            [
                {
                    "source": "youtube",
                    "entity_id": "a",
                    "ts": "2024-01-01T10:00:00",
                    "event_type": "row_appeared",
                    "payload": "{}",
                },
                {
                    "source": "youtube",
                    "entity_id": "a",
                    "ts": "2024-01-01T10:00:00",
                    "event_type": "field_changed",
                    "payload": _payload("view_count", "0", "100"),
                },
            ]
        )
        model = MetricsModel(
            name="yt_daily",
            source="youtube",
            grain="daily",
            metric_fields=[
                MetricField(field_name="view_count", aggregation="sum_delta"),
            ],
        )
        result = aggregate_metrics(events, model).execute()
        assert len(result) == 1
        assert result["view_count_sum_delta"].iloc[0] == 100.0

    def test_empty_events(self):
        events = _make_events(
            [
                {
                    "source": "other",
                    "entity_id": "a",
                    "ts": "2024-01-01T10:00:00",
                    "event_type": "field_changed",
                    "payload": _payload("view_count", "0", "100"),
                },
            ]
        )
        model = MetricsModel(
            name="yt_daily",
            source="youtube",
            grain="daily",
            metric_fields=[
                MetricField(field_name="view_count", aggregation="sum_delta"),
            ],
        )
        result = aggregate_metrics(events, model).execute()
        assert len(result) == 0
