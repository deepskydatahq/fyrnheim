"""Tests for the metrics engine."""

import inspect
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


def test_metrics_aggregation_compiles_to_bigquery_group_by_sql() -> None:
    events = _make_events(
        [
            {
                "source": "website",
                "entity_id": "u1",
                "ts": "2024-01-01T10:00:00",
                "event_type": "field_changed",
                "payload": _payload("score", "1", "3"),
            },
            {
                "source": "website",
                "entity_id": "u1",
                "ts": "2024-01-01T10:05:00",
                "event_type": "signup",
                "payload": json.dumps({"session_id": "s1"}),
            },
        ]
    )
    model = MetricsModel(
        name="engagement",
        sources=["website"],
        grain="daily",
        dimensions=["entity_id"],
        metric_fields=[
            MetricField(field_name="score", aggregation="sum_delta"),
            MetricField(field_name="signup", aggregation="count"),
            MetricField(
                field_name="signup",
                aggregation="count_distinct",
                distinct_field="session_id",
            ),
        ],
    )

    sql = ibis.to_sql(aggregate_metrics(events, model), dialect="bigquery")

    assert "GROUP BY" in sql
    assert "FORMAT_DATETIME" in sql
    assert "JSON" in sql
    assert "COUNT" in sql
    assert "DISTINCT" in sql


def test_aggregate_metrics_does_not_materialize_inputs() -> None:
    source = inspect.getsource(aggregate_metrics)

    assert ".execute(" not in source
    assert "groupby(" not in source


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
            sources=["youtube"],
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
            sources=["youtube"],
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
            sources=["youtube"],
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
            sources=["youtube"],
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
            sources=["youtube"],
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
            sources=["youtube"],
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
            sources=["src"],
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
            sources=["src"],
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
            sources=["src"],
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
            sources=["youtube"],
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
            sources=["youtube"],
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
            sources=["youtube"],
            grain="daily",
            metric_fields=[
                MetricField(field_name="view_count", aggregation="sum_delta"),
            ],
        )
        result = aggregate_metrics(events, model).execute()
        assert len(result) == 0


class TestCount:
    def test_basic_count_daily(self):
        events = _make_events(
            [
                {
                    "source": "website_events",
                    "entity_id": "session_1",
                    "ts": "2024-01-01T10:00:00",
                    "event_type": "blog_post_opened",
                    "payload": json.dumps({"data_page_path": "/blog/post-1"}),
                },
                {
                    "source": "website_events",
                    "entity_id": "session_2",
                    "ts": "2024-01-01T14:00:00",
                    "event_type": "blog_post_opened",
                    "payload": json.dumps({"data_page_path": "/blog/post-2"}),
                },
                {
                    "source": "website_events",
                    "entity_id": "session_3",
                    "ts": "2024-01-02T09:00:00",
                    "event_type": "blog_post_opened",
                    "payload": json.dumps({"data_page_path": "/blog/post-1"}),
                },
            ]
        )
        model = MetricsModel(
            name="website_daily",
            sources=["website_events"],
            grain="daily",
            metric_fields=[
                MetricField(field_name="blog_post_opened", aggregation="count"),
            ],
        )
        result = aggregate_metrics(events, model).execute()
        assert len(result) == 2
        day1 = result[result["_date"] == "2024-01-01"]
        day2 = result[result["_date"] == "2024-01-02"]
        assert day1["blog_post_opened_count"].iloc[0] == 2
        assert day2["blog_post_opened_count"].iloc[0] == 1

    def test_count_filters_source(self):
        events = _make_events(
            [
                {
                    "source": "website_events",
                    "entity_id": "s1",
                    "ts": "2024-01-01T10:00:00",
                    "event_type": "blog_post_opened",
                    "payload": "{}",
                },
                {
                    "source": "other_source",
                    "entity_id": "s2",
                    "ts": "2024-01-01T10:00:00",
                    "event_type": "blog_post_opened",
                    "payload": "{}",
                },
            ]
        )
        model = MetricsModel(
            name="website_daily",
            sources=["website_events"],
            grain="daily",
            metric_fields=[
                MetricField(field_name="blog_post_opened", aggregation="count"),
            ],
        )
        result = aggregate_metrics(events, model).execute()
        assert len(result) == 1
        assert result["blog_post_opened_count"].iloc[0] == 1

    def test_count_ignores_field_changed_events(self):
        events = _make_events(
            [
                {
                    "source": "website_events",
                    "entity_id": "s1",
                    "ts": "2024-01-01T10:00:00",
                    "event_type": "blog_post_opened",
                    "payload": "{}",
                },
                {
                    "source": "website_events",
                    "entity_id": "s1",
                    "ts": "2024-01-01T10:00:00",
                    "event_type": "field_changed",
                    "payload": _payload("view_count", "0", "100"),
                },
            ]
        )
        model = MetricsModel(
            name="website_daily",
            sources=["website_events"],
            grain="daily",
            metric_fields=[
                MetricField(field_name="blog_post_opened", aggregation="count"),
            ],
        )
        result = aggregate_metrics(events, model).execute()
        assert len(result) == 1
        assert result["blog_post_opened_count"].iloc[0] == 1

    def test_count_with_dimensions(self):
        events = _make_events(
            [
                {
                    "source": "website_events",
                    "entity_id": "session_1",
                    "ts": "2024-01-01T10:00:00",
                    "event_type": "blog_post_opened",
                    "payload": "{}",
                },
                {
                    "source": "website_events",
                    "entity_id": "session_2",
                    "ts": "2024-01-01T14:00:00",
                    "event_type": "blog_post_opened",
                    "payload": "{}",
                },
                {
                    "source": "website_events",
                    "entity_id": "session_1",
                    "ts": "2024-01-01T16:00:00",
                    "event_type": "purchase_clicked",
                    "payload": "{}",
                },
            ]
        )
        model = MetricsModel(
            name="website_daily",
            sources=["website_events"],
            grain="daily",
            metric_fields=[
                MetricField(field_name="blog_post_opened", aggregation="count"),
            ],
            dimensions=["entity_id"],
        )
        result = aggregate_metrics(events, model).execute()
        assert len(result) == 2
        s1 = result[result["entity_id"] == "session_1"]
        s2 = result[result["entity_id"] == "session_2"]
        assert s1["blog_post_opened_count"].iloc[0] == 1
        assert s2["blog_post_opened_count"].iloc[0] == 1

    def test_count_empty_when_no_matching_events(self):
        events = _make_events(
            [
                {
                    "source": "website_events",
                    "entity_id": "s1",
                    "ts": "2024-01-01T10:00:00",
                    "event_type": "purchase_clicked",
                    "payload": "{}",
                },
            ]
        )
        model = MetricsModel(
            name="website_daily",
            sources=["website_events"],
            grain="daily",
            metric_fields=[
                MetricField(field_name="blog_post_opened", aggregation="count"),
            ],
        )
        result = aggregate_metrics(events, model).execute()
        assert len(result) == 0

    def test_multiple_event_types_counted(self):
        events = _make_events(
            [
                {
                    "source": "website_events",
                    "entity_id": "s1",
                    "ts": "2024-01-01T10:00:00",
                    "event_type": "blog_post_opened",
                    "payload": "{}",
                },
                {
                    "source": "website_events",
                    "entity_id": "s1",
                    "ts": "2024-01-01T11:00:00",
                    "event_type": "blog_post_opened",
                    "payload": "{}",
                },
                {
                    "source": "website_events",
                    "entity_id": "s1",
                    "ts": "2024-01-01T12:00:00",
                    "event_type": "purchase_clicked",
                    "payload": "{}",
                },
            ]
        )
        model = MetricsModel(
            name="website_daily",
            sources=["website_events"],
            grain="daily",
            metric_fields=[
                MetricField(field_name="blog_post_opened", aggregation="count"),
                MetricField(field_name="purchase_clicked", aggregation="count"),
            ],
        )
        result = aggregate_metrics(events, model).execute()
        assert len(result) == 1
        assert result["blog_post_opened_count"].iloc[0] == 2
        assert result["purchase_clicked_count"].iloc[0] == 1


class TestCountDistinct:
    def test_basic_count_distinct(self):
        events = _make_events(
            [
                {
                    "source": "website_events",
                    "entity_id": "s1",
                    "ts": "2024-01-01T10:00:00",
                    "event_type": "blog_post_opened",
                    "payload": json.dumps({"data_page_path": "/blog/post-1"}),
                },
                {
                    "source": "website_events",
                    "entity_id": "s2",
                    "ts": "2024-01-01T11:00:00",
                    "event_type": "blog_post_opened",
                    "payload": json.dumps({"data_page_path": "/blog/post-1"}),
                },
                {
                    "source": "website_events",
                    "entity_id": "s3",
                    "ts": "2024-01-01T12:00:00",
                    "event_type": "blog_post_opened",
                    "payload": json.dumps({"data_page_path": "/blog/post-2"}),
                },
            ]
        )
        model = MetricsModel(
            name="website_daily",
            sources=["website_events"],
            grain="daily",
            metric_fields=[
                MetricField(
                    field_name="blog_post_opened",
                    aggregation="count_distinct",
                    distinct_field="data_page_path",
                ),
            ],
        )
        result = aggregate_metrics(events, model).execute()
        assert len(result) == 1
        assert result["blog_post_opened_count_distinct"].iloc[0] == 2

    def test_count_distinct_across_days(self):
        events = _make_events(
            [
                {
                    "source": "website_events",
                    "entity_id": "s1",
                    "ts": "2024-01-01T10:00:00",
                    "event_type": "blog_post_opened",
                    "payload": json.dumps({"data_user_hash": "user_a"}),
                },
                {
                    "source": "website_events",
                    "entity_id": "s2",
                    "ts": "2024-01-01T11:00:00",
                    "event_type": "blog_post_opened",
                    "payload": json.dumps({"data_user_hash": "user_a"}),
                },
                {
                    "source": "website_events",
                    "entity_id": "s3",
                    "ts": "2024-01-02T10:00:00",
                    "event_type": "blog_post_opened",
                    "payload": json.dumps({"data_user_hash": "user_a"}),
                },
                {
                    "source": "website_events",
                    "entity_id": "s4",
                    "ts": "2024-01-02T11:00:00",
                    "event_type": "blog_post_opened",
                    "payload": json.dumps({"data_user_hash": "user_b"}),
                },
            ]
        )
        model = MetricsModel(
            name="website_daily",
            sources=["website_events"],
            grain="daily",
            metric_fields=[
                MetricField(
                    field_name="blog_post_opened",
                    aggregation="count_distinct",
                    distinct_field="data_user_hash",
                ),
            ],
        )
        result = aggregate_metrics(events, model).execute()
        assert len(result) == 2
        day1 = result[result["_date"] == "2024-01-01"]
        day2 = result[result["_date"] == "2024-01-02"]
        assert day1["blog_post_opened_count_distinct"].iloc[0] == 1
        assert day2["blog_post_opened_count_distinct"].iloc[0] == 2


class TestMixedAggregations:
    def test_state_and_event_metrics_combined(self):
        """Test that a single MetricsModel can combine state-based and event-based metrics."""
        events = _make_events(
            [
                # State-based: field_changed events from youtube
                {
                    "source": "youtube",
                    "entity_id": "video_a",
                    "ts": "2024-01-01T10:00:00",
                    "event_type": "field_changed",
                    "payload": _payload("view_count", "100", "200"),
                },
                # Event-based: page view events from youtube
                {
                    "source": "youtube",
                    "entity_id": "video_a",
                    "ts": "2024-01-01T11:00:00",
                    "event_type": "video_played",
                    "payload": "{}",
                },
                {
                    "source": "youtube",
                    "entity_id": "video_a",
                    "ts": "2024-01-01T12:00:00",
                    "event_type": "video_played",
                    "payload": "{}",
                },
            ]
        )
        model = MetricsModel(
            name="yt_daily",
            sources=["youtube"],
            grain="daily",
            metric_fields=[
                MetricField(field_name="view_count", aggregation="sum_delta"),
                MetricField(field_name="video_played", aggregation="count"),
            ],
        )
        result = aggregate_metrics(events, model).execute()
        assert len(result) == 1
        assert result["view_count_sum_delta"].iloc[0] == 100.0
        assert result["video_played_count"].iloc[0] == 2

    def test_website_engagement_use_case(self):
        """Real-world use case from the mission: website engagement daily metrics."""
        events = _make_events(
            [
                {
                    "source": "website_events",
                    "entity_id": "s1",
                    "ts": "2024-01-01T10:00:00",
                    "event_type": "blog_post_opened",
                    "payload": json.dumps({"data_page_path": "/blog/post-1", "data_page_type": "blog"}),
                },
                {
                    "source": "website_events",
                    "entity_id": "s2",
                    "ts": "2024-01-01T11:00:00",
                    "event_type": "blog_post_opened",
                    "payload": json.dumps({"data_page_path": "/blog/post-2", "data_page_type": "blog"}),
                },
                {
                    "source": "website_events",
                    "entity_id": "s3",
                    "ts": "2024-01-01T12:00:00",
                    "event_type": "purchase_clicked",
                    "payload": json.dumps({"data_link_url": "https://example.com", "data_page_type": "product"}),
                },
                {
                    "source": "website_events",
                    "entity_id": "s4",
                    "ts": "2024-01-02T10:00:00",
                    "event_type": "blog_post_opened",
                    "payload": json.dumps({"data_page_path": "/blog/post-1", "data_page_type": "blog"}),
                },
            ]
        )
        model = MetricsModel(
            name="website_engagement_daily",
            sources=["website_events"],
            grain="daily",
            metric_fields=[
                MetricField(field_name="blog_post_opened", aggregation="count"),
                MetricField(field_name="purchase_clicked", aggregation="count"),
            ],
        )
        result = aggregate_metrics(events, model).execute()
        assert len(result) == 2
        day1 = result[result["_date"] == "2024-01-01"]
        day2 = result[result["_date"] == "2024-01-02"]
        assert day1["blog_post_opened_count"].iloc[0] == 2
        assert day1["purchase_clicked_count"].iloc[0] == 1
        assert day2["blog_post_opened_count"].iloc[0] == 1
        # purchase_clicked is NaN on day2 (outer join)
        assert pd.isna(day2["purchase_clicked_count"].iloc[0])
