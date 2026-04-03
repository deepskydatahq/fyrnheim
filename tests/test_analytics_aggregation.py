"""Tests for the analytics aggregation engine."""

import json

import ibis
import pytest

from fyrnheim.core.analytics_model import StreamAnalyticsModel, StreamMetric
from fyrnheim.engine.analytics_aggregation import aggregate_analytics


def _make_enriched_events(rows: list[dict]) -> ibis.expr.types.Table:
    """Helper to create enriched events memtable."""
    return ibis.memtable(rows)


@pytest.fixture
def basic_events():
    """Basic enriched events for testing."""
    return _make_enriched_events(
        [
            {
                "source": "crm",
                "entity_id": "u1",
                "ts": "2024-01-15",
                "event_type": "signup",
                "payload": "{}",
                "canonical_id": "c1",
            },
            {
                "source": "crm",
                "entity_id": "u2",
                "ts": "2024-01-15",
                "event_type": "purchase",
                "payload": json.dumps({"amount": 50.0}),
                "canonical_id": "c2",
            },
            {
                "source": "billing",
                "entity_id": "u3",
                "ts": "2024-01-16",
                "event_type": "signup",
                "payload": "{}",
                "canonical_id": "c3",
            },
            {
                "source": "crm",
                "entity_id": "u1",
                "ts": "2024-01-16",
                "event_type": "purchase",
                "payload": json.dumps({"amount": 75.0}),
                "canonical_id": "c1",
            },
        ]
    )


class TestDailyAggregation:
    """Test daily date grain aggregation."""

    def test_groups_events_by_daily_date_grain(self, basic_events):
        model = StreamAnalyticsModel(
            name="daily_metrics",
            identity_graph="test_graph",
            date_grain="daily",
            metrics=[
                StreamMetric(
                    name="total_events",
                    expression="*",
                    metric_type="count",
                ),
            ],
        )
        result = aggregate_analytics(basic_events, model).execute()

        # Should have 2 rows (2024-01-15 and 2024-01-16)
        assert len(result) == 2
        assert set(result["_date"]) == {"2024-01-15", "2024-01-16"}

    def test_count_metric_counts_all_events(self, basic_events):
        model = StreamAnalyticsModel(
            name="daily_metrics",
            identity_graph="test_graph",
            date_grain="daily",
            metrics=[
                StreamMetric(
                    name="total_events",
                    expression="*",
                    metric_type="count",
                ),
            ],
        )
        result = aggregate_analytics(basic_events, model).execute()

        day_15 = result[result["_date"] == "2024-01-15"]
        day_16 = result[result["_date"] == "2024-01-16"]
        assert day_15["total_events"].iloc[0] == 2
        assert day_16["total_events"].iloc[0] == 2


class TestCountMetricWithFilter:
    """Test count metric with event_filter."""

    def test_count_metric_with_event_filter(self, basic_events):
        model = StreamAnalyticsModel(
            name="daily_metrics",
            identity_graph="test_graph",
            date_grain="daily",
            metrics=[
                StreamMetric(
                    name="signups",
                    expression="*",
                    metric_type="count",
                    event_filter="signup",
                ),
            ],
        )
        result = aggregate_analytics(basic_events, model).execute()

        day_15 = result[result["_date"] == "2024-01-15"]
        day_16 = result[result["_date"] == "2024-01-16"]
        assert day_15["signups"].iloc[0] == 1
        assert day_16["signups"].iloc[0] == 1

    def test_event_filter_restricts_aggregation(self, basic_events):
        model = StreamAnalyticsModel(
            name="daily_metrics",
            identity_graph="test_graph",
            date_grain="daily",
            metrics=[
                StreamMetric(
                    name="purchases",
                    expression="*",
                    metric_type="count",
                    event_filter="purchase",
                ),
            ],
        )
        result = aggregate_analytics(basic_events, model).execute()

        day_15 = result[result["_date"] == "2024-01-15"]
        day_16 = result[result["_date"] == "2024-01-16"]
        assert day_15["purchases"].iloc[0] == 1
        assert day_16["purchases"].iloc[0] == 1


class TestSumMetric:
    """Test sum metric type."""

    def test_sum_metric_sums_payload_field(self, basic_events):
        model = StreamAnalyticsModel(
            name="daily_metrics",
            identity_graph="test_graph",
            date_grain="daily",
            metrics=[
                StreamMetric(
                    name="total_amount",
                    expression="amount",
                    metric_type="sum",
                    event_filter="purchase",
                ),
            ],
        )
        result = aggregate_analytics(basic_events, model).execute()

        day_15 = result[result["_date"] == "2024-01-15"]
        day_16 = result[result["_date"] == "2024-01-16"]
        assert day_15["total_amount"].iloc[0] == 50.0
        assert day_16["total_amount"].iloc[0] == 75.0


class TestSnapshotMetric:
    """Test snapshot metric type (cumulative distinct canonical_id)."""

    def test_snapshot_counts_distinct_canonical_ids_single_day(self):
        events = _make_enriched_events(
            [
                {
                    "source": "crm",
                    "entity_id": "u1",
                    "ts": "2024-01-15",
                    "event_type": "signup",
                    "payload": "{}",
                    "canonical_id": "c1",
                },
                {
                    "source": "crm",
                    "entity_id": "u2",
                    "ts": "2024-01-15",
                    "event_type": "signup",
                    "payload": "{}",
                    "canonical_id": "c1",
                },
                {
                    "source": "crm",
                    "entity_id": "u3",
                    "ts": "2024-01-15",
                    "event_type": "signup",
                    "payload": "{}",
                    "canonical_id": "c2",
                },
            ]
        )
        model = StreamAnalyticsModel(
            name="daily_metrics",
            identity_graph="test_graph",
            date_grain="daily",
            metrics=[
                StreamMetric(
                    name="unique_customers",
                    expression="canonical_id",
                    metric_type="snapshot",
                ),
            ],
        )
        result = aggregate_analytics(events, model).execute()

        assert len(result) == 1
        assert result["unique_customers"].iloc[0] == 2

    def test_snapshot_computes_cumulative_distinct_counts(self):
        """Day 1 has 3 unique canonical_ids, Day 2 adds 1 new => cumulative = 4."""
        events = _make_enriched_events(
            [
                # Day 1: c1, c2, c3 => 3 unique
                {
                    "source": "crm",
                    "entity_id": "u1",
                    "ts": "2024-01-15",
                    "event_type": "signup",
                    "payload": "{}",
                    "canonical_id": "c1",
                },
                {
                    "source": "crm",
                    "entity_id": "u2",
                    "ts": "2024-01-15",
                    "event_type": "signup",
                    "payload": "{}",
                    "canonical_id": "c2",
                },
                {
                    "source": "crm",
                    "entity_id": "u3",
                    "ts": "2024-01-15",
                    "event_type": "signup",
                    "payload": "{}",
                    "canonical_id": "c3",
                },
                # Day 2: c4 is new, c1 is returning => cumulative = 4
                {
                    "source": "crm",
                    "entity_id": "u4",
                    "ts": "2024-01-16",
                    "event_type": "signup",
                    "payload": "{}",
                    "canonical_id": "c4",
                },
                {
                    "source": "crm",
                    "entity_id": "u1",
                    "ts": "2024-01-16",
                    "event_type": "signup",
                    "payload": "{}",
                    "canonical_id": "c1",
                },
            ]
        )
        model = StreamAnalyticsModel(
            name="daily_metrics",
            identity_graph="test_graph",
            date_grain="daily",
            metrics=[
                StreamMetric(
                    name="unique_customers",
                    expression="canonical_id",
                    metric_type="snapshot",
                ),
            ],
        )
        result = aggregate_analytics(events, model).execute()

        assert len(result) == 2
        day_15 = result[result["_date"] == "2024-01-15"]
        day_16 = result[result["_date"] == "2024-01-16"]
        assert day_15["unique_customers"].iloc[0] == 3
        assert day_16["unique_customers"].iloc[0] == 4


class TestAggregationWithoutCanonicalId:
    """aggregate_analytics works when events have no canonical_id column."""

    def test_count_metric_without_canonical_id(self):
        events = _make_enriched_events([
            {
                "source": "crm",
                "entity_id": "u1",
                "ts": "2024-01-15",
                "event_type": "signup",
                "payload": "{}",
            },
            {
                "source": "crm",
                "entity_id": "u2",
                "ts": "2024-01-15",
                "event_type": "signup",
                "payload": "{}",
            },
        ])
        model = StreamAnalyticsModel(
            name="daily_metrics",
            date_grain="daily",
            metrics=[
                StreamMetric(name="signups", expression="*", metric_type="count"),
            ],
        )
        result = aggregate_analytics(events, model).execute()
        assert len(result) == 1
        assert result["signups"].iloc[0] == 2

    def test_sum_metric_without_canonical_id(self):
        events = _make_enriched_events([
            {
                "source": "billing",
                "entity_id": "u1",
                "ts": "2024-01-15",
                "event_type": "purchase",
                "payload": json.dumps({"amount": 50.0}),
            },
            {
                "source": "billing",
                "entity_id": "u2",
                "ts": "2024-01-15",
                "event_type": "purchase",
                "payload": json.dumps({"amount": 75.0}),
            },
        ])
        model = StreamAnalyticsModel(
            name="daily_metrics",
            date_grain="daily",
            metrics=[
                StreamMetric(
                    name="total_amount",
                    expression="amount",
                    metric_type="sum",
                    event_filter="purchase",
                ),
            ],
        )
        result = aggregate_analytics(events, model).execute()
        assert result["total_amount"].iloc[0] == 125.0

    def test_snapshot_metric_uses_entity_id_when_no_canonical_id(self):
        events = _make_enriched_events([
            {
                "source": "crm",
                "entity_id": "u1",
                "ts": "2024-01-15",
                "event_type": "signup",
                "payload": "{}",
            },
            {
                "source": "crm",
                "entity_id": "u1",
                "ts": "2024-01-15",
                "event_type": "login",
                "payload": "{}",
            },
            {
                "source": "crm",
                "entity_id": "u2",
                "ts": "2024-01-15",
                "event_type": "signup",
                "payload": "{}",
            },
            {
                "source": "crm",
                "entity_id": "u3",
                "ts": "2024-01-16",
                "event_type": "signup",
                "payload": "{}",
            },
        ])
        model = StreamAnalyticsModel(
            name="daily_metrics",
            date_grain="daily",
            metrics=[
                StreamMetric(
                    name="unique_entities",
                    expression="entity_id",
                    metric_type="snapshot",
                ),
            ],
        )
        result = aggregate_analytics(events, model).execute()
        day_15 = result[result["_date"] == "2024-01-15"]
        day_16 = result[result["_date"] == "2024-01-16"]
        assert day_15["unique_entities"].iloc[0] == 2  # u1, u2
        assert day_16["unique_entities"].iloc[0] == 3  # u1, u2, u3 cumulative


class TestDimensions:
    """Test dimension-based grouping."""

    def test_dimensions_create_additional_grouping(self, basic_events):
        model = StreamAnalyticsModel(
            name="daily_metrics",
            identity_graph="test_graph",
            date_grain="daily",
            metrics=[
                StreamMetric(
                    name="event_count",
                    expression="*",
                    metric_type="count",
                ),
            ],
            dimensions=["source"],
        )
        result = aggregate_analytics(basic_events, model).execute()

        # Day 15: crm=2, no billing events
        # Day 16: billing=1, crm=1
        day_15_crm = result[
            (result["_date"] == "2024-01-15") & (result["source"] == "crm")
        ]
        day_16_billing = result[
            (result["_date"] == "2024-01-16") & (result["source"] == "billing")
        ]
        day_16_crm = result[
            (result["_date"] == "2024-01-16") & (result["source"] == "crm")
        ]

        assert day_15_crm["event_count"].iloc[0] == 2
        assert day_16_billing["event_count"].iloc[0] == 1
        assert day_16_crm["event_count"].iloc[0] == 1


class TestDateGrains:
    """Test weekly and monthly date grains."""

    def test_weekly_grain_truncates_to_monday(self):
        events = _make_enriched_events(
            [
                {
                    "source": "crm",
                    "entity_id": "u1",
                    "ts": "2024-01-15",  # Monday
                    "event_type": "signup",
                    "payload": "{}",
                    "canonical_id": "c1",
                },
                {
                    "source": "crm",
                    "entity_id": "u2",
                    "ts": "2024-01-17",  # Wednesday (same week)
                    "event_type": "signup",
                    "payload": "{}",
                    "canonical_id": "c2",
                },
                {
                    "source": "crm",
                    "entity_id": "u3",
                    "ts": "2024-01-22",  # Next Monday
                    "event_type": "signup",
                    "payload": "{}",
                    "canonical_id": "c3",
                },
            ]
        )
        model = StreamAnalyticsModel(
            name="weekly_metrics",
            identity_graph="test_graph",
            date_grain="weekly",
            metrics=[
                StreamMetric(
                    name="signups",
                    expression="*",
                    metric_type="count",
                ),
            ],
        )
        result = aggregate_analytics(events, model).execute()

        assert len(result) == 2
        week_15 = result[result["_date"] == "2024-01-15"]
        week_22 = result[result["_date"] == "2024-01-22"]
        assert week_15["signups"].iloc[0] == 2
        assert week_22["signups"].iloc[0] == 1

    def test_monthly_grain_truncates_to_first(self):
        events = _make_enriched_events(
            [
                {
                    "source": "crm",
                    "entity_id": "u1",
                    "ts": "2024-01-05",
                    "event_type": "signup",
                    "payload": "{}",
                    "canonical_id": "c1",
                },
                {
                    "source": "crm",
                    "entity_id": "u2",
                    "ts": "2024-01-25",
                    "event_type": "signup",
                    "payload": "{}",
                    "canonical_id": "c2",
                },
                {
                    "source": "crm",
                    "entity_id": "u3",
                    "ts": "2024-02-10",
                    "event_type": "signup",
                    "payload": "{}",
                    "canonical_id": "c3",
                },
            ]
        )
        model = StreamAnalyticsModel(
            name="monthly_metrics",
            identity_graph="test_graph",
            date_grain="monthly",
            metrics=[
                StreamMetric(
                    name="signups",
                    expression="*",
                    metric_type="count",
                ),
            ],
        )
        result = aggregate_analytics(events, model).execute()

        assert len(result) == 2
        jan = result[result["_date"] == "2024-01-01"]
        feb = result[result["_date"] == "2024-02-01"]
        assert jan["signups"].iloc[0] == 2
        assert feb["signups"].iloc[0] == 1
