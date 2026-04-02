"""End-to-end tests for analytics aggregation.

Tests the full flow from enriched events through aggregate_analytics
with realistic multi-day, multi-source data.
"""

import ibis

from fyrnheim.core.analytics_model import StreamAnalyticsModel, StreamMetric
from fyrnheim.engine.analytics_aggregation import aggregate_analytics


def _build_enriched_events() -> ibis.expr.types.Table:
    """Build enriched events matching the e2e scenario.

    Day 1 (2024-01-01): 3 signup events from 3 different canonical_ids
        - 2 from source 'crm', 1 from 'billing'
    Day 2 (2024-01-02): 2 signup events
        - 1 new canonical_id, 1 returning (c1)
        - All from 'crm'
    """
    return ibis.memtable(
        [
            # Day 1 - crm signups
            {
                "source": "crm",
                "entity_id": "u1",
                "ts": "2024-01-01",
                "event_type": "signup",
                "payload": "{}",
                "canonical_id": "c1",
            },
            {
                "source": "crm",
                "entity_id": "u2",
                "ts": "2024-01-01",
                "event_type": "signup",
                "payload": "{}",
                "canonical_id": "c2",
            },
            # Day 1 - billing signup
            {
                "source": "billing",
                "entity_id": "u3",
                "ts": "2024-01-01",
                "event_type": "signup",
                "payload": "{}",
                "canonical_id": "c3",
            },
            # Day 2 - crm signups (c1 returns, c4 is new)
            {
                "source": "crm",
                "entity_id": "u4",
                "ts": "2024-01-02",
                "event_type": "signup",
                "payload": "{}",
                "canonical_id": "c4",
            },
            {
                "source": "crm",
                "entity_id": "u1",
                "ts": "2024-01-02",
                "event_type": "signup",
                "payload": "{}",
                "canonical_id": "c1",
            },
        ]
    )


def _build_analytics_model() -> StreamAnalyticsModel:
    """Build analytics model with count + snapshot metrics and source dimension."""
    return StreamAnalyticsModel(
        name="signup_analytics",
        identity_graph="test_graph",
        date_grain="daily",
        metrics=[
            StreamMetric(
                name="signups",
                expression="*",
                metric_type="count",
                event_filter="signup",
            ),
            StreamMetric(
                name="total_customers",
                expression="canonical_id",
                metric_type="snapshot",
            ),
        ],
        dimensions=["source"],
    )


class TestE2eDailyAggregation:
    """End-to-end daily aggregation tests."""

    def test_daily_aggregation_produces_one_row_per_date_source(self):
        events = _build_enriched_events()
        model = _build_analytics_model()
        result = aggregate_analytics(events, model).execute()

        # Day 1: crm + billing = 2 rows; Day 2: crm = 1 row => 3 total
        assert len(result) == 3

    def test_count_metric_counts_signup_events_per_day(self):
        events = _build_enriched_events()
        model = _build_analytics_model()
        result = aggregate_analytics(events, model).execute()

        # Day 1, crm: 2 signups
        day1_crm = result[
            (result["_date"] == "2024-01-01") & (result["source"] == "crm")
        ]
        assert day1_crm["signups"].iloc[0] == 2

        # Day 1, billing: 1 signup
        day1_billing = result[
            (result["_date"] == "2024-01-01") & (result["source"] == "billing")
        ]
        assert day1_billing["signups"].iloc[0] == 1

        # Day 2, crm: 2 signups
        day2_crm = result[
            (result["_date"] == "2024-01-02") & (result["source"] == "crm")
        ]
        assert day2_crm["signups"].iloc[0] == 2

    def test_snapshot_metric_computes_cumulative_distinct_canonical_ids(self):
        events = _build_enriched_events()
        model = _build_analytics_model()
        result = aggregate_analytics(events, model).execute()

        # Day 1, crm: c1, c2 => 2 cumulative unique
        day1_crm = result[
            (result["_date"] == "2024-01-01") & (result["source"] == "crm")
        ]
        assert day1_crm["total_customers"].iloc[0] == 2

        # Day 1, billing: c3 => 1 cumulative unique
        day1_billing = result[
            (result["_date"] == "2024-01-01") & (result["source"] == "billing")
        ]
        assert day1_billing["total_customers"].iloc[0] == 1

        # Day 2, crm: c1 (returning) + c4 (new) => cumulative 3 (c1, c2, c4)
        day2_crm = result[
            (result["_date"] == "2024-01-02") & (result["source"] == "crm")
        ]
        assert day2_crm["total_customers"].iloc[0] == 3

    def test_dimensions_create_correct_sub_groupings(self):
        events = _build_enriched_events()
        model = _build_analytics_model()
        result = aggregate_analytics(events, model).execute()

        # Verify source dimension creates sub-groupings
        day1_sources = set(
            result[result["_date"] == "2024-01-01"]["source"].tolist()
        )
        day2_sources = set(
            result[result["_date"] == "2024-01-02"]["source"].tolist()
        )

        assert day1_sources == {"crm", "billing"}
        assert day2_sources == {"crm"}
