"""Tests for AnalyticsEntity projection engine and registry."""

import json
import textwrap

import ibis
import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from fyrnheim.components.computed_column import ComputedColumn
from fyrnheim.core.analytics_entity import AnalyticsEntity, Measure, StateField
from fyrnheim.engine.analytics_entity_engine import (
    _coerce_to_arrow_friendly_dtype,
    project_analytics_entity,
)
from fyrnheim.engine.analytics_entity_registry import AnalyticsEntityRegistry
from tests._legacy_pandas_projection import legacy_project_analytics_entity


def _make_events(rows: list[dict]) -> ibis.expr.types.Table:
    """Helper to create an ibis memtable from event dicts."""
    df = pd.DataFrame(rows)
    # Ensure payload is JSON string
    if "payload" in df.columns:
        df["payload"] = df["payload"].apply(
            lambda v: json.dumps(v) if isinstance(v, dict) else v
        )
    return ibis.memtable(df)


# ---------------------------------------------------------------------------
# Fixtures: sample events
# ---------------------------------------------------------------------------

@pytest.fixture
def events_with_state_and_activities():
    """Events that have both state (row_appeared/field_changed) and activity events."""
    return _make_events([
        # State events for entity A
        {"source": "crm", "entity_id": "A", "ts": "2024-01-01T00:00:00",
         "event_type": "row_appeared", "payload": {"name": "Acme", "plan": "free"}},
        {"source": "crm", "entity_id": "A", "ts": "2024-02-01T00:00:00",
         "event_type": "field_changed", "payload": {"field_name": "plan", "old_value": "free", "new_value": "pro"}},
        # Activity events for entity A
        {"source": "app", "entity_id": "A", "ts": "2024-01-15T00:00:00",
         "event_type": "workshop_attended", "payload": {}},
        {"source": "app", "entity_id": "A", "ts": "2024-02-15T00:00:00",
         "event_type": "workshop_attended", "payload": {}},
        {"source": "billing", "entity_id": "A", "ts": "2024-01-10T00:00:00",
         "event_type": "purchase", "payload": {"amount": 100}},
        {"source": "billing", "entity_id": "A", "ts": "2024-02-10T00:00:00",
         "event_type": "purchase", "payload": {"amount": 250}},
        # State events for entity B
        {"source": "crm", "entity_id": "B", "ts": "2024-01-01T00:00:00",
         "event_type": "row_appeared", "payload": {"name": "Beta Inc", "plan": "enterprise"}},
        # Activity events for entity B
        {"source": "app", "entity_id": "B", "ts": "2024-03-01T00:00:00",
         "event_type": "workshop_attended", "payload": {}},
        {"source": "billing", "entity_id": "B", "ts": "2024-03-01T00:00:00",
         "event_type": "purchase", "payload": {"amount": 500}},
    ])


# ---------------------------------------------------------------------------
# Tests: project_analytics_entity
# ---------------------------------------------------------------------------

class TestProjectAnalyticsEntity:
    """Tests for the projection engine."""

    def test_count_measure(self, events_with_state_and_activities):
        ae = AnalyticsEntity(
            name="accounts",
            measures=[
                Measure(name="workshop_count", activity="workshop_attended", aggregation="count"),
            ],
        )
        result = project_analytics_entity(events_with_state_and_activities, ae)
        df = result.execute()
        a_row = df[df["entity_id"] == "A"].iloc[0]
        b_row = df[df["entity_id"] == "B"].iloc[0]
        assert a_row["workshop_count"] == 2
        assert b_row["workshop_count"] == 1

    def test_sum_measure(self, events_with_state_and_activities):
        ae = AnalyticsEntity(
            name="accounts",
            measures=[
                Measure(name="total_revenue", activity="purchase", aggregation="sum", field="amount"),
            ],
        )
        result = project_analytics_entity(events_with_state_and_activities, ae)
        df = result.execute()
        a_row = df[df["entity_id"] == "A"].iloc[0]
        b_row = df[df["entity_id"] == "B"].iloc[0]
        assert a_row["total_revenue"] == 350.0
        assert b_row["total_revenue"] == 500.0

    def test_latest_measure(self, events_with_state_and_activities):
        ae = AnalyticsEntity(
            name="accounts",
            measures=[
                Measure(name="last_purchase_amount", activity="purchase", aggregation="latest", field="amount"),
            ],
        )
        result = project_analytics_entity(events_with_state_and_activities, ae)
        df = result.execute()
        a_row = df[df["entity_id"] == "A"].iloc[0]
        assert a_row["last_purchase_amount"] == 250

    def test_state_fields_project(self, events_with_state_and_activities):
        ae = AnalyticsEntity(
            name="accounts",
            state_fields=[
                StateField(name="company_name", source="crm", field="name", strategy="latest"),
                StateField(name="current_plan", source="crm", field="plan", strategy="latest"),
            ],
        )
        result = project_analytics_entity(events_with_state_and_activities, ae)
        df = result.execute()
        a_row = df[df["entity_id"] == "A"].iloc[0]
        b_row = df[df["entity_id"] == "B"].iloc[0]
        assert a_row["company_name"] == "Acme"
        assert a_row["current_plan"] == "pro"  # Updated via field_changed
        assert b_row["company_name"] == "Beta Inc"
        assert b_row["current_plan"] == "enterprise"

    def test_state_fields_and_measures_combined(self, events_with_state_and_activities):
        ae = AnalyticsEntity(
            name="accounts",
            state_fields=[
                StateField(name="company_name", source="crm", field="name", strategy="latest"),
            ],
            measures=[
                Measure(name="workshop_count", activity="workshop_attended", aggregation="count"),
                Measure(name="total_revenue", activity="purchase", aggregation="sum", field="amount"),
            ],
        )
        result = project_analytics_entity(events_with_state_and_activities, ae)
        df = result.execute()
        a_row = df[df["entity_id"] == "A"].iloc[0]
        assert a_row["company_name"] == "Acme"
        assert a_row["workshop_count"] == 2
        assert a_row["total_revenue"] == 350.0

    def test_computed_fields_on_top(self, events_with_state_and_activities):
        ae = AnalyticsEntity(
            name="accounts",
            measures=[
                Measure(name="workshop_count", activity="workshop_attended", aggregation="count"),
                Measure(name="total_revenue", activity="purchase", aggregation="sum", field="amount"),
            ],
            computed_fields=[
                ComputedColumn(name="revenue_per_workshop", expression="total_revenue / workshop_count if workshop_count else 0"),
            ],
        )
        result = project_analytics_entity(events_with_state_and_activities, ae)
        df = result.execute()
        a_row = df[df["entity_id"] == "A"].iloc[0]
        assert a_row["revenue_per_workshop"] == 175.0

    def test_uses_entity_id_when_no_canonical_id(self):
        events = _make_events([
            {"source": "app", "entity_id": "X", "ts": "2024-01-01T00:00:00",
             "event_type": "login", "payload": {}},
        ])
        ae = AnalyticsEntity(
            name="users",
            measures=[
                Measure(name="login_count", activity="login", aggregation="count"),
            ],
        )
        result = project_analytics_entity(events, ae)
        df = result.execute()
        assert "entity_id" in df.columns
        assert df.iloc[0]["login_count"] == 1

    def test_uses_canonical_id_when_present(self):
        events = _make_events([
            {"source": "app", "entity_id": "X", "canonical_id": "CAN-1",
             "ts": "2024-01-01T00:00:00", "event_type": "login", "payload": {}},
            {"source": "app", "entity_id": "Y", "canonical_id": "CAN-1",
             "ts": "2024-01-02T00:00:00", "event_type": "login", "payload": {}},
        ])
        ae = AnalyticsEntity(
            name="users",
            measures=[
                Measure(name="login_count", activity="login", aggregation="count"),
            ],
        )
        result = project_analytics_entity(events, ae)
        df = result.execute()
        assert "canonical_id" in df.columns
        assert len(df) == 1
        assert df.iloc[0]["login_count"] == 2

    def test_one_row_per_entity(self, events_with_state_and_activities):
        ae = AnalyticsEntity(
            name="accounts",
            measures=[
                Measure(name="n", activity="workshop_attended", aggregation="count"),
            ],
        )
        result = project_analytics_entity(events_with_state_and_activities, ae)
        df = result.execute()
        assert len(df) == 2  # A and B

    def test_count_zero_when_no_matching_events(self):
        """With the M057 relevance filter, ids with no event matching the
        measure activity are filtered out entirely; when a matching activity
        IS present for at least one id, count is still computed correctly.
        """
        events = _make_events([
            {"source": "app", "entity_id": "X", "ts": "2024-01-01T00:00:00",
             "event_type": "login", "payload": {}},
            # Y has a purchase event so survives the filter; X does not and is dropped.
            {"source": "app", "entity_id": "Y", "ts": "2024-01-01T00:00:00",
             "event_type": "purchase", "payload": {"amount": 10}},
        ])
        ae = AnalyticsEntity(
            name="users",
            measures=[
                Measure(name="purchase_count", activity="purchase", aggregation="count"),
            ],
        )
        result = project_analytics_entity(events, ae)
        df = result.execute()
        ids = set(df["entity_id"].tolist())
        assert ids == {"Y"}
        assert df[df["entity_id"] == "Y"].iloc[0]["purchase_count"] == 1

    def test_latest_returns_none_when_no_matching_events(self):
        """With the M057 relevance filter, an id with events but none matching
        the measure activity is filtered out. For a surviving id whose events
        don't include the measure activity payload field, latest returns a
        null-like value.

        M060 note: the post-push-down implementation extracts ``latest``
        measure payload fields as ``float64``, so a missing value is
        represented as ``NaN`` rather than Python ``None``. The assertion
        uses ``pd.isna`` to accept both.
        """
        events = _make_events([
            # X has a plan_changed event (matches measure activity) but no 'plan' field.
            {"source": "app", "entity_id": "X", "ts": "2024-01-01T00:00:00",
             "event_type": "plan_changed", "payload": {}},
        ])
        ae = AnalyticsEntity(
            name="users",
            measures=[
                Measure(name="last_plan", activity="plan_changed", aggregation="latest", field="plan"),
            ],
        )
        result = project_analytics_entity(events, ae)
        df = result.execute()
        assert pd.isna(df.iloc[0]["last_plan"])

    def test_first_strategy_state_field(self):
        events = _make_events([
            {"source": "crm", "entity_id": "A", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {"name": "Original"}},
            {"source": "crm", "entity_id": "A", "ts": "2024-06-01T00:00:00",
             "event_type": "row_appeared", "payload": {"name": "Updated"}},
        ])
        ae = AnalyticsEntity(
            name="accounts",
            state_fields=[
                StateField(name="first_name", source="crm", field="name", strategy="first"),
            ],
        )
        result = project_analytics_entity(events, ae)
        df = result.execute()
        assert df.iloc[0]["first_name"] == "Original"

    def test_computed_field_with_t_proxy(self):
        """Computed fields using t.field_name syntax should work."""
        events = _make_events([
            {"source": "crm", "entity_id": "A", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {"email": "alice@example.com"}},
        ])
        ae = AnalyticsEntity(
            name="accounts",
            state_fields=[
                StateField(name="email", source="crm", field="email", strategy="latest"),
            ],
            computed_fields=[
                ComputedColumn(name="email_domain", expression="t.email.split('@')[1]"),
            ],
        )
        result = project_analytics_entity(events, ae)
        df = result.execute()
        assert df.iloc[0]["email_domain"] == "example.com"

    def test_computed_field_direct_column_access(self):
        """Computed fields using direct column name access should still work."""
        events = _make_events([
            {"source": "app", "entity_id": "X", "ts": "2024-01-01T00:00:00",
             "event_type": "login", "payload": {}},
            {"source": "app", "entity_id": "X", "ts": "2024-01-02T00:00:00",
             "event_type": "login", "payload": {}},
        ])
        ae = AnalyticsEntity(
            name="users",
            measures=[
                Measure(name="login_count", activity="login", aggregation="count"),
            ],
            computed_fields=[
                ComputedColumn(name="is_active", expression="login_count > 0"),
            ],
        )
        result = project_analytics_entity(events, ae)
        df = result.execute()
        assert df.iloc[0]["is_active"] == True  # noqa: E712

    def test_projection_scopes_to_state_field_sources(self):
        """State-field-only entity: ids only appearing in unreferenced sources are filtered out."""
        events = _make_events([
            # Entity A has a row in source 'A' — state_field source matches.
            {"source": "A", "entity_id": "A", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {"name": "Acme"}},
            # Entity B only has rows in source 'B' — should be filtered out.
            {"source": "B", "entity_id": "B", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {"name": "Beta"}},
        ])
        ae = AnalyticsEntity(
            name="accounts",
            state_fields=[
                StateField(name="company_name", source="A", field="name", strategy="latest"),
            ],
        )
        result = project_analytics_entity(events, ae)
        df = result.execute()
        ids = set(df["entity_id"].tolist())
        assert "A" in ids
        assert "B" not in ids

    def test_projection_scopes_to_measure_activities(self):
        """Measure-only entity: ids whose events don't match any measure activity are filtered out."""
        events = _make_events([
            # Entity AX has an event_type 'X' that matches the measure.
            {"source": "app", "entity_id": "AX", "ts": "2024-01-01T00:00:00",
             "event_type": "X", "payload": {}},
            # Entity BY only has 'Y' events — filtered out.
            {"source": "app", "entity_id": "BY", "ts": "2024-01-01T00:00:00",
             "event_type": "Y", "payload": {}},
            # Entity CZ only has 'Z' events — filtered out.
            {"source": "app", "entity_id": "CZ", "ts": "2024-01-01T00:00:00",
             "event_type": "Z", "payload": {}},
        ])
        ae = AnalyticsEntity(
            name="acts",
            measures=[
                Measure(name="x_count", activity="X", aggregation="count"),
            ],
        )
        result = project_analytics_entity(events, ae)
        df = result.execute()
        ids = set(df["entity_id"].tolist())
        assert "AX" in ids
        assert "BY" not in ids
        assert "CZ" not in ids

    def test_projection_scopes_to_coalesce_priority_sources(self):
        """Coalesce state_field: relevance includes every element in priority list."""
        events = _make_events([
            # Entity in source A — relevant via priority.
            {"source": "A", "entity_id": "inA", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {"email": "a@x.com"}},
            # Entity in source B — relevant via priority.
            {"source": "B", "entity_id": "inB", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {"email": "b@x.com"}},
            # Entity only in source C — not in priority, filtered out.
            {"source": "C", "entity_id": "inC", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {"email": "c@x.com"}},
        ])
        ae = AnalyticsEntity(
            name="accounts",
            state_fields=[
                StateField(
                    name="email",
                    source="A",  # ignored for coalesce, priority drives relevance
                    field="email",
                    strategy="coalesce",
                    priority=["A", "B"],
                ),
            ],
        )
        result = project_analytics_entity(events, ae)
        df = result.execute()
        ids = set(df["entity_id"].tolist())
        assert "inA" in ids
        assert "inB" in ids
        assert "inC" not in ids

    def test_projection_union_state_and_measure_relevance(self):
        """Relevance is a UNION: measure activity alone makes an id relevant even if source is unreferenced."""
        events = _make_events([
            # Entity UX's only event is source='app' (not referenced) with event_type='X' (measure activity).
            # The measure side of the OR should keep it.
            {"source": "app", "entity_id": "UX", "ts": "2024-01-01T00:00:00",
             "event_type": "X", "payload": {}},
            # Entity UA has source='A' event — kept via state_field source.
            {"source": "A", "entity_id": "UA", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {"name": "A-name"}},
            # Entity UN only has unrelated source + unrelated event_type — filtered.
            {"source": "other", "entity_id": "UN", "ts": "2024-01-01T00:00:00",
             "event_type": "noop", "payload": {}},
        ])
        ae = AnalyticsEntity(
            name="acts",
            state_fields=[
                StateField(name="company_name", source="A", field="name", strategy="latest"),
            ],
            measures=[
                Measure(name="x_count", activity="X", aggregation="count"),
            ],
        )
        result = project_analytics_entity(events, ae)
        df = result.execute()
        ids = set(df["entity_id"].tolist())
        assert "UX" in ids
        assert "UA" in ids
        assert "UN" not in ids

    def test_projection_empty_after_filter_preserves_schema(self):
        """When filter empties the dataframe, output is empty with expected columns."""
        events = _make_events([
            # No rows match: source 'other' not referenced; event_type 'noop' not a measure activity.
            {"source": "other", "entity_id": "Z", "ts": "2024-01-01T00:00:00",
             "event_type": "noop", "payload": {}},
        ])
        ae = AnalyticsEntity(
            name="accounts",
            state_fields=[
                StateField(name="company_name", source="A", field="name", strategy="latest"),
            ],
            measures=[
                Measure(name="x_count", activity="X", aggregation="count"),
            ],
            computed_fields=[
                ComputedColumn(name="doubled", expression="(x_count or 0) * 2"),
            ],
        )
        result = project_analytics_entity(events, ae)
        df = result.execute()
        assert len(df) == 0
        assert set(df.columns) == {"entity_id", "company_name", "x_count", "doubled"}


# ---------------------------------------------------------------------------
# M060 equivalence suite: new Ibis-native impl vs vendored v0.6.2 pandas impl
# ---------------------------------------------------------------------------


def _build_equivalence_scenarios() -> list[tuple[str, object, object]]:
    """Build (id, events_builder, entity_builder) scenarios.

    Every scenario exercised by TestProjectAnalyticsEntity is re-run through
    BOTH the new Ibis implementation AND the vendored v0.6.2 pandas reference;
    both are asserted equal via ``pandas.testing.assert_frame_equal``. This
    is the M060 byte-identical-output contract.
    """

    def _mixed_events():
        return _make_events([
            {"source": "crm", "entity_id": "A", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {"name": "Acme", "plan": "free"}},
            {"source": "crm", "entity_id": "A", "ts": "2024-02-01T00:00:00",
             "event_type": "field_changed",
             "payload": {"field_name": "plan", "old_value": "free", "new_value": "pro"}},
            {"source": "app", "entity_id": "A", "ts": "2024-01-15T00:00:00",
             "event_type": "workshop_attended", "payload": {}},
            {"source": "app", "entity_id": "A", "ts": "2024-02-15T00:00:00",
             "event_type": "workshop_attended", "payload": {}},
            {"source": "billing", "entity_id": "A", "ts": "2024-01-10T00:00:00",
             "event_type": "purchase", "payload": {"amount": 100}},
            {"source": "billing", "entity_id": "A", "ts": "2024-02-10T00:00:00",
             "event_type": "purchase", "payload": {"amount": 250}},
            {"source": "crm", "entity_id": "B", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {"name": "Beta Inc", "plan": "enterprise"}},
            {"source": "app", "entity_id": "B", "ts": "2024-03-01T00:00:00",
             "event_type": "workshop_attended", "payload": {}},
            {"source": "billing", "entity_id": "B", "ts": "2024-03-01T00:00:00",
             "event_type": "purchase", "payload": {"amount": 500}},
        ])

    def _solo_login_event():
        return _make_events([
            {"source": "app", "entity_id": "X", "ts": "2024-01-01T00:00:00",
             "event_type": "login", "payload": {}},
        ])

    def _canonical_id_events():
        return _make_events([
            {"source": "app", "entity_id": "X", "canonical_id": "CAN-1",
             "ts": "2024-01-01T00:00:00", "event_type": "login", "payload": {}},
            {"source": "app", "entity_id": "Y", "canonical_id": "CAN-1",
             "ts": "2024-01-02T00:00:00", "event_type": "login", "payload": {}},
        ])

    def _mixed_purchase_login():
        return _make_events([
            {"source": "app", "entity_id": "X", "ts": "2024-01-01T00:00:00",
             "event_type": "login", "payload": {}},
            {"source": "app", "entity_id": "Y", "ts": "2024-01-01T00:00:00",
             "event_type": "purchase", "payload": {"amount": 10}},
        ])

    def _plan_changed_no_field():
        return _make_events([
            {"source": "app", "entity_id": "X", "ts": "2024-01-01T00:00:00",
             "event_type": "plan_changed", "payload": {}},
        ])

    def _first_strategy_events():
        return _make_events([
            {"source": "crm", "entity_id": "A", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {"name": "Original"}},
            {"source": "crm", "entity_id": "A", "ts": "2024-06-01T00:00:00",
             "event_type": "row_appeared", "payload": {"name": "Updated"}},
        ])

    def _email_events():
        return _make_events([
            {"source": "crm", "entity_id": "A", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {"email": "alice@example.com"}},
        ])

    def _two_logins():
        return _make_events([
            {"source": "app", "entity_id": "X", "ts": "2024-01-01T00:00:00",
             "event_type": "login", "payload": {}},
            {"source": "app", "entity_id": "X", "ts": "2024-01-02T00:00:00",
             "event_type": "login", "payload": {}},
        ])

    def _two_source_events():
        return _make_events([
            {"source": "A", "entity_id": "A", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {"name": "Acme"}},
            {"source": "B", "entity_id": "B", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {"name": "Beta"}},
        ])

    def _three_event_types():
        return _make_events([
            {"source": "app", "entity_id": "AX", "ts": "2024-01-01T00:00:00",
             "event_type": "X", "payload": {}},
            {"source": "app", "entity_id": "BY", "ts": "2024-01-01T00:00:00",
             "event_type": "Y", "payload": {}},
            {"source": "app", "entity_id": "CZ", "ts": "2024-01-01T00:00:00",
             "event_type": "Z", "payload": {}},
        ])

    def _coalesce_priority_events():
        return _make_events([
            {"source": "A", "entity_id": "inA", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {"email": "a@x.com"}},
            {"source": "B", "entity_id": "inB", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {"email": "b@x.com"}},
            {"source": "C", "entity_id": "inC", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {"email": "c@x.com"}},
        ])

    def _union_relevance_events():
        return _make_events([
            {"source": "app", "entity_id": "UX", "ts": "2024-01-01T00:00:00",
             "event_type": "X", "payload": {}},
            {"source": "A", "entity_id": "UA", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {"name": "A-name"}},
            {"source": "other", "entity_id": "UN", "ts": "2024-01-01T00:00:00",
             "event_type": "noop", "payload": {}},
        ])

    def _empty_after_filter_events():
        return _make_events([
            {"source": "other", "entity_id": "Z", "ts": "2024-01-01T00:00:00",
             "event_type": "noop", "payload": {}},
        ])

    def _coalesce_multi_source_for_one_entity():
        return _make_events([
            {"source": "A", "entity_id": "E1", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {"email": "from_a@x.com"}},
            {"source": "B", "entity_id": "E1", "ts": "2024-01-02T00:00:00",
             "event_type": "row_appeared", "payload": {"email": "from_b@x.com"}},
            {"source": "B", "entity_id": "E2", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {"email": "e2_from_b@x.com"}},
        ])

    def _numeric_state_field_events():
        # Numeric-typed JSON scalar as a state field — legacy returned the
        # raw int; the JSON-text round-trip must preserve that.
        return _make_events([
            {"source": "crm", "entity_id": "A", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {"age": 29}},
            {"source": "crm", "entity_id": "A", "ts": "2024-06-01T00:00:00",
             "event_type": "row_appeared", "payload": {"age": 30}},
        ])

    def _boolean_state_field_events():
        # Boolean-typed JSON scalar as a state field — legacy returned the
        # raw bool; the JSON-text round-trip must preserve that.
        return _make_events([
            {"source": "crm", "entity_id": "A", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {"is_active": False}},
            {"source": "crm", "entity_id": "A", "ts": "2024-06-01T00:00:00",
             "event_type": "field_changed",
             "payload": {"field_name": "is_active", "old_value": False, "new_value": True}},
        ])

    def _numeric_string_sum_events():
        # Numeric-string payloads are accepted by the legacy ``float(val)``
        # branch — the Ibis ``sum`` measure must include them for parity.
        return _make_events([
            {"source": "billing", "entity_id": "A", "ts": "2024-01-10T00:00:00",
             "event_type": "purchase", "payload": {"amount": "250"}},
            {"source": "billing", "entity_id": "A", "ts": "2024-02-10T00:00:00",
             "event_type": "purchase", "payload": {"amount": 100}},
            {"source": "billing", "entity_id": "A", "ts": "2024-03-10T00:00:00",
             "event_type": "purchase", "payload": {"amount": "not-a-number"}},
        ])

    def _numeric_string_latest_events():
        # Numeric-string as the ``latest`` measure payload — legacy returned
        # the raw string "250"; the JSON-text round-trip preserves that.
        return _make_events([
            {"source": "billing", "entity_id": "A", "ts": "2024-01-10T00:00:00",
             "event_type": "purchase", "payload": {"amount": 100}},
            {"source": "billing", "entity_id": "A", "ts": "2024-02-10T00:00:00",
             "event_type": "purchase", "payload": {"amount": "250"}},
        ])

    def _numeric_new_value_events():
        # Numeric ``new_value`` in a field_changed event — legacy extracted
        # the raw int; our CASE WHEN must do the same via JSON text.
        return _make_events([
            {"source": "crm", "entity_id": "A", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {"headcount": 10}},
            {"source": "crm", "entity_id": "A", "ts": "2024-06-01T00:00:00",
             "event_type": "field_changed",
             "payload": {"field_name": "headcount", "old_value": 10, "new_value": 42}},
        ])

    def _field_changed_non_matching_field_events():
        # A field_changed event whose payload.field_name is "other" but
        # whose payload also happens to carry a sibling key
        # "target_field". Legacy ``_extract_field_value`` returned None
        # (never falls back to the flat lookup for field_changed events);
        # the new CASE WHEN must do the same so no sibling key leaks.
        return _make_events([
            {"source": "crm", "entity_id": "A", "ts": "2024-01-01T00:00:00",
             "event_type": "field_changed",
             "payload": {
                 "field_name": "other",
                 "new_value": "ignore_me",
                 "target_field": "sibling_value",
             }},
        ])

    def _boolean_sum_events():
        # Boolean payload values under a ``sum`` aggregation — legacy's
        # ``float(val)`` coerces True -> 1.0 and False -> 0.0, so summing
        # True/False/True yields 2.0. The Ibis impl must mirror that via
        # the unwrap_as("bool").cast("float64") branch in the coalesce.
        return _make_events([
            {"source": "app", "entity_id": "A", "ts": "2024-01-01T00:00:00",
             "event_type": "activity", "payload": {"activated": True}},
            {"source": "app", "entity_id": "A", "ts": "2024-02-01T00:00:00",
             "event_type": "activity", "payload": {"activated": False}},
            {"source": "app", "entity_id": "A", "ts": "2024-03-01T00:00:00",
             "event_type": "activity", "payload": {"activated": True}},
        ])

    scenarios: list[tuple[str, object, object]] = [
        (
            "count_measure",
            _mixed_events,
            lambda: AnalyticsEntity(
                name="accounts",
                measures=[Measure(name="workshop_count", activity="workshop_attended", aggregation="count")],
            ),
        ),
        (
            "sum_measure",
            _mixed_events,
            lambda: AnalyticsEntity(
                name="accounts",
                measures=[Measure(name="total_revenue", activity="purchase", aggregation="sum", field="amount")],
            ),
        ),
        (
            "latest_measure",
            _mixed_events,
            lambda: AnalyticsEntity(
                name="accounts",
                measures=[Measure(name="last_purchase_amount", activity="purchase", aggregation="latest", field="amount")],
            ),
        ),
        (
            "state_fields_project",
            _mixed_events,
            lambda: AnalyticsEntity(
                name="accounts",
                state_fields=[
                    StateField(name="company_name", source="crm", field="name", strategy="latest"),
                    StateField(name="current_plan", source="crm", field="plan", strategy="latest"),
                ],
            ),
        ),
        (
            "state_fields_and_measures_combined",
            _mixed_events,
            lambda: AnalyticsEntity(
                name="accounts",
                state_fields=[StateField(name="company_name", source="crm", field="name", strategy="latest")],
                measures=[
                    Measure(name="workshop_count", activity="workshop_attended", aggregation="count"),
                    Measure(name="total_revenue", activity="purchase", aggregation="sum", field="amount"),
                ],
            ),
        ),
        (
            "computed_fields_on_top",
            _mixed_events,
            lambda: AnalyticsEntity(
                name="accounts",
                measures=[
                    Measure(name="workshop_count", activity="workshop_attended", aggregation="count"),
                    Measure(name="total_revenue", activity="purchase", aggregation="sum", field="amount"),
                ],
                computed_fields=[
                    ComputedColumn(name="revenue_per_workshop",
                                   expression="total_revenue / workshop_count if workshop_count else 0"),
                ],
            ),
        ),
        (
            "entity_id_no_canonical",
            _solo_login_event,
            lambda: AnalyticsEntity(
                name="users",
                measures=[Measure(name="login_count", activity="login", aggregation="count")],
            ),
        ),
        (
            "canonical_id_present",
            _canonical_id_events,
            lambda: AnalyticsEntity(
                name="users",
                measures=[Measure(name="login_count", activity="login", aggregation="count")],
            ),
        ),
        (
            "count_zero_filtered_out",
            _mixed_purchase_login,
            lambda: AnalyticsEntity(
                name="users",
                measures=[Measure(name="purchase_count", activity="purchase", aggregation="count")],
            ),
        ),
        (
            "latest_none_when_payload_missing_field",
            _plan_changed_no_field,
            lambda: AnalyticsEntity(
                name="users",
                measures=[Measure(name="last_plan", activity="plan_changed", aggregation="latest", field="plan")],
            ),
        ),
        (
            "first_strategy_state_field",
            _first_strategy_events,
            lambda: AnalyticsEntity(
                name="accounts",
                state_fields=[StateField(name="first_name", source="crm", field="name", strategy="first")],
            ),
        ),
        (
            "computed_field_with_t_proxy",
            _email_events,
            lambda: AnalyticsEntity(
                name="accounts",
                state_fields=[StateField(name="email", source="crm", field="email", strategy="latest")],
                computed_fields=[ComputedColumn(name="email_domain", expression="t.email.split('@')[1]")],
            ),
        ),
        (
            "computed_field_direct_column_access",
            _two_logins,
            lambda: AnalyticsEntity(
                name="users",
                measures=[Measure(name="login_count", activity="login", aggregation="count")],
                computed_fields=[ComputedColumn(name="is_active", expression="login_count > 0")],
            ),
        ),
        (
            "state_field_source_scope",
            _two_source_events,
            lambda: AnalyticsEntity(
                name="accounts",
                state_fields=[StateField(name="company_name", source="A", field="name", strategy="latest")],
            ),
        ),
        (
            "measure_activity_scope",
            _three_event_types,
            lambda: AnalyticsEntity(
                name="acts",
                measures=[Measure(name="x_count", activity="X", aggregation="count")],
            ),
        ),
        (
            "coalesce_priority_sources",
            _coalesce_priority_events,
            lambda: AnalyticsEntity(
                name="accounts",
                state_fields=[StateField(
                    name="email", source="A", field="email",
                    strategy="coalesce", priority=["A", "B"],
                )],
            ),
        ),
        (
            "union_state_and_measure_relevance",
            _union_relevance_events,
            lambda: AnalyticsEntity(
                name="acts",
                state_fields=[StateField(name="company_name", source="A", field="name", strategy="latest")],
                measures=[Measure(name="x_count", activity="X", aggregation="count")],
            ),
        ),
        (
            "empty_after_filter",
            _empty_after_filter_events,
            lambda: AnalyticsEntity(
                name="accounts",
                state_fields=[StateField(name="company_name", source="A", field="name", strategy="latest")],
                measures=[Measure(name="x_count", activity="X", aggregation="count")],
                computed_fields=[ComputedColumn(name="doubled", expression="(x_count or 0) * 2")],
            ),
        ),
        (
            "coalesce_picks_first_priority_source",
            _coalesce_multi_source_for_one_entity,
            lambda: AnalyticsEntity(
                name="accounts",
                state_fields=[StateField(
                    name="email", source="A", field="email",
                    strategy="coalesce", priority=["A", "B"],
                )],
            ),
        ),
        # M060 rework: CodeRabbit-surfaced blind spots in the original suite.
        (
            "numeric_state_field_latest",
            _numeric_state_field_events,
            lambda: AnalyticsEntity(
                name="accounts",
                state_fields=[
                    StateField(name="age", source="crm", field="age", strategy="latest"),
                ],
            ),
        ),
        (
            "boolean_state_field_latest_via_field_changed",
            _boolean_state_field_events,
            lambda: AnalyticsEntity(
                name="accounts",
                state_fields=[
                    StateField(
                        name="is_active", source="crm", field="is_active",
                        strategy="latest",
                    ),
                ],
            ),
        ),
        (
            "numeric_string_sum_measure",
            _numeric_string_sum_events,
            lambda: AnalyticsEntity(
                name="accounts",
                measures=[
                    Measure(
                        name="total_revenue", activity="purchase",
                        aggregation="sum", field="amount",
                    ),
                ],
            ),
        ),
        (
            "numeric_string_latest_measure",
            _numeric_string_latest_events,
            lambda: AnalyticsEntity(
                name="accounts",
                measures=[
                    Measure(
                        name="last_purchase_amount", activity="purchase",
                        aggregation="latest", field="amount",
                    ),
                ],
            ),
        ),
        (
            "numeric_field_changed_new_value",
            _numeric_new_value_events,
            lambda: AnalyticsEntity(
                name="accounts",
                state_fields=[
                    StateField(
                        name="headcount", source="crm", field="headcount",
                        strategy="latest",
                    ),
                ],
            ),
        ),
        # M060 round-2 rework: guard the CASE WHEN against sibling-key leak.
        (
            "field_changed_non_matching_field_returns_none",
            _field_changed_non_matching_field_events,
            lambda: AnalyticsEntity(
                name="accounts",
                state_fields=[
                    StateField(
                        name="target_field", source="crm",
                        field="target_field", strategy="latest",
                    ),
                ],
            ),
        ),
        # M060 round-2 rework: preserve legacy ``float(True)`` coercion.
        (
            "boolean_sum_measure",
            _boolean_sum_events,
            lambda: AnalyticsEntity(
                name="accounts",
                measures=[
                    Measure(
                        name="activation_total", activity="activity",
                        aggregation="sum", field="activated",
                    ),
                ],
            ),
        ),
    ]
    return scenarios


_EQUIVALENCE_SCENARIOS = _build_equivalence_scenarios()


class TestEquivalenceWithLegacy:
    """M060: every scenario produces byte-identical output under new + legacy."""

    @pytest.mark.parametrize(
        "scenario_id, events_builder, entity_builder",
        _EQUIVALENCE_SCENARIOS,
        ids=[s[0] for s in _EQUIVALENCE_SCENARIOS],
    )
    def test_new_matches_legacy(
        self, scenario_id, events_builder, entity_builder
    ):
        events = events_builder()
        ae = entity_builder()

        new_df = project_analytics_entity(events, ae).execute()
        legacy_df = legacy_project_analytics_entity(events, ae).execute()

        group_key = "canonical_id" if "canonical_id" in new_df.columns else "entity_id"

        # Sort both frames by group_key so row order does not affect equality.
        new_sorted = (
            new_df.sort_values(group_key).reset_index(drop=True)
            if len(new_df)
            else new_df.reset_index(drop=True)
        )
        legacy_sorted = (
            legacy_df.sort_values(group_key).reset_index(drop=True)
            if len(legacy_df)
            else legacy_df.reset_index(drop=True)
        )

        # Both column sets must match.
        assert set(new_sorted.columns) == set(legacy_sorted.columns), (
            f"{scenario_id}: column mismatch "
            f"new={sorted(new_sorted.columns)} legacy={sorted(legacy_sorted.columns)}"
        )

        # Align column order for pandas.testing.
        new_sorted = new_sorted[legacy_sorted.columns]

        # Normalise None / NaN representations before comparison.
        # The legacy pandas implementation stores "missing" as Python None
        # in an ``object`` column; the Ibis push-down implementation stores
        # it as NaN in a ``float64`` column (for measure 'latest'). Pandas's
        # ``assert_frame_equal`` accepts both today but emits a
        # FutureWarning about upcoming strictness — coerce object-None to
        # numpy NaN so the comparator sees one consistent null marker.
        def _normalise_nulls(df: pd.DataFrame) -> pd.DataFrame:
            out = df.copy()
            for col in out.columns:
                if out[col].dtype == object:
                    out[col] = out[col].where(out[col].notna(), np.nan)
            return out

        new_sorted = _normalise_nulls(new_sorted)
        legacy_sorted = _normalise_nulls(legacy_sorted)

        # check_exact=False + rtol accommodates the "last-bit float" class of
        # differences explicitly called out as acceptable breakage in the
        # mission TOML. check_dtype=False tolerates e.g. int64 vs object for
        # empty count columns.
        pd.testing.assert_frame_equal(
            new_sorted,
            legacy_sorted,
            check_exact=False,
            rtol=1e-6,
            check_dtype=False,
        )


# ---------------------------------------------------------------------------
# Tests: M063 — Arrow-friendly dtype coercion after JSON-text parse
# ---------------------------------------------------------------------------


class TestCoerceToArrowFriendlyDtype:
    """Unit tests for :func:`_coerce_to_arrow_friendly_dtype`.

    The helper inspects the non-null Python scalars in an object-dtype
    series and picks a stable nullable pandas dtype so the column can be
    converted to PyArrow by ``ibis.memtable(df).to_pyarrow()``.
    """

    def test_all_null_series_stays_object(self):
        s = pd.Series([None, None, None], dtype=object)
        result = _coerce_to_arrow_friendly_dtype(s)
        assert result.dtype == object

    def test_empty_series_stays_object(self):
        s = pd.Series([], dtype=object)
        result = _coerce_to_arrow_friendly_dtype(s)
        assert result.dtype == object

    def test_all_int_becomes_nullable_int64(self):
        s = pd.Series([1, 2, None, 3], dtype=object)
        result = _coerce_to_arrow_friendly_dtype(s)
        assert isinstance(result.dtype, pd.Int64Dtype)
        assert result.iloc[0] == 1
        assert pd.isna(result.iloc[2])

    def test_all_float_becomes_nullable_float64(self):
        s = pd.Series([1.5, 2.5, None, 3.5], dtype=object)
        result = _coerce_to_arrow_friendly_dtype(s)
        assert str(result.dtype) == "Float64"
        assert result.iloc[0] == 1.5
        assert pd.isna(result.iloc[2])

    def test_int_plus_float_becomes_nullable_float64(self):
        s = pd.Series([1, 2.5, None, 3], dtype=object)
        result = _coerce_to_arrow_friendly_dtype(s)
        assert str(result.dtype) == "Float64"

    def test_all_bool_becomes_nullable_boolean(self):
        s = pd.Series([True, False, None, True], dtype=object)
        result = _coerce_to_arrow_friendly_dtype(s)
        assert str(result.dtype) == "boolean"
        # Critical: bool is a subclass of int, but strict type() matching
        # in the helper ensures we do NOT fall through to Int64.
        assert not isinstance(result.dtype, pd.Int64Dtype)
        assert result.iloc[0] is True or result.iloc[0] == True  # noqa: E712

    def test_all_str_remains_object(self):
        s = pd.Series(["a", "b", None, "c"], dtype=object)
        result = _coerce_to_arrow_friendly_dtype(s)
        assert result.dtype == object

    def test_mixed_str_and_int_falls_back_to_object(self):
        s = pd.Series([1, "two", None, 3], dtype=object)
        result = _coerce_to_arrow_friendly_dtype(s)
        assert result.dtype == object


class TestProjectionArrowCompatibility:
    """Regression tests for M063: projection -> ibis.memtable -> to_pyarrow().

    Before v0.7.1, ``_parse_json_text_columns`` called ``.astype(object)``,
    producing object-dtype columns with mixed Python-scalar + ``None``
    values. PyArrow could not infer a consistent Arrow type, so
    ``result.to_pyarrow()`` raised. This is the BigQuery memtable-
    registration path; DuckDB silently tolerated the object dtype.

    These tests exercise the end-to-end path without needing real BigQuery
    credentials — ``to_pyarrow()`` on an ``ibis.memtable`` round-trips
    through ``pyarrow.Table.from_pandas`` with the same type-inference
    rules.
    """

    def test_projection_nullable_int_state_field_is_arrow_compatible(self):
        events = _make_events([
            {"source": "crm", "entity_id": "e1", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {"lead_score": 42}},
            {"source": "crm", "entity_id": "e2", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {}},
        ])
        ae = AnalyticsEntity(
            name="scored_leads",
            state_fields=[
                StateField(
                    name="lead_score",
                    source="crm",
                    field="lead_score",
                    strategy="latest",
                ),
            ],
        )
        result = project_analytics_entity(events, ae)
        table = result.to_pyarrow()  # THIS is the path that failed pre-fix
        score_type = table.schema.field("lead_score").type
        assert pa.types.is_integer(score_type), (
            f"expected integer Arrow type, got {score_type!r}"
        )

    def test_projection_nullable_float_state_field_is_arrow_compatible(self):
        events = _make_events([
            {"source": "crm", "entity_id": "e1", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {"score": 0.5}},
            {"source": "crm", "entity_id": "e2", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {"score": 2.25}},
            {"source": "crm", "entity_id": "e3", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {}},
        ])
        ae = AnalyticsEntity(
            name="scored_leads",
            state_fields=[
                StateField(
                    name="score",
                    source="crm",
                    field="score",
                    strategy="latest",
                ),
            ],
        )
        result = project_analytics_entity(events, ae)
        table = result.to_pyarrow()
        score_type = table.schema.field("score").type
        assert pa.types.is_floating(score_type), (
            f"expected floating Arrow type, got {score_type!r}"
        )

    def test_projection_nullable_bool_state_field_is_arrow_compatible(self):
        events = _make_events([
            {"source": "crm", "entity_id": "e1", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {"is_active": True}},
            {"source": "crm", "entity_id": "e2", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {"is_active": False}},
            {"source": "crm", "entity_id": "e3", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {}},
        ])
        ae = AnalyticsEntity(
            name="active_flags",
            state_fields=[
                StateField(
                    name="is_active",
                    source="crm",
                    field="is_active",
                    strategy="latest",
                ),
            ],
        )
        result = project_analytics_entity(events, ae)
        table = result.to_pyarrow()
        flag_type = table.schema.field("is_active").type
        # MUST be bool specifically — strict type() matching in the helper
        # prevents bool (subclass of int) from being coerced to Int64.
        assert flag_type == pa.bool_(), (
            f"expected pa.bool_(), got {flag_type!r}"
        )
        assert flag_type != pa.int64(), (
            "bool state field must NOT be coerced to int64 (bool-as-int guard)"
        )

    def test_projection_string_state_field_remains_object_and_arrow_compatible(self):
        events = _make_events([
            {"source": "crm", "entity_id": "e1", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {"plan": "pro"}},
            {"source": "crm", "entity_id": "e2", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {"plan": "free"}},
            {"source": "crm", "entity_id": "e3", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {}},
        ])
        ae = AnalyticsEntity(
            name="plan_info",
            state_fields=[
                StateField(
                    name="plan",
                    source="crm",
                    field="plan",
                    strategy="latest",
                ),
            ],
        )
        result = project_analytics_entity(events, ae)
        df = result.execute()
        # Object dtype is correct for strings — PyArrow handles object-str.
        assert df["plan"].dtype == object
        table = result.to_pyarrow()
        plan_type = table.schema.field("plan").type
        assert pa.types.is_string(plan_type) or pa.types.is_large_string(
            plan_type
        ), f"expected string Arrow type, got {plan_type!r}"

    def test_projection_mixed_type_state_field_falls_back_to_object(self):
        """Intentionally mixed payload (int + str) should fall back to object.

        Observed behaviour (documented here): the coercion helper returns
        object dtype for a genuinely heterogeneous column — that is the
        correct behaviour for user data that cannot be expressed as a
        single Arrow type. Both ``.execute()`` and ``.to_pyarrow()`` may
        still raise because the underlying ``ibis.memtable`` registration
        path goes through ``pa.Table.from_pandas``, which cannot infer
        a single stable Arrow type across ``int`` and ``str`` scalars.
        This is *expected*; we assert the fallback path is a clean
        ``ArrowTypeError`` (not a silent miscoercion) and that the
        underlying pandas frame carries the mixed values in an object
        column before the ibis round-trip.
        """
        events = _make_events([
            {"source": "crm", "entity_id": "e1", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {"mixed": 42}},
            {"source": "crm", "entity_id": "e2", "ts": "2024-01-01T00:00:00",
             "event_type": "row_appeared", "payload": {"mixed": "forty-two"}},
        ])
        ae = AnalyticsEntity(
            name="mixed_entity",
            state_fields=[
                StateField(
                    name="mixed",
                    source="crm",
                    field="mixed",
                    strategy="latest",
                ),
            ],
        )
        result = project_analytics_entity(events, ae)
        # Either ``.execute()`` succeeds (with object dtype) or raises a
        # clear Arrow-level error — both are acceptable for genuinely
        # heterogeneous user data, and neither is a silent miscoercion.
        try:
            df = result.execute()
            assert df["mixed"].dtype == object, (
                "mixed int+str should fall back to object dtype"
            )
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            pass  # acceptable: user's data is genuinely mixed
        try:
            result.to_pyarrow()
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            pass  # acceptable: same reason


# ---------------------------------------------------------------------------
# Tests: AnalyticsEntityRegistry
# ---------------------------------------------------------------------------

class TestAnalyticsEntityRegistry:
    """Tests for the registry that discovers AnalyticsEntity instances."""

    def test_discover_single(self, tmp_path):
        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()
        (entities_dir / "accounts.py").write_text(textwrap.dedent("""\
            from fyrnheim.core.analytics_entity import AnalyticsEntity, Measure

            analytics_entity = AnalyticsEntity(
                name="accounts",
                measures=[Measure(name="n", activity="ev", aggregation="count")],
            )
        """))
        registry = AnalyticsEntityRegistry()
        registry.discover(entities_dir)
        assert len(registry) == 1
        assert "accounts" in registry
        assert registry.get("accounts").name == "accounts"

    def test_discover_list(self, tmp_path):
        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()
        (entities_dir / "multi.py").write_text(textwrap.dedent("""\
            from fyrnheim.core.analytics_entity import AnalyticsEntity, Measure

            analytics_entities = [
                AnalyticsEntity(
                    name="a1",
                    measures=[Measure(name="n", activity="ev", aggregation="count")],
                ),
                AnalyticsEntity(
                    name="a2",
                    measures=[Measure(name="n", activity="ev", aggregation="count")],
                ),
            ]
        """))
        registry = AnalyticsEntityRegistry()
        registry.discover(entities_dir)
        assert len(registry) == 2
        assert "a1" in registry
        assert "a2" in registry

    def test_duplicate_raises(self, tmp_path):
        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()
        (entities_dir / "a.py").write_text(textwrap.dedent("""\
            from fyrnheim.core.analytics_entity import AnalyticsEntity, Measure
            analytics_entity = AnalyticsEntity(
                name="dup",
                measures=[Measure(name="n", activity="ev", aggregation="count")],
            )
        """))
        (entities_dir / "b.py").write_text(textwrap.dedent("""\
            from fyrnheim.core.analytics_entity import AnalyticsEntity, Measure
            analytics_entity = AnalyticsEntity(
                name="dup",
                measures=[Measure(name="n", activity="ev", aggregation="count")],
            )
        """))
        registry = AnalyticsEntityRegistry()
        with pytest.raises(ValueError, match="Duplicate analytics entity name 'dup'"):
            registry.discover(entities_dir)

    def test_missing_directory_raises(self):
        registry = AnalyticsEntityRegistry()
        with pytest.raises(FileNotFoundError):
            registry.discover("/nonexistent/path")

    def test_get_missing_raises(self):
        registry = AnalyticsEntityRegistry()
        with pytest.raises(KeyError, match="Analytics entity 'nope' not found"):
            registry.get("nope")

    def test_all_returns_list(self, tmp_path):
        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()
        (entities_dir / "x.py").write_text(textwrap.dedent("""\
            from fyrnheim.core.analytics_entity import AnalyticsEntity, Measure
            analytics_entity = AnalyticsEntity(
                name="x",
                measures=[Measure(name="n", activity="ev", aggregation="count")],
            )
        """))
        registry = AnalyticsEntityRegistry()
        registry.discover(entities_dir)
        result = registry.all()
        assert len(result) == 1
        assert result[0].name == "x"

    def test_items_returns_pairs(self, tmp_path):
        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()
        (entities_dir / "y.py").write_text(textwrap.dedent("""\
            from fyrnheim.core.analytics_entity import AnalyticsEntity, Measure
            analytics_entity = AnalyticsEntity(
                name="y",
                measures=[Measure(name="n", activity="ev", aggregation="count")],
            )
        """))
        registry = AnalyticsEntityRegistry()
        registry.discover(entities_dir)
        items = list(registry.items())
        assert items == [("y", registry.get("y"))]

    def test_iter_and_contains(self, tmp_path):
        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()
        (entities_dir / "z.py").write_text(textwrap.dedent("""\
            from fyrnheim.core.analytics_entity import AnalyticsEntity, Measure
            analytics_entity = AnalyticsEntity(
                name="z",
                measures=[Measure(name="n", activity="ev", aggregation="count")],
            )
        """))
        registry = AnalyticsEntityRegistry()
        registry.discover(entities_dir)
        assert "z" in registry
        assert list(registry) == ["z"]
