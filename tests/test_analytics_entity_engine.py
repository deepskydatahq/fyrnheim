"""Tests for AnalyticsEntity projection engine and registry."""

import json
import textwrap

import ibis
import pandas as pd
import pytest

from fyrnheim.components.computed_column import ComputedColumn
from fyrnheim.core.analytics_entity import AnalyticsEntity, Measure, StateField
from fyrnheim.engine.analytics_entity_engine import project_analytics_entity
from fyrnheim.engine.analytics_entity_registry import AnalyticsEntityRegistry


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
        don't include the measure activity payload field, latest returns None.
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
        assert df.iloc[0]["last_plan"] is None

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
