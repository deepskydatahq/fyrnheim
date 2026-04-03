"""Tests for computed_columns on StateSource and EventSource."""

from fyrnheim.components.computed_column import ComputedColumn
from fyrnheim.core.source import EventSource, StateSource


class TestStateSourceComputedColumns:
    """Tests for StateSource.computed_columns field."""

    def test_state_source_with_computed_columns(self):
        src = StateSource(
            project="proj",
            dataset="ds",
            table="users",
            name="users",
            id_field="user_id",
            computed_columns=[
                ComputedColumn(name="email_lower", expression="t.email.lower()"),
            ],
        )
        assert len(src.computed_columns) == 1
        assert src.computed_columns[0].name == "email_lower"
        assert src.computed_columns[0].expression == "t.email.lower()"

    def test_state_source_defaults_to_empty_list(self):
        src = StateSource(
            project="proj",
            dataset="ds",
            table="users",
            name="users",
            id_field="user_id",
        )
        assert src.computed_columns == []

    def test_state_source_multiple_computed_columns(self):
        src = StateSource(
            project="proj",
            dataset="ds",
            table="users",
            name="users",
            id_field="user_id",
            computed_columns=[
                ComputedColumn(name="email_lower", expression="t.email.lower()"),
                ComputedColumn(name="signup_date", expression='t.created_at.cast("date")'),
            ],
        )
        assert len(src.computed_columns) == 2


class TestEventSourceComputedColumns:
    """Tests for EventSource.computed_columns field."""

    def test_event_source_with_computed_columns(self):
        src = EventSource(
            project="proj",
            dataset="ds",
            table="events",
            name="page_views",
            entity_id_field="user_id",
            timestamp_field="ts",
            computed_columns=[
                ComputedColumn(name="domain", expression='t.url.split("/")[2]'),
            ],
        )
        assert len(src.computed_columns) == 1
        assert src.computed_columns[0].name == "domain"

    def test_event_source_defaults_to_empty_list(self):
        src = EventSource(
            project="proj",
            dataset="ds",
            table="events",
            name="page_views",
            entity_id_field="user_id",
            timestamp_field="ts",
        )
        assert src.computed_columns == []

    def test_event_source_with_event_type_and_computed_columns(self):
        src = EventSource(
            project="proj",
            dataset="ds",
            table="events",
            name="page_views",
            entity_id_field="user_id",
            timestamp_field="ts",
            event_type="pageview",
            computed_columns=[
                ComputedColumn(name="email_lower", expression="t.email.lower()"),
            ],
        )
        assert src.event_type == "pageview"
        assert len(src.computed_columns) == 1
