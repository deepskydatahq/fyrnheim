"""E2E test: diff engine -> activity definitions -> named events."""

from __future__ import annotations

import ibis
import pandas as pd

from fyrnheim.core.activity import (
    ActivityDefinition,
    FieldChanged,
    RowAppeared,
)
from fyrnheim.engine.activity_engine import apply_activity_definitions
from fyrnheim.engine.diff_engine import diff_snapshots


def _make_customer_table(rows: list[dict]) -> ibis.Table:
    """Create an Ibis memtable from customer dicts."""
    return ibis.memtable(pd.DataFrame(rows))


class TestE2EActivityPipeline:
    """End-to-end: diff_snapshots -> apply_activity_definitions -> named events."""

    def test_cold_start_produces_signup_events(self):
        """Cold start row_appeared events become 'signup' named events."""
        customers_v1 = _make_customer_table(
            [
                {"id": "1", "name": "alice", "email": "alice@example.com", "plan": "free"},
                {"id": "2", "name": "bob", "email": "bob@example.com", "plan": "free"},
            ]
        )

        # Cold start: previous=None
        raw_events = diff_snapshots(
            current=customers_v1,
            previous=None,
            source_name="customers",
            id_field="id",
            snapshot_date="2024-01-01",
        )

        definitions = [
            ActivityDefinition(
                name="signup",
                source="customers",
                trigger=RowAppeared(),
            ),
        ]

        named_events = apply_activity_definitions(raw_events, definitions).execute()

        assert len(named_events) == 2
        assert all(named_events["event_type"] == "signup")
        assert set(named_events["entity_id"]) == {"1", "2"}
        assert all(named_events["source"] == "customers")
        assert all(named_events["ts"] == "2024-01-01")

    def test_field_change_produces_became_paying_events(self):
        """Field changes on 'plan' with to_values=['pro'] become 'became_paying'."""
        customers_v1 = _make_customer_table(
            [
                {"id": "1", "name": "alice", "email": "alice@example.com", "plan": "free"},
                {"id": "2", "name": "bob", "email": "bob@example.com", "plan": "free"},
            ]
        )
        customers_v2 = _make_customer_table(
            [
                {"id": "1", "name": "alice", "email": "alice@example.com", "plan": "pro"},
                {"id": "2", "name": "bob", "email": "bob@example.com", "plan": "free"},
            ]
        )

        # First snapshot (cold start)
        diff_snapshots(
            current=customers_v1,
            previous=None,
            source_name="customers",
            id_field="id",
            snapshot_date="2024-01-01",
        )

        # Second snapshot (alice upgrades to pro)
        raw_events = diff_snapshots(
            current=customers_v2,
            previous=customers_v1,
            source_name="customers",
            id_field="id",
            snapshot_date="2024-01-02",
        )

        definitions = [
            ActivityDefinition(
                name="became_paying",
                source="customers",
                trigger=FieldChanged(field="plan", to_values=["pro"]),
            ),
        ]

        named_events = apply_activity_definitions(raw_events, definitions).execute()

        assert len(named_events) == 1
        assert named_events.iloc[0]["event_type"] == "became_paying"
        assert named_events.iloc[0]["entity_id"] == "1"
        assert named_events.iloc[0]["ts"] == "2024-01-02"

    def test_multiple_definitions_produce_unioned_stream(self):
        """Multiple activity definitions produce a unioned stream of named events."""
        customers_v1 = _make_customer_table(
            [
                {"id": "1", "name": "alice", "email": "alice@example.com", "plan": "free"},
            ]
        )
        customers_v2 = _make_customer_table(
            [
                {"id": "1", "name": "alice", "email": "alice@example.com", "plan": "pro"},
                {"id": "2", "name": "bob", "email": "bob@example.com", "plan": "free"},
            ]
        )

        # Cold start raw events
        cold_start_events = diff_snapshots(
            current=customers_v1,
            previous=None,
            source_name="customers",
            id_field="id",
            snapshot_date="2024-01-01",
        )

        # Second snapshot events
        change_events = diff_snapshots(
            current=customers_v2,
            previous=customers_v1,
            source_name="customers",
            id_field="id",
            snapshot_date="2024-01-02",
        )

        # Union all raw events together
        all_raw = ibis.union(cold_start_events, change_events)

        definitions = [
            ActivityDefinition(
                name="signup",
                source="customers",
                trigger=RowAppeared(),
            ),
            ActivityDefinition(
                name="became_paying",
                source="customers",
                trigger=FieldChanged(field="plan", to_values=["pro"]),
            ),
        ]

        named_events = apply_activity_definitions(all_raw, definitions).execute()

        # Cold start: alice (row_appeared) -> signup
        # Second snapshot: bob (row_appeared) -> signup, alice plan change -> became_paying
        assert len(named_events) == 3

        event_types = set(named_events["event_type"])
        assert event_types == {"signup", "became_paying"}

        signups = named_events[named_events["event_type"] == "signup"]
        assert len(signups) == 2

        paying = named_events[named_events["event_type"] == "became_paying"]
        assert len(paying) == 1
        assert paying.iloc[0]["entity_id"] == "1"

    def test_named_events_preserve_source_entity_id_ts(self):
        """Named events preserve source, entity_id, ts columns from raw events."""
        customers_v1 = _make_customer_table(
            [
                {"id": "42", "name": "charlie", "email": "c@example.com", "plan": "trial"},
            ]
        )

        raw_events = diff_snapshots(
            current=customers_v1,
            previous=None,
            source_name="customers",
            id_field="id",
            snapshot_date="2024-03-15",
        )

        definitions = [
            ActivityDefinition(
                name="signup",
                source="customers",
                trigger=RowAppeared(),
            ),
        ]

        named_events = apply_activity_definitions(raw_events, definitions).execute()

        assert len(named_events) == 1
        row = named_events.iloc[0]
        assert row["source"] == "customers"
        assert row["entity_id"] == "42"
        assert row["ts"] == "2024-03-15"
        assert row["event_type"] == "signup"
