"""Tests for the diff engine producing raw events."""

from __future__ import annotations

import json

import ibis
import pytest

from fyrnheim.engine.diff_engine import diff_snapshots


@pytest.fixture()
def duckdb_conn():
    """Create an in-memory DuckDB connection."""
    conn = ibis.duckdb.connect()
    yield conn
    conn.disconnect()


# ---------------------------------------------------------------------------
# Story 1: Core diff algorithm detecting row changes
# ---------------------------------------------------------------------------


class TestRowAppeared:
    """diff_snapshots returns row_appeared events for new IDs."""

    def test_new_rows_produce_appeared_events(self) -> None:
        current = ibis.memtable({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        previous = ibis.memtable({"id": [1], "name": ["a"]})

        result = diff_snapshots(
            current,
            previous,
            source_name="users",
            id_field="id",
            snapshot_date="2026-03-30",
        )
        df = result.execute()
        appeared = df[df["event_type"] == "row_appeared"]
        assert set(appeared["entity_id"]) == {"2", "3"}

    def test_all_rows_appeared_when_previous_empty(self) -> None:
        current = ibis.memtable({"id": [10, 20], "status": ["active", "inactive"]})
        previous = ibis.memtable({"id": [], "status": []})

        result = diff_snapshots(
            current,
            previous,
            source_name="accounts",
            id_field="id",
            snapshot_date="2026-03-30",
        )
        df = result.execute()
        assert len(df) == 2
        assert all(df["event_type"] == "row_appeared")


class TestRowDisappeared:
    """diff_snapshots returns row_disappeared events for removed IDs."""

    def test_removed_rows_produce_disappeared_events(self) -> None:
        current = ibis.memtable({"id": [1], "name": ["a"]})
        previous = ibis.memtable({"id": [1, 2, 3], "name": ["a", "b", "c"]})

        result = diff_snapshots(
            current,
            previous,
            source_name="users",
            id_field="id",
            snapshot_date="2026-03-30",
        )
        df = result.execute()
        disappeared = df[df["event_type"] == "row_disappeared"]
        assert set(disappeared["entity_id"]) == {"2", "3"}


class TestFieldChanged:
    """diff_snapshots returns field_changed events for modified fields."""

    def test_changed_field_produces_event(self) -> None:
        current = ibis.memtable({"id": [1], "name": ["bob"], "age": [30]})
        previous = ibis.memtable({"id": [1], "name": ["alice"], "age": [30]})

        result = diff_snapshots(
            current,
            previous,
            source_name="users",
            id_field="id",
            snapshot_date="2026-03-30",
        )
        df = result.execute()
        changed = df[df["event_type"] == "field_changed"]
        assert len(changed) == 1
        payload = json.loads(changed.iloc[0]["payload"])
        assert payload["field_name"] == "name"
        assert payload["old_value"] == "alice"
        assert payload["new_value"] == "bob"

    def test_one_event_per_changed_field(self) -> None:
        current = ibis.memtable({"id": [1], "name": ["bob"], "age": [31]})
        previous = ibis.memtable({"id": [1], "name": ["alice"], "age": [30]})

        result = diff_snapshots(
            current,
            previous,
            source_name="users",
            id_field="id",
            snapshot_date="2026-03-30",
        )
        df = result.execute()
        changed = df[df["event_type"] == "field_changed"]
        assert len(changed) == 2
        field_names = {json.loads(r["payload"])["field_name"] for _, r in changed.iterrows()}
        assert field_names == {"name", "age"}

    def test_identical_rows_produce_no_events(self) -> None:
        current = ibis.memtable({"id": [1, 2], "name": ["a", "b"]})
        previous = ibis.memtable({"id": [1, 2], "name": ["a", "b"]})

        result = diff_snapshots(
            current,
            previous,
            source_name="users",
            id_field="id",
            snapshot_date="2026-03-30",
        )
        df = result.execute()
        assert len(df) == 0


# ---------------------------------------------------------------------------
# Story 2: Cold start handling and field exclusions
# ---------------------------------------------------------------------------


class TestColdStart:
    """When previous is None, all rows are row_appeared."""

    def test_none_previous_produces_all_appeared(self) -> None:
        current = ibis.memtable({"id": [1, 2, 3], "name": ["a", "b", "c"]})

        result = diff_snapshots(
            current,
            previous=None,
            source_name="users",
            id_field="id",
            snapshot_date="2026-03-30",
        )
        df = result.execute()
        assert len(df) == 3
        assert all(df["event_type"] == "row_appeared")
        assert set(df["entity_id"]) == {"1", "2", "3"}


class TestFieldExclusions:
    """exclude_fields suppresses field_changed but not row_appeared."""

    def test_excluded_field_suppresses_field_changed(self) -> None:
        current = ibis.memtable(
            {"id": [1], "name": ["alice"], "last_synced_at": ["2026-03-30"]}
        )
        previous = ibis.memtable(
            {"id": [1], "name": ["alice"], "last_synced_at": ["2026-03-29"]}
        )

        result = diff_snapshots(
            current,
            previous,
            source_name="users",
            id_field="id",
            snapshot_date="2026-03-30",
            exclude_fields=["last_synced_at"],
        )
        df = result.execute()
        assert len(df) == 0  # no field_changed since only excluded field changed

    def test_excluded_fields_still_in_row_appeared(self) -> None:
        current = ibis.memtable(
            {"id": [1], "name": ["alice"], "last_synced_at": ["2026-03-30"]}
        )

        result = diff_snapshots(
            current,
            previous=None,
            source_name="users",
            id_field="id",
            snapshot_date="2026-03-30",
            exclude_fields=["last_synced_at"],
        )
        df = result.execute()
        assert len(df) == 1
        payload = json.loads(df.iloc[0]["payload"])
        assert "last_synced_at" in payload  # excluded field still present in appeared


# ---------------------------------------------------------------------------
# Story 3: Universal event schema for all event types
# ---------------------------------------------------------------------------


class TestUniversalEventSchema:
    """All events follow source, entity_id, ts, event_type, payload."""

    EXPECTED_COLUMNS = {"source", "entity_id", "ts", "event_type", "payload"}

    def test_row_appeared_schema(self) -> None:
        current = ibis.memtable({"id": [1], "name": ["alice"], "age": [30]})

        result = diff_snapshots(
            current,
            previous=None,
            source_name="crm",
            id_field="id",
            snapshot_date="2026-03-30",
        )
        df = result.execute()
        assert set(df.columns) == self.EXPECTED_COLUMNS
        row = df.iloc[0]
        assert row["source"] == "crm"
        assert row["entity_id"] == "1"
        assert row["ts"] == "2026-03-30"
        assert row["event_type"] == "row_appeared"
        payload = json.loads(row["payload"])
        # Issue #93: primitives preserved as native JSON types (int stays int)
        assert payload == {"name": "alice", "age": 30}

    def test_field_changed_schema(self) -> None:
        current = ibis.memtable({"id": [1], "name": ["bob"]})
        previous = ibis.memtable({"id": [1], "name": ["alice"]})

        result = diff_snapshots(
            current,
            previous,
            source_name="crm",
            id_field="id",
            snapshot_date="2026-03-30",
        )
        df = result.execute()
        assert set(df.columns) == self.EXPECTED_COLUMNS
        row = df.iloc[0]
        assert row["source"] == "crm"
        assert row["entity_id"] == "1"
        assert row["ts"] == "2026-03-30"
        assert row["event_type"] == "field_changed"
        payload = json.loads(row["payload"])
        assert payload == {
            "field_name": "name",
            "old_value": "alice",
            "new_value": "bob",
        }

    def test_row_disappeared_schema(self) -> None:
        current = ibis.memtable({"id": [], "name": []})
        previous = ibis.memtable({"id": [1], "name": ["alice"]})

        result = diff_snapshots(
            current,
            previous,
            source_name="crm",
            id_field="id",
            snapshot_date="2026-03-30",
        )
        df = result.execute()
        assert set(df.columns) == self.EXPECTED_COLUMNS
        row = df.iloc[0]
        assert row["source"] == "crm"
        assert row["entity_id"] == "1"
        assert row["ts"] == "2026-03-30"
        assert row["event_type"] == "row_disappeared"
        payload = json.loads(row["payload"])
        assert payload == {"name": "alice"}

    def test_all_event_types_union_into_single_table(self) -> None:
        """All three event types can coexist in a single result table."""
        current = ibis.memtable({"id": [1, 3], "name": ["alice_v2", "charlie"]})
        previous = ibis.memtable({"id": [1, 2], "name": ["alice", "bob"]})

        result = diff_snapshots(
            current,
            previous,
            source_name="crm",
            id_field="id",
            snapshot_date="2026-03-30",
        )
        df = result.execute()
        assert set(df.columns) == self.EXPECTED_COLUMNS
        event_types = set(df["event_type"])
        assert event_types == {"row_appeared", "field_changed", "row_disappeared"}

        # Verify we can do Ibis operations on the unified table
        assert result.count().execute() == 3


# ---------------------------------------------------------------------------
# M051 regression tests (issue #93)
# ---------------------------------------------------------------------------


class TestM051SerializeValue:
    """Issue #93: _serialize_value must return Python None for NaN and
    preserve primitives as-is so _resolve_latest's null-aware logic works."""

    def test_serialize_value_returns_none_for_nan(self) -> None:
        import pandas as pd

        from fyrnheim.engine.diff_engine import _serialize_value

        assert _serialize_value(None) is None
        assert _serialize_value(float("nan")) is None
        assert _serialize_value(pd.NaT) is None

    def test_serialize_value_preserves_primitives(self) -> None:
        from fyrnheim.engine.diff_engine import _serialize_value

        assert _serialize_value(42) == 42
        assert _serialize_value(3.14) == 3.14
        assert _serialize_value(True) is True
        assert _serialize_value("hi") == "hi"

    def test_serialize_value_stringifies_exotic_types(self) -> None:
        import uuid

        from fyrnheim.engine.diff_engine import _serialize_value

        u = uuid.UUID("12345678-1234-5678-1234-567812345678")
        result = _serialize_value(u)
        assert isinstance(result, str)
        assert result == str(u)


class TestM051ResolveLatestFallsThroughNone:
    """Issue #93: _resolve_latest must skip None rows instead of returning
    the string 'null' as a present value."""

    def test_resolve_latest_falls_through_none_rows(self) -> None:
        import pandas as pd

        from fyrnheim.engine.analytics_entity_engine import _resolve_latest

        events = pd.DataFrame(
            [
                {
                    "source": "crm",
                    "entity_id": "u1",
                    "ts": "2026-01-01",
                    "event_type": "row_appeared",
                    "payload": json.dumps({"company": "Acme"}),
                },
                {
                    "source": "crm",
                    "entity_id": "u1",
                    "ts": "2026-01-02",
                    "event_type": "row_appeared",
                    "payload": json.dumps({"company": None}),
                },
            ]
        )
        # Latest row has None; must fall through to the earlier real value.
        assert _resolve_latest(events, "company") == "Acme"
