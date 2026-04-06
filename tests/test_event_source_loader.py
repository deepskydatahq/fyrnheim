"""Tests for event_source_loader: EventSource -> standard event schema."""

from __future__ import annotations

import json

import ibis
import pandas as pd
import pytest

from fyrnheim.core.source import EventSource
from fyrnheim.engine.event_source_loader import load_event_source


def _make_event_source(
    *,
    name: str = "page_views",
    entity_id_field: str = "user_id",
    timestamp_field: str = "viewed_at",
    event_type: str | None = None,
    event_type_field: str | None = None,
    parquet_path: str = "",
) -> EventSource:
    return EventSource(
        name=name,
        project="test",
        dataset="test",
        table="test",
        duckdb_path=parquet_path,
        entity_id_field=entity_id_field,
        timestamp_field=timestamp_field,
        event_type=event_type,
        event_type_field=event_type_field,
    )


class TestLoadEventSource:
    """Tests for load_event_source()."""

    def test_static_event_type(self, tmp_path: pytest.TempPathFactory) -> None:
        """Static event_type is applied to all rows."""
        parquet_file = tmp_path / "events.parquet"
        df = pd.DataFrame(
            {
                "user_id": ["u1", "u2"],
                "viewed_at": ["2024-01-01", "2024-01-02"],
                "page": ["/home", "/about"],
            }
        )
        df.to_parquet(str(parquet_file))

        es = _make_event_source(
            event_type="page_view",
            parquet_path=str(parquet_file),
        )
        conn = ibis.duckdb.connect()
        result = load_event_source(conn, es).execute()

        assert len(result) == 2
        assert set(result.columns) == {"source", "entity_id", "ts", "event_type", "payload"}
        assert all(result["event_type"] == "page_view")
        assert all(result["source"] == "page_views")
        assert set(result["entity_id"]) == {"u1", "u2"}

    def test_event_type_field(self, tmp_path: pytest.TempPathFactory) -> None:
        """event_type_field reads event type from the specified column."""
        parquet_file = tmp_path / "events.parquet"
        df = pd.DataFrame(
            {
                "user_id": ["u1", "u2"],
                "viewed_at": ["2024-01-01", "2024-01-02"],
                "action": ["click", "scroll"],
                "page": ["/home", "/about"],
            }
        )
        df.to_parquet(str(parquet_file))

        es = _make_event_source(
            event_type_field="action",
            parquet_path=str(parquet_file),
        )
        conn = ibis.duckdb.connect()
        result = load_event_source(conn, es).execute()

        assert len(result) == 2
        assert set(result["event_type"]) == {"click", "scroll"}
        # action should NOT be in payload since it's the event_type_field
        for payload_str in result["payload"]:
            payload = json.loads(payload_str)
            assert "action" not in payload
            assert "page" in payload

    def test_fallback_to_source_name(self, tmp_path: pytest.TempPathFactory) -> None:
        """Neither event_type nor event_type_field uses source name."""
        parquet_file = tmp_path / "events.parquet"
        df = pd.DataFrame(
            {
                "user_id": ["u1"],
                "viewed_at": ["2024-01-01"],
                "page": ["/home"],
            }
        )
        df.to_parquet(str(parquet_file))

        es = _make_event_source(parquet_path=str(parquet_file))
        conn = ibis.duckdb.connect()
        result = load_event_source(conn, es).execute()

        assert len(result) == 1
        assert result.iloc[0]["event_type"] == "page_views"

    def test_entity_id_mapped(self, tmp_path: pytest.TempPathFactory) -> None:
        """entity_id_field is mapped to entity_id column."""
        parquet_file = tmp_path / "events.parquet"
        df = pd.DataFrame(
            {
                "customer_id": ["c42"],
                "event_time": ["2024-06-15"],
                "amount": [99.99],
            }
        )
        df.to_parquet(str(parquet_file))

        es = _make_event_source(
            name="purchases",
            entity_id_field="customer_id",
            timestamp_field="event_time",
            event_type="purchase",
            parquet_path=str(parquet_file),
        )
        conn = ibis.duckdb.connect()
        result = load_event_source(conn, es).execute()

        assert result.iloc[0]["entity_id"] == "c42"
        assert result.iloc[0]["ts"] == "2024-06-15"

    def test_remaining_cols_in_payload(self, tmp_path: pytest.TempPathFactory) -> None:
        """Remaining columns (not entity_id, ts, event_type) are in payload."""
        parquet_file = tmp_path / "events.parquet"
        df = pd.DataFrame(
            {
                "user_id": ["u1"],
                "viewed_at": ["2024-01-01"],
                "page": ["/home"],
                "referrer": ["google.com"],
            }
        )
        df.to_parquet(str(parquet_file))

        es = _make_event_source(
            event_type="page_view",
            parquet_path=str(parquet_file),
        )
        conn = ibis.duckdb.connect()
        result = load_event_source(conn, es).execute()

        payload = json.loads(result.iloc[0]["payload"])
        assert payload["page"] == "/home"
        assert payload["referrer"] == "google.com"
        # entity_id and ts should NOT be in payload
        assert "user_id" not in payload
        assert "viewed_at" not in payload

    def test_empty_source(self, tmp_path: pytest.TempPathFactory) -> None:
        """Empty source returns empty table with correct schema."""
        parquet_file = tmp_path / "events.parquet"
        df = pd.DataFrame(
            {
                "user_id": pd.Series([], dtype="str"),
                "viewed_at": pd.Series([], dtype="str"),
                "page": pd.Series([], dtype="str"),
            }
        )
        df.to_parquet(str(parquet_file))

        es = _make_event_source(
            event_type="page_view",
            parquet_path=str(parquet_file),
        )
        conn = ibis.duckdb.connect()
        result = load_event_source(conn, es).execute()

        assert len(result) == 0
        assert set(result.columns) == {"source", "entity_id", "ts", "event_type", "payload"}
