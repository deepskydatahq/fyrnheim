"""Tests for SnapshotStore parquet persistence."""

from __future__ import annotations

import datetime
from pathlib import Path

import ibis
import pytest

from fyrnheim.engine.snapshot_store import SnapshotStore


@pytest.fixture()
def duckdb_conn():
    """Create an in-memory DuckDB connection."""
    conn = ibis.duckdb.connect()
    yield conn
    conn.disconnect()


@pytest.fixture()
def store(tmp_path: Path, duckdb_conn: ibis.BaseBackend) -> SnapshotStore:
    """Create a SnapshotStore backed by a temp directory."""
    return SnapshotStore(base_dir=tmp_path, conn=duckdb_conn)


def _sample_table(conn: ibis.BaseBackend) -> ibis.Table:
    """Create a small in-memory table for testing."""
    return ibis.memtable(
        {"id": [1, 2, 3], "name": ["alice", "bob", "charlie"]},
    )


class TestSnapshotStoreSave:
    """Tests for SnapshotStore.save()."""

    def test_save_writes_parquet_file(
        self, store: SnapshotStore, duckdb_conn: ibis.BaseBackend, tmp_path: Path
    ) -> None:
        """save() writes a parquet file at {base_dir}/{source_name}/{date}.parquet."""
        table = _sample_table(duckdb_conn)
        date = datetime.date(2026, 3, 15)

        result_path = store.save("customers", date, table)

        expected = tmp_path / "customers" / "2026-03-15.parquet"
        assert result_path == expected
        assert expected.exists()

    def test_save_creates_source_directory(
        self, store: SnapshotStore, duckdb_conn: ibis.BaseBackend, tmp_path: Path
    ) -> None:
        """save() creates the source subdirectory if it does not exist."""
        table = _sample_table(duckdb_conn)
        store.save("new_source", datetime.date(2026, 1, 1), table)

        assert (tmp_path / "new_source").is_dir()

    def test_save_overwrites_existing_snapshot(
        self, store: SnapshotStore, duckdb_conn: ibis.BaseBackend, tmp_path: Path
    ) -> None:
        """save() overwrites if a snapshot for the same source+date already exists."""
        date = datetime.date(2026, 3, 15)

        # First save
        table_v1 = ibis.memtable({"id": [1], "name": ["alice"]})
        store.save("customers", date, table_v1)

        # Second save with different data
        table_v2 = ibis.memtable({"id": [10, 20], "name": ["xavier", "yara"]})
        store.save("customers", date, table_v2)

        # Read it back and verify it has the v2 data
        result = duckdb_conn.read_parquet(
            str(tmp_path / "customers" / "2026-03-15.parquet")
        )
        assert result.count().execute() == 2

    def test_save_data_roundtrips(
        self, store: SnapshotStore, duckdb_conn: ibis.BaseBackend, tmp_path: Path
    ) -> None:
        """Saved data can be read back with correct values."""
        table = _sample_table(duckdb_conn)
        date = datetime.date(2026, 3, 15)

        store.save("customers", date, table)

        result = duckdb_conn.read_parquet(
            str(tmp_path / "customers" / "2026-03-15.parquet")
        )
        df = result.order_by("id").execute()
        assert list(df["id"]) == [1, 2, 3]
        assert list(df["name"]) == ["alice", "bob", "charlie"]


class TestSnapshotStoreGetPrevious:
    """Tests for SnapshotStore.get_previous()."""

    def test_get_previous_returns_none_when_no_snapshots(
        self, store: SnapshotStore
    ) -> None:
        """get_previous() returns None when no previous snapshot exists."""
        result = store.get_previous("customers", datetime.date(2026, 3, 15))
        assert result is None

    def test_get_previous_returns_none_when_no_earlier_snapshot(
        self, store: SnapshotStore, duckdb_conn: ibis.BaseBackend
    ) -> None:
        """get_previous() returns None when only same-day or later snapshots exist."""
        table = _sample_table(duckdb_conn)
        store.save("customers", datetime.date(2026, 3, 15), table)

        # Same date -> no previous
        assert store.get_previous("customers", datetime.date(2026, 3, 15)) is None
        # Earlier date -> no previous
        assert store.get_previous("customers", datetime.date(2026, 3, 10)) is None

    def test_get_previous_returns_most_recent_before_date(
        self, store: SnapshotStore, duckdb_conn: ibis.BaseBackend
    ) -> None:
        """get_previous() returns the most recent snapshot before the given date."""
        t1 = ibis.memtable({"id": [1], "val": ["day1"]})
        t2 = ibis.memtable({"id": [1], "val": ["day2"]})
        t3 = ibis.memtable({"id": [1], "val": ["day3"]})

        store.save("src", datetime.date(2026, 3, 10), t1)
        store.save("src", datetime.date(2026, 3, 12), t2)
        store.save("src", datetime.date(2026, 3, 14), t3)

        result = store.get_previous("src", datetime.date(2026, 3, 14))
        assert result is not None
        df = result.execute()
        assert list(df["val"]) == ["day2"]

    def test_get_previous_returns_ibis_table(
        self, store: SnapshotStore, duckdb_conn: ibis.BaseBackend
    ) -> None:
        """get_previous() returns an Ibis table expression, not a DataFrame."""
        table = _sample_table(duckdb_conn)
        store.save("customers", datetime.date(2026, 3, 10), table)

        result = store.get_previous("customers", datetime.date(2026, 3, 15))
        assert result is not None
        # Should be an Ibis table that we can further operate on
        assert hasattr(result, "columns")
        assert result.count().execute() == 3

    def test_get_previous_ignores_non_date_files(
        self, store: SnapshotStore, duckdb_conn: ibis.BaseBackend, tmp_path: Path
    ) -> None:
        """get_previous() ignores parquet files that don't match YYYY-MM-DD pattern."""
        table = _sample_table(duckdb_conn)
        store.save("customers", datetime.date(2026, 3, 10), table)

        # Create a non-date parquet file
        bogus = tmp_path / "customers" / "not-a-date.parquet"
        bogus.write_bytes(b"not real parquet")

        result = store.get_previous("customers", datetime.date(2026, 3, 15))
        assert result is not None
        assert result.count().execute() == 3


class TestSnapshotStoreDatetimeSave:
    """Tests for SnapshotStore.save() with datetime inputs."""

    def test_save_datetime_produces_datetime_filename(
        self, store: SnapshotStore, duckdb_conn: ibis.BaseBackend, tmp_path: Path
    ) -> None:
        """save() with datetime produces YYYY-MM-DDTHH-MM-SS.parquet filename."""
        table = _sample_table(duckdb_conn)
        dt = datetime.datetime(2024, 1, 15, 14, 0, 0)

        result_path = store.save("customers", dt, table)

        expected = tmp_path / "customers" / "2024-01-15T14-00-00.parquet"
        assert result_path == expected
        assert expected.exists()

    def test_save_date_still_produces_date_filename(
        self, store: SnapshotStore, duckdb_conn: ibis.BaseBackend, tmp_path: Path
    ) -> None:
        """save() with date still produces YYYY-MM-DD.parquet filename."""
        table = _sample_table(duckdb_conn)
        d = datetime.date(2024, 1, 15)

        result_path = store.save("customers", d, table)

        expected = tmp_path / "customers" / "2024-01-15.parquet"
        assert result_path == expected
        assert expected.exists()

    def test_save_datetime_data_roundtrips(
        self, store: SnapshotStore, duckdb_conn: ibis.BaseBackend, tmp_path: Path
    ) -> None:
        """Saved datetime snapshot data can be read back correctly."""
        table = _sample_table(duckdb_conn)
        dt = datetime.datetime(2024, 1, 15, 14, 0, 0)

        store.save("customers", dt, table)

        result = duckdb_conn.read_parquet(
            str(tmp_path / "customers" / "2024-01-15T14-00-00.parquet")
        )
        df = result.order_by("id").execute()
        assert list(df["id"]) == [1, 2, 3]
        assert list(df["name"]) == ["alice", "bob", "charlie"]


class TestSnapshotStoreGetPreviousDatetime:
    """Tests for SnapshotStore.get_previous() with datetime inputs."""

    def test_get_previous_datetime_finds_hourly_snapshot(
        self, store: SnapshotStore, duckdb_conn: ibis.BaseBackend
    ) -> None:
        """get_previous() with datetime finds the most recent hourly snapshot."""
        t1 = ibis.memtable({"id": [1], "val": ["hour1"]})
        t2 = ibis.memtable({"id": [1], "val": ["hour2"]})

        store.save("src", datetime.datetime(2024, 1, 15, 10, 0, 0), t1)
        store.save("src", datetime.datetime(2024, 1, 15, 12, 0, 0), t2)

        result = store.get_previous("src", datetime.datetime(2024, 1, 15, 14, 0, 0))
        assert result is not None
        df = result.execute()
        assert list(df["val"]) == ["hour2"]

    def test_get_previous_datetime_mixes_date_and_datetime_candidates(
        self, store: SnapshotStore, duckdb_conn: ibis.BaseBackend
    ) -> None:
        """get_previous() considers both date and datetime snapshots."""
        t_daily = ibis.memtable({"id": [1], "val": ["daily"]})
        t_hourly = ibis.memtable({"id": [1], "val": ["hourly"]})

        store.save("src", datetime.date(2024, 1, 15), t_daily)
        store.save("src", datetime.datetime(2024, 1, 15, 10, 0, 0), t_hourly)

        # Hourly is later than daily (midnight), so it should be returned
        result = store.get_previous("src", datetime.datetime(2024, 1, 15, 14, 0, 0))
        assert result is not None
        df = result.execute()
        assert list(df["val"]) == ["hourly"]

    def test_get_previous_date_still_works_with_datetime_snapshots(
        self, store: SnapshotStore, duckdb_conn: ibis.BaseBackend
    ) -> None:
        """get_previous() with a date reference still finds datetime snapshots."""
        t1 = ibis.memtable({"id": [1], "val": ["hourly"]})
        store.save("src", datetime.datetime(2024, 1, 14, 18, 0, 0), t1)

        result = store.get_previous("src", datetime.date(2024, 1, 15))
        assert result is not None
        df = result.execute()
        assert list(df["val"]) == ["hourly"]

    def test_get_previous_datetime_returns_none_when_no_earlier(
        self, store: SnapshotStore, duckdb_conn: ibis.BaseBackend
    ) -> None:
        """get_previous() returns None when no snapshot is earlier than reference."""
        t1 = ibis.memtable({"id": [1], "val": ["later"]})
        store.save("src", datetime.datetime(2024, 1, 15, 14, 0, 0), t1)

        result = store.get_previous("src", datetime.datetime(2024, 1, 15, 10, 0, 0))
        assert result is None
