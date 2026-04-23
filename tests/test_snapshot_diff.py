"""Tests for SnapshotDiffPipeline."""

from __future__ import annotations

import datetime
import json
from pathlib import Path

import ibis
import pandas as pd
import pytest

from fyrnheim.config import ResolvedConfig
from fyrnheim.core.source import StateSource
from fyrnheim.engine.pipeline import _load_state_source
from fyrnheim.engine.snapshot_diff import SnapshotDiffPipeline
from fyrnheim.engine.snapshot_store import SnapshotStore


@pytest.fixture()
def duckdb_conn():
    """Create an in-memory DuckDB connection."""
    return ibis.duckdb.connect()


@pytest.fixture()
def pipeline(tmp_path, duckdb_conn):
    """Create a SnapshotDiffPipeline with a temp store."""
    store = SnapshotStore(base_dir=tmp_path / "snapshots", conn=duckdb_conn)
    return SnapshotDiffPipeline(store=store, conn=duckdb_conn)


# ---------------------------------------------------------------------------
# Story 1: Unit tests for SnapshotDiffPipeline
# ---------------------------------------------------------------------------


class TestSnapshotDiffPipelineUnit:
    """Unit tests for SnapshotDiffPipeline.run()."""

    def test_run_saves_snapshot_and_returns_events(self, pipeline, duckdb_conn):
        """run() saves snapshot and returns events on cold start."""
        data = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        current = ibis.memtable(data)

        events = pipeline.run(
            source_name="users",
            current_table=current,
            id_field="id",
            snapshot_date=datetime.date(2026, 1, 1),
        )

        result = events.execute()
        assert len(result) == 2
        assert set(result["event_type"]) == {"row_appeared"}
        assert set(result["entity_id"]) == {"1", "2"}
        assert set(result.columns) == {
            "source",
            "entity_id",
            "ts",
            "event_type",
            "payload",
        }

    def test_run_accepts_exclude_fields(self, pipeline):
        """run() accepts optional exclude_fields parameter."""
        data = pd.DataFrame(
            {"id": [1], "name": ["Alice"], "updated_at": ["2026-01-01"]}
        )
        current = ibis.memtable(data)

        # Should not raise
        events = pipeline.run(
            source_name="users",
            current_table=current,
            id_field="id",
            snapshot_date=datetime.date(2026, 1, 1),
            exclude_fields=["updated_at"],
        )

        result = events.execute()
        assert len(result) == 1

    def test_run_replays_appeared_events_when_diff_is_empty(self, pipeline):
        """M066: when a previous snapshot exists and current data matches it,
        run() replays every current row as a synthetic row_appeared event
        instead of returning an empty table. This fixes the silent
        0-rows-downstream bug for stable StateSources.
        """
        data = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})

        # Run 1: cold start
        pipeline.run(
            source_name="users",
            current_table=ibis.memtable(data),
            id_field="id",
            snapshot_date=datetime.date(2026, 1, 1),
        )

        # Run 2: same data, next day — diff is empty but previous exists,
        # so we expect a full row_appeared replay.
        events = pipeline.run(
            source_name="users",
            current_table=ibis.memtable(data),
            id_field="id",
            snapshot_date=datetime.date(2026, 1, 2),
        )

        result = events.execute()
        assert len(result) == len(data)
        assert set(result["event_type"]) == {"row_appeared"}
        assert set(result["entity_id"]) == {"1", "2"}
        assert set(result.columns) == {
            "source",
            "entity_id",
            "ts",
            "event_type",
            "payload",
        }

    def test_run_replay_uses_current_snapshot_date(self, pipeline):
        """M066: replay events carry the CURRENT snapshot_date.isoformat()
        as their ts, not the previous snapshot's date.
        """
        data = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})

        # Run 1: cold start on 2026-01-01
        pipeline.run(
            source_name="users",
            current_table=ibis.memtable(data),
            id_field="id",
            snapshot_date=datetime.date(2026, 1, 1),
        )

        # Run 2: same data on 2026-01-15 — empty diff -> replay should carry
        # 2026-01-15 as ts.
        run2_date = datetime.date(2026, 1, 15)
        events = pipeline.run(
            source_name="users",
            current_table=ibis.memtable(data),
            id_field="id",
            snapshot_date=run2_date,
        )

        result = events.execute()
        assert len(result) == 2
        assert set(result["ts"]) == {run2_date.isoformat()}


# ---------------------------------------------------------------------------
# Story 2: End-to-end test with multiple pipeline runs
# ---------------------------------------------------------------------------


class TestSnapshotDiffPipelineE2E:
    """E2e tests simulating multiple daily runs with changing data."""

    @staticmethod
    def _customers_day1() -> pd.DataFrame:
        return pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "email": ["alice@x.com", "bob@x.com", "charlie@x.com"],
                "plan": ["free", "pro", "free"],
            }
        )

    @staticmethod
    def _customers_day2() -> pd.DataFrame:
        # Add Dave, change Alice plan free->pro, remove Charlie
        return pd.DataFrame(
            {
                "id": [1, 2, 4],
                "name": ["Alice", "Bob", "Dave"],
                "email": ["alice@x.com", "bob@x.com", "dave@x.com"],
                "plan": ["pro", "pro", "free"],
            }
        )

    @staticmethod
    def _customers_day3() -> pd.DataFrame:
        # Same as day 2
        return pd.DataFrame(
            {
                "id": [1, 2, 4],
                "name": ["Alice", "Bob", "Dave"],
                "email": ["alice@x.com", "bob@x.com", "dave@x.com"],
                "plan": ["pro", "pro", "free"],
            }
        )

    def test_e2e_cold_start_all_rows_appeared(self, pipeline):
        """Run 1 (cold start): all rows produce row_appeared events."""
        events = pipeline.run(
            source_name="customers",
            current_table=ibis.memtable(self._customers_day1()),
            id_field="id",
            snapshot_date=datetime.date(2026, 1, 1),
        )

        result = events.execute()
        assert len(result) == 3
        assert set(result["event_type"]) == {"row_appeared"}
        assert set(result["entity_id"]) == {"1", "2", "3"}

    def test_e2e_changes_produce_correct_events(self, pipeline):
        """Run 2 (with changes): correct mix of event types."""
        # Run 1 first
        pipeline.run(
            source_name="customers",
            current_table=ibis.memtable(self._customers_day1()),
            id_field="id",
            snapshot_date=datetime.date(2026, 1, 1),
        )

        # Run 2
        events = pipeline.run(
            source_name="customers",
            current_table=ibis.memtable(self._customers_day2()),
            id_field="id",
            snapshot_date=datetime.date(2026, 1, 2),
        )

        result = events.execute()

        # Dave appeared
        appeared = result[result["event_type"] == "row_appeared"]
        assert len(appeared) == 1
        assert appeared.iloc[0]["entity_id"] == "4"

        # Charlie disappeared
        disappeared = result[result["event_type"] == "row_disappeared"]
        assert len(disappeared) == 1
        assert disappeared.iloc[0]["entity_id"] == "3"

        # Alice plan changed free->pro
        changed = result[result["event_type"] == "field_changed"]
        assert len(changed) == 1
        assert changed.iloc[0]["entity_id"] == "1"
        payload = json.loads(changed.iloc[0]["payload"])
        assert payload["field_name"] == "plan"
        assert payload["old_value"] == "free"
        assert payload["new_value"] == "pro"

    def test_e2e_no_changes_replays_appeared_events(self, pipeline):
        """M066: Run 3 (same data as run 2) replays every current row as
        row_appeared rather than producing an empty table, so downstream
        entity materialization keeps working on stable StateSources.
        """
        # Run 1
        pipeline.run(
            source_name="customers",
            current_table=ibis.memtable(self._customers_day1()),
            id_field="id",
            snapshot_date=datetime.date(2026, 1, 1),
        )

        # Run 2
        pipeline.run(
            source_name="customers",
            current_table=ibis.memtable(self._customers_day2()),
            id_field="id",
            snapshot_date=datetime.date(2026, 1, 2),
        )

        # Run 3: same data as day 2 — empty diff + previous exists ->
        # full replay of all 3 current rows as row_appeared.
        day3 = self._customers_day3()
        events = pipeline.run(
            source_name="customers",
            current_table=ibis.memtable(day3),
            id_field="id",
            snapshot_date=datetime.date(2026, 1, 3),
        )

        result = events.execute()
        assert len(result) == len(day3)
        assert set(result["event_type"]) == {"row_appeared"}
        assert set(result["entity_id"]) == {
            str(eid) for eid in day3["id"].tolist()
        }

    def test_e2e_events_union_into_consistent_log(self, pipeline):
        """Events from all runs union into a single consistent event log."""
        # Run 1
        events1 = pipeline.run(
            source_name="customers",
            current_table=ibis.memtable(self._customers_day1()),
            id_field="id",
            snapshot_date=datetime.date(2026, 1, 1),
        )

        # Run 2
        events2 = pipeline.run(
            source_name="customers",
            current_table=ibis.memtable(self._customers_day2()),
            id_field="id",
            snapshot_date=datetime.date(2026, 1, 2),
        )

        # Run 3
        events3 = pipeline.run(
            source_name="customers",
            current_table=ibis.memtable(self._customers_day3()),
            id_field="id",
            snapshot_date=datetime.date(2026, 1, 3),
        )

        # Union all events
        all_events = ibis.union(events1, events2, events3)
        result = all_events.execute()

        # Run 1: 3 appeared. Run 2: 1 appeared + 1 disappeared + 1 changed.
        # Run 3: M066 empty-diff replay -> 3 appeared for each current row.
        assert len(result) == 9
        assert set(result.columns) == {
            "source",
            "entity_id",
            "ts",
            "event_type",
            "payload",
        }

        # Verify event type counts
        type_counts = result["event_type"].value_counts().to_dict()
        # 3 (run1) + 1 (run2 dave) + 3 (run3 replay) = 7
        assert type_counts["row_appeared"] == 7
        assert type_counts["row_disappeared"] == 1
        assert type_counts["field_changed"] == 1

        # All events have same source
        assert set(result["source"]) == {"customers"}


# ---------------------------------------------------------------------------
# M066: StateSource.full_refresh flag tests
# ---------------------------------------------------------------------------


def _make_config(tmp_path: Path) -> ResolvedConfig:
    return ResolvedConfig(
        entities_dir=tmp_path / "entities",
        data_dir=tmp_path / "data",
        output_dir=tmp_path / "output",
        backend="duckdb",
        project_root=tmp_path,
    )


def _write_parquet(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(str(path))


class TestStateSourceFullRefresh:
    """Tests for the M066 full_refresh escape hatch on StateSource."""

    def test_state_source_full_refresh_defaults_false(self):
        """StateSource.full_refresh defaults to False so the new replay-on-
        empty default is what users get without config changes.
        """
        source = StateSource(
            name="s",
            id_field="id",
            project="p",
            dataset="d",
            table="t",
        )
        assert source.full_refresh is False

    def test_state_source_full_refresh_skips_snapshot_store(
        self, tmp_path: Path, duckdb_conn
    ):
        """With full_refresh=True, _load_state_source emits row_appeared
        for every current row on every run AND never creates any files
        under the snapshot directory.
        """
        config = _make_config(tmp_path)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        parquet_path = data_dir / "users.parquet"
        df = pd.DataFrame(
            {"id": ["1", "2", "3"], "name": ["alice", "bob", "carol"]}
        )
        _write_parquet(parquet_path, df)

        source = StateSource(
            name="users",
            project="test",
            dataset="test",
            table="users",
            duckdb_path=str(parquet_path),
            id_field="id",
            full_refresh=True,
        )

        snapshot_dir = Path(config.output_dir) / "snapshots"

        for _run in range(2):
            events = _load_state_source(source, config, duckdb_conn)
            result = events.execute()
            assert len(result) == len(df)
            assert set(result["event_type"]) == {"row_appeared"}
            assert set(result["entity_id"]) == {"1", "2", "3"}

        # SnapshotStore was never consulted — no files under snapshot_dir
        # (the dir may or may not exist; either way it must be empty).
        if snapshot_dir.exists():
            assert list(snapshot_dir.rglob("*.parquet")) == []
