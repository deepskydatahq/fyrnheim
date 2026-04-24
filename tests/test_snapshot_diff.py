"""Tests for SnapshotDiffPipeline."""

from __future__ import annotations

import datetime
import json
from pathlib import Path

import ibis
import pandas as pd
import pytest

from fyrnheim.components.computed_column import ComputedColumn
from fyrnheim.config import ResolvedConfig
from fyrnheim.core.source import Field, Rename, SourceTransforms, StateSource
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

    def test_run_skips_replay_when_current_is_empty(self, tmp_path, duckdb_conn):
        """M067: a StateSource whose upstream returns 0 rows must not crash
        the pipeline on subsequent runs. The v0.8.0 replay branch would try
        to ``_make_appeared_events`` on a 0-row DataFrame, causing
        ``ibis.memtable`` to reject it with "Provided table/dataframe must
        have at least one column". The M067 guard narrows the replay branch
        so it skips when current is also empty — nothing to replay — and
        returns the empty events table unchanged.
        """
        snapshot_dir = tmp_path / "snapshots"
        store = SnapshotStore(base_dir=snapshot_dir, conn=duckdb_conn)
        pipeline = SnapshotDiffPipeline(store=store, conn=duckdb_conn)

        # 0-row current table (empty placeholder source — e.g. Salesforce
        # placeholders backed by ``SELECT ... FROM UNNEST([1]) LIMIT 0``).
        empty_df = pd.DataFrame(
            {
                "id": pd.Series([], dtype="int64"),
                "name": pd.Series([], dtype="object"),
            }
        )

        # Run 1 — cold start, previous is None
        result1 = pipeline.run(
            source_name="empty_source",
            current_table=ibis.memtable(empty_df),
            id_field="id",
            snapshot_date=datetime.date(2026, 1, 1),
        )
        assert len(result1.execute()) == 0

        # Run 2 — previous snapshot exists (empty); current still empty.
        # Without the M067 guard this would crash with
        # "Provided table/dataframe must have at least one column".
        result2 = pipeline.run(
            source_name="empty_source",
            current_table=ibis.memtable(empty_df),
            id_field="id",
            snapshot_date=datetime.date(2026, 1, 2),
        )
        assert len(result2.execute()) == 0

        # Both snapshots saved (the skip path still calls store.save so the
        # "previous snapshot exists on subsequent runs" invariant holds).
        source_dir = snapshot_dir / "empty_source"
        parquet_files = sorted(source_dir.glob("*.parquet"))
        assert len(parquet_files) == 2


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
# M071 regression: int-shape entity_ids end-to-end through SnapshotDiffPipeline
# ---------------------------------------------------------------------------


class TestM071Int64EntityIdIntegration:
    """M071: integer-typed id columns must emit int-shaped entity_ids
    (``'1'``, not ``'1.0'``) through the full SnapshotDiffPipeline — both
    on cold start and across the M066 empty-diff replay path.

    Phase 1 showed the promotion is NOT in ``ibis.memtable.execute()`` or
    the parquet round-trip (both preserve int64) — it surfaces inside
    ``pd.DataFrame.iterrows()`` when the row contains any float column,
    because iterrows packs each row into a homogeneous-dtype ``Series``
    and promotes the int to float64.
    """

    def test_int64_id_column_preserves_int_shape_on_cold_start(self, pipeline):
        """Cold-start run (Phase 1 minimal reproducer): int64 id + float64
        column → entity_ids must be ``['1', '2', '3']``, not
        ``['1.0', '2.0', '3.0']``.
        """
        df = pd.DataFrame({"id": [1, 2, 3], "score": [3.14, 2.71, 1.41]})
        events = pipeline.run(
            source_name="src",
            current_table=ibis.memtable(df),
            id_field="id",
            snapshot_date=datetime.date(2026, 1, 1),
        )

        result = events.execute()
        entity_ids = result["entity_id"].tolist()
        assert entity_ids == ["1", "2", "3"]
        assert all("." not in eid for eid in entity_ids)

    def test_int64_id_column_preserves_int_shape_on_m066_replay(self, pipeline):
        """M066 empty-diff replay path: run the pipeline twice with the
        same int64-id + float-column data. Run 2 hits the replay branch
        (empty diff + prior snapshot) which re-exercises
        ``_make_appeared_events`` over the parquet-round-tripped current
        table. Assert entity_ids remain int-shaped.
        """
        df = pd.DataFrame({"id": [1, 2, 3], "score": [3.14, 2.71, 1.41]})

        # Run 1 (cold start)
        pipeline.run(
            source_name="src",
            current_table=ibis.memtable(df),
            id_field="id",
            snapshot_date=datetime.date(2026, 1, 1),
        )

        # Run 2 (empty diff → replay)
        events = pipeline.run(
            source_name="src",
            current_table=ibis.memtable(df),
            id_field="id",
            snapshot_date=datetime.date(2026, 1, 2),
        )
        result = events.execute()
        assert len(result) == 3
        assert set(result["event_type"]) == {"row_appeared"}
        entity_ids = sorted(result["entity_id"].tolist())
        assert entity_ids == ["1", "2", "3"]
        assert all("." not in eid for eid in entity_ids)

    def test_int64_id_column_preserves_int_shape_on_row_disappeared(
        self, pipeline
    ):
        """Disappeared events go through ``_make_disappeared_events`` —
        same iterrows-promotion hazard. Run 1 with 3 int-id rows, run 2
        with the same id column dropped (empty current), expect 3
        row_disappeared events with int-shaped entity_ids.
        """
        df1 = pd.DataFrame({"id": [1, 2, 3], "score": [3.14, 2.71, 1.41]})
        df2 = pd.DataFrame(
            {
                "id": pd.Series([], dtype="int64"),
                "score": pd.Series([], dtype="float64"),
            }
        )

        pipeline.run(
            source_name="src",
            current_table=ibis.memtable(df1),
            id_field="id",
            snapshot_date=datetime.date(2026, 1, 1),
        )
        events = pipeline.run(
            source_name="src",
            current_table=ibis.memtable(df2),
            id_field="id",
            snapshot_date=datetime.date(2026, 1, 2),
        )
        result = events.execute()
        disappeared = result[result["event_type"] == "row_disappeared"]
        entity_ids = sorted(disappeared["entity_id"].tolist())
        assert entity_ids == ["1", "2", "3"]
        assert all("." not in eid for eid in entity_ids)


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


# ---------------------------------------------------------------------------
# M068: StateSource.transforms and StateSource.computed_columns tests
# ---------------------------------------------------------------------------


class TestStateSourceTransformsAndComputedColumns:
    """M068: verify `_load_state_source` now honours `transforms` and
    `computed_columns` (previously dead pydantic fields).
    """

    def test_state_source_transforms_rename(
        self, tmp_path: Path, duckdb_conn
    ) -> None:
        """A rename transform on a StateSource is visible in the saved
        snapshot and in the emitted events: ``id_field='account_id'``
        resolves against the RENAMED column (original column ``id``).
        """
        config = _make_config(tmp_path)
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        parquet_path = data_dir / "accounts.parquet"
        df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        _write_parquet(parquet_path, df)

        source = StateSource(
            name="accounts",
            project="test",
            dataset="test",
            table="accounts",
            duckdb_path=str(parquet_path),
            id_field="account_id",  # post-transform name
            transforms=SourceTransforms(
                renames=[Rename(from_name="id", to_name="account_id")]
            ),
        )

        events = _load_state_source(source, config, duckdb_conn)
        result = events.execute()

        # Entity ids come from the renamed column.
        assert len(result) == 2
        assert set(result["entity_id"]) == {"1", "2"}
        assert set(result["event_type"]) == {"row_appeared"}

        # Saved snapshot reflects the transformed schema (renamed column).
        snapshot_dir = Path(config.output_dir) / "snapshots" / "accounts"
        parquet_files = list(snapshot_dir.glob("*.parquet"))
        assert len(parquet_files) == 1
        saved = pd.read_parquet(str(parquet_files[0]))
        assert "account_id" in saved.columns
        assert "id" not in saved.columns

    def test_state_source_computed_columns_applied(
        self, tmp_path: Path, duckdb_conn
    ) -> None:
        """A StateSource ``computed_columns`` entry appears in the emitted
        row_appeared event payload (mirrors the existing EventSource
        behavior — M068 wires this for StateSource).
        """
        config = _make_config(tmp_path)
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        parquet_path = data_dir / "people.parquet"
        df = pd.DataFrame(
            {
                "id": [1, 2],
                "first": ["Alice", "Bob"],
                "last": ["Smith", "Jones"],
            }
        )
        _write_parquet(parquet_path, df)

        source = StateSource(
            name="people",
            project="test",
            dataset="test",
            table="people",
            duckdb_path=str(parquet_path),
            id_field="id",
            computed_columns=[
                ComputedColumn(
                    name="full_name",
                    expression='t.first + " " + t.last',
                ),
            ],
        )

        events = _load_state_source(source, config, duckdb_conn)
        result = events.execute()

        assert len(result) == 2
        assert set(result["event_type"]) == {"row_appeared"}
        payloads = [json.loads(p) for p in result["payload"].tolist()]
        full_names = {p["full_name"] for p in payloads}
        assert full_names == {"Alice Smith", "Bob Jones"}

    def test_computed_columns_see_transformed_columns(
        self, tmp_path: Path, duckdb_conn
    ) -> None:
        """A computed_column expression can reference a column RENAMED by
        transforms. Proves transforms apply BEFORE computed_columns.
        """
        config = _make_config(tmp_path)
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        parquet_path = data_dir / "accounts.parquet"
        df = pd.DataFrame({"id": ["A1", "A2"], "name": ["Alice", "Bob"]})
        _write_parquet(parquet_path, df)

        source = StateSource(
            name="accounts",
            project="test",
            dataset="test",
            table="accounts",
            duckdb_path=str(parquet_path),
            id_field="account_id",  # renamed-to name
            transforms=SourceTransforms(
                renames=[Rename(from_name="id", to_name="account_id")]
            ),
            computed_columns=[
                ComputedColumn(
                    name="prefix",
                    expression="t.account_id",  # resolves post-rename
                ),
            ],
        )

        # If transforms applied AFTER computed_columns, the eval would
        # fail because ``account_id`` would not exist on the pre-rename
        # schema. A successful emit proves the ordering.
        events = _load_state_source(source, config, duckdb_conn)
        result = events.execute()

        assert len(result) == 2
        payloads = [json.loads(p) for p in result["payload"].tolist()]
        prefixes = {p["prefix"] for p in payloads}
        assert prefixes == {"A1", "A2"}


# ---------------------------------------------------------------------------
# M069: StateSource.filter + Field.json_path tests
# ---------------------------------------------------------------------------


class TestStateSourceFilterAndJsonPath:
    """M069: filter and json_path apply at source load time; the filtered
    table is what the snapshot store saves, so the replay path respects
    the filter on subsequent runs."""

    def test_state_source_filter_drops_rows(
        self, tmp_path: Path, duckdb_conn
    ) -> None:
        """StateSource.filter='t.deleted != True' drops deleted rows from
        the emitted events, and — because the filter runs before the
        snapshot save — the replay path on run 2 also sees only the
        non-deleted rows.
        """
        config = _make_config(tmp_path)
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        parquet_path = data_dir / "records.parquet"
        df = pd.DataFrame(
            {
                "id": ["r1", "r2", "r3"],
                "name": ["Alice", "Bob", "Ghost"],
                "deleted": [False, False, True],
            }
        )
        _write_parquet(parquet_path, df)

        source = StateSource(
            name="records",
            project="test",
            dataset="test",
            table="records",
            duckdb_path=str(parquet_path),
            id_field="id",
            filter="t.deleted != True",
        )

        # Run 1: cold-start — filter drops the deleted row.
        events1 = _load_state_source(source, config, duckdb_conn).execute()
        assert len(events1) == 2
        assert set(events1["entity_id"]) == {"r1", "r2"}
        # The deleted row must not leak into any emitted event.
        assert "r3" not in set(events1["entity_id"])

        # Run 2: snapshot exists from run 1, same underlying data — the
        # M066 empty-diff replay emits every current-filtered row as
        # row_appeared. If the filter had leaked through only on run 1,
        # run 2's replay would see all 3 raw rows — this asserts the
        # snapshot store saves the POST-filter table.
        events2 = _load_state_source(source, config, duckdb_conn).execute()
        assert len(events2) == 2
        assert set(events2["event_type"]) == {"row_appeared"}
        assert set(events2["entity_id"]) == {"r1", "r2"}

    def test_state_source_filter_references_computed_column(
        self, tmp_path: Path, duckdb_conn
    ) -> None:
        """Filter is applied AFTER computed_columns — it can reference
        columns produced by computed_columns (pipeline-stage order:
        computed_columns → filter).
        """
        config = _make_config(tmp_path)
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        parquet_path = data_dir / "items.parquet"
        df = pd.DataFrame(
            {"id": ["I1", "I2", "I3"], "cents": [50, 200, 750]}
        )
        _write_parquet(parquet_path, df)

        source = StateSource(
            name="items",
            project="test",
            dataset="test",
            table="items",
            duckdb_path=str(parquet_path),
            id_field="id",
            computed_columns=[
                ComputedColumn(name="dollars", expression="t.cents / 100"),
            ],
            filter="t.dollars > 1",
        )

        events = _load_state_source(source, config, duckdb_conn).execute()
        # cents/100 > 1 keeps I2 (2.0) and I3 (7.5) only. If the filter
        # applied BEFORE computed_columns, the eval would fail because
        # ``dollars`` would not exist on the pre-mutate schema.
        assert set(events["entity_id"]) == {"I2", "I3"}

    def test_state_source_json_path_surfaces_in_payload(
        self, tmp_path: Path, duckdb_conn
    ) -> None:
        """Field.json_path extracts a typed column; the extracted value
        shows up in the row_appeared event payload."""
        config = _make_config(tmp_path)
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        parquet_path = data_dir / "accounts.parquet"
        df = pd.DataFrame(
            {
                "id": [1, 2],
                "custom_type": [
                    '{"value": "premium"}',
                    '{"value": "free"}',
                ],
            }
        )
        _write_parquet(parquet_path, df)

        source = StateSource(
            name="accounts",
            project="test",
            dataset="test",
            table="accounts",
            duckdb_path=str(parquet_path),
            id_field="id",
            fields=[
                Field(
                    name="account_type",
                    type="STRING",
                    json_path="$.value",
                    source_column="custom_type",
                )
            ],
        )

        events = _load_state_source(source, config, duckdb_conn).execute()
        payloads = [json.loads(p) for p in events["payload"].tolist()]
        extracted = {p["account_type"] for p in payloads}
        assert extracted == {"premium", "free"}
