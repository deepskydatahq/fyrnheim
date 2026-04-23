"""End-to-end test for AnalyticsEntity materialization='table' on duckdb."""

from __future__ import annotations

import datetime

import ibis
import pandas as pd

from fyrnheim.core.analytics_entity import AnalyticsEntity, Measure, StateField
from fyrnheim.core.identity import IdentityGraph, IdentitySource
from fyrnheim.engine.analytics_entity_engine import project_analytics_entity
from fyrnheim.engine.executor import IbisExecutor
from fyrnheim.engine.identity_engine import enrich_events, resolve_identities
from fyrnheim.engine.snapshot_diff import SnapshotDiffPipeline
from fyrnheim.engine.snapshot_store import SnapshotStore


def _make_events() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "source": ["s", "s", "s"],
            "entity_id": ["a", "a", "b"],
            "ts": pd.to_datetime(["2025-01-01", "2025-01-02", "2025-01-03"]),
            "event_type": ["workshop_attended", "workshop_attended", "workshop_attended"],
            "payload": ["{}", "{}", "{}"],
        }
    )


def test_entity_materialization_to_duckdb_table(tmp_path):
    db_path = tmp_path / "test.db"
    executor = IbisExecutor.duckdb(db_path=str(db_path))
    try:
        events_df = _make_events()
        events = ibis.memtable(events_df)

        ae = AnalyticsEntity(
            name="users",
            measures=[
                Measure(name="workshop_count", activity="workshop_attended", aggregation="count")
            ],
            materialization="table",
            project="ignored",
            dataset="marts",
        )

        projected = project_analytics_entity(events, ae)
        df = projected.execute()
        executor.write_table(ae.project, ae.dataset, ae.table, df)
        written = len(df)
    finally:
        executor.close()

    # Fresh connection, same file — proves persistence
    conn2 = ibis.duckdb.connect(str(db_path))
    try:
        result = conn2.table("users", database="marts").execute()
        assert len(result) == written
        assert "workshop_count" in result.columns
    finally:
        conn2.disconnect()


def test_entity_materialization_is_idempotent(tmp_path):
    db_path = tmp_path / "test.db"

    def _run_once() -> int:
        executor = IbisExecutor.duckdb(db_path=str(db_path))
        try:
            events = ibis.memtable(_make_events())
            ae = AnalyticsEntity(
                name="users",
                measures=[
                    Measure(
                        name="workshop_count",
                        activity="workshop_attended",
                        aggregation="count",
                    )
                ],
                materialization="table",
                project="ignored",
                dataset="marts",
            )
            df = project_analytics_entity(events, ae).execute()
            executor.write_table(ae.project, ae.dataset, ae.table, df)
            return len(df)
        finally:
            executor.close()

    count1 = _run_once()
    count2 = _run_once()
    assert count1 == count2

    conn = ibis.duckdb.connect(str(db_path))
    try:
        result = conn.table("users", database="marts").execute()
        assert len(result) == count1  # WRITE_TRUNCATE — no duplication
    finally:
        conn.disconnect()


def test_entity_output_stable_across_persistent_snapshot_runs(tmp_path):
    """M066 regression: wire a StateSource through SnapshotDiffPipeline +
    identity graph + project_analytics_entity. Run the pipeline twice
    across two different snapshot_dates, reusing the SAME snapshot
    directory so the second run sees the first run's saved snapshot.

    Before the M066 fix: the second run's empty diff produced 0 events,
    and the projected entity was empty (0 rows). This test fails on
    0.7.3 and passes with the M066 empty-diff replay.
    """
    snapshot_dir = tmp_path / "snapshots"
    conn = ibis.duckdb.connect()

    # Stable StateSource data — identical across both runs.
    state_df = pd.DataFrame(
        {
            "id": ["u1", "u2", "u3"],
            "company_name": ["Acme", "Globex", "Initech"],
            "region": ["EU", "US", "US"],
        }
    )

    # Companion event-shaped rows so IdentityGraph has >=2 sources.
    # Same ids, just one event per user — lets identity resolution run.
    companion_events_df = pd.DataFrame(
        {
            "source": ["users_events"] * 3,
            "entity_id": ["evt-1", "evt-2", "evt-3"],
            "ts": ["2026-01-01", "2026-01-01", "2026-01-01"],
            "event_type": ["session_start", "session_start", "session_start"],
            "payload": [
                '{"id": "u1"}',
                '{"id": "u2"}',
                '{"id": "u3"}',
            ],
        }
    )

    graph = IdentityGraph(
        name="users_graph",
        canonical_id="canonical_user_id",
        sources=[
            IdentitySource(
                source="users_state",
                id_field="id",
                match_key_field="id",
            ),
            IdentitySource(
                source="users_events",
                id_field="event_id",
                match_key_field="id",
            ),
        ],
    )

    entity = AnalyticsEntity(
        name="users",
        identity_graph="users_graph",
        state_fields=[
            StateField(
                name="company_name",
                source="users_state",
                field="company_name",
                strategy="latest",
            ),
            StateField(
                name="region",
                source="users_state",
                field="region",
                strategy="latest",
            ),
        ],
    )

    def _run_once(snapshot_date: datetime.date) -> pd.DataFrame:
        store = SnapshotStore(base_dir=snapshot_dir, conn=conn)
        pipeline = SnapshotDiffPipeline(store=store, conn=conn)
        current = ibis.memtable(state_df)
        state_events = pipeline.run(
            source_name="users_state",
            current_table=current,
            id_field="id",
            snapshot_date=snapshot_date,
        )
        companion_events = ibis.memtable(companion_events_df)
        all_events = ibis.memtable(
            pd.concat(
                [state_events.execute(), companion_events.execute()],
                ignore_index=True,
            )
        )
        mapping = resolve_identities(all_events, graph)
        enriched = enrich_events(all_events, mapping)
        projected = project_analytics_entity(enriched, entity)
        return projected.execute().sort_values("canonical_id").reset_index(
            drop=True
        )

    run1 = _run_once(datetime.date(2026, 1, 1))
    run2 = _run_once(datetime.date(2026, 1, 2))

    # Run 1 is cold start -> 3 row_appeared -> 3 projected rows.
    # Run 2 has the empty-diff replay fix -> still 3 projected rows
    # with identical state-field values.
    assert len(run1) == len(state_df)
    assert len(run2) == len(state_df)

    assert set(run1["company_name"]) == {"Acme", "Globex", "Initech"}
    assert set(run2["company_name"]) == {"Acme", "Globex", "Initech"}

    # State-field payloads round-trip identically run-over-run.
    pd.testing.assert_frame_equal(
        run1[["company_name", "region"]].reset_index(drop=True),
        run2[["company_name", "region"]].reset_index(drop=True),
    )
