"""End-to-end test for AnalyticsEntity materialization='table' on duckdb."""

from __future__ import annotations

import ibis
import pandas as pd

from fyrnheim.core.analytics_entity import AnalyticsEntity, Measure
from fyrnheim.engine.analytics_entity_engine import project_analytics_entity
from fyrnheim.engine.executor import IbisExecutor


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
