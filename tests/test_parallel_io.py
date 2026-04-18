"""Tests for M059 parallel I/O fan-out in ``run_pipeline`` + DuckDB-safe executor.

Covers:
- ``test_parallel_source_load_preserves_order``: Phase 1 source fan-out
  preserves ``config.sources`` ordering regardless of completion order.
- ``test_parallel_entity_writes_all_happen``: Phase 4 entity-write fan-out
  invokes ``executor.write_table`` exactly once per analytics entity with
  the correct args.
- ``test_exception_in_worker_propagates``: a worker exception surfaces
  verbatim to the caller of ``run_pipeline``.
- ``test_duckdb_executor_is_thread_safe``: 8 threads driving
  ``execute_parameterized`` concurrently on a single DuckDB IbisExecutor
  produce correct results with no exceptions.
- ``test_max_parallel_io_one_sources_serial``: with ``max_parallel_io=1``
  all three source loaders run on the same thread.
- ``test_max_parallel_io_one_entity_writes_serial``: with
  ``max_parallel_io=1`` entity writes never temporally overlap.
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import ibis
import pandas as pd
import pytest

from fyrnheim.config import ResolvedConfig
from fyrnheim.core.analytics_entity import AnalyticsEntity
from fyrnheim.core.source import EventSource
from fyrnheim.engine.executor import IbisExecutor
from fyrnheim.engine.pipeline import run_pipeline


def _make_config(tmp_path: Path, *, max_parallel_io: int = 3) -> ResolvedConfig:
    return ResolvedConfig(
        entities_dir=tmp_path / "entities",
        data_dir=tmp_path / "data",
        output_dir=tmp_path / "output",
        backend="duckdb",
        project_root=tmp_path,
        max_parallel_io=max_parallel_io,
    )


# ---------------------------------------------------------------------------
# Phase 1 ordering
# ---------------------------------------------------------------------------


def test_parallel_source_load_preserves_order(
    tmp_path: Path,
) -> None:
    """3 sources load via the parallel fan-out; the resulting
    ``event_tables`` preserves ``config.sources`` ordering.

    Ordering is enforced by index-based assignment (not ``as_completed``),
    so the ``source_loads`` timing dict keys come back in the same order
    as the input ``config.sources``. Names are picked so alphabetic
    sorting would give a different order — ruling out a happy accident.
    """
    config = _make_config(tmp_path, max_parallel_io=3)
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    source_names = ["zulu_source", "mike_source", "alpha_source"]
    for name in source_names:
        pd.DataFrame(
            {"user_id": ["u1"], "event_time": ["2024-01-01"], "page": ["/"]}
        ).to_parquet(str(data_dir / f"{name}.parquet"))

    sources = [
        EventSource(
            name=name,
            project="test",
            dataset="test",
            table=name,
            duckdb_path=str(data_dir / f"{name}.parquet"),
            entity_id_field="user_id",
            timestamp_field="event_time",
            event_type="page_view",
        )
        for name in source_names
    ]

    assets = {
        "sources": sources,
        "activities": [],
        "identity_graphs": [],
        "analytics_entities": [],
        "metrics_models": [],
    }

    executor = IbisExecutor.duckdb()
    result = run_pipeline(assets, config, executor)

    # All 3 loaded.
    assert result.source_count == 3
    # Timings (and event_tables) come back in INPUT order, not alpha order.
    assert list(result.timings.source_loads.keys()) == source_names


# ---------------------------------------------------------------------------
# Phase 4 entity-write fan-out
# ---------------------------------------------------------------------------


def test_parallel_entity_writes_all_happen(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With 3 ``materialization="table"`` analytics entities and a mocked
    executor, ``write_table`` is called exactly 3 times (once per entity)
    with the entity's project/dataset/table args.

    ``project_analytics_entity`` is stubbed to a trivial 1-row Ibis
    memtable so the test exercises only the fan-out + write step, not
    the projection engine (that's M060's axis).
    """
    config = _make_config(tmp_path, max_parallel_io=3)
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    parquet_path = data_dir / "events.parquet"
    pd.DataFrame(
        {
            "user_id": ["u1", "u2", "u3"],
            "event_time": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "page": ["/a", "/b", "/c"],
        }
    ).to_parquet(str(parquet_path))

    source = EventSource(
        name="events",
        project="test",
        dataset="test",
        table="events",
        duckdb_path=str(parquet_path),
        entity_id_field="user_id",
        timestamp_field="event_time",
        event_type="page_view",
    )

    from fyrnheim.core.analytics_entity import Measure

    entities = [
        AnalyticsEntity(
            name=f"ae_{i}",
            project="p",
            dataset="d",
            table=f"t_{i}",
            materialization="table",
            measures=[
                Measure(
                    name="n",
                    activity="page_view",
                    aggregation="count",
                )
            ],
        )
        for i in range(3)
    ]

    # Stub projection so the test is about fan-out, not projection logic.
    from fyrnheim.engine import pipeline as pipeline_module

    def _stub_projection(_events: ibis.Table, ae: AnalyticsEntity) -> ibis.Table:
        return ibis.memtable(pd.DataFrame({"name": [ae.name], "count": [1]}))

    monkeypatch.setattr(
        pipeline_module, "project_analytics_entity", _stub_projection
    )

    real_executor = IbisExecutor.duckdb()
    mock_write = MagicMock()
    monkeypatch.setattr(real_executor, "write_table", mock_write)

    assets = {
        "sources": [source],
        "activities": [],
        "identity_graphs": [],
        "analytics_entities": entities,
        "metrics_models": [],
    }

    result = run_pipeline(assets, config, real_executor)

    # One call per entity.
    assert mock_write.call_count == 3
    # Each entity's write_table received project/dataset/table_name + df.
    call_names = {call.args[2] for call in mock_write.call_args_list}
    assert call_names == {"t_0", "t_1", "t_2"}
    # And result.output_count tracked them.
    assert result.output_count == 3


# ---------------------------------------------------------------------------
# Exception propagation from a worker
# ---------------------------------------------------------------------------


class _ExplodingSource(EventSource):
    """EventSource subclass whose loader raises at read_table time."""

    def read_table(self, conn: Any, backend: str, data_dir: Any = None) -> Any:
        raise RuntimeError("boom")


def test_exception_in_worker_propagates(
    tmp_path: Path,
) -> None:
    """With 3 sources where the second raises ``RuntimeError('boom')``,
    ``run_pipeline`` re-raises ``RuntimeError('boom')`` — not a wrapper.
    """
    config = _make_config(tmp_path, max_parallel_io=3)
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    # Two OK sources on disk + one that explodes.
    for name in ("ok_1", "ok_2"):
        pd.DataFrame(
            {"user_id": ["u"], "event_time": ["2024-01-01"], "page": ["/"]}
        ).to_parquet(str(data_dir / f"{name}.parquet"))

    good_1 = EventSource(
        name="ok_1",
        project="test",
        dataset="test",
        table="ok_1",
        duckdb_path=str(data_dir / "ok_1.parquet"),
        entity_id_field="user_id",
        timestamp_field="event_time",
        event_type="page_view",
    )
    exploder = _ExplodingSource(
        name="boom_source",
        project="test",
        dataset="test",
        table="boom",
        duckdb_path=str(data_dir / "whatever.parquet"),
        entity_id_field="user_id",
        timestamp_field="event_time",
        event_type="page_view",
    )
    good_2 = EventSource(
        name="ok_2",
        project="test",
        dataset="test",
        table="ok_2",
        duckdb_path=str(data_dir / "ok_2.parquet"),
        entity_id_field="user_id",
        timestamp_field="event_time",
        event_type="page_view",
    )

    assets = {
        "sources": [good_1, exploder, good_2],
        "activities": [],
        "identity_graphs": [],
        "analytics_entities": [],
        "metrics_models": [],
    }

    executor = IbisExecutor.duckdb()
    with pytest.raises(RuntimeError, match="boom"):
        run_pipeline(assets, config, executor)


# ---------------------------------------------------------------------------
# DuckDB executor thread safety
# ---------------------------------------------------------------------------


def test_duckdb_executor_is_thread_safe() -> None:
    """Eight threads calling ``execute_parameterized`` concurrently on a
    shared DuckDB IbisExecutor produce the expected result with no
    exceptions. Exercises the ``_conn_lock`` path.
    """
    executor = IbisExecutor.duckdb()

    def _query(value: int) -> tuple[int, ...]:
        rows = executor.execute_parameterized(
            "SELECT @x AS x", {"x": value}
        )
        assert rows, "query returned no rows"
        return rows[0]

    values = list(range(8))
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(_query, values))

    assert [r[0] for r in results] == values


# ---------------------------------------------------------------------------
# max_parallel_io=1 — strictly serial regression coverage (M061)
# ---------------------------------------------------------------------------


_THREAD_IDS: list[int] = []


class _ThreadRecordingSource(EventSource):
    """EventSource subclass that records the OS thread it ran on.

    ``read_table`` is the pipeline's source-load seam, so by recording
    ``threading.get_ident()`` in it we observe exactly which worker thread
    executed the load. Results are appended to the module-level
    ``_THREAD_IDS`` list (the Pydantic BaseModel ancestor rejects per-
    instance state, so we keep the recorder external).
    """

    def read_table(self, conn: Any, backend: str, data_dir: Any = None) -> Any:
        _THREAD_IDS.append(threading.get_ident())
        # Return a minimal Ibis memtable shaped like the other loaders.
        return ibis.memtable(
            pd.DataFrame(
                {
                    "user_id": ["u"],
                    "event_time": ["2024-01-01"],
                    "page": ["/"],
                }
            )
        )


def test_max_parallel_io_one_sources_serial(tmp_path: Path) -> None:
    """With ``max_parallel_io=1`` the source fan-out uses a single worker,
    so all three loaders run on the same OS thread.
    """
    config = _make_config(tmp_path, max_parallel_io=1)
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    # Reset the module-level recorder so other tests don't bleed in.
    _THREAD_IDS.clear()

    sources = [
        _ThreadRecordingSource(
            name=f"src_{i}",
            project="test",
            dataset="test",
            table=f"src_{i}",
            duckdb_path=str(data_dir / f"src_{i}.parquet"),
            entity_id_field="user_id",
            timestamp_field="event_time",
            event_type="page_view",
        )
        for i in range(3)
    ]

    assets = {
        "sources": sources,
        "activities": [],
        "identity_graphs": [],
        "analytics_entities": [],
        "metrics_models": [],
    }

    executor = IbisExecutor.duckdb()
    result = run_pipeline(assets, config, executor)

    assert result.source_count == 3
    tids = list(_THREAD_IDS)
    assert len(tids) == 3
    assert len(set(tids)) == 1, (
        f"max_parallel_io=1 should serialize source loads onto one thread, "
        f"got {len(set(tids))} distinct thread ids: {tids}"
    )


def test_max_parallel_io_one_entity_writes_serial(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With ``max_parallel_io=1`` entity writes run back-to-back — the
    next write never enters before the previous one has exited.

    We wrap ``executor.write_table`` in a stub that records an
    (enter, exit) timestamp pair per call, holds the connection long
    enough to make overlap detectable, then asserts that every
    interval's entry is at-or-after the previous interval's exit.
    """
    config = _make_config(tmp_path, max_parallel_io=1)
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    parquet_path = data_dir / "events.parquet"
    pd.DataFrame(
        {
            "user_id": ["u1", "u2", "u3"],
            "event_time": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "page": ["/a", "/b", "/c"],
        }
    ).to_parquet(str(parquet_path))

    source = EventSource(
        name="events",
        project="test",
        dataset="test",
        table="events",
        duckdb_path=str(parquet_path),
        entity_id_field="user_id",
        timestamp_field="event_time",
        event_type="page_view",
    )

    from fyrnheim.core.analytics_entity import Measure

    entities = [
        AnalyticsEntity(
            name=f"ae_{i}",
            project="p",
            dataset="d",
            table=f"t_{i}",
            materialization="table",
            measures=[
                Measure(
                    name="n",
                    activity="page_view",
                    aggregation="count",
                )
            ],
        )
        for i in range(3)
    ]

    # Stub projection to avoid pulling in M060 engine work.
    from fyrnheim.engine import pipeline as pipeline_module

    def _stub_projection(
        _events: ibis.Table, ae: AnalyticsEntity
    ) -> ibis.Table:
        return ibis.memtable(pd.DataFrame({"name": [ae.name], "count": [1]}))

    monkeypatch.setattr(
        pipeline_module, "project_analytics_entity", _stub_projection
    )

    real_executor = IbisExecutor.duckdb()

    intervals: list[tuple[float, float]] = []
    intervals_lock = threading.Lock()

    def _recording_write_table(
        project: str, dataset: str, name: str, df: pd.DataFrame
    ) -> None:
        enter_ts = time.monotonic()
        # Small sleep so a concurrent call would produce overlap we can detect.
        time.sleep(0.02)
        exit_ts = time.monotonic()
        with intervals_lock:
            intervals.append((enter_ts, exit_ts))

    monkeypatch.setattr(real_executor, "write_table", _recording_write_table)

    assets = {
        "sources": [source],
        "activities": [],
        "identity_graphs": [],
        "analytics_entities": entities,
        "metrics_models": [],
    }

    result = run_pipeline(assets, config, real_executor)
    assert result.output_count == 3
    assert len(intervals) == 3

    ordered = sorted(intervals, key=lambda iv: iv[0])
    # Pairwise (prev, next) over ordered intervals; different lengths by
    # construction, so strict=False is intentional here.
    for (_, prev_exit), (next_enter, _) in zip(
        ordered, ordered[1:], strict=False
    ):
        assert next_enter >= prev_exit, (
            f"max_parallel_io=1 must serialize writes, but one started at "
            f"{next_enter:.6f} before the previous finished at "
            f"{prev_exit:.6f}"
        )
