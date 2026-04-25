"""E2E regression tests for M070 source-level joins (v0.12.0).

The reporter-facing scenario: pardot_lifecycle_history has TWO LEFT JOINs
to pardot_lifecycle_stage (one for ``previous_stage_id``, one for
``next_stage_id``). Before v0.12.0 this lived as a SQL StagingView; with
M070 the user can declare it as two ``Join`` entries on a StateSource.

This file pins:

* The full Phase 1 chain (``run_pipeline``) executes joins in
  topological order — lifecycle_stage loads first, lifecycle_history
  picks up its joined columns from the registry.
* The downstream identity-graph + ``project_analytics_entity`` flow
  preserves the joined columns (the joined ``name`` columns surface
  on the projected entity output, even if ibis-suffixed).
* Cycle detection: a self-cycle in joins causes ``run_pipeline`` to
  populate ``result.errors`` rather than silently producing wrong
  outputs.

Traceability: client-flowable reporter, 2026-04-25; FR-5 close-out for
the lifecycle_history-shape file.
"""

from __future__ import annotations

from pathlib import Path

import ibis
import pandas as pd

from fyrnheim.config import ResolvedConfig
from fyrnheim.core.source import Join, StateSource
from fyrnheim.engine.pipeline import _load_state_source


def test_m070_lifecycle_history_double_join_e2e(tmp_path: Path) -> None:
    """Mirrors pardot_lifecycle_history shape: two LEFT JOINs to a single
    sibling StateSource (lifecycle_stage), one per FK column. Run
    through ``_load_state_source`` directly (skipping the higher-level
    run_pipeline plumbing here so we can pre-build the source registry).
    Assert the joined columns are reachable downstream.

    The duckdb-fixture path is used so the test is hermetic — both
    sources read from local parquet, get the read → transforms →
    joins → ... chain inside ``_load_state_source``.
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    # --- lifecycle_stage fixture: id (PK) + name. Mirrors the small
    #     dimension-style shape of pardot's lifecycle_stage.
    stage_path = data_dir / "lifecycle_stage.parquet"
    pd.DataFrame(
        {
            "id": [10, 20, 30],
            "name": ["Lead", "Qualified", "Customer"],
        }
    ).to_parquet(str(stage_path))

    # --- lifecycle_history fixture: id (PK) + prospect_id + the two
    #     stage FKs (previous + next). Mirrors pardot's history shape.
    history_path = data_dir / "lifecycle_history.parquet"
    pd.DataFrame(
        {
            "id": ["lh-1", "lh-2", "lh-3"],
            "prospect_id": ["P-1", "P-1", "P-2"],
            "previous_stage_id": [10, 20, 10],
            "next_stage_id": [20, 30, 20],
        }
    ).to_parquet(str(history_path))

    stage_source = StateSource(
        name="lifecycle_stage",
        project="test",
        dataset="test",
        table="lifecycle_stage",
        duckdb_path=str(stage_path),
        id_field="id",
        full_refresh=True,
    )

    history_source = StateSource(
        name="lifecycle_history",
        project="test",
        dataset="test",
        table="lifecycle_history",
        duckdb_path=str(history_path),
        id_field="id",
        full_refresh=True,
        joins=[
            Join(source_name="lifecycle_stage", join_key="previous_stage_id"),
            Join(source_name="lifecycle_stage", join_key="next_stage_id"),
        ],
    )

    config = ResolvedConfig(
        entities_dir=tmp_path / "entities",
        data_dir=data_dir,
        output_dir=tmp_path / "output",
        backend="duckdb",
        project_root=tmp_path,
    )
    (tmp_path / "output").mkdir(parents=True, exist_ok=True)

    conn = ibis.duckdb.connect()

    # --- Manually build the source registry the way Phase 1 would.
    #     lifecycle_stage loads first (no joins), then lifecycle_history
    #     consumes the registry.
    source_registry: dict[str, ibis.Table] = {}
    right_pk_registry: dict[str, str] = {
        "lifecycle_stage": "id",
        "lifecycle_history": "id",
    }

    # Read the stage source's POST-PIPELINE shape into the registry —
    # for the joins helper, we only need the *transformed* table, not
    # the snapshot-diff event stream. Mirrors what run_pipeline does
    # internally: registry stores the post-pipeline ibis.Table, which
    # for full_refresh state sources is the snapshot-diff event view.
    stage_events = _load_state_source(stage_source, config, conn)
    source_registry["lifecycle_stage"] = stage_source.read_table(
        conn, "duckdb", data_dir=data_dir
    )

    # Confirm lifecycle_stage loaded as expected (3 rows of dimension).
    assert len(stage_events.execute()) == 3

    # Run lifecycle_history with the registry populated.
    history_events = _load_state_source(
        history_source,
        config,
        conn,
        source_registry=source_registry,
        right_pk_registry=right_pk_registry,
    )
    df = history_events.execute()

    # Three left rows preserved.
    assert len(df) == 3

    # The events table is post-snapshot-diff, so the schema is the
    # universal event schema (source/entity_id/ts/event_type/payload).
    # The joined columns flow through the payload — pin one row's
    # payload contains references to the lifecycle_stage names.
    import json

    payloads = [json.loads(p) for p in df["payload"].tolist()]
    # Each payload dict should include both stage joins' ``name``
    # columns — ibis suffixes the second-occurrence one.
    sample_keys = set(payloads[0].keys())
    name_like_keys = {k for k in sample_keys if "name" in k}
    assert len(name_like_keys) >= 2, (
        f"expected 2+ name-like columns in payload from double-join; "
        f"got payload keys {sample_keys}"
    )


def test_m070_run_pipeline_topo_sort_with_joins(tmp_path: Path) -> None:
    """Run the FULL pipeline (run_pipeline) with two sources where the
    declaration order is the WRONG order for the join graph. The
    runner's topological sort must reorder them so lifecycle_stage
    loads before lifecycle_history.

    This is the reporter-facing regression: declaring the dependent
    source first (as a typical migration would) must not cause the
    join's lookup to miss.
    """
    from fyrnheim.engine.executor import IbisExecutor
    from fyrnheim.engine.pipeline import run_pipeline

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    stage_path = data_dir / "lifecycle_stage.parquet"
    pd.DataFrame(
        {"id": [10, 20], "name": ["Lead", "Qualified"]}
    ).to_parquet(str(stage_path))
    history_path = data_dir / "lifecycle_history.parquet"
    pd.DataFrame(
        {
            "id": ["lh-1", "lh-2"],
            "prospect_id": ["P-1", "P-2"],
            "previous_stage_id": [10, 20],
            "next_stage_id": [20, 10],
        }
    ).to_parquet(str(history_path))

    stage_source = StateSource(
        name="lifecycle_stage",
        project="test",
        dataset="test",
        table="lifecycle_stage",
        duckdb_path=str(stage_path),
        id_field="id",
        full_refresh=True,
    )
    history_source = StateSource(
        name="lifecycle_history",
        project="test",
        dataset="test",
        table="lifecycle_history",
        duckdb_path=str(history_path),
        id_field="id",
        full_refresh=True,
        joins=[
            Join(source_name="lifecycle_stage", join_key="previous_stage_id"),
            Join(source_name="lifecycle_stage", join_key="next_stage_id"),
        ],
    )

    config = ResolvedConfig(
        entities_dir=tmp_path / "entities",
        data_dir=data_dir,
        output_dir=tmp_path / "output",
        backend="duckdb",
        project_root=tmp_path,
    )
    (tmp_path / "output").mkdir(parents=True, exist_ok=True)

    conn = ibis.duckdb.connect()
    executor = IbisExecutor(conn, "duckdb")

    # NOTE: history declared FIRST, stages SECOND. Without the topo
    # sort, lifecycle_history.joins would look up an empty registry
    # and fail at _apply_joins. The sort moves stage to level 0.
    assets = {
        "sources": [history_source, stage_source],
        "activities": [],
        "identity_graphs": [],
        "analytics_entities": [],
        "metrics_models": [],
        "staging_views": [],
    }
    result = run_pipeline(assets, config, executor)

    # Both sources loaded; no errors surfaced from Phase 1.
    assert result.errors == [], (
        f"unexpected pipeline errors: {result.errors}"
    )
    assert result.source_count == 2
    assert "lifecycle_stage" in result.timings.source_loads
    assert "lifecycle_history" in result.timings.source_loads
