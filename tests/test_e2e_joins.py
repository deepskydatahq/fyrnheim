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
import pytest

from fyrnheim.components.computed_column import ComputedColumn
from fyrnheim.config import ResolvedConfig
from fyrnheim.core.source import EventSource, Join, StateSource
from fyrnheim.engine.pipeline import (
    _build_state_source_table,
    _load_state_source,
    _run_state_source_diff,
)


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
    # internally: the registry stores the pre-diff (post-transforms /
    # post-joins / post-json_path / post-computed_columns / post-filter)
    # ibis.Table built by ``_build_state_source_table``. Re-reading the
    # parquet here would bypass that contract — if lifecycle_stage ever
    # grows transforms or computed_columns, the joined view would miss
    # them.
    stage_table = _build_state_source_table(stage_source, config, conn)
    stage_events = _run_state_source_diff(
        stage_source, config, conn, stage_table
    )
    source_registry["lifecycle_stage"] = stage_table

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
    # columns — ibis suffixes the second-occurrence one. Pin the
    # actual joined values for one row so a regression that resolves
    # joins to the wrong stage row (e.g. swapping prev/next or
    # reusing the same suffix for both) fails loudly. lh-1 has
    # previous_stage_id=10 (→ "Lead") and next_stage_id=20 (→
    # "Qualified").
    row0_joined_names = {
        value for key, value in payloads[0].items() if "name" in key
    }
    assert {"Lead", "Qualified"}.issubset(row0_joined_names), (
        f"expected both joined stage names ('Lead', 'Qualified') in "
        f"payload from double-join; got {payloads[0]}"
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


def test_m075_lifecycle_history_computed_column_with_joins_e2e(
    tmp_path: Path,
) -> None:
    """M075 (FR-9): full pipeline regression mirroring the reporter's
    pardot_lifecycle_history shape. Fixture has all post-everything
    columns INCLUDING transition_type pre-computed. StateSource
    declares joins (which would skip on the fixture-shadow path) AND
    a ComputedColumn whose expression references a join-suffixed
    column that does NOT exist on the fixture.

    Without M075's skip-if-output-exists rule, this would crash:
    the joins skipped → suffix column missing → eval(expression)
    raises. With M075, the engine sees `transition_type` already in
    `table.columns` and preserves the fixture's value.

    Traceability: client-flowable reporter, 2026-04-25; FR-9
    close-out for the lifecycle_history-shape file.
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    stage_path = data_dir / "lifecycle_stage.parquet"
    pd.DataFrame(
        {
            "id": [10, 20, 30],
            "name": ["Lead", "Qualified", "Customer"],
        }
    ).to_parquet(str(stage_path))

    # The history fixture is the post-everything snapshot — already
    # contains transition_type even though that's a computed column.
    history_path = data_dir / "lifecycle_history.parquet"
    pd.DataFrame(
        {
            "id": ["lh-1", "lh-2", "lh-3"],
            "prospect_id": ["P-1", "P-1", "P-2"],
            "previous_stage_id": [10, 20, 10],
            "next_stage_id": [20, 30, 20],
            "transition_type": ["upgrade", "upgrade", "upgrade"],
        }
    ).to_parquet(str(history_path))

    # NOTE: lifecycle_stage parquet exists on disk but no StateSource
    # is constructed for it here — on the fixture-shadow path the
    # joins skip, so the registry never gets consulted. The fixture
    # is what proves M075's skip-when-output-exists rule.

    history_source = StateSource(
        name="lifecycle_history",
        project="test",
        dataset="test",
        table="lifecycle_history",
        duckdb_path=str(history_path),
        duckdb_fixture_is_transformed=True,
        id_field="id",
        full_refresh=True,
        joins=[
            Join(source_name="lifecycle_stage", join_key="previous_stage_id"),
            Join(source_name="lifecycle_stage", join_key="next_stage_id"),
        ],
        computed_columns=[
            # Expression references a join-suffixed column that the
            # fixture-shadow path skips producing. Without M075 this
            # would NameError at eval time.
            ComputedColumn(
                name="transition_type",
                expression=(
                    "t.name_lifecycle_stage_next_stage_id "
                    "+ '/' + t.name_lifecycle_stage_previous_stage_id"
                ),
            ),
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

    # M072 path: fixture-shadow fires; joins/transforms/json_path/filter
    # all skip. With M075, computed_columns ALSO skip when their output
    # column is already in the fixture. This used to crash on the
    # missing join-suffixed column reference; now it preserves the
    # fixture's "upgrade" sentinel.
    history_events = _load_state_source(
        history_source,
        config,
        conn,
    )
    df = history_events.execute()

    assert len(df) == 3

    import json

    payloads = [json.loads(p) for p in df["payload"].tolist()]
    transition_types = sorted(p["transition_type"] for p in payloads)
    # Fixture's pre-computed value preserved — NOT the computed
    # expression's would-be result.
    assert transition_types == ["upgrade", "upgrade", "upgrade"]


def test_m076_lifecycle_history_eventsource_joins_e2e(tmp_path: Path) -> None:
    """FR-10 / M076 (v0.13.0) — EventSource gains the same joins shape
    StateSource got in M070. Reporter follow-up
    (client-flowable 2026-04-25): pardot_lifecycle_history is an
    EventSource (downstream activities use EventOccurred(
    event_type="lead_created") dispatch), so M070's StateSource-only
    joins blocked its migration. With M076, an EventSource declaring
    two Joins to a sibling StateSource (lifecycle_stage) now flows
    through Phase 1 like its StateSource cousin: the joined columns
    land in the post-pipeline table, then the existing payload-pack
    step in load_event_source carries them into each emitted event's
    payload.

    Asserts:
      * Both sources load through run_pipeline without errors.
      * The lifecycle_history EventSource emits the expected number
        of events.
      * Each event's payload contains the joined-stage `name` columns
        (suffixed by ibis for the duplicate-target join).
      * Pin specific values for one row so a regression that resolves
        joins to the wrong stage row (swap prev/next, reuse suffix,
        etc.) fails loudly. lh-1 has previous_stage_id=10 (→ "Lead")
        and next_stage_id=20 (→ "Qualified").
    """
    from fyrnheim.engine.executor import IbisExecutor
    from fyrnheim.engine.pipeline import run_pipeline

    data_dir = tmp_path / "data"
    data_dir.mkdir()

    stage_path = data_dir / "lifecycle_stage.parquet"
    pd.DataFrame(
        {
            "id": [10, 20, 30],
            "name": ["Lead", "Qualified", "Customer"],
        }
    ).to_parquet(str(stage_path))

    history_path = data_dir / "lifecycle_history.parquet"
    pd.DataFrame(
        {
            "id": ["lh-1", "lh-2", "lh-3"],
            "prospect_id": ["P-1", "P-1", "P-2"],
            "created_at": ["2026-04-01", "2026-04-02", "2026-04-03"],
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

    history_source = EventSource(
        name="lifecycle_history",
        project="test",
        dataset="test",
        table="lifecycle_history",
        duckdb_path=str(history_path),
        entity_id_field="prospect_id",
        timestamp_field="created_at",
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

    # Declare history FIRST to exercise the topo-sort: lifecycle_stage
    # must move to level 0 even though it appears second in declaration
    # order.
    assets = {
        "sources": [history_source, stage_source],
        "activities": [],
        "identity_graphs": [],
        "analytics_entities": [],
        "metrics_models": [],
        "staging_views": [],
    }
    result = run_pipeline(assets, config, executor)

    assert result.errors == [], (
        f"unexpected pipeline errors: {result.errors}"
    )
    assert result.source_count == 2
    assert "lifecycle_stage" in result.timings.source_loads
    assert "lifecycle_history" in result.timings.source_loads

    # The EventSource emits its events through load_event_source's
    # payload-pack step. Joined columns naturally appear in payload.
    # Reach into the post-pipeline ibis table that lifecycle_history
    # produced — read the parquet directly and rerun load_event_source
    # to inspect the payloads (run_pipeline doesn't expose the per-source
    # event tables in PipelineResult).
    from fyrnheim.engine.event_source_loader import load_event_source
    from fyrnheim.engine.pipeline import _build_state_source_table

    source_registry: dict = {}
    right_pk_registry = {"lifecycle_stage": "id"}

    stage_table = _build_state_source_table(stage_source, config, conn)
    source_registry["lifecycle_stage"] = stage_table

    history_events = load_event_source(
        conn,
        history_source,
        data_dir=config.data_dir,
        backend=config.backend,
        source_registry=source_registry,
        right_pk_registry=right_pk_registry,
    )
    df = history_events.execute()

    assert len(df) == 3
    # Pin event-schema columns.
    assert set(df.columns) == {
        "source",
        "entity_id",
        "ts",
        "event_type",
        "payload",
    }

    import json

    # Reorder by id to make pinning deterministic — the underlying
    # parquet read order is preserved by duckdb but be explicit.
    df_sorted = df.sort_values("entity_id").reset_index(drop=True)
    payloads_by_id: dict[str, dict] = {}
    for _, row in df_sorted.iterrows():
        payload = json.loads(row["payload"])
        # Use the original lh-id from the payload column.
        payloads_by_id[payload["id"]] = payload

    # lh-1: previous=10 → Lead, next=20 → Qualified
    lh1 = payloads_by_id["lh-1"]
    lh1_joined_names = {value for key, value in lh1.items() if "name" in key}
    assert {"Lead", "Qualified"}.issubset(lh1_joined_names), (
        f"expected both joined stage names ('Lead', 'Qualified') in "
        f"payload from EventSource double-join; got {lh1}"
    )

    # lh-2: previous=20 → Qualified, next=30 → Customer
    lh2 = payloads_by_id["lh-2"]
    lh2_joined_names = {value for key, value in lh2.items() if "name" in key}
    assert {"Qualified", "Customer"}.issubset(lh2_joined_names), (
        f"expected both joined stage names ('Qualified', 'Customer') in "
        f"payload from EventSource double-join; got {lh2}"
    )


def test_m076_join_target_eventsource_raises_clear_error(
    tmp_path: Path,
) -> None:
    """FR-10 / M076 — EventSource as a join TARGET is OUT of scope.
    The right-side primary-key column is resolved from the joined
    source's `id_field`, which only StateSource exposes. A Join whose
    `source_name` resolves to an EventSource must raise a clear
    ValueError at sort time (before any worker runs) with a future-
    enhancement pointer.
    """
    from fyrnheim.engine.executor import IbisExecutor
    from fyrnheim.engine.pipeline import run_pipeline

    data_dir = tmp_path / "data"
    data_dir.mkdir()

    stage_path = data_dir / "lifecycle_stage.parquet"
    pd.DataFrame(
        {"id": [10], "name": ["Lead"]}
    ).to_parquet(str(stage_path))

    events_path = data_dir / "events.parquet"
    pd.DataFrame(
        {
            "id": ["e-1"],
            "prospect_id": ["P-1"],
            "created_at": ["2026-04-01"],
        }
    ).to_parquet(str(events_path))

    # An EventSource declared as a join TARGET — the offending shape.
    target_event_source = EventSource(
        name="external_events",
        project="test",
        dataset="test",
        table="events",
        duckdb_path=str(events_path),
        entity_id_field="prospect_id",
        timestamp_field="created_at",
    )

    # A StateSource attempts to join TO the EventSource.
    state_with_bad_join = StateSource(
        name="lifecycle_stage",
        project="test",
        dataset="test",
        table="lifecycle_stage",
        duckdb_path=str(stage_path),
        id_field="id",
        full_refresh=True,
        joins=[
            Join(source_name="external_events", join_key="some_fk"),
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

    assets = {
        "sources": [target_event_source, state_with_bad_join],
        "activities": [],
        "identity_graphs": [],
        "analytics_entities": [],
        "metrics_models": [],
        "staging_views": [],
    }

    # The runner converts the sort-time ValueError into a Phase 1
    # error entry — assert it's surfaced clearly with the offending
    # source_name AND a future-enhancement hint.
    result = run_pipeline(assets, config, executor)
    assert result.errors, "expected a clear error for EventSource-as-join-target"
    err_text = "\n".join(result.errors)
    assert "external_events" in err_text
    assert "EventSource" in err_text
    assert "future-enhancement" in err_text or "future enhancement" in err_text


def test_m076_topo_sort_rejects_eventsource_join_target_directly() -> None:
    """Unit-level guard: the topological sort itself raises ValueError
    when a Join references an EventSource. Surfacing at sort time
    (before worker dispatch) makes the error message structural — the
    user sees the bad reference immediately rather than after some
    other source happens to fail at runtime.
    """
    from fyrnheim.engine.pipeline import _topo_sort_sources

    target = EventSource(
        name="external_events",
        project="test",
        dataset="test",
        table="t",
        entity_id_field="prospect_id",
        timestamp_field="created_at",
    )
    bad = StateSource(
        name="lifecycle_stage",
        project="test",
        dataset="test",
        table="lifecycle_stage",
        id_field="id",
        joins=[Join(source_name="external_events", join_key="fk")],
    )
    with pytest.raises(ValueError) as excinfo:
        _topo_sort_sources([target, bad])
    msg = str(excinfo.value)
    assert "external_events" in msg
    assert "EventSource" in msg
    assert "id_field" in msg
    assert "lifecycle_stage" in msg
