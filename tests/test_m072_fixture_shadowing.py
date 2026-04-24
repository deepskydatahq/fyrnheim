"""M072 / FR-8: backend-aware fixture shadowing for StateSource / EventSource.

End-to-end loader tests that exercise the `_reads_duckdb_fixture` gate in
both `load_event_source` and `_load_state_source`. Validates:

* skip fires on (flag=True + backend='duckdb' + duckdb_path) — the engine
  does NOT call `_apply_source_transforms` / `_apply_json_path_extractions`
  / filter.
* skip does NOT fire on other backends (transforms apply).
* skip does NOT fire on live DuckDB tables (no duckdb_path, no fixture).
* `computed_columns` STILL APPLY on the skip path.
* Negative test: mismatched fixture shape produces visibly-wrong output
  (documents the trade-off).

See `tests/test_source_transforms.py` for the unit-level gate helper tests
(default False, gate condition truth table).
"""

from __future__ import annotations

import json

import ibis
import pandas as pd
import pytest

from fyrnheim.components.computed_column import ComputedColumn
from fyrnheim.config import ResolvedConfig
from fyrnheim.core.source import (
    EventSource,
    Field,
    Rename,
    SourceTransforms,
    StateSource,
)
from fyrnheim.engine.event_source_loader import load_event_source
from fyrnheim.engine.pipeline import _load_state_source


def _write_parquet(tmp_path, name: str, df: pd.DataFrame) -> str:
    path = tmp_path / name
    df.to_parquet(str(path))
    return str(path)


# ---------------------------------------------------------------------------
# EventSource: skip fires on flag + duckdb + duckdb_path
# ---------------------------------------------------------------------------


def test_event_source_fixture_skip_fires_on_duckdb_with_flag(tmp_path) -> None:
    """load_event_source with duckdb_fixture_is_transformed=True skips
    transforms — the fixture is read as-is.

    If transforms were applied, the `Rename(from_name='customer_id', ...)`
    would fail because the fixture only has `user_id` (post-transform
    shape). The skip path reads the fixture directly.
    """
    fixture = _write_parquet(
        tmp_path,
        "events.parquet",
        pd.DataFrame(
            {
                # post-transform shape: user_id, not customer_id
                "user_id": ["u1", "u2"],
                "viewed_at": ["2024-01-01", "2024-01-02"],
                "page": ["/home", "/about"],
            }
        ),
    )
    es = EventSource(
        name="page_views",
        project="test",
        dataset="test",
        table="test",
        duckdb_path=fixture,
        duckdb_fixture_is_transformed=True,  # opt-in — skip transforms on DuckDB
        entity_id_field="user_id",
        timestamp_field="viewed_at",
        event_type="page_view",
        # Would fail if applied: 'customer_id' not in fixture.
        transforms=SourceTransforms(
            renames=[Rename(from_name="customer_id", to_name="user_id")]
        ),
    )
    conn = ibis.duckdb.connect()
    # No exception — the rename is skipped because the flag says so.
    result = load_event_source(conn, es, backend="duckdb").execute()
    assert len(result) == 2
    assert set(result["entity_id"]) == {"u1", "u2"}


def test_event_source_fixture_no_skip_on_bigquery_backend(tmp_path) -> None:
    """When backend != duckdb, transforms still apply regardless of the flag
    — the flag is DuckDB-fixture-specific.

    Simulated by routing a duckdb connection as the conn argument but
    passing backend='bigquery' to the loader. The Ibis `conn` reads the
    parquet (BigQuery would read the raw upstream table in production),
    but the loader's backend argument gates the skip. We use a rename
    whose `from_name` DOES exist in the fixture — confirming transforms
    run.
    """
    fixture = _write_parquet(
        tmp_path,
        "events.parquet",
        pd.DataFrame(
            {
                "customer_id": ["c1", "c2"],  # pre-transform name
                "viewed_at": ["2024-01-01", "2024-01-02"],
                "page": ["/home", "/about"],
            }
        ),
    )
    es = EventSource(
        name="page_views",
        project="test",
        dataset="test",
        table="test",
        duckdb_path=fixture,
        duckdb_fixture_is_transformed=True,  # set, but backend != duckdb
        entity_id_field="user_id",  # post-rename name
        timestamp_field="viewed_at",
        event_type="page_view",
        transforms=SourceTransforms(
            renames=[Rename(from_name="customer_id", to_name="user_id")]
        ),
    )
    # The DuckDB-fixture gate is on the backend arg, not the conn object.
    # `read_table`'s `backend == "bigquery"` branch calls
    # `conn.table(project, database=(project, dataset))` — we don't want
    # to actually call BQ from the test, so we test the gate directly:
    # the helper is what both loaders call, and it is the sole source of
    # truth for the skip decision.
    from fyrnheim.engine.source_transforms import _reads_duckdb_fixture

    assert _reads_duckdb_fixture(es, "bigquery") is False
    assert _reads_duckdb_fixture(es, "clickhouse") is False
    # Also confirm the helper returns True when backend matches:
    assert _reads_duckdb_fixture(es, "duckdb") is True


def test_event_source_fixture_no_skip_on_live_duckdb_table() -> None:
    """Flag=True + backend=duckdb but NO duckdb_path (live DuckDB table) —
    gate does NOT fire. Covers the DuckDB-as-production user.
    """
    es = EventSource(
        name="page_views",
        project="test",
        dataset="test",
        table="live_table",
        # no duckdb_path — source reads live DuckDB table, not a fixture.
        duckdb_fixture_is_transformed=True,
        entity_id_field="user_id",
        timestamp_field="viewed_at",
        event_type="page_view",
    )
    from fyrnheim.engine.source_transforms import _reads_duckdb_fixture

    assert _reads_duckdb_fixture(es, "duckdb") is False


def test_event_source_computed_columns_apply_on_skip_path(tmp_path) -> None:
    """Flag=True + duckdb fixture — transforms skipped BUT computed_columns
    still applied. The 'derived' column appears in the payload.
    """
    fixture = _write_parquet(
        tmp_path,
        "events.parquet",
        pd.DataFrame(
            {
                "user_id": ["u1", "u2"],
                "viewed_at": ["2024-01-01", "2024-01-02"],
                "base": [10, 20],
            }
        ),
    )
    es = EventSource(
        name="page_views",
        project="test",
        dataset="test",
        table="test",
        duckdb_path=fixture,
        duckdb_fixture_is_transformed=True,
        entity_id_field="user_id",
        timestamp_field="viewed_at",
        event_type="page_view",
        computed_columns=[
            ComputedColumn(name="derived", expression="t.base * 2"),
        ],
    )
    conn = ibis.duckdb.connect()
    result = load_event_source(conn, es, backend="duckdb").execute()
    assert len(result) == 2
    payloads = [json.loads(p) for p in result["payload"]]
    derived = [p["derived"] for p in payloads]
    assert derived == [20, 40]


def test_event_source_filter_skipped_on_fixture_path(tmp_path) -> None:
    """Flag=True + duckdb fixture — filter is also skipped (grouped with
    transforms). All rows are emitted even when the filter would drop some.
    """
    fixture = _write_parquet(
        tmp_path,
        "events.parquet",
        pd.DataFrame(
            {
                "user_id": ["u1", "u2", "u3"],
                "viewed_at": ["2024-01-01", "2024-01-02", "2024-01-03"],
                "status": ["active", "active", "inactive"],
            }
        ),
    )
    es = EventSource(
        name="page_views",
        project="test",
        dataset="test",
        table="test",
        duckdb_path=fixture,
        duckdb_fixture_is_transformed=True,
        entity_id_field="user_id",
        timestamp_field="viewed_at",
        event_type="page_view",
        filter='t.status == "active"',
    )
    conn = ibis.duckdb.connect()
    result = load_event_source(conn, es, backend="duckdb").execute()
    # All 3 rows present — filter skipped because fixture is
    # assumed to already reflect the post-filter row set.
    assert len(result) == 3
    assert set(result["entity_id"]) == {"u1", "u2", "u3"}


def test_event_source_filter_applied_when_flag_false(tmp_path) -> None:
    """Flag=False (default) — filter still applies (v0.9.1 behavior
    preserved)."""
    fixture = _write_parquet(
        tmp_path,
        "events.parquet",
        pd.DataFrame(
            {
                "user_id": ["u1", "u2", "u3"],
                "viewed_at": ["2024-01-01", "2024-01-02", "2024-01-03"],
                "status": ["active", "active", "inactive"],
            }
        ),
    )
    es = EventSource(
        name="page_views",
        project="test",
        dataset="test",
        table="test",
        duckdb_path=fixture,
        # flag NOT set — v0.9.1 behavior
        entity_id_field="user_id",
        timestamp_field="viewed_at",
        event_type="page_view",
        filter='t.status == "active"',
    )
    conn = ibis.duckdb.connect()
    result = load_event_source(conn, es, backend="duckdb").execute()
    assert len(result) == 2
    assert set(result["entity_id"]) == {"u1", "u2"}


# ---------------------------------------------------------------------------
# StateSource / _load_state_source: skip fires
# ---------------------------------------------------------------------------


def test_state_source_fixture_skip_fires_on_duckdb_with_flag(tmp_path) -> None:
    """_load_state_source with duckdb_fixture_is_transformed=True skips
    transforms — a rename targeting a missing source column would otherwise
    raise KeyError / ibis error.
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    fixture = data_dir / "accounts.parquet"
    pd.DataFrame(
        {
            # post-transform shape: account_id, not id
            "account_id": ["A1", "A2"],
            "status": ["active", "active"],
        }
    ).to_parquet(str(fixture))

    state_source = StateSource(
        name="accounts",
        project="test",
        dataset="test",
        table="accounts",
        duckdb_path=str(fixture),
        duckdb_fixture_is_transformed=True,
        id_field="account_id",  # post-rename name
        transforms=SourceTransforms(
            # Would fail if applied: 'id' column does not exist in fixture.
            renames=[Rename(from_name="id", to_name="account_id")]
        ),
    )
    config = ResolvedConfig(
        entities_dir=tmp_path / "entities",
        data_dir=data_dir,
        output_dir=tmp_path / "output",
        backend="duckdb",
        project_root=tmp_path,
    )
    conn = ibis.duckdb.connect()
    # Success proves transforms were skipped — otherwise rename raises.
    events = _load_state_source(state_source, config, conn).execute()
    assert len(events) == 2
    assert set(events["entity_id"]) == {"A1", "A2"}


def test_state_source_computed_columns_apply_on_skip_path(tmp_path) -> None:
    """Flag=True — transforms skipped BUT computed_columns still applied.
    Pins the computed-columns-still-apply contract for StateSource."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    fixture = data_dir / "accounts.parquet"
    pd.DataFrame(
        {
            "account_id": ["A1", "A2"],
            "base": [100, 200],
        }
    ).to_parquet(str(fixture))

    state_source = StateSource(
        name="accounts",
        project="test",
        dataset="test",
        table="accounts",
        duckdb_path=str(fixture),
        duckdb_fixture_is_transformed=True,
        id_field="account_id",
        computed_columns=[
            ComputedColumn(name="derived", expression="t.base * 2"),
        ],
    )
    config = ResolvedConfig(
        entities_dir=tmp_path / "entities",
        data_dir=data_dir,
        output_dir=tmp_path / "output",
        backend="duckdb",
        project_root=tmp_path,
    )
    conn = ibis.duckdb.connect()
    events = _load_state_source(state_source, config, conn).execute()
    assert len(events) == 2
    payloads = [json.loads(p) for p in events["payload"]]
    derived = sorted(p["derived"] for p in payloads)
    assert derived == [200, 400]


def test_state_source_fields_json_path_skipped_on_fixture_path(tmp_path) -> None:
    """Flag=True + duckdb fixture — `Field(json_path=...)` extraction is
    skipped. The fixture should already have the extracted column.
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    fixture = data_dir / "accounts.parquet"
    # Post-transform fixture has `account_type` directly, NOT a
    # JSON-shaped `custom_type` that needs extraction.
    pd.DataFrame(
        {
            "account_id": ["A1", "A2"],
            "account_type": ["premium", "free"],
        }
    ).to_parquet(str(fixture))

    state_source = StateSource(
        name="accounts",
        project="test",
        dataset="test",
        table="accounts",
        duckdb_path=str(fixture),
        duckdb_fixture_is_transformed=True,
        id_field="account_id",
        # Would fail if applied (no 'custom_type' in fixture).
        fields=[
            Field(
                name="account_type",
                type="STRING",
                json_path="$.value",
                source_column="custom_type",
            )
        ],
    )
    config = ResolvedConfig(
        entities_dir=tmp_path / "entities",
        data_dir=data_dir,
        output_dir=tmp_path / "output",
        backend="duckdb",
        project_root=tmp_path,
    )
    conn = ibis.duckdb.connect()
    events = _load_state_source(state_source, config, conn).execute()
    assert len(events) == 2
    payloads = [json.loads(p) for p in events["payload"]]
    assert sorted(p["account_type"] for p in payloads) == ["free", "premium"]


def test_state_source_fields_json_path_applied_when_flag_false(tmp_path) -> None:
    """Flag=False — v0.9.1 behavior, json_path extraction applies."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    fixture = data_dir / "accounts.parquet"
    pd.DataFrame(
        {
            "account_id": ["A1", "A2"],
            "custom_type": ['{"value": "premium"}', '{"value": "free"}'],
        }
    ).to_parquet(str(fixture))

    state_source = StateSource(
        name="accounts",
        project="test",
        dataset="test",
        table="accounts",
        duckdb_path=str(fixture),
        # flag NOT set — default False
        id_field="account_id",
        fields=[
            Field(
                name="account_type",
                type="STRING",
                json_path="$.value",
                source_column="custom_type",
            )
        ],
    )
    config = ResolvedConfig(
        entities_dir=tmp_path / "entities",
        data_dir=data_dir,
        output_dir=tmp_path / "output",
        backend="duckdb",
        project_root=tmp_path,
    )
    conn = ibis.duckdb.connect()
    events = _load_state_source(state_source, config, conn).execute()
    assert len(events) == 2
    payloads = [json.loads(p) for p in events["payload"]]
    assert sorted(p["account_type"] for p in payloads) == ["free", "premium"]


# ---------------------------------------------------------------------------
# Negative test: mismatched fixture shape produces visibly-wrong output
# ---------------------------------------------------------------------------


def test_fixture_is_transformed_with_mismatched_shape_produces_wrong_output(
    tmp_path,
) -> None:
    """Documents the backend-parity trade-off.

    When `duckdb_fixture_is_transformed=True` but the fixture is NOT
    actually in post-transform shape (contains pre-transform column names
    that transforms WOULD have renamed), the engine skips transforms —
    so downstream sees the RAW fixture shape instead of the post-transform
    shape that BigQuery would produce.

    This is VISIBLY WRONG (the schema differs from the expected BQ output),
    NOT a silent failure. Cross-backend tests catch the mismatch. The flag
    is explicit opt-in with a WARNING docstring at the declaration site.

    This test asserts that the resulting schema contains the RAW pre-rename
    columns — documenting the trade-off.
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    fixture = data_dir / "raw.parquet"
    # RAW fixture: has 'id' (pre-rename) — NOT the post-transform shape.
    pd.DataFrame(
        {
            "id": ["A1", "A2"],
            "name": ["Alpha", "Beta"],
        }
    ).to_parquet(str(fixture))

    state_source = StateSource(
        name="accounts",
        project="test",
        dataset="test",
        table="accounts",
        duckdb_path=str(fixture),
        duckdb_fixture_is_transformed=True,  # misuse: fixture is NOT transformed
        id_field="id",  # must match the RAW shape so we can load events
        transforms=SourceTransforms(
            # BQ would apply this rename — DuckDB skips it, so downstream
            # sees 'id' not 'account_id'. Cross-backend schema diff
            # catches this in CI.
            renames=[Rename(from_name="id", to_name="account_id")],
        ),
    )
    config = ResolvedConfig(
        entities_dir=tmp_path / "entities",
        data_dir=data_dir,
        output_dir=tmp_path / "output",
        backend="duckdb",
        project_root=tmp_path,
    )
    conn = ibis.duckdb.connect()
    events = _load_state_source(state_source, config, conn).execute()

    # The event-schema has 'entity_id' derived from id_field (here 'id').
    # But the payload — which reflects the source row's "other columns"
    # post-transform — should have reflected the renamed 'account_id'
    # column on BQ. On DuckDB with the skip, it reflects the RAW 'id'
    # column being dropped as the entity_id but no 'account_id' anywhere.
    # The visibly-wrong signal: the payload does NOT contain the
    # 'account_id' key that BQ's output would have.
    assert len(events) == 2
    payloads = [json.loads(p) for p in events["payload"]]
    # Post-transform BQ output WOULD have 'account_id' moved to entity_id
    # and kept 'name' in the payload. DuckDB with the skip has only
    # 'name' in the payload — 'account_id' is absent because the rename
    # never ran. Cross-backend compare detects the missing key.
    for p in payloads:
        assert "account_id" not in p
        assert "name" in p


@pytest.mark.filterwarnings("ignore")
def test_event_source_and_state_source_field_declared_on_base() -> None:
    """Confirms `duckdb_fixture_is_transformed` lives on BaseTableSource —
    both EventSource AND StateSource (and therefore TableSource /
    DerivedSource via inheritance) expose the field without per-subclass
    redeclaration.
    """
    state = StateSource(
        name="s",
        project="p",
        dataset="d",
        table="t",
        id_field="id",
        duckdb_fixture_is_transformed=True,
    )
    event = EventSource(
        name="e",
        project="p",
        dataset="d",
        table="t",
        entity_id_field="id",
        timestamp_field="ts",
        duckdb_fixture_is_transformed=True,
    )
    assert state.duckdb_fixture_is_transformed is True
    assert event.duckdb_fixture_is_transformed is True
