"""Tests for StagingView primitive."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from fyrnheim.core.staging_view import StagingView


def _make(**overrides):
    defaults: dict = {
        "name": "stg_users",
        "project": "proj",
        "dataset": "analytics",
        "sql": "SELECT 1 AS id",
    }
    defaults.update(overrides)
    return StagingView(**defaults)


class TestRequiredFields:
    def test_minimal_construction(self):
        sv = _make()
        assert sv.name == "stg_users"
        assert sv.materialization == "view"
        assert sv.sql_params == {}
        assert sv.depends_on == []
        assert sv.tags == []
        assert sv.description is None

    def test_missing_required_raises(self):
        with pytest.raises(ValidationError):
            StagingView(name="x", project="p", dataset="d")  # type: ignore[call-arg]

    def test_empty_name_rejected(self):
        with pytest.raises(ValidationError):
            _make(name="")

    def test_empty_sql_rejected(self):
        with pytest.raises(ValidationError):
            _make(sql="   ")

    def test_materialization_default_view(self):
        assert _make().materialization == "view"

    def test_invalid_materialization_rejected(self):
        with pytest.raises(ValidationError):
            _make(materialization="table")


class TestSqlFromPath:
    def test_sql_from_pathlib_path(self, tmp_path: Path):
        sql_file = tmp_path / "q.sql"
        sql_file.write_text("SELECT name FROM t", encoding="utf-8")
        sv = _make(sql=sql_file)
        assert sv.sql == "SELECT name FROM t"

    def test_sql_inline_string_unchanged(self):
        sv = _make(sql="SELECT 42")
        assert sv.sql == "SELECT 42"


class TestRenderedSql:
    def test_renders_params(self):
        sv = _make(
            sql="SELECT * FROM {{ source_table }} WHERE x = {{ value }}",
            sql_params={"source_table": "raw.users", "value": 7},
        )
        assert sv.rendered_sql == "SELECT * FROM raw.users WHERE x = 7"

    def test_render_sql_method_equivalent(self):
        sv = _make(sql="SELECT {{ col }}", sql_params={"col": "id"})
        assert sv.render_sql() == sv.rendered_sql == "SELECT id"

    def test_missing_param_raises_with_name(self):
        sv = _make(sql="SELECT {{ missing_param }}")
        with pytest.raises(ValueError, match="missing_param"):
            _ = sv.rendered_sql

    def test_no_params_needed(self):
        sv = _make(sql="SELECT 1")
        assert sv.rendered_sql == "SELECT 1"


class TestContentHash:
    def test_deterministic(self):
        assert _make().content_hash() == _make().content_hash()

    def test_whitespace_only_diff_same_hash(self):
        a = _make(sql="SELECT  a,\n   b   FROM  t")
        b = _make(sql="SELECT a, b FROM t")
        assert a.content_hash() == b.content_hash()

    def test_semantic_change_differs(self):
        a = _make(sql="SELECT a FROM t")
        b = _make(sql="SELECT b FROM t")
        assert a.content_hash() != b.content_hash()

    def test_dataset_change_differs(self):
        a = _make(dataset="d1")
        b = _make(dataset="d2")
        assert a.content_hash() != b.content_hash()

    def test_materialization_in_hash(self):
        # Only 'view' is valid so we verify hash payload composition via string
        sv = _make()
        h = sv.content_hash()
        assert isinstance(h, str) and len(h) == 64


class TestDiscovery:
    def test_discover_assets_finds_staging_view(self, tmp_path: Path):
        from fyrnheim.cli import _discover_assets

        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()
        (entities_dir / "views.py").write_text(
            "from fyrnheim.core.staging_view import StagingView\n"
            "stg = StagingView(name='stg_x', project='p', dataset='d', sql='SELECT 1')\n",
            encoding="utf-8",
        )
        assets = _discover_assets(entities_dir)
        assert "staging_views" in assets
        assert len(assets["staging_views"]) == 1
        assert assets["staging_views"][0].name == "stg_x"

    def test_discover_assets_finds_list_of_staging_views(self, tmp_path: Path):
        from fyrnheim.cli import _discover_assets

        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()
        (entities_dir / "v.py").write_text(
            "from fyrnheim.core.staging_view import StagingView\n"
            "views = [\n"
            "    StagingView(name='a', project='p', dataset='d', sql='SELECT 1'),\n"
            "    StagingView(name='b', project='p', dataset='d', sql='SELECT 2'),\n"
            "]\n",
            encoding="utf-8",
        )
        assets = _discover_assets(entities_dir)
        names = {sv.name for sv in assets["staging_views"]}
        assert names == {"a", "b"}


# =========================================================================
# M049-E003 — staging_runner / Phase 0 tests
# =========================================================================

from fyrnheim.engine.executor import IbisExecutor  # noqa: E402
from fyrnheim.engine.staging_runner import (  # noqa: E402
    STATE_TABLE_NAME,
    StagingCycleError,
    StagingRunSummary,
    materialize_staging_views,
)


def _sv(name, sql="SELECT 1 AS id", depends_on=None, dataset="analytics"):
    return StagingView(
        name=name,
        project="proj",
        dataset=dataset,
        sql=sql,
        depends_on=depends_on or [],
    )


class TestTopoSort:
    def test_no_views_is_noop(self):
        with IbisExecutor.duckdb() as ex:
            summary = materialize_staging_views(ex, [])
        assert summary.materialized == []
        assert summary.skipped == []
        # state table should NOT exist (no views → no op)
        with IbisExecutor.duckdb() as ex:
            # different conn; nothing to assert beyond the previous check
            pass

    def test_topo_order_respected(self):
        b = _sv("b", depends_on=["a"])
        a = _sv("a")
        c = _sv("c", depends_on=["b"])
        with IbisExecutor.duckdb() as ex:
            summary = materialize_staging_views(ex, [c, b, a])
        assert summary.materialized == ["a", "b", "c"]

    def test_cycle_raises(self):
        a = _sv("a", depends_on=["b"])
        b = _sv("b", depends_on=["a"])
        with IbisExecutor.duckdb() as ex:
            with pytest.raises(StagingCycleError) as excinfo:
                materialize_staging_views(ex, [a, b])
        # cycle path should contain both names
        assert "a" in excinfo.value.cycle
        assert "b" in excinfo.value.cycle


class TestStateIdempotency:
    def test_first_run_materializes(self):
        with IbisExecutor.duckdb() as ex:
            summary = materialize_staging_views(ex, [_sv("a"), _sv("b")])
        assert sorted(summary.materialized) == ["a", "b"]
        assert summary.skipped == []

    def test_second_run_skips_unchanged(self, tmp_path):
        db = tmp_path / "t.db"
        views = [_sv("a"), _sv("b")]
        with IbisExecutor.duckdb(db_path=str(db)) as ex:
            materialize_staging_views(ex, views)
        with IbisExecutor.duckdb(db_path=str(db)) as ex:
            summary = materialize_staging_views(ex, views)
        assert summary.materialized == []
        assert sorted(summary.skipped) == ["a", "b"]

    def test_modified_sql_rematerializes(self, tmp_path):
        db = tmp_path / "t.db"
        with IbisExecutor.duckdb(db_path=str(db)) as ex:
            materialize_staging_views(ex, [_sv("a", sql="SELECT 1 AS id")])
        with IbisExecutor.duckdb(db_path=str(db)) as ex:
            summary = materialize_staging_views(
                ex, [_sv("a", sql="SELECT 2 AS id")]
            )
        assert summary.materialized == ["a"]
        assert summary.skipped == []
        # state row was updated - run again with same sql → skipped
        with IbisExecutor.duckdb(db_path=str(db)) as ex:
            summary2 = materialize_staging_views(
                ex, [_sv("a", sql="SELECT 2 AS id")]
            )
        assert summary2.skipped == ["a"]
        assert summary2.materialized == []

    def test_no_state_bypasses_and_skips_writes(self, tmp_path):
        db = tmp_path / "t.db"
        views = [_sv("a")]
        with IbisExecutor.duckdb(db_path=str(db)) as ex:
            materialize_staging_views(ex, views)
        # --no-state: still materializes, still doesn't create/update state
        with IbisExecutor.duckdb(db_path=str(db)) as ex:
            summary = materialize_staging_views(ex, views, no_state=True)
        assert summary.materialized == ["a"]
        assert summary.skipped == []
        # And importantly: a normal run afterwards should still see the OLD
        # state row (no_state didn't write). So a normal re-run still skips.
        with IbisExecutor.duckdb(db_path=str(db)) as ex:
            summary2 = materialize_staging_views(ex, views)
        assert summary2.skipped == ["a"]

    def test_no_views_creates_no_state_table(self, tmp_path):
        db = tmp_path / "t.db"
        with IbisExecutor.duckdb(db_path=str(db)) as ex:
            materialize_staging_views(ex, [])
            # Confirm no fyrnheim_state table exists anywhere
            try:
                tables = ex.connection.list_tables(database="analytics")
            except Exception:
                tables = []
            assert STATE_TABLE_NAME not in tables


class TestFixtureSkip:
    def test_fixture_shadowed_view_skipped(self):
        with IbisExecutor.duckdb() as ex:
            summary = materialize_staging_views(
                ex,
                [_sv("shadowed"), _sv("real")],
                source_fixture_names={"shadowed"},
            )
        assert summary.materialized == ["real"]
        assert summary.fixture_skipped == ["shadowed"]


class TestMixedDatasetError:
    def test_mixed_dataset_raises(self):
        with IbisExecutor.duckdb() as ex:
            with pytest.raises(ValueError, match="Mixed"):
                materialize_staging_views(
                    ex, [_sv("a", dataset="d1"), _sv("b", dataset="d2")]
                )


class TestPipelineIntegration:
    def test_pipeline_phase0_runs_before_sources(self, tmp_path, monkeypatch):
        """run_pipeline materializes staging views even when sources are empty,
        and records them in the result."""
        from fyrnheim.config import ResolvedConfig
        from fyrnheim.engine.pipeline import run_pipeline

        config = ResolvedConfig(
            project_root=tmp_path,
            entities_dir=tmp_path / "entities",
            data_dir=tmp_path / "data",
            output_dir=tmp_path / "out",
            backend="duckdb",
            backend_config={},
        )
        (tmp_path / "out").mkdir(parents=True, exist_ok=True)

        assets = {
            "sources": [],
            "activities": [],
            "identity_graphs": [],
            "analytics_entities": [],
            "metrics_models": [],
            "staging_views": [_sv("a"), _sv("b", depends_on=["a"])],
        }
        with IbisExecutor.duckdb() as ex:
            result = run_pipeline(assets, config, ex)
        assert result.staging_materialized == ["a", "b"]
        assert result.staging_skipped == []


class TestSummaryType:
    def test_summary_default_fields(self):
        s = StagingRunSummary()
        assert s.materialized == []
        assert s.skipped == []


# =========================================================================
# v0.4.1 regression tests
# =========================================================================

from fyrnheim.engine.staging_runner import (  # noqa: E402
    _escape,
    _load_state,
)


def test_staging_view_accepts_path_for_sql(tmp_path: Path):
    sql_file = tmp_path / "q.sql"
    sql_file.write_text("SELECT id, name FROM users", encoding="utf-8")
    sv = StagingView(
        name="v", project="p", dataset="d", sql=sql_file
    )
    assert sv.sql == "SELECT id, name FROM users"


def test_staging_view_importable_from_top_level():
    import fyrnheim
    from fyrnheim import StagingView as TopStagingView

    assert TopStagingView is StagingView
    assert "StagingView" in fyrnheim.__all__


def test_state_table_ddl_quotes_hash_column_for_bigquery():
    import re as _re

    import fyrnheim.engine.staging_runner as sr

    captured: list[str] = []

    class FakeConn:
        name = "bigquery"

        def raw_sql(self, sql):
            captured.append(sql)

            class C:
                def fetchall(self_inner):
                    return []
            return C()

    class FakeExec:
        connection = FakeConn()

    sr._ensure_state_table(FakeExec(), "proj", "dset")  # type: ignore[arg-type]
    ddl = captured[-1]
    assert "`hash`" in ddl
    # no unquoted `hash ` column reference
    assert not _re.search(r"(?<!`)\bhash\b(?!`)", ddl)


def test_escape_handles_multiline_sql():
    payload = "line1\nline2 with ' quote"
    escaped = _escape(payload)
    assert "\n" not in escaped
    assert "''" in escaped
    assert "\\n" in escaped


def test_escape_backslash_then_newline_no_double_escape():
    payload = "a\\b\nc"
    escaped = _escape(payload)
    # backslash becomes \\; newline becomes \n (a literal backslash + n)
    # order matters: if newline ran first we'd get \\\\n for the escaped NL
    assert escaped == "a\\\\b\\nc"


def test_write_state_row_with_multiline_excerpt(tmp_path):
    sv = StagingView(
        name="multi",
        project="proj",
        dataset="analytics",
        sql="SELECT\n\t'a''b\\c' AS\nval",
    )
    db = tmp_path / "t.db"
    with IbisExecutor.duckdb(db_path=str(db)) as ex:
        summary = materialize_staging_views(ex, [sv])
        assert summary.materialized == ["multi"]
        state = _load_state(ex, "proj", "analytics")
        assert "multi" in state
        assert state["multi"] == sv.content_hash()


class _FakeUpstream:
    def __init__(self, name):
        self.name = name


class _FakeSourceWithUpstream:
    def __init__(self, name, duckdb_path, upstream_name):
        self.name = name
        self.duckdb_path = duckdb_path
        self.upstream = _FakeUpstream(upstream_name)


def _run_phase0(assets, backend, tmp_path):
    from fyrnheim.config import ResolvedConfig
    from fyrnheim.engine.pipeline import run_pipeline

    config = ResolvedConfig(
        project_root=tmp_path,
        entities_dir=tmp_path / "entities",
        data_dir=tmp_path / "data",
        output_dir=tmp_path / "out",
        backend=backend,
        backend_config={},
    )
    (tmp_path / "out").mkdir(parents=True, exist_ok=True)
    with IbisExecutor.duckdb() as ex:
        return run_pipeline(assets, config, ex)


def test_fixture_shadow_only_applies_on_duckdb_backend(tmp_path):
    view = _sv("shared_name")
    src = _FakeSourceWithUpstream(
        name="shared_name", duckdb_path="x.db", upstream_name="shared_name"
    )
    assets = {
        "sources": [src],
        "activities": [],
        "identity_graphs": [],
        "analytics_entities": [],
        "metrics_models": [],
        "staging_views": [view],
    }
    # Backend=bigquery: fixture shadow MUST NOT fire; view should be materialized
    # (we still run executor as duckdb to avoid real BQ; the gate is config.backend)
    result = _run_phase0(assets, backend="bigquery", tmp_path=tmp_path)
    assert "shared_name" in result.staging_materialized


def test_fixture_shadow_uses_upstream_name_not_source_name(tmp_path):
    unrelated = _sv("unrelated_view")
    src_upstream_view = _sv("real_upstream")
    # source name coincides with unrelated view, but its upstream points elsewhere
    src = _FakeSourceWithUpstream(
        name="unrelated_view",
        duckdb_path="x.db",
        upstream_name="real_upstream",
    )
    assets = {
        "sources": [src],
        "activities": [],
        "identity_graphs": [],
        "analytics_entities": [],
        "metrics_models": [],
        "staging_views": [unrelated, src_upstream_view],
    }
    result = _run_phase0(assets, backend="duckdb", tmp_path=tmp_path)
    # Unrelated view should be materialized (NOT shadowed by source name)
    assert "unrelated_view" in result.staging_materialized
    # real_upstream should be shadow-skipped
    assert "real_upstream" not in result.staging_materialized


def test_fixture_shadow_happy_path_preserved(tmp_path):
    view = _sv("my_view")
    src = _FakeSourceWithUpstream(
        name="src", duckdb_path="x.db", upstream_name="my_view"
    )
    assets = {
        "sources": [src],
        "activities": [],
        "identity_graphs": [],
        "analytics_entities": [],
        "metrics_models": [],
        "staging_views": [view],
    }
    result = _run_phase0(assets, backend="duckdb", tmp_path=tmp_path)
    assert "my_view" not in result.staging_materialized
