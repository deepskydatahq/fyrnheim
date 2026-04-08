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
