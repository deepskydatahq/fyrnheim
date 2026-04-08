"""Tests for fyr materialize / drop / list-staging CLI subcommands."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from click.testing import CliRunner

from fyrnheim.cli import main

ENTITIES_SRC = '''
from fyrnheim.core.staging_view import StagingView

stg_a = StagingView(
    name="stg_a",
    project="main",
    dataset="analytics",
    sql="SELECT 1 AS x",
)

stg_b = StagingView(
    name="stg_b",
    project="main",
    dataset="analytics",
    sql="SELECT 2 AS y",
)
'''


@pytest.fixture
def project(tmp_path: Path) -> Path:
    entities_dir = tmp_path / "entities"
    entities_dir.mkdir()
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    (entities_dir / "entities.py").write_text(ENTITIES_SRC)

    db_path = tmp_path / "test.duckdb"
    yaml_content = f"""
entities_dir: {entities_dir}
data_dir: {data_dir}
output_dir: {output_dir}
backend: duckdb
backend_config:
  db_path: {db_path}
"""
    (tmp_path / "fyrnheim.yaml").write_text(yaml_content)
    return tmp_path


def _invoke(project: Path, *args: str):
    runner = CliRunner()
    original = os.getcwd()
    os.chdir(project)
    try:
        return runner.invoke(main, list(args), catch_exceptions=False)
    finally:
        os.chdir(original)


class TestMaterialize:
    def test_materialize_all(self, project: Path) -> None:
        result = _invoke(project, "materialize")
        assert result.exit_code == 0, result.output
        assert "2 materialized" in result.output
        assert "stg_a" in result.output
        assert "stg_b" in result.output

    def test_materialize_single_view(self, project: Path) -> None:
        result = _invoke(project, "materialize", "--view", "stg_a")
        assert result.exit_code == 0, result.output
        assert "1 materialized" in result.output
        assert "stg_a" in result.output

    def test_materialize_unknown_view_errors(self, project: Path) -> None:
        result = _invoke(project, "materialize", "--view", "nope")
        assert result.exit_code != 0
        assert "Unknown staging view" in result.output
        assert "stg_a" in result.output  # lists available

    def test_materialize_twice_skips(self, project: Path) -> None:
        r1 = _invoke(project, "materialize")
        assert r1.exit_code == 0, r1.output
        r2 = _invoke(project, "materialize")
        assert r2.exit_code == 0, r2.output
        assert "0 materialized" in r2.output
        assert "2 skipped" in r2.output


class TestListStaging:
    def test_list_unmaterialized(self, project: Path) -> None:
        result = _invoke(project, "list-staging")
        assert result.exit_code == 0, result.output
        assert "stg_a" in result.output
        assert "stg_b" in result.output
        assert "unmaterialized" in result.output

    def test_list_fresh_after_materialize(self, project: Path) -> None:
        _invoke(project, "materialize")
        result = _invoke(project, "list-staging")
        assert result.exit_code == 0, result.output
        assert "fresh" in result.output


class TestDrop:
    def test_drop_view(self, project: Path) -> None:
        r1 = _invoke(project, "materialize")
        assert r1.exit_code == 0, r1.output
        r2 = _invoke(project, "drop", "--view", "stg_a")
        assert r2.exit_code == 0, r2.output
        assert "Dropped view stg_a" in r2.output

        # list-staging should now show stg_a as unmaterialized again
        r3 = _invoke(project, "list-staging")
        assert r3.exit_code == 0, r3.output
        lines = [line for line in r3.output.splitlines() if "stg_a" in line]
        assert lines, r3.output
        assert "unmaterialized" in lines[0]

    def test_drop_unknown_view_errors(self, project: Path) -> None:
        result = _invoke(project, "drop", "--view", "nope")
        assert result.exit_code != 0
        assert "Unknown staging view" in result.output
