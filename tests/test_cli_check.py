"""Tests for fyr check command."""


import ibis
from click.testing import CliRunner

from fyrnheim.cli import main

ENTITY_WITH_QUALITY = """\
from fyrnheim import (
    Entity, LayersConfig, DimensionLayer, PrepLayer, TableSource,
    QualityConfig, NotNull, Unique, InRange,
)

entity = Entity(
    name="customers",
    description="Test entity",
    source=TableSource(project="p", dataset="d", table="customers", duckdb_path="customers.parquet"),
    layers=LayersConfig(
        prep=PrepLayer(model_name="prep_customers"),
        dimension=DimensionLayer(model_name="dim_customers"),
    ),
    quality=QualityConfig(
        primary_key="id",
        checks=[
            NotNull("email"),
            Unique("id"),
            InRange("amount_cents", min=0),
        ],
    ),
)
"""

ENTITY_NO_QUALITY = """\
from fyrnheim import Entity, LayersConfig, PrepLayer, TableSource

entity = Entity(
    name="simple",
    description="Test entity without quality",
    source=TableSource(project="p", dataset="d", table="simple"),
    layers=LayersConfig(prep=PrepLayer(model_name="prep_simple")),
)
"""


def _setup_check_env(tmp_path, *, with_table=True, with_quality=True, null_email=False):
    """Create entities dir, fyrnheim.yaml, and optionally a DuckDB with data."""
    entities_dir = tmp_path / "entities"
    entities_dir.mkdir()

    if with_quality:
        (entities_dir / "customers.py").write_text(ENTITY_WITH_QUALITY)
    else:
        (entities_dir / "simple.py").write_text(ENTITY_NO_QUALITY)

    db_path = tmp_path / "fyrnheim.duckdb"
    if with_table:
        conn = ibis.duckdb.connect(str(db_path))
        if null_email:
            conn.raw_sql("""
                CREATE TABLE dim_customers AS
                SELECT 1 as id, NULL as email, 500 as amount_cents
                UNION ALL
                SELECT 2, 'bob@example.com', 1000
            """)
        else:
            conn.raw_sql("""
                CREATE TABLE dim_customers AS
                SELECT 1 as id, 'alice@example.com' as email, 500 as amount_cents
                UNION ALL
                SELECT 2, 'bob@example.com' as email, 1000 as amount_cents
            """)
        conn.disconnect()

    (tmp_path / "fyrnheim.yaml").write_text(
        f"entities_dir: {entities_dir}\noutput_dir: {tmp_path / 'generated'}\n"
    )
    return tmp_path


class TestCheckAllPass:
    def test_all_pass(self, tmp_path, monkeypatch):
        _setup_check_env(tmp_path)
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(main, ["check", "--db-path", str(tmp_path / "fyrnheim.duckdb")])
        assert result.exit_code == 0
        assert "passed" in result.output
        assert "0 failed" in result.output

    def test_per_check_output(self, tmp_path, monkeypatch):
        _setup_check_env(tmp_path)
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(main, ["check", "--db-path", str(tmp_path / "fyrnheim.duckdb")])
        assert "customers:" in result.output
        assert "pass" in result.output


class TestCheckFailures:
    def test_check_failures_exit_2(self, tmp_path, monkeypatch):
        _setup_check_env(tmp_path, null_email=True)
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(main, ["check", "--db-path", str(tmp_path / "fyrnheim.duckdb")])
        assert result.exit_code == 2
        assert "FAIL" in result.output


class TestCheckEntityFlag:
    def test_entity_flag(self, tmp_path, monkeypatch):
        _setup_check_env(tmp_path)
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(
            main, ["check", "--entity", "customers", "--db-path", str(tmp_path / "fyrnheim.duckdb")]
        )
        assert result.exit_code == 0
        assert "customers:" in result.output

    def test_entity_not_found(self, tmp_path, monkeypatch):
        _setup_check_env(tmp_path)
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(main, ["check", "--entity", "nonexistent"])
        assert result.exit_code == 1
        assert "not found" in result.output


class TestCheckMissingTable:
    def test_missing_table(self, tmp_path, monkeypatch):
        _setup_check_env(tmp_path, with_table=False)
        # Create empty DuckDB so connection works
        db_path = tmp_path / "fyrnheim.duckdb"
        conn = ibis.duckdb.connect(str(db_path))
        conn.disconnect()
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(main, ["check", "--db-path", str(db_path)])
        assert result.exit_code == 1
        assert "not found" in result.output
        assert "fyr run" in result.output


class TestCheckNoQuality:
    def test_skipped_no_quality(self, tmp_path, monkeypatch):
        _setup_check_env(tmp_path, with_quality=False, with_table=False)
        db_path = tmp_path / "fyrnheim.duckdb"
        conn = ibis.duckdb.connect(str(db_path))
        conn.disconnect()
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(main, ["check", "--db-path", str(db_path)])
        assert result.exit_code == 0
        assert "skipped" in result.output


class TestCheckEdgeCases:
    def test_missing_entities_dir(self):
        result = CliRunner().invoke(main, ["check", "--entities-dir", "/nonexistent_xyz"])
        assert result.exit_code == 1
        assert "Entities directory not found" in result.output

    def test_summary_line(self, tmp_path, monkeypatch):
        _setup_check_env(tmp_path)
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(main, ["check", "--db-path", str(tmp_path / "fyrnheim.duckdb")])
        assert "Checks:" in result.output
        assert "across" in result.output
        assert "entity" in result.output
