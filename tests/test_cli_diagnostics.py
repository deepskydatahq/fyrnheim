"""Tests for specific diagnostic messages for all CLI failure modes."""


from click.testing import CliRunner

from fyrnheim.cli import main

VALID_ENTITY = """\
from fyrnheim import Entity, LayersConfig, PrepLayer, TableSource

entity = Entity(
    name="customers",
    description="Test entity",
    source=TableSource(project="p", dataset="d", table="customers", duckdb_path="customers.parquet"),
    layers=LayersConfig(prep=PrepLayer(model_name="prep_customers")),
)
"""


class TestMissingEntitiesDir:
    def test_missing_entities_dir(self, tmp_path, monkeypatch):
        (tmp_path / "fyrnheim.yaml").write_text("entities_dir: nonexistent\n")
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(main, ["list"])
        assert result.exit_code == 1
        assert "Entities directory not found" in result.output


class TestNoEntitiesFound:
    def test_no_entities_run(self, tmp_path, monkeypatch):
        ent_dir = tmp_path / "entities"
        ent_dir.mkdir()
        (tmp_path / "fyrnheim.yaml").write_text(f"entities_dir: {ent_dir}\n")
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(main, ["run"])
        assert result.exit_code == 0
        assert "Nothing to run" in result.output

    def test_no_entities_list(self, tmp_path, monkeypatch):
        ent_dir = tmp_path / "entities"
        ent_dir.mkdir()
        (tmp_path / "fyrnheim.yaml").write_text(f"entities_dir: {ent_dir}\n")
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(main, ["list"])
        assert result.exit_code == 0
        assert "No entities found" in result.output


class TestMissingDataFile:
    def test_missing_data_file(self, tmp_path, monkeypatch):
        """Entity references a parquet file that does not exist."""
        ent_dir = tmp_path / "entities"
        ent_dir.mkdir()
        (ent_dir / "customers.py").write_text(VALID_ENTITY)
        (tmp_path / "data").mkdir()
        # No customers.parquet in data dir
        (tmp_path / "fyrnheim.yaml").write_text(
            f"entities_dir: {ent_dir}\ndata_dir: {tmp_path / 'data'}\n"
        )
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(main, ["run"])
        assert result.exit_code == 1
        assert "Data file not found" in result.output
        assert "entity: customers" in result.output


class TestEntitySyntaxError:
    def test_syntax_error(self, tmp_path, monkeypatch):
        ent_dir = tmp_path / "entities"
        ent_dir.mkdir()
        (ent_dir / "bad.py").write_text("def broken(\n")
        (tmp_path / "fyrnheim.yaml").write_text(f"entities_dir: {ent_dir}\n")
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(main, ["list"])
        assert result.exit_code == 1
        assert "Error loading" in result.output
        assert "line" in result.output


class TestDuplicateEntityName:
    def test_duplicate_entity(self, tmp_path, monkeypatch):
        ent_dir = tmp_path / "entities"
        ent_dir.mkdir()
        (ent_dir / "customers1.py").write_text(VALID_ENTITY)
        (ent_dir / "customers2.py").write_text(VALID_ENTITY)
        (tmp_path / "fyrnheim.yaml").write_text(f"entities_dir: {ent_dir}\n")
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(main, ["list"])
        assert result.exit_code == 1
        assert "Duplicate entity name" in result.output


class TestHintMessages:
    def test_source_not_found_hint(self, tmp_path, monkeypatch):
        ent_dir = tmp_path / "entities"
        ent_dir.mkdir()
        (ent_dir / "customers.py").write_text(VALID_ENTITY)
        (tmp_path / "data").mkdir()
        (tmp_path / "fyrnheim.yaml").write_text(
            f"entities_dir: {ent_dir}\ndata_dir: {tmp_path / 'data'}\n"
        )
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(main, ["run"])
        assert "data_dir in fyrnheim.yaml" in result.output or "Data file not found" in result.output
