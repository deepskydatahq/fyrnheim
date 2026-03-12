"""Tests for fyr list command."""


from click.testing import CliRunner

from fyrnheim.cli import main

MINIMAL_ENTITY = """\
from fyrnheim import Entity, LayersConfig, PrepLayer, TableSource

entity = Entity(
    name="{name}",
    description="Test entity",
    source=TableSource(project="test", dataset="raw", table="{name}"),
    layers=LayersConfig(prep=PrepLayer(model_name="prep_{name}")),
)
"""

HELPER_ENTITY = """\
from fyrnheim.core.entity import HelperEntity
from fyrnheim import LayersConfig, PrepLayer, TableSource

entity = HelperEntity(
    name="{name}",
    description="Helper entity",
    source=TableSource(project="test", dataset="raw", table="{name}"),
    layers=LayersConfig(prep=PrepLayer(model_name="prep_{name}")),
)
"""

ENTITY_WITH_DIM = """\
from fyrnheim import Entity, DimensionLayer, LayersConfig, PrepLayer, TableSource

entity = Entity(
    name="{name}",
    description="Test entity",
    source=TableSource(project="test", dataset="raw", table="{name}"),
    layers=LayersConfig(
        prep=PrepLayer(model_name="prep_{name}"),
        dimension=DimensionLayer(model_name="dim_{name}"),
    ),
)
"""


def _make_project(tmp_path, entities):
    """Create a project dir with fyrnheim.yaml and entity files."""
    entities_dir = tmp_path / "entities"
    entities_dir.mkdir()
    (tmp_path / "fyrnheim.yaml").write_text(
        f"entities_dir: {entities_dir}\n"
    )
    for name, template in entities:
        (entities_dir / f"{name}.py").write_text(template.format(name=name))
    return entities_dir


class TestListDiscoversEntities:
    def test_list_discovers_entities(self, tmp_path, monkeypatch):
        _make_project(tmp_path, [("customers", ENTITY_WITH_DIM)])
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(main, ["list"])
        assert result.exit_code == 0
        assert "customers" in result.output
        assert "prep, dimension" in result.output
        assert "1 entities found" in result.output

    def test_list_empty_directory(self, tmp_path):
        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()
        result = CliRunner().invoke(main, ["list", "--entities-dir", str(entities_dir)])
        assert result.exit_code == 0
        assert "No entities found" in result.output

    def test_list_missing_directory(self):
        result = CliRunner().invoke(main, ["list", "--entities-dir", "/nonexistent_xyz"])
        assert result.exit_code == 1
        assert "Entities directory not found" in result.output


class TestListFlags:
    def test_entities_dir_flag_overrides_config(self, tmp_path, monkeypatch):
        # Config points to config_entities (empty), flag points to flag_entities (has entity)
        config_dir = tmp_path / "config_entities"
        config_dir.mkdir()
        flag_dir = tmp_path / "flag_entities"
        flag_dir.mkdir()
        (flag_dir / "orders.py").write_text(MINIMAL_ENTITY.format(name="orders"))
        (tmp_path / "fyrnheim.yaml").write_text(f"entities_dir: {config_dir}\n")
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(main, ["list", "--entities-dir", str(flag_dir)])
        assert result.exit_code == 0
        assert "orders" in result.output


class TestListOutput:
    def test_shows_layers(self, tmp_path, monkeypatch):
        _make_project(tmp_path, [("customers", ENTITY_WITH_DIM)])
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(main, ["list"])
        assert "prep, dimension" in result.output

    def test_summary_count(self, tmp_path, monkeypatch):
        _make_project(tmp_path, [
            ("customers", MINIMAL_ENTITY),
            ("orders", MINIMAL_ENTITY),
        ])
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(main, ["list"])
        assert result.exit_code == 0
        assert "2 entities found" in result.output


class TestListHelperMarker:
    def test_helper_entity_shows_marker(self, tmp_path, monkeypatch):
        """fyr list output contains '[helper]' for a HelperEntity file."""
        _make_project(tmp_path, [("mapping", HELPER_ENTITY)])
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(main, ["list"])
        assert result.exit_code == 0
        assert "[helper]" in result.output
        assert "mapping" in result.output

    def test_regular_entity_no_marker(self, tmp_path, monkeypatch):
        """fyr list output does NOT contain '[helper]' for a regular Entity file."""
        _make_project(tmp_path, [("customers", MINIMAL_ENTITY)])
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(main, ["list"])
        assert result.exit_code == 0
        assert "customers" in result.output
        assert "[helper]" not in result.output
