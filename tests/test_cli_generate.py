"""Tests for fyr generate command."""


from click.testing import CliRunner

from fyrnheim.cli import main

MINIMAL_ENTITY = """\
from fyrnheim import Entity, LayersConfig, PrepLayer, TableSource

entity = Entity(
    name="{name}",
    description="Test entity",
    source=TableSource(project="p", dataset="d", table="t"),
    layers=LayersConfig(prep=PrepLayer(model_name="prep_{name}")),
)
"""


def _make_project(tmp_path, entities=None):
    """Create a minimal project with fyrnheim.yaml and entity files."""
    entities_dir = tmp_path / "entities"
    entities_dir.mkdir()
    (tmp_path / "generated").mkdir()
    (tmp_path / "fyrnheim.yaml").write_text(
        f"entities_dir: {entities_dir}\noutput_dir: {tmp_path / 'generated'}\n"
    )
    for name in (entities or ["test_entity"]):
        (entities_dir / f"{name}.py").write_text(MINIMAL_ENTITY.format(name=name))
    return tmp_path


class TestGenerateWritesFile:
    def test_generates_transform_file(self, tmp_path, monkeypatch):
        _make_project(tmp_path)
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(main, ["generate"])
        assert result.exit_code == 0
        assert "written" in result.output
        assert (tmp_path / "generated" / "test_entity_transforms.py").is_file()

    def test_unchanged_on_rerun(self, tmp_path, monkeypatch):
        _make_project(tmp_path)
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(main, ["generate"])
        result = CliRunner().invoke(main, ["generate"])
        assert result.exit_code == 0
        assert "unchanged" in result.output


class TestGenerateDryRun:
    def test_dry_run_no_write(self, tmp_path, monkeypatch):
        _make_project(tmp_path)
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(main, ["generate", "--dry-run"])
        assert result.exit_code == 0
        assert not (tmp_path / "generated" / "test_entity_transforms.py").exists()
        assert "dry-run" in result.output

    def test_dry_run_label(self, tmp_path, monkeypatch):
        _make_project(tmp_path)
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(main, ["generate", "--dry-run"])
        assert "Dry run" in result.output


class TestGenerateSummary:
    def test_summary_counts(self, tmp_path, monkeypatch):
        _make_project(tmp_path, ["alpha", "beta"])
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(main, ["generate"])
        assert result.exit_code == 0
        assert "2 written" in result.output


class TestGenerateFlags:
    def test_entities_dir_flag(self, tmp_path, monkeypatch):
        alt_dir = tmp_path / "alt_entities"
        alt_dir.mkdir()
        (alt_dir / "custom.py").write_text(MINIMAL_ENTITY.format(name="custom"))
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(main, ["generate", "--entities-dir", str(alt_dir)])
        assert result.exit_code == 0
        assert "custom" in result.output

    def test_output_dir_flag(self, tmp_path, monkeypatch):
        _make_project(tmp_path)
        alt_out = tmp_path / "alt_output"
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(
            main, ["generate", "--output-dir", str(alt_out)]
        )
        assert result.exit_code == 0
        assert (alt_out / "test_entity_transforms.py").is_file()


class TestGenerateErrors:
    def test_missing_entities_dir(self):
        result = CliRunner().invoke(main, ["generate", "--entities-dir", "/nonexistent_xyz"])
        assert result.exit_code == 1
        assert "Entities directory not found" in result.output

    def test_no_entities(self, tmp_path, monkeypatch):
        empty_dir = tmp_path / "entities"
        empty_dir.mkdir()
        (tmp_path / "fyrnheim.yaml").write_text(f"entities_dir: {empty_dir}\n")
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(main, ["generate"])
        assert result.exit_code == 0
        assert "No entities found" in result.output
