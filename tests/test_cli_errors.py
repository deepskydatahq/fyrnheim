"""Tests for CLI error handling and --verbose flag."""

from unittest.mock import patch

from click.testing import CliRunner

from fyrnheim.cli import main

MINIMAL_ENTITY = (
    'from fyrnheim import Entity, LayersConfig, PrepLayer, TableSource\n'
    'entity = Entity(name="test", description="Test entity",'
    ' source=TableSource(project="p", dataset="d", table="t"),'
    ' layers=LayersConfig(prep=PrepLayer(model_name="prep_test")))\n'
)


def _setup_run_project(tmp_path, monkeypatch):
    """Create a project dir with one entity so the run command reaches the runner."""
    monkeypatch.chdir(tmp_path)
    ent_dir = tmp_path / "entities"
    ent_dir.mkdir()
    (ent_dir / "test.py").write_text(MINIMAL_ENTITY)
    (tmp_path / "fyrnheim.yaml").write_text(f"entities_dir: {ent_dir}\n")


class TestErrorHandlerNoVerbose:
    def test_unhandled_error_no_traceback(self, tmp_path, monkeypatch):
        """Unhandled exception prints Error + Hint, no traceback."""
        _setup_run_project(tmp_path, monkeypatch)
        with patch("fyrnheim.engine.runner.run", side_effect=RuntimeError("something broke")):
            result = CliRunner().invoke(main, ["run"])
        assert result.exit_code == 1
        assert "Error: something broke" in result.output
        assert "Hint:" in result.output
        assert "Traceback" not in result.output

    def test_unhandled_error_with_verbose(self, tmp_path, monkeypatch):
        """With --verbose, unhandled exception prints traceback."""
        _setup_run_project(tmp_path, monkeypatch)
        with patch("fyrnheim.engine.runner.run", side_effect=RuntimeError("something broke")):
            result = CliRunner().invoke(main, ["--verbose", "run"])
        assert result.exit_code == 1
        assert "Traceback" in result.output
        assert "RuntimeError" in result.output


class TestErrorHints:
    def _invoke_with_error(self, exc, tmp_path, monkeypatch):
        _setup_run_project(tmp_path, monkeypatch)
        with patch("fyrnheim.engine.runner.run", side_effect=exc):
            return CliRunner().invoke(main, ["run"])

    def test_file_not_found_hint(self, tmp_path, monkeypatch):
        result = self._invoke_with_error(FileNotFoundError("missing.py"), tmp_path, monkeypatch)
        assert "Hint: Check that the path exists" in result.output

    def test_value_error_hint(self, tmp_path, monkeypatch):
        result = self._invoke_with_error(ValueError("bad config"), tmp_path, monkeypatch)
        assert "Hint: Check your fyrnheim.yaml" in result.output

    def test_import_error_hint(self, tmp_path, monkeypatch):
        result = self._invoke_with_error(ImportError("No module named 'duckdb'"), tmp_path, monkeypatch)
        assert "pip install fyrnheim[duckdb]" in result.output

    def test_unknown_error_hint(self, tmp_path, monkeypatch):
        result = self._invoke_with_error(RuntimeError("unknown"), tmp_path, monkeypatch)
        assert "Use --verbose" in result.output


class TestVerboseFlag:
    def test_verbose_flag_accepted(self):
        result = CliRunner().invoke(main, ["--verbose", "--help"])
        assert result.exit_code == 0

    def test_help_shows_verbose_option(self):
        result = CliRunner().invoke(main, ["--help"])
        assert "--verbose" in result.output or "-v" in result.output
