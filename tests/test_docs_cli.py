"""Tests for fyr docs generate and fyr docs serve CLI commands."""

from unittest.mock import patch

from click.testing import CliRunner

from fyrnheim.cli import main

MINIMAL_ENTITY = """\
from fyrnheim import Entity, LayersConfig, PrepLayer, TableSource

entity = Entity(
    name="{name}",
    description="Test entity for docs",
    source=TableSource(project="p", dataset="d", table="t", duckdb_path="data/t/*.parquet"),
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
    for name in entities or ["test_entity"]:
        (entities_dir / f"{name}.py").write_text(MINIMAL_ENTITY.format(name=name))
    return tmp_path


class TestDocsGenerate:
    def test_creates_index_html(self, tmp_path, monkeypatch):
        _make_project(tmp_path)
        monkeypatch.chdir(tmp_path)
        out_dir = tmp_path / "docs_out"
        result = CliRunner().invoke(main, ["docs", "generate", "--output-dir", str(out_dir)])
        assert result.exit_code == 0
        assert (out_dir / "index.html").is_file()

    def test_index_html_contains_entity_data(self, tmp_path, monkeypatch):
        _make_project(tmp_path, ["customers"])
        monkeypatch.chdir(tmp_path)
        out_dir = tmp_path / "docs_out"
        result = CliRunner().invoke(main, ["docs", "generate", "--output-dir", str(out_dir)])
        assert result.exit_code == 0
        html = (out_dir / "index.html").read_text()
        assert "customers" in html

    def test_output_dir_flag(self, tmp_path, monkeypatch):
        _make_project(tmp_path)
        monkeypatch.chdir(tmp_path)
        custom_dir = tmp_path / "custom_docs"
        result = CliRunner().invoke(main, ["docs", "generate", "--output-dir", str(custom_dir)])
        assert result.exit_code == 0
        assert (custom_dir / "index.html").is_file()
        assert "Documentation written to" in result.output

    def test_default_output_dir(self, tmp_path, monkeypatch):
        _make_project(tmp_path)
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(main, ["docs", "generate"])
        assert result.exit_code == 0
        assert (tmp_path / "docs_site" / "index.html").is_file()

    def test_missing_entities_dir(self):
        result = CliRunner().invoke(main, ["docs", "generate", "--entities-dir", "/nonexistent_xyz"])
        assert result.exit_code == 1
        assert "Entities directory not found" in result.output

    def test_no_entities_exits_cleanly(self, tmp_path, monkeypatch):
        empty_dir = tmp_path / "entities"
        empty_dir.mkdir()
        (tmp_path / "fyrnheim.yaml").write_text(f"entities_dir: {empty_dir}\n")
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(main, ["docs", "generate"])
        assert result.exit_code == 0
        assert "No entities found" in result.output

    def test_discovered_count_message(self, tmp_path, monkeypatch):
        _make_project(tmp_path, ["alpha", "beta"])
        monkeypatch.chdir(tmp_path)
        out_dir = tmp_path / "docs_out"
        result = CliRunner().invoke(main, ["docs", "generate", "--output-dir", str(out_dir)])
        assert result.exit_code == 0
        assert "Discovered 2 entities" in result.output


class TestDocsServe:
    def test_serve_missing_index_html(self, tmp_path):
        result = CliRunner().invoke(main, ["docs", "serve", "--output-dir", str(tmp_path)])
        assert result.exit_code == 1
        assert "not found" in result.output
        assert "fyr docs generate" in result.output

    def test_serve_port_flag_accepted(self):
        result = CliRunner().invoke(main, ["docs", "serve", "--help"])
        assert "--port" in result.output
        assert result.exit_code == 0

    @patch("http.server.HTTPServer")
    @patch("webbrowser.open")
    def test_serve_starts_server(self, mock_browser, mock_server_cls, tmp_path):
        # Create a fake index.html
        (tmp_path / "index.html").write_text("<html></html>")

        # Make serve_forever raise KeyboardInterrupt to exit
        mock_server = mock_server_cls.return_value
        mock_server.serve_forever.side_effect = KeyboardInterrupt

        result = CliRunner().invoke(main, ["docs", "serve", "--output-dir", str(tmp_path)])
        # Exit code 0 because KeyboardInterrupt is handled gracefully
        assert result.exit_code == 0
        assert "Serving docs at" in result.output
        mock_server_cls.assert_called_once()
        mock_server.server_close.assert_called_once()

    @patch("http.server.HTTPServer")
    @patch("webbrowser.open")
    def test_serve_custom_port(self, mock_browser, mock_server_cls, tmp_path):
        (tmp_path / "index.html").write_text("<html></html>")
        mock_server = mock_server_cls.return_value
        mock_server.serve_forever.side_effect = KeyboardInterrupt

        result = CliRunner().invoke(main, ["docs", "serve", "--port", "9090", "--output-dir", str(tmp_path)])
        assert result.exit_code == 0
        assert "localhost:9090" in result.output
        # Verify the server was bound to the right port
        call_args = mock_server_cls.call_args
        assert call_args[0][0] == ("localhost", 9090)


class TestDocsHelpIntegration:
    def test_docs_group_in_main_help(self):
        result = CliRunner().invoke(main, ["--help"])
        assert "docs" in result.output

    def test_docs_help_lists_subcommands(self):
        result = CliRunner().invoke(main, ["docs", "--help"])
        assert "generate" in result.output
        assert "serve" in result.output
