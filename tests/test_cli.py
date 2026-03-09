"""Tests for the fyr CLI."""

from click.testing import CliRunner

from fyrnheim.cli import main


class TestCLIGroup:
    def test_main_is_click_group(self):
        import click

        assert isinstance(main, click.Group)

    def test_help_exits_zero(self):
        result = CliRunner().invoke(main, ["--help"])
        assert result.exit_code == 0

    def test_help_lists_all_commands(self):
        result = CliRunner().invoke(main, ["--help"])
        for cmd in ("init", "generate", "run", "check", "list", "test", "docs"):
            assert cmd in result.output

    def test_version(self):
        from fyrnheim import __version__

        result = CliRunner().invoke(main, ["--version"])
        assert result.exit_code == 0
        assert __version__ in result.output
        assert "fyr" in result.output
