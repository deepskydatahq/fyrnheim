"""Tests verifying README includes fyr test documentation."""

from pathlib import Path

_README = (Path(__file__).resolve().parent.parent / "README.md").read_text()


class TestReadmeFyrTest:
    def test_readme_includes_fyr_test_command(self):
        assert "fyr test" in _README

    def test_readme_has_testing_section(self):
        assert "## Testing" in _README

    def test_readme_has_entity_test_example(self):
        assert "EntityTest" in _README
        assert "from fyrnheim.testing import EntityTest" in _README

    def test_readme_shows_given_run_assert_workflow(self):
        assert ".given(" in _README
        assert ".run()" in _README
        assert "assert result.row_count" in _README
