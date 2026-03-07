"""Tests for the fyr test CLI command."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
from click.testing import CliRunner

from fyrnheim.cli import main

_PASSING_TEST = textwrap.dedent("""\
    from fyrnheim import (
        ComputedColumn,
        Entity,
        LayersConfig,
        PrepLayer,
        TableSource,
    )
    from fyrnheim.testing import EntityTest

    _entity = Entity(
        name="widgets",
        description="Sample entity",
        source=TableSource(
            project="test", dataset="raw", table="widgets",
            duckdb_path="widgets.parquet",
        ),
        layers=LayersConfig(
            prep=PrepLayer(
                model_name="prep_widgets",
                computed_columns=[
                    ComputedColumn(
                        name="price_dollars",
                        expression="t.price_cents / 100.0",
                        description="Price in dollars",
                    ),
                ],
            ),
        ),
    )

    class TestWidgets(EntityTest):
        entity = _entity

        def test_row_count(self):
            result = self.given(
                {"source_widgets": [{"id": 1, "name": "W", "price_cents": 100}]}
            ).run()
            assert result.row_count == 1
""")

_FAILING_TEST = textwrap.dedent("""\
    from fyrnheim import (
        ComputedColumn,
        Entity,
        LayersConfig,
        PrepLayer,
        TableSource,
    )
    from fyrnheim.testing import EntityTest

    _entity = Entity(
        name="gadgets",
        description="Sample entity",
        source=TableSource(
            project="test", dataset="raw", table="gadgets",
            duckdb_path="gadgets.parquet",
        ),
        layers=LayersConfig(
            prep=PrepLayer(
                model_name="prep_gadgets",
                computed_columns=[
                    ComputedColumn(
                        name="price_dollars",
                        expression="t.price_cents / 100.0",
                        description="Price in dollars",
                    ),
                ],
            ),
        ),
    )

    class TestGadgets(EntityTest):
        entity = _entity

        def test_should_fail(self):
            result = self.given(
                {"source_gadgets": [{"id": 1, "name": "G", "price_cents": 100}]}
            ).run()
            assert result.row_count == 999, "Expected 999 rows"
""")

_ANOTHER_ENTITY_TEST = textwrap.dedent("""\
    from fyrnheim import (
        ComputedColumn,
        Entity,
        LayersConfig,
        PrepLayer,
        TableSource,
    )
    from fyrnheim.testing import EntityTest

    _entity = Entity(
        name="things",
        description="Another entity",
        source=TableSource(
            project="test", dataset="raw", table="things",
            duckdb_path="things.parquet",
        ),
        layers=LayersConfig(
            prep=PrepLayer(
                model_name="prep_things",
                computed_columns=[
                    ComputedColumn(
                        name="doubled",
                        expression="t.val * 2",
                        description="Doubled",
                    ),
                ],
            ),
        ),
    )

    class TestThings(EntityTest):
        entity = _entity

        def test_basic(self):
            result = self.given(
                {"source_things": [{"id": 1, "val": 5}]}
            ).run()
            assert result.row_count == 1
""")


@pytest.fixture()
def passing_tests_dir(tmp_path: Path) -> Path:
    (tmp_path / "test_widgets.py").write_text(_PASSING_TEST)
    return tmp_path


@pytest.fixture()
def failing_tests_dir(tmp_path: Path) -> Path:
    (tmp_path / "test_gadgets.py").write_text(_FAILING_TEST)
    return tmp_path


@pytest.fixture()
def mixed_tests_dir(tmp_path: Path) -> Path:
    (tmp_path / "test_widgets.py").write_text(_PASSING_TEST)
    (tmp_path / "test_gadgets.py").write_text(_FAILING_TEST)
    return tmp_path


@pytest.fixture()
def multi_entity_dir(tmp_path: Path) -> Path:
    (tmp_path / "test_widgets.py").write_text(_PASSING_TEST)
    (tmp_path / "test_things.py").write_text(_ANOTHER_ENTITY_TEST)
    return tmp_path


class TestFyrTestCommand:
    """Tests for the fyr test CLI command."""

    def test_command_exists(self) -> None:
        """fyr test command exists and is callable."""
        runner = CliRunner()
        result = runner.invoke(main, ["test", "--help"])
        assert result.exit_code == 0
        assert "entity" in result.output.lower()

    def test_all_pass_exit_zero(self, passing_tests_dir: Path) -> None:
        """Exit code 0 when all tests pass."""
        runner = CliRunner()
        result = runner.invoke(main, ["test", "--tests-dir", str(passing_tests_dir)])
        assert result.exit_code == 0

    def test_failure_exit_one(self, failing_tests_dir: Path) -> None:
        """Exit code 1 when any test fails."""
        runner = CliRunner()
        result = runner.invoke(main, ["test", "--tests-dir", str(failing_tests_dir)])
        assert result.exit_code == 1

    def test_output_shows_pass(self, passing_tests_dir: Path) -> None:
        """Output shows test name and PASS status."""
        runner = CliRunner()
        result = runner.invoke(main, ["test", "--tests-dir", str(passing_tests_dir)])
        assert "PASS" in result.output
        assert "TestWidgets.test_row_count" in result.output

    def test_output_shows_fail(self, failing_tests_dir: Path) -> None:
        """Output shows test name and FAIL status."""
        runner = CliRunner()
        result = runner.invoke(main, ["test", "--tests-dir", str(failing_tests_dir)])
        assert "FAIL" in result.output
        assert "TestGadgets.test_should_fail" in result.output

    def test_summary_line(self, mixed_tests_dir: Path) -> None:
        """Summary line shows total passed/failed count."""
        runner = CliRunner()
        result = runner.invoke(main, ["test", "--tests-dir", str(mixed_tests_dir)])
        assert "1 passed" in result.output
        assert "1 failed" in result.output

    def test_entity_filter(self, multi_entity_dir: Path) -> None:
        """fyr test --entity <name> filters tests to that entity."""
        runner = CliRunner()
        result = runner.invoke(
            main, ["test", "--tests-dir", str(multi_entity_dir), "--entity", "widgets"]
        )
        assert result.exit_code == 0
        assert "TestWidgets" in result.output
        assert "TestThings" not in result.output

    def test_nonexistent_tests_dir(self, tmp_path: Path) -> None:
        """Error when tests directory doesn't exist."""
        runner = CliRunner()
        result = runner.invoke(
            main, ["test", "--tests-dir", str(tmp_path / "nonexistent")]
        )
        assert result.exit_code == 1

    def test_empty_tests_dir(self, tmp_path: Path) -> None:
        """No tests found in empty directory."""
        runner = CliRunner()
        result = runner.invoke(main, ["test", "--tests-dir", str(tmp_path)])
        assert result.exit_code == 0
        assert "No entity tests found" in result.output
