"""Unit tests for fyrnheim.testing.runner module (test discovery and execution)."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from fyrnheim.testing.runner import TestOutcome, discover_tests, run_tests


@pytest.fixture()
def test_dir(tmp_path: Path) -> Path:
    """Create a temp directory with test_*.py files containing EntityTest subclasses."""
    # Write a valid entity test file
    (tmp_path / "test_sample.py").write_text(
        textwrap.dedent("""\
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
            description="Sample entity for runner tests",
            source=TableSource(
                project="test",
                dataset="raw",
                table="widgets",
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

            def test_column_values(self):
                result = self.given(
                    {"source_widgets": [{"id": 1, "name": "W", "price_cents": 500}]}
                ).run()
                assert result.column("price_dollars") == [5.0]
        """)
    )
    return tmp_path


@pytest.fixture()
def test_dir_with_failure(tmp_path: Path) -> Path:
    """Create a temp directory with a test file that has a failing test."""
    (tmp_path / "test_failing.py").write_text(
        textwrap.dedent("""\
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
            description="Entity for failure testing",
            source=TableSource(
                project="test",
                dataset="raw",
                table="gadgets",
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

            def test_passes(self):
                result = self.given(
                    {"source_gadgets": [{"id": 1, "name": "G", "price_cents": 100}]}
                ).run()
                assert result.row_count == 1

            def test_fails(self):
                result = self.given(
                    {"source_gadgets": [{"id": 1, "name": "G", "price_cents": 100}]}
                ).run()
                assert result.row_count == 999, "Expected 999 rows"
        """)
    )
    return tmp_path


@pytest.fixture()
def test_dir_two_entities(tmp_path: Path) -> Path:
    """Create a temp directory with test files for two different entities."""
    (tmp_path / "test_alpha.py").write_text(
        textwrap.dedent("""\
        from fyrnheim import (
            ComputedColumn,
            Entity,
            LayersConfig,
            PrepLayer,
            TableSource,
        )
        from fyrnheim.testing import EntityTest

        _entity = Entity(
            name="alpha",
            description="Alpha entity",
            source=TableSource(
                project="test", dataset="raw", table="alpha",
                duckdb_path="alpha.parquet",
            ),
            layers=LayersConfig(
                prep=PrepLayer(
                    model_name="prep_alpha",
                    computed_columns=[
                        ComputedColumn(
                            name="val_doubled",
                            expression="t.val * 2",
                            description="Doubled value",
                        ),
                    ],
                ),
            ),
        )

        class TestAlpha(EntityTest):
            entity = _entity

            def test_basic(self):
                result = self.given(
                    {"source_alpha": [{"id": 1, "val": 10}]}
                ).run()
                assert result.row_count == 1
        """)
    )
    (tmp_path / "test_beta.py").write_text(
        textwrap.dedent("""\
        from fyrnheim import (
            ComputedColumn,
            Entity,
            LayersConfig,
            PrepLayer,
            TableSource,
        )
        from fyrnheim.testing import EntityTest

        _entity = Entity(
            name="beta",
            description="Beta entity",
            source=TableSource(
                project="test", dataset="raw", table="beta",
                duckdb_path="beta.parquet",
            ),
            layers=LayersConfig(
                prep=PrepLayer(
                    model_name="prep_beta",
                    computed_columns=[
                        ComputedColumn(
                            name="val_tripled",
                            expression="t.val * 3",
                            description="Tripled value",
                        ),
                    ],
                ),
            ),
        )

        class TestBeta(EntityTest):
            entity = _entity

            def test_basic(self):
                result = self.given(
                    {"source_beta": [{"id": 1, "val": 5}]}
                ).run()
                assert result.row_count == 1
        """)
    )
    return tmp_path


class TestDiscoverTests:
    """Tests for discover_tests() function."""

    def test_finds_test_files(self, test_dir: Path) -> None:
        """discover_tests(path) finds test_*.py files in a directory."""
        results = discover_tests(test_dir)
        assert len(results) > 0

    def test_returns_class_method_tuples(self, test_dir: Path) -> None:
        """discover_tests returns list of (class, method_name) tuples for EntityTest subclasses."""
        results = discover_tests(test_dir)
        assert len(results) == 2  # test_row_count and test_column_values

        classes = {cls.__name__ for cls, _ in results}
        assert "TestWidgets" in classes

        methods = {method for _, method in results}
        assert "test_row_count" in methods
        assert "test_column_values" in methods

    def test_empty_directory(self, tmp_path: Path) -> None:
        """discover_tests on an empty directory returns empty list."""
        results = discover_tests(tmp_path)
        assert results == []

    def test_nonexistent_directory(self, tmp_path: Path) -> None:
        """discover_tests on a nonexistent path returns empty list."""
        results = discover_tests(tmp_path / "nonexistent")
        assert results == []

    def test_ignores_non_test_files(self, tmp_path: Path) -> None:
        """discover_tests ignores files that don't match test_*.py."""
        (tmp_path / "helper.py").write_text("x = 1\n")
        (tmp_path / "conftest.py").write_text("x = 1\n")
        results = discover_tests(tmp_path)
        assert results == []


class TestRunTests:
    """Tests for run_tests() function."""

    def test_executes_and_returns_outcomes(self, test_dir: Path) -> None:
        """run_tests() executes each test method and returns list of TestOutcome."""
        tests = discover_tests(test_dir)
        outcomes = run_tests(tests)
        assert len(outcomes) == 2
        assert all(isinstance(o, TestOutcome) for o in outcomes)
        assert all(o.passed for o in outcomes)

    def test_failed_tests_capture_error(self, test_dir_with_failure: Path) -> None:
        """Failed tests capture the exception message for reporting."""
        tests = discover_tests(test_dir_with_failure)
        outcomes = run_tests(tests)
        assert len(outcomes) == 2

        passed = [o for o in outcomes if o.passed]
        failed = [o for o in outcomes if not o.passed]
        assert len(passed) == 1
        assert len(failed) == 1
        assert failed[0].error is not None
        assert "Expected 999 rows" in failed[0].error

    def test_entity_filter(self, test_dir_two_entities: Path) -> None:
        """run_tests(entity_filter='name') only runs tests where entity matches filter."""
        tests = discover_tests(test_dir_two_entities)
        assert len(tests) == 2  # one from each file

        outcomes = run_tests(tests, entity_filter="alpha")
        assert len(outcomes) == 1
        assert "TestAlpha" in outcomes[0].test_name

    def test_entity_filter_no_match(self, test_dir_two_entities: Path) -> None:
        """run_tests with non-matching entity_filter returns empty list."""
        tests = discover_tests(test_dir_two_entities)
        outcomes = run_tests(tests, entity_filter="nonexistent")
        assert len(outcomes) == 0

    def test_outcome_has_duration(self, test_dir: Path) -> None:
        """TestOutcome includes duration_seconds."""
        tests = discover_tests(test_dir)
        outcomes = run_tests(tests)
        assert all(o.duration_seconds >= 0 for o in outcomes)

    def test_outcome_test_name_format(self, test_dir: Path) -> None:
        """TestOutcome.test_name is ClassName.method_name format."""
        tests = discover_tests(test_dir)
        outcomes = run_tests(tests)
        for o in outcomes:
            assert "." in o.test_name
            cls_name, method_name = o.test_name.split(".", 1)
            assert cls_name == "TestWidgets"
            assert method_name.startswith("test_")
