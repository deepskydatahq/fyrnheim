"""Unit tests for quality check runner."""

from unittest.mock import MagicMock

import ibis
import pandas as pd
import pytest

from fyrnheim.quality import MatchesPattern, QualityRunner


class TestQualityRunnerConstructor:
    def test_accepts_connection_and_dataset(self) -> None:
        mock_conn = MagicMock()
        runner = QualityRunner(connection=mock_conn, dataset="my_dataset")
        assert runner.connection is mock_conn
        assert runner.dataset == "my_dataset"

    def test_dataset_defaults_to_none(self) -> None:
        mock_conn = MagicMock()
        runner = QualityRunner(connection=mock_conn)
        assert runner.connection is mock_conn
        assert runner.dataset is None


class TestTableRef:
    def test_with_dataset(self) -> None:
        mock_conn = MagicMock()
        runner = QualityRunner(connection=mock_conn, dataset="my_dataset")
        assert runner._table_ref("users") == "my_dataset.users"

    def test_without_dataset(self) -> None:
        mock_conn = MagicMock()
        runner = QualityRunner(connection=mock_conn)
        assert runner._table_ref("users") == "users"

    def test_with_empty_string_dataset(self) -> None:
        mock_conn = MagicMock()
        runner = QualityRunner(connection=mock_conn, dataset="")
        # Empty string is falsy, so should return just the table name
        assert runner._table_ref("users") == "users"


class TestMatchesPatternIntegration:
    """Integration test: MatchesPattern on DuckDB via Ibis."""

    @pytest.fixture()
    def duckdb_with_data(self):
        conn = ibis.duckdb.connect(":memory:")
        df = pd.DataFrame({
            "id": [1, 2, 3, 4],
            "email": ["alice@example.com", "bob@test.org", "bad-email", "carol@example.com"],
        })
        conn.create_table("users", df, overwrite=True)
        yield conn
        conn.disconnect()

    def test_pattern_match_detects_failures(self, duckdb_with_data):
        runner = QualityRunner(connection=duckdb_with_data)
        check = MatchesPattern("email", r"^.+@.+\..+$")
        result = runner.run_check("users", check, primary_key="id")
        assert not result.passed
        assert result.failure_count == 1
        assert len(result.sample_failures) == 1
        assert result.sample_failures[0]["email"] == "bad-email"

    def test_pattern_match_all_pass(self, duckdb_with_data):
        runner = QualityRunner(connection=duckdb_with_data)
        check = MatchesPattern("email", r".*")  # matches everything
        result = runner.run_check("users", check, primary_key="id")
        assert result.passed
        assert result.failure_count == 0

    def test_pattern_match_all_fail(self):
        conn = ibis.duckdb.connect(":memory:")
        df = pd.DataFrame({"id": [1, 2], "val": ["abc", "def"]})
        conn.create_table("t", df, overwrite=True)
        runner = QualityRunner(connection=conn)
        check = MatchesPattern("val", r"^\d+$")  # only digits
        result = runner.run_check("t", check, primary_key="id")
        assert not result.passed
        assert result.failure_count == 2
        conn.disconnect()
