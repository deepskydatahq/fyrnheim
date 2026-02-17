"""Unit tests for quality check runner."""

from unittest.mock import MagicMock

from typedata.quality import QualityRunner


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
