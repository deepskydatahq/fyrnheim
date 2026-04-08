"""Tests for IbisExecutor."""

import os
from unittest.mock import MagicMock, patch

import pytest

from fyrnheim.engine.executor import IbisExecutor


class TestFromConfig:
    def test_duckdb_backend(self):
        executor = IbisExecutor.from_config(backend="duckdb")
        assert executor._backend == "duckdb"
        assert executor.connection is not None
        executor.close()

    def test_duckdb_with_config(self):
        executor = IbisExecutor.from_config(
            backend="duckdb",
            backend_config={"db_path": ":memory:"},
        )
        assert executor._backend == "duckdb"
        executor.close()

    def test_unsupported_backend_raises(self):
        with pytest.raises(ValueError, match="Unsupported backend: 'postgres'"):
            IbisExecutor.from_config(backend="postgres")

    def test_clickhouse_backend_raises_without_server(self):
        """ClickHouse connect fails without a running server, but the method exists."""
        with pytest.raises(Exception, match="nonexistent-host|connection|connect|resolve"):  # noqa: B017
            IbisExecutor.from_config(
                backend="clickhouse",
                backend_config={"host": "nonexistent-host", "port": "9999"},
            )

    def test_unsupported_backend_message_lists_bigquery(self):
        with pytest.raises(ValueError, match="bigquery"):
            IbisExecutor.from_config(backend="snowflake")


class TestBigQueryBackend:
    """Tests for the BigQuery backend factory and from_config branch."""

    def test_from_config_invokes_bigquery_connect_with_project_id(self):
        fake_conn = MagicMock(name="bigquery_connection")
        with patch("ibis.bigquery.connect", return_value=fake_conn) as mock_connect:
            executor = IbisExecutor.from_config(
                backend="bigquery",
                backend_config={"project_id": "my-proj"},
            )
        mock_connect.assert_called_once_with(project_id="my-proj")
        assert executor._backend == "bigquery"
        assert executor.connection is fake_conn

    def test_from_config_passes_optional_kwargs(self):
        fake_conn = MagicMock(name="bigquery_connection")
        with patch("ibis.bigquery.connect", return_value=fake_conn) as mock_connect:
            IbisExecutor.from_config(
                backend="bigquery",
                backend_config={
                    "project_id": "my-proj",
                    "dataset_id": "analytics_363352015",
                    "location": "EU",
                },
            )
        mock_connect.assert_called_once_with(
            project_id="my-proj",
            dataset_id="analytics_363352015",
            location="EU",
        )

    def test_from_config_missing_project_id_raises(self):
        with pytest.raises(ValueError, match="project_id"):
            IbisExecutor.from_config(
                backend="bigquery",
                backend_config={"dataset_id": "x"},
            )

    def test_credentials_path_sets_environment_variable(self, tmp_path, monkeypatch):
        monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
        creds = tmp_path / "creds.json"
        creds.write_text("{}")

        fake_conn = MagicMock(name="bigquery_connection")
        with patch("ibis.bigquery.connect", return_value=fake_conn):
            IbisExecutor.from_config(
                backend="bigquery",
                backend_config={
                    "project_id": "my-proj",
                    "credentials_path": str(creds),
                },
            )
        assert os.environ["GOOGLE_APPLICATION_CREDENTIALS"] == str(creds)

    def test_no_credentials_path_leaves_env_alone(self, monkeypatch):
        monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/preset/path.json")
        fake_conn = MagicMock(name="bigquery_connection")
        with patch("ibis.bigquery.connect", return_value=fake_conn):
            IbisExecutor.from_config(
                backend="bigquery",
                backend_config={"project_id": "my-proj"},
            )
        # Existing env var is untouched (falls through to ADC behavior)
        assert os.environ["GOOGLE_APPLICATION_CREDENTIALS"] == "/preset/path.json"
