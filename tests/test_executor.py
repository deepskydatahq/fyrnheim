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


class TestMaterializeView:
    """Tests for materialize_view / view_exists / drop_view."""

    def test_duckdb_end_to_end(self):
        executor = IbisExecutor.duckdb()
        try:
            executor.materialize_view(
                project="ignored",
                dataset="staging",
                name="v_one",
                sql="SELECT 1 AS x",
            )
            assert executor.view_exists("ignored", "staging", "v_one") is True
            result = executor.connection.table("v_one", database="staging").execute()
            assert result["x"].tolist() == [1]

            # Replacing works
            executor.materialize_view(
                project="ignored",
                dataset="staging",
                name="v_one",
                sql="SELECT 2 AS x",
            )
            result = executor.connection.table("v_one", database="staging").execute()
            assert result["x"].tolist() == [2]

            executor.drop_view("ignored", "staging", "v_one")
            assert executor.view_exists("ignored", "staging", "v_one") is False
        finally:
            executor.close()

    def test_bigquery_materialize_view_uses_raw_sql(self):
        fake_conn = MagicMock(name="bigquery_connection")
        fake_conn.name = "bigquery"
        executor = IbisExecutor(conn=fake_conn, backend="bigquery")
        executor.materialize_view(
            project="my-proj",
            dataset="staging",
            name="v_one",
            sql="SELECT 1 AS x",
        )
        fake_conn.raw_sql.assert_called_once_with(
            "CREATE OR REPLACE VIEW `my-proj.staging.v_one` AS SELECT 1 AS x"
        )

    def test_bigquery_drop_view_uses_raw_sql(self):
        fake_conn = MagicMock(name="bigquery_connection")
        fake_conn.name = "bigquery"
        executor = IbisExecutor(conn=fake_conn, backend="bigquery")
        executor.drop_view(project="my-proj", dataset="staging", name="v_one")
        fake_conn.raw_sql.assert_called_once_with(
            "DROP VIEW IF EXISTS `my-proj.staging.v_one`"
        )

    def test_bigquery_view_exists_true(self):
        fake_conn = MagicMock(name="bigquery_connection")
        fake_conn.name = "bigquery"
        fake_conn.list_tables.return_value = ["v_one", "other"]
        executor = IbisExecutor(conn=fake_conn, backend="bigquery")
        assert executor.view_exists("my-proj", "staging", "v_one") is True
        fake_conn.list_tables.assert_called_once_with(database="my-proj.staging")

    def test_bigquery_view_exists_false_on_error(self):
        fake_conn = MagicMock(name="bigquery_connection")
        fake_conn.name = "bigquery"
        fake_conn.list_tables.side_effect = Exception("dataset not found")
        executor = IbisExecutor(conn=fake_conn, backend="bigquery")
        assert executor.view_exists("my-proj", "missing", "v_one") is False

    def test_clickhouse_materialize_view_raises_not_implemented(self):
        fake_conn = MagicMock(name="clickhouse_connection")
        fake_conn.name = "clickhouse"
        executor = IbisExecutor(conn=fake_conn, backend="clickhouse")
        with pytest.raises(NotImplementedError, match="v1 supports BigQuery and DuckDB"):
            executor.materialize_view("p", "d", "n", "SELECT 1")

    def test_postgres_view_exists_raises_not_implemented(self):
        fake_conn = MagicMock(name="postgres_connection")
        fake_conn.name = "postgres"
        executor = IbisExecutor(conn=fake_conn, backend="postgres")
        with pytest.raises(NotImplementedError, match="v1 supports BigQuery and DuckDB"):
            executor.view_exists("p", "d", "n")

    def test_postgres_drop_view_raises_not_implemented(self):
        fake_conn = MagicMock(name="postgres_connection")
        fake_conn.name = "postgres"
        executor = IbisExecutor(conn=fake_conn, backend="postgres")
        with pytest.raises(NotImplementedError, match="v1 supports BigQuery and DuckDB"):
            executor.drop_view("p", "d", "n")


class TestWriteTable:
    """Tests for write_table across backends."""

    def test_duckdb_end_to_end(self):
        import pandas as pd
        executor = IbisExecutor.duckdb()
        try:
            df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
            executor.write_table("ignored", "marts", "users", df)
            result = executor.connection.table("users", database="marts").execute()
            assert sorted(result["id"].tolist()) == [1, 2, 3]
            assert sorted(result["name"].tolist()) == ["a", "b", "c"]
        finally:
            executor.close()

    def test_duckdb_overwrite(self):
        import pandas as pd
        executor = IbisExecutor.duckdb()
        try:
            df1 = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
            executor.write_table("ignored", "marts", "users", df1)
            df2 = pd.DataFrame({"id": [99], "name": ["z"]})
            executor.write_table("ignored", "marts", "users", df2)
            result = executor.connection.table("users", database="marts").execute()
            assert result["id"].tolist() == [99]
            assert result["name"].tolist() == ["z"]
        finally:
            executor.close()

    def test_duckdb_creates_schema(self):
        import pandas as pd
        executor = IbisExecutor.duckdb()
        try:
            df = pd.DataFrame({"x": [1]})
            executor.write_table("ignored", "brand_new_schema", "t", df)
            result = executor.connection.table("t", database="brand_new_schema").execute()
            assert result["x"].tolist() == [1]
        finally:
            executor.close()

    def test_bigquery_mocked(self):
        import pandas as pd
        fake_conn = MagicMock(name="bigquery_connection")
        fake_conn.name = "bigquery"
        fake_client = MagicMock(name="bq_client")
        fake_conn.client = fake_client
        fake_job = MagicMock()
        fake_client.load_table_from_dataframe.return_value = fake_job

        executor = IbisExecutor(conn=fake_conn, backend="bigquery")
        df = pd.DataFrame({"a": [1]})
        executor.write_table("my-proj", "marts", "users", df)

        fake_client.load_table_from_dataframe.assert_called_once()
        args, kwargs = fake_client.load_table_from_dataframe.call_args
        # df, fqn positional
        assert args[1] == "my-proj.marts.users"
        job_config = kwargs["job_config"]
        assert job_config.write_disposition == "WRITE_TRUNCATE"
        fake_job.result.assert_called_once()

    def test_clickhouse_raises_not_implemented(self):
        import pandas as pd
        fake_conn = MagicMock(name="clickhouse_connection")
        fake_conn.name = "clickhouse"
        executor = IbisExecutor(conn=fake_conn, backend="clickhouse")
        with pytest.raises(NotImplementedError, match="v1 supports BigQuery and DuckDB"):
            executor.write_table("p", "d", "n", pd.DataFrame({"x": [1]}))

    def test_postgres_raises_not_implemented(self):
        import pandas as pd
        fake_conn = MagicMock(name="postgres_connection")
        fake_conn.name = "postgres"
        executor = IbisExecutor(conn=fake_conn, backend="postgres")
        with pytest.raises(NotImplementedError, match="v1 supports BigQuery and DuckDB"):
            executor.write_table("p", "d", "n", pd.DataFrame({"x": [1]}))
