"""Tests for create_connection() factory function."""

from unittest.mock import MagicMock, patch

import ibis
import pytest

from fyrnheim.engine.connection import create_connection


class TestCreateConnectionDuckDB:
    """Test create_connection for DuckDB backend."""

    def test_default_in_memory(self):
        conn = create_connection("duckdb")
        assert conn is not None
        conn.disconnect()

    def test_explicit_db_path(self, tmp_path):
        db_path = tmp_path / "test.duckdb"
        conn = create_connection("duckdb", db_path=db_path)
        assert conn is not None
        conn.disconnect()

    def test_returns_ibis_backend(self):
        conn = create_connection("duckdb")
        assert isinstance(conn, ibis.BaseBackend)
        conn.disconnect()


class TestCreateConnectionBigQuery:
    """Test create_connection for BigQuery backend (mocked)."""

    def test_calls_ibis_bigquery_connect(self):
        mock_conn = MagicMock(spec=ibis.BaseBackend)
        with patch("fyrnheim.engine.connection.ibis.bigquery.connect", return_value=mock_conn) as mock_connect, \
             patch.dict("sys.modules", {"ibis.backends.bigquery": MagicMock()}):
            conn = create_connection("bigquery", project_id="my-project", dataset_id="my_dataset")
            mock_connect.assert_called_once_with(project_id="my-project", dataset_id="my_dataset")
            assert conn is mock_conn

    def test_missing_extras_raises_import_error(self):
        with patch.dict("sys.modules", {"ibis.backends.bigquery": None}):
            with pytest.raises(ImportError, match="BigQuery backend requires extra dependencies"):
                create_connection("bigquery", project_id="x", dataset_id="y")

    def test_import_error_includes_install_hint(self):
        with patch.dict("sys.modules", {"ibis.backends.bigquery": None}):
            with pytest.raises(ImportError, match="pip install"):
                create_connection("bigquery", project_id="x", dataset_id="y")


class TestCreateConnectionUnsupported:
    """Test create_connection with unsupported backends."""

    def test_unknown_backend_raises_value_error(self):
        with pytest.raises(ValueError, match="Unsupported backend"):
            create_connection("postgres")

    def test_error_lists_supported_backends(self):
        with pytest.raises(ValueError, match="duckdb"):
            create_connection("unknown")

    def test_error_lists_bigquery(self):
        with pytest.raises(ValueError, match="bigquery"):
            create_connection("unknown")


class TestCreateConnectionLazyImport:
    """Test that create_connection is accessible from top-level package."""

    def test_importable_from_engine(self):
        from fyrnheim.engine import create_connection as cc
        assert callable(cc)

    def test_importable_from_package(self):
        import fyrnheim
        assert callable(fyrnheim.create_connection)
