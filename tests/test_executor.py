"""Tests for IbisExecutor."""

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
        with pytest.raises(Exception):
            IbisExecutor.from_config(
                backend="clickhouse",
                backend_config={"host": "nonexistent-host", "port": "9999"},
            )
