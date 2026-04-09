"""Ibis execution engine for fyrnheim pipelines."""

from __future__ import annotations

import datetime as _dt
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import ibis
import pandas as pd

from fyrnheim.engine.errors import SourceNotFoundError

log = logging.getLogger("fyrnheim.engine")

_AT_NAME_RE = re.compile(r"@(\w+)")


def _infer_bq_type(value: Any) -> str:
    """Map a Python value to a BigQuery ScalarQueryParameter type string.

    Order matters: bool must be checked before int (bool is a subclass of int).
    None defaults to STRING (a NULL STRING works in most state-table contexts).
    """
    if value is None:
        return "STRING"
    if isinstance(value, bool):
        return "BOOL"
    if isinstance(value, int):
        return "INT64"
    if isinstance(value, float):
        return "FLOAT64"
    if isinstance(value, _dt.datetime):
        return "TIMESTAMP"
    if isinstance(value, _dt.date):
        return "DATE"
    return "STRING"


@dataclass(frozen=True)
class ExecutionResult:
    """Result of executing a single entity's transformation."""

    entity_name: str
    target_name: str
    row_count: int
    columns: list[str]
    success: bool
    error: str | None = None


class IbisExecutor:
    """Execute entity transformations on any Ibis-supported backend.

    Usage::

        with IbisExecutor.duckdb() as executor:
            executor.register_parquet("source_customers", Path("data/customers.parquet"))
            # ... use executor.connection for new pipeline
    """

    def __init__(
        self,
        conn: ibis.BaseBackend,
        backend: str,
        generated_dir: str | Path | None = None,
    ) -> None:
        self._conn = conn
        self._backend = backend
        self._generated_dir = Path(generated_dir) if generated_dir else None
        self._registered_sources: dict[str, Path] = {}
        log.info("%s backend connected", self._backend)

    @classmethod
    def duckdb(
        cls,
        db_path: str | Path = ":memory:",
        generated_dir: str | Path | None = None,
    ) -> IbisExecutor:
        """Create an IbisExecutor with a DuckDB backend."""
        conn = ibis.duckdb.connect(str(db_path))
        return cls(conn=conn, backend="duckdb", generated_dir=generated_dir)

    @classmethod
    def clickhouse(
        cls,
        *,
        host: str = "localhost",
        port: int = 8123,
        database: str = "default",
        user: str = "default",
        password: str = "",
        generated_dir: str | Path | None = None,
        **kwargs: str,
    ) -> IbisExecutor:
        """Create an IbisExecutor with a ClickHouse backend."""
        conn = ibis.clickhouse.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            **kwargs,
        )
        return cls(conn=conn, backend="clickhouse", generated_dir=generated_dir)

    @classmethod
    def bigquery(
        cls,
        project_id: str,
        *,
        dataset_id: str | None = None,
        credentials_path: str | None = None,
        location: str | None = None,
        generated_dir: str | Path | None = None,
    ) -> IbisExecutor:
        """Create an IbisExecutor with a BigQuery backend.

        Credentials resolution order:
          1. Explicit credentials_path argument
          2. GOOGLE_APPLICATION_CREDENTIALS environment variable
          3. Application Default Credentials (gcloud auth application-default login)

        Args:
            project_id: GCP project ID containing the BigQuery datasets.
            dataset_id: Optional default dataset for the connection.
            credentials_path: Optional path to a service account JSON file.
            location: Optional BigQuery location (e.g. "EU", "US").
            generated_dir: Optional directory for generated artifacts.
        """
        if credentials_path:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

        kwargs: dict[str, Any] = {"project_id": project_id}
        if dataset_id:
            kwargs["dataset_id"] = dataset_id
        if location:
            kwargs["location"] = location

        conn = ibis.bigquery.connect(**kwargs)
        return cls(conn=conn, backend="bigquery", generated_dir=generated_dir)

    @classmethod
    def from_config(
        cls,
        backend: str,
        backend_config: dict[str, str] | None = None,
        generated_dir: str | Path | None = None,
    ) -> IbisExecutor:
        """Create an IbisExecutor from config values.

        Args:
            backend: Backend name ("duckdb", "clickhouse", etc.)
            backend_config: Backend-specific connection parameters.
            generated_dir: Optional directory for generated artifacts.
        """
        config = backend_config or {}

        if backend == "duckdb":
            return cls.duckdb(
                db_path=config.get("db_path", ":memory:"),
                generated_dir=generated_dir,
            )
        elif backend == "clickhouse":
            return cls.clickhouse(
                host=config.get("host", "localhost"),
                port=int(config.get("port", "8123")),
                database=config.get("database", "default"),
                user=config.get("user", "default"),
                password=config.get("password", ""),
                generated_dir=generated_dir,
            )
        elif backend == "bigquery":
            if "project_id" not in config:
                raise ValueError(
                    "bigquery backend requires 'project_id' in backend_config"
                )
            return cls.bigquery(
                project_id=config["project_id"],
                dataset_id=config.get("dataset_id"),
                credentials_path=config.get("credentials_path"),
                location=config.get("location"),
                generated_dir=generated_dir,
            )
        else:
            raise ValueError(
                f"Unsupported backend: {backend!r}. "
                "Supported: duckdb, clickhouse, bigquery"
            )

    @property
    def connection(self) -> ibis.BaseBackend:
        """The underlying Ibis backend connection."""
        return self._conn

    def register_parquet(self, name: str, path: str | Path) -> None:
        """Register a parquet file (or glob pattern) as a named source table.

        Args:
            name: Table name in DuckDB's catalog.
            path: Path to parquet file or glob pattern.

        Raises:
            SourceNotFoundError: If a non-glob path does not exist.
        """
        path = Path(path)
        path_str = str(path)

        # Validate non-glob paths exist
        if "*" not in path_str and "?" not in path_str and not path.exists():
            raise SourceNotFoundError(f"Parquet file not found: {path}")

        self._conn.read_parquet(path_str, table_name=name)
        self._registered_sources[name] = path
        log.debug("Registered source: %s -> %s", name, path)

    def materialize_view(
        self, project: str, dataset: str, name: str, sql: str
    ) -> None:
        """Create or replace a view on the active backend.

        Args:
            project: GCP project (BigQuery) or ignored for DuckDB.
            dataset: Dataset/schema name.
            name: View name.
            sql: SELECT statement forming the view body.
        """
        backend = self._conn.name
        if backend == "duckdb":
            self._conn.raw_sql(f'CREATE SCHEMA IF NOT EXISTS "{dataset}"')
            self._conn.raw_sql(
                f'CREATE OR REPLACE VIEW "{dataset}"."{name}" AS {sql}'
            )
        elif backend == "bigquery":
            self._conn.raw_sql(
                f"CREATE OR REPLACE VIEW `{project}.{dataset}.{name}` AS {sql}"
            )
        else:
            raise NotImplementedError(
                f"materialize_view not supported for backend {backend!r}; "
                "v1 supports BigQuery and DuckDB"
            )

    def write_table(
        self, project: str, dataset: str, name: str, df: pd.DataFrame
    ) -> None:
        """Write a pandas DataFrame to a warehouse table on the active backend.

        Uses WRITE_TRUNCATE / CREATE OR REPLACE semantics — existing tables
        are overwritten, no duplication on re-run.

        Args:
            project: GCP project (BigQuery) or ignored for DuckDB.
            dataset: Dataset/schema name. Created if it doesn't exist.
            name: Destination table name.
            df: Pandas DataFrame to write.
        """
        backend = self._conn.name
        if backend == "duckdb":
            self._conn.raw_sql(f'CREATE SCHEMA IF NOT EXISTS "{dataset}"')
            self._conn.create_table(name, df, database=dataset, overwrite=True)
        elif backend == "bigquery":
            from google.cloud import bigquery as bq

            client = getattr(self._conn, "client", None)
            if client is None:
                client = getattr(self._conn, "_client", None)
            if client is None:
                raise RuntimeError(
                    "Could not obtain underlying google.cloud.bigquery.Client "
                    "from the ibis bigquery connection"
                )
            job_config = bq.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            fqn = f"{project}.{dataset}.{name}"
            client.load_table_from_dataframe(
                df, fqn, job_config=job_config
            ).result()
        else:
            raise NotImplementedError(
                f"write_table not supported for backend {backend!r}; "
                "v1 supports BigQuery and DuckDB"
            )

    def execute_parameterized(
        self,
        sql: str,
        params: dict[str, Any],
    ) -> list[tuple[Any, ...]]:
        """Execute a parameterized SQL statement on the active backend.

        The sql template uses @name placeholders (BigQuery-native style).
        On DuckDB, @name is converted to $name at execution time.

        params maps placeholder name (without the @) to the Python value.
        Native Python types (str, int, float, bool, datetime, None) are
        supported — no escaping required.

        Always returns a list of tuples. SELECT statements return their rows;
        DDL/DML and zero-row queries return an empty list.
        """
        backend = self._conn.name
        if backend == "duckdb":
            duckdb_sql = _AT_NAME_RE.sub(r"$\1", sql)
            raw = getattr(self._conn, "con", None)
            if raw is None:
                raise RuntimeError(
                    "Could not obtain raw duckdb connection from the ibis "
                    "duckdb backend (expected attribute 'con')"
                )
            cursor = raw.execute(duckdb_sql, params or {})
            try:
                rows = cursor.fetchall()
            except Exception:
                return []
            return [tuple(r) for r in rows]
        elif backend == "bigquery":
            from google.cloud import bigquery as bq

            client = getattr(self._conn, "client", None)
            if client is None:
                client = getattr(self._conn, "_client", None)
            if client is None:
                raise RuntimeError(
                    "Could not obtain underlying google.cloud.bigquery.Client "
                    "from the ibis bigquery connection"
                )
            bq_params = [
                bq.ScalarQueryParameter(name, _infer_bq_type(value), value)
                for name, value in params.items()
            ]
            job = client.query(
                sql, job_config=bq.QueryJobConfig(query_parameters=bq_params)
            )
            result = job.result()
            return [tuple(row.values()) for row in result]
        else:
            raise NotImplementedError(
                f"execute_parameterized is not supported for backend "
                f"{backend!r}. v1 supports BigQuery and DuckDB."
            )

    def view_exists(self, project: str, dataset: str, name: str) -> bool:
        """Return True if the given view exists in the backend."""
        backend = self._conn.name
        if backend == "duckdb":
            try:
                tables = self._conn.list_tables(database=dataset)
            except Exception:
                return False
            return name in tables
        elif backend == "bigquery":
            try:
                tables = self._conn.list_tables(database=f"{project}.{dataset}")
            except Exception:
                return False
            return name in tables
        else:
            raise NotImplementedError(
                f"view_exists not supported for backend {backend!r}; "
                "v1 supports BigQuery and DuckDB"
            )

    def drop_view(self, project: str, dataset: str, name: str) -> None:
        """Drop a view if it exists on the active backend."""
        backend = self._conn.name
        if backend == "duckdb":
            self._conn.raw_sql(f'DROP VIEW IF EXISTS "{dataset}"."{name}"')
        elif backend == "bigquery":
            self._conn.raw_sql(
                f"DROP VIEW IF EXISTS `{project}.{dataset}.{name}`"
            )
        else:
            raise NotImplementedError(
                f"drop_view not supported for backend {backend!r}; "
                "v1 supports BigQuery and DuckDB"
            )

    def close(self) -> None:
        """Close the backend connection."""
        if self._conn is not None:
            self._conn.disconnect()
            log.info("%s backend disconnected", self._backend)

    def __enter__(self) -> IbisExecutor:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
