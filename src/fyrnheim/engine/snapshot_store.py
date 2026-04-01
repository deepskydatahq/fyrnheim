"""Snapshot store for state source data.

Persists daily copies of state source data as parquet files on disk,
keyed by source name and date. Supports saving current snapshots and
retrieving previous snapshots for diffing.

Directory layout::

    {base_dir}/{source_name}/{YYYY-MM-DD}.parquet
"""

from __future__ import annotations

import datetime
from pathlib import Path

import ibis


class SnapshotStore:
    """Persist and retrieve daily snapshots as parquet files.

    Args:
        base_dir: Root directory for snapshot storage.
        conn: An Ibis backend connection (used for reading parquet files).
    """

    def __init__(self, base_dir: str | Path, conn: ibis.BaseBackend) -> None:
        self._base_dir = Path(base_dir)
        self._conn = conn

    def save(
        self,
        source_name: str,
        date: datetime.date,
        table: ibis.Table,
    ) -> Path:
        """Save a snapshot of the given table for a source on a date.

        Materializes the Ibis table expression to a parquet file at
        ``{base_dir}/{source_name}/{YYYY-MM-DD}.parquet``.  Overwrites
        any existing file for the same source+date combination.

        Args:
            source_name: Logical name of the state source.
            date: Snapshot date.
            table: Ibis table expression to persist.

        Returns:
            Path to the written parquet file.
        """
        dir_path = self._base_dir / source_name
        dir_path.mkdir(parents=True, exist_ok=True)
        file_path = dir_path / f"{date.isoformat()}.parquet"

        # Materialize to pandas then write parquet via the connection
        # Use to_parquet on the connection for DuckDB compatibility
        self._conn.to_parquet(table, str(file_path))

        return file_path

    def get_previous(
        self,
        source_name: str,
        date: datetime.date,
    ) -> ibis.Table | None:
        """Retrieve the most recent snapshot strictly before the given date.

        Scans ``{base_dir}/{source_name}/`` for parquet files whose
        filename (``YYYY-MM-DD.parquet``) is earlier than *date* and
        returns the newest one as an Ibis table.

        Args:
            source_name: Logical name of the state source.
            date: Reference date; only snapshots before this date are
                considered.

        Returns:
            Ibis table from the most recent prior snapshot, or ``None``
            if no earlier snapshot exists.
        """
        dir_path = self._base_dir / source_name
        if not dir_path.is_dir():
            return None

        candidates: list[datetime.date] = []
        for parquet_file in dir_path.glob("*.parquet"):
            try:
                file_date = datetime.date.fromisoformat(parquet_file.stem)
            except ValueError:
                continue
            if file_date < date:
                candidates.append(file_date)

        if not candidates:
            return None

        latest = max(candidates)
        file_path = dir_path / f"{latest.isoformat()}.parquet"
        return self._conn.read_parquet(str(file_path))
