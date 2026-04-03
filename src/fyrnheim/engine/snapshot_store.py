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
        date: datetime.date | datetime.datetime,
        table: ibis.Table,
    ) -> Path:
        """Save a snapshot of the given table for a source on a date.

        Materializes the Ibis table expression to a parquet file at
        ``{base_dir}/{source_name}/{YYYY-MM-DD}.parquet`` for date inputs,
        or ``{base_dir}/{source_name}/{YYYY-MM-DDTHH-MM-SS}.parquet`` for
        datetime inputs.  Overwrites any existing file for the same
        source+date combination.

        Args:
            source_name: Logical name of the state source.
            date: Snapshot date or datetime.
            table: Ibis table expression to persist.

        Returns:
            Path to the written parquet file.
        """
        dir_path = self._base_dir / source_name
        dir_path.mkdir(parents=True, exist_ok=True)
        if isinstance(date, datetime.datetime):
            stem = date.strftime("%Y-%m-%dT%H-%M-%S")
        else:
            stem = date.isoformat()
        file_path = dir_path / f"{stem}.parquet"

        # Materialize to pandas then write parquet via the connection
        # Use to_parquet on the connection for DuckDB compatibility
        self._conn.to_parquet(table, str(file_path))

        return file_path

    @staticmethod
    def _parse_stem(stem: str) -> datetime.date | datetime.datetime | None:
        """Parse a filename stem into a date or datetime.

        Tries datetime format ``YYYY-MM-DDTHH-MM-SS`` first, then falls
        back to date format ``YYYY-MM-DD``.  Returns ``None`` if neither
        matches.
        """
        # Try datetime first (contains 'T')
        if "T" in stem:
            try:
                return datetime.datetime.strptime(stem, "%Y-%m-%dT%H-%M-%S")
            except ValueError:
                pass
        # Fall back to date
        try:
            return datetime.date.fromisoformat(stem)
        except ValueError:
            return None

    @staticmethod
    def _to_datetime(d: datetime.date | datetime.datetime) -> datetime.datetime:
        """Promote a date to a datetime at midnight for comparison."""
        if isinstance(d, datetime.datetime):
            return d
        return datetime.datetime(d.year, d.month, d.day)

    @staticmethod
    def _stem_from_parsed(d: datetime.date | datetime.datetime) -> str:
        """Reconstruct the filename stem from a parsed date/datetime."""
        if isinstance(d, datetime.datetime):
            return d.strftime("%Y-%m-%dT%H-%M-%S")
        return d.isoformat()

    def get_previous(
        self,
        source_name: str,
        date: datetime.date | datetime.datetime,
    ) -> ibis.Table | None:
        """Retrieve the most recent snapshot strictly before the given date.

        Scans ``{base_dir}/{source_name}/`` for parquet files whose
        filename (``YYYY-MM-DD.parquet`` or ``YYYY-MM-DDTHH-MM-SS.parquet``)
        is earlier than *date* and returns the newest one as an Ibis table.

        Args:
            source_name: Logical name of the state source.
            date: Reference date or datetime; only snapshots before this
                are considered.

        Returns:
            Ibis table from the most recent prior snapshot, or ``None``
            if no earlier snapshot exists.
        """
        dir_path = self._base_dir / source_name
        if not dir_path.is_dir():
            return None

        ref = self._to_datetime(date)
        candidates: list[tuple[datetime.datetime, datetime.date | datetime.datetime]] = []
        for parquet_file in dir_path.glob("*.parquet"):
            parsed = self._parse_stem(parquet_file.stem)
            if parsed is None:
                continue
            parsed_dt = self._to_datetime(parsed)
            if parsed_dt < ref:
                candidates.append((parsed_dt, parsed))

        if not candidates:
            return None

        _, latest_original = max(candidates, key=lambda x: x[0])
        stem = self._stem_from_parsed(latest_original)
        file_path = dir_path / f"{stem}.parquet"
        return self._conn.read_parquet(str(file_path))
