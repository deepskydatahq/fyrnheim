"""Snapshot-diff pipeline: end-to-end orchestration of store and diff.

Combines :class:`SnapshotStore` and :func:`diff_snapshots` into a single
``run()`` call that reads the previous snapshot, diffs against the current
data, saves the new snapshot, and returns the resulting event table.
"""

from __future__ import annotations

import datetime

import ibis

from fyrnheim.engine.diff_engine import diff_snapshots
from fyrnheim.engine.snapshot_store import SnapshotStore


class SnapshotDiffPipeline:
    """Orchestrate snapshot storage and diffing in one step.

    Args:
        store: A :class:`SnapshotStore` for persisting/retrieving snapshots.
        conn: An Ibis backend connection.
    """

    def __init__(self, store: SnapshotStore, conn: ibis.BaseBackend) -> None:
        self._store = store
        self._conn = conn

    def run(
        self,
        source_name: str,
        current_table: ibis.Table,
        id_field: str = "id",
        snapshot_date: datetime.date | None = None,
        exclude_fields: list[str] | None = None,
    ) -> ibis.Table:
        """Execute the full snapshot-diff cycle.

        1. Retrieve the previous snapshot from the store.
        2. Diff current data against the previous snapshot.
        3. Save the current data as the new snapshot.
        4. Return the event table.

        Args:
            source_name: Logical name of the state source.
            current_table: Current state data as an Ibis table.
            id_field: Column used as entity identifier.
            snapshot_date: Date for this snapshot. Defaults to today.
            exclude_fields: Fields to ignore for field_changed detection.

        Returns:
            Ibis table with columns: source, entity_id, ts, event_type, payload.
        """
        if snapshot_date is None:
            snapshot_date = datetime.date.today()

        # Step 1: get previous snapshot
        previous = self._store.get_previous(source_name, snapshot_date)

        # Step 2: diff
        events = diff_snapshots(
            current=current_table,
            previous=previous,
            source_name=source_name,
            id_field=id_field,
            snapshot_date=snapshot_date.isoformat(),
            exclude_fields=exclude_fields,
        )

        # Step 3: save current as new snapshot
        self._store.save(source_name, snapshot_date, current_table)

        # Step 4: return events
        return events
