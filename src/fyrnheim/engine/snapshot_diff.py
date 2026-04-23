"""Snapshot-diff pipeline: end-to-end orchestration of store and diff.

Combines :class:`SnapshotStore` and :func:`diff_snapshots` into a single
``run()`` call that reads the previous snapshot, diffs against the current
data, saves the new snapshot, and returns the resulting event table.
"""

from __future__ import annotations

import datetime
import logging

import ibis
import pandas as pd

from fyrnheim.engine.diff_engine import _make_appeared_events, diff_snapshots
from fyrnheim.engine.snapshot_store import SnapshotStore

log = logging.getLogger(__name__)


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
        3. If the diff is empty but a previous snapshot exists AND the
           current table has rows, replay every current row as a synthetic
           ``row_appeared`` event so downstream state-field materialization
           continues to produce correct output (M066: fixes silent
           0-rows-for-stable-StateSources bug). If current is also empty
           (M067), skip the replay and return the empty events table.
        4. Save the current data as the new snapshot.
        5. Return the event table.

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

        # Step 3: M066 empty-diff replay, M067 guarded against empty current.
        # When a prior snapshot exists and current state matches it exactly,
        # diff_snapshots returns 0 events. Downstream AnalyticsEntity
        # materialization would then emit 0 rows (silent pipeline failure).
        # Replay every current row as a synthetic row_appeared so state-field
        # extraction sees current data.
        #
        # M067: if current_table is ALSO 0 rows (empty placeholder sources —
        # e.g. Salesforce placeholders backed by ``SELECT ... FROM UNNEST([1])
        # LIMIT 0``), the replay has nothing to emit and the downstream
        # ``ibis.memtable(pd.DataFrame([]))`` rejects a 0-column DataFrame
        # with ``Invalid Input Error: Provided table/dataframe must have at
        # least one column``. Skip the replay in that case and return the
        # empty events table (the pre-v0.8.0 behavior for this specific case).
        event_count = int(events.count().execute())
        current_count = int(current_table.count().execute())
        if event_count == 0 and previous is not None and current_count > 0:
            replay_events = _make_appeared_events(
                current_table.execute(),
                source_name,
                id_field,
                snapshot_date.isoformat(),
            )
            events = ibis.memtable(pd.DataFrame(replay_events))
            log.info(
                "StateSource %s: diff empty, replayed %d rows as row_appeared",
                source_name,
                current_count,
            )
        elif event_count == 0 and previous is not None and current_count == 0:
            log.info(
                "StateSource %s: diff empty and current has 0 rows, "
                "skipping replay",
                source_name,
            )
        else:
            log.info(
                "StateSource %s: diff produced %d events", source_name, event_count
            )

        # Step 4: save current as new snapshot
        self._store.save(source_name, snapshot_date, current_table)

        # Step 5: return events
        return events
