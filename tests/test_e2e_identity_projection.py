"""E2E regression test for M051: the client-flowable (M006) reproducer.

Exercises the identity-resolution + projection path end-to-end for a
scenario where:

  - A StateSource has match_key_field == id_field (bug #91)
  - An EventSource contributes events with non-row_appeared event_type (#92)
  - State payloads contain None values that must NOT round-trip as the
    string "null" (bug #93)
  - Activity-rewritten events must still yield state_field values (#94)

The assertion that no output value equals the literal string "null" is
the canary for the silent-failure chain discovered during the client
engagement.
"""

from __future__ import annotations

import ibis
import pandas as pd

from fyrnheim.core.analytics_entity import AnalyticsEntity, StateField
from fyrnheim.core.identity import IdentityGraph, IdentitySource
from fyrnheim.engine.analytics_entity_engine import project_analytics_entity
from fyrnheim.engine.diff_engine import diff_snapshots
from fyrnheim.engine.identity_engine import enrich_events, resolve_identities


def test_m006_reproducer_identity_projection_no_null_string() -> None:
    # --- StateSource snapshot: two anon users, one with a real company, one None
    anon_current = ibis.memtable(
        pd.DataFrame(
            {
                "anon_id": ["anon-1", "anon-2"],
                "company_name": ["Northwind Bank", None],
                "region": ["EU", "US"],
            }
        )
    )
    state_events = diff_snapshots(
        current=anon_current,
        previous=None,
        source_name="anon_attrs",
        id_field="anon_id",
        snapshot_date="2026-04-01",
    )

    # --- EventSource-style events (already in universal schema)
    evt_events = ibis.memtable(
        pd.DataFrame(
            {
                "source": ["ga4_events", "ga4_events"],
                "entity_id": ["evt-1", "evt-2"],
                "ts": ["2026-04-01T10:00:00", "2026-04-01T11:00:00"],
                "event_type": ["session_start", "session_start"],
                "payload": [
                    '{"anon_id": "anon-1", "page": "/home"}',
                    '{"anon_id": "anon-2", "page": "/pricing"}',
                ],
            }
        )
    )

    # --- Union the two streams
    state_df = state_events.execute()
    evt_df = evt_events.execute()
    all_events = ibis.memtable(pd.concat([state_df, evt_df], ignore_index=True))

    # --- Identity graph: match_key_field == id_field for anon_attrs (#91),
    #     and EventSource events are non-row_appeared (#92).
    graph = IdentityGraph(
        name="anon_graph",
        canonical_id="canonical_anon_id",
        sources=[
            IdentitySource(
                source="anon_attrs",
                id_field="anon_id",
                match_key_field="anon_id",
            ),
            IdentitySource(
                source="ga4_events",
                id_field="event_id",
                match_key_field="anon_id",
            ),
        ],
    )

    mapping = resolve_identities(all_events, graph)
    mapping_df = mapping.execute()

    # (#91 + #92) Mapping is non-empty and spans both sources
    assert len(mapping_df) > 0
    assert set(mapping_df["source"]) == {"anon_attrs", "ga4_events"}

    # Shared anon id -> shared canonical
    anon1_canon = mapping_df[
        (mapping_df["source"] == "anon_attrs") & (mapping_df["entity_id"] == "anon-1")
    ].iloc[0]["canonical_id"]
    evt1_canon = mapping_df[
        (mapping_df["source"] == "ga4_events") & (mapping_df["entity_id"] == "evt-1")
    ].iloc[0]["canonical_id"]
    assert anon1_canon == evt1_canon

    # --- Enrich + project
    enriched = enrich_events(all_events, mapping)

    entity = AnalyticsEntity(
        name="anon_user",
        identity_graph="anon_graph",
        state_fields=[
            StateField(
                name="company_name",
                source="anon_attrs",
                field="company_name",
                strategy="latest",
            ),
            StateField(
                name="region",
                source="anon_attrs",
                field="region",
                strategy="latest",
            ),
        ],
    )
    projected = project_analytics_entity(enriched, entity).execute()

    # (#93) Northwind row projects the real string, not None and not "null"
    northwind_rows = projected[projected["company_name"] == "Northwind Bank"]
    assert len(northwind_rows) == 1

    # Canary: no value in the output equals the literal string "null"
    for col in projected.columns:
        for v in projected[col].tolist():
            assert v != "null", f"column {col!r} contains literal 'null' string"
