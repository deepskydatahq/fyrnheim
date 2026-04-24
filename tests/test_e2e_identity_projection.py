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

import json
from pathlib import Path

import ibis
import pandas as pd

from fyrnheim.components.computed_column import ComputedColumn
from fyrnheim.config import ResolvedConfig
from fyrnheim.core.analytics_entity import AnalyticsEntity, StateField
from fyrnheim.core.identity import IdentityGraph, IdentitySource
from fyrnheim.core.source import Field, Rename, SourceTransforms, StateSource
from fyrnheim.engine.analytics_entity_engine import project_analytics_entity
from fyrnheim.engine.diff_engine import diff_snapshots
from fyrnheim.engine.identity_engine import enrich_events, resolve_identities
from fyrnheim.engine.pipeline import _load_state_source


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


def test_m068_statesource_transforms_and_computed_columns_flow_e2e(
    tmp_path: Path,
) -> None:
    """M068 e2e: a StateSource with ``transforms`` (rename) +
    ``computed_columns`` flows through ``_load_state_source`` + identity
    resolution + ``project_analytics_entity``. The downstream entity
    materialization reflects the transformed schema — the renamed
    ``opportunity_id`` and the computed column both surface as state
    fields on the projected entity.
    """
    # --- Fixture: a tiny CRM-shaped parquet with ``id`` (to be renamed),
    #     ``stage``, and ``amount`` columns.
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    parquet_path = data_dir / "opportunities.parquet"
    df = pd.DataFrame(
        {
            "id": ["OPP-1", "OPP-2"],
            "stage": ["Prospecting", "Closed Won"],
            "amount": [1000.0, 5000.0],
        }
    )
    df.to_parquet(str(parquet_path))

    # --- StateSource with transforms + computed_columns
    state_source = StateSource(
        name="opportunities",
        project="test",
        dataset="test",
        table="opportunities",
        duckdb_path=str(parquet_path),
        id_field="opportunity_id",  # post-rename name
        transforms=SourceTransforms(
            renames=[Rename(from_name="id", to_name="opportunity_id")]
        ),
        computed_columns=[
            ComputedColumn(
                name="is_closed",
                expression="t.stage == 'Closed Won'",
            ),
        ],
    )

    config = ResolvedConfig(
        entities_dir=tmp_path / "entities",
        data_dir=data_dir,
        output_dir=tmp_path / "output",
        backend="duckdb",
        project_root=tmp_path,
    )

    conn = ibis.duckdb.connect()
    state_events = _load_state_source(state_source, config, conn)

    # Sanity-check: events carry transformed schema.
    se_df = state_events.execute()
    assert len(se_df) == 2
    # entity_id resolves from the RENAMED id_field (opportunity_id) —
    # proves the transform reached identity-key resolution.
    assert set(se_df["entity_id"]) == {"OPP-1", "OPP-2"}
    payloads = [json.loads(p) for p in se_df["payload"].tolist()]
    # The computed column surfaces in the payload; the id_field (renamed
    # ``opportunity_id``) is moved to ``entity_id`` by ``_make_appeared_events``
    # so it is not duplicated in the payload.
    assert {"stage", "amount", "is_closed"} <= set(payloads[0].keys())
    assert "opportunity_id" not in payloads[0]

    # --- Add a second source so IdentityGraph (requires >= 2 sources) is valid.
    evt_events = ibis.memtable(
        pd.DataFrame(
            {
                "source": ["crm_activity"],
                "entity_id": ["evt-1"],
                "ts": ["2026-04-01T10:00:00"],
                "event_type": ["note_added"],
                "payload": ['{"opportunity_id": "OPP-1"}'],
            }
        )
    )

    all_events = ibis.memtable(
        pd.concat([se_df, evt_events.execute()], ignore_index=True)
    )

    graph = IdentityGraph(
        name="opp_graph",
        canonical_id="canonical_opp_id",
        sources=[
            IdentitySource(
                source="opportunities",
                id_field="opportunity_id",
                match_key_field="opportunity_id",
            ),
            IdentitySource(
                source="crm_activity",
                id_field="event_id",
                match_key_field="opportunity_id",
            ),
        ],
    )
    mapping = resolve_identities(all_events, graph)
    enriched = enrich_events(all_events, mapping)

    entity = AnalyticsEntity(
        name="opportunity",
        identity_graph="opp_graph",
        state_fields=[
            StateField(
                name="stage",
                source="opportunities",
                field="stage",
                strategy="latest",
            ),
            StateField(
                name="is_closed",
                source="opportunities",
                field="is_closed",
                strategy="latest",
            ),
        ],
    )
    projected = project_analytics_entity(enriched, entity).execute()

    # Entity materialization sees the transformed schema: the rename
    # reached identity resolution (canonical_opp_id maps from
    # opportunity_id) and the computed column reached state projection.
    assert len(projected) >= 2  # one row per canonical id from opportunities

    # Closed-won opportunity has is_closed True; Prospecting is False.
    # Values may be serialised as booleans or JSON booleans depending
    # on the payload round-trip; check for truthy/falsy semantics.
    closed_values = set()
    for v in projected["is_closed"].tolist():
        if v is None:
            continue
        closed_values.add(str(v).lower())
    assert closed_values >= {"true", "false"} or closed_values == {"true", "false"}


def test_m069_statesource_json_path_and_filter_e2e(tmp_path: Path) -> None:
    """M069 e2e: a StateSource with ``Field.json_path`` extraction AND
    ``filter`` flows through ``_load_state_source`` + identity resolution
    + ``project_analytics_entity``. The extracted column surfaces in
    downstream entity projection; the filter excludes inactive rows;
    identity resolution still works against the primary id_field.
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    parquet_path = data_dir / "accounts.parquet"
    df = pd.DataFrame(
        {
            "account_id": ["A1", "A2", "A3"],
            "status": ["active", "active", "inactive"],
            "custom_type": [
                '{"value": "premium"}',
                '{"value": "free"}',
                '{"value": "premium"}',  # filtered out — inactive
            ],
        }
    )
    df.to_parquet(str(parquet_path))

    state_source = StateSource(
        name="accounts",
        project="test",
        dataset="test",
        table="accounts",
        duckdb_path=str(parquet_path),
        id_field="account_id",
        fields=[
            Field(
                name="account_type",
                type="STRING",
                json_path="$.value",
                source_column="custom_type",
            )
        ],
        filter='t.status == "active"',
    )

    config = ResolvedConfig(
        entities_dir=tmp_path / "entities",
        data_dir=data_dir,
        output_dir=tmp_path / "output",
        backend="duckdb",
        project_root=tmp_path,
    )

    conn = ibis.duckdb.connect()
    state_events = _load_state_source(state_source, config, conn)
    se_df = state_events.execute()

    # Filter dropped the inactive row; only A1 + A2 emit events.
    assert len(se_df) == 2
    assert set(se_df["entity_id"]) == {"A1", "A2"}
    # Inactive row must not leak through.
    assert "A3" not in set(se_df["entity_id"])

    # The extracted json_path column is present in each payload.
    payloads = [json.loads(p) for p in se_df["payload"].tolist()]
    extracted = {p["account_type"] for p in payloads}
    assert extracted == {"premium", "free"}

    # --- Add a second source so IdentityGraph is valid (>= 2 sources).
    evt_events = ibis.memtable(
        pd.DataFrame(
            {
                "source": ["crm_activity"],
                "entity_id": ["evt-1"],
                "ts": ["2026-04-22T10:00:00"],
                "event_type": ["note_added"],
                "payload": ['{"account_id": "A1"}'],
            }
        )
    )
    all_events = ibis.memtable(
        pd.concat([se_df, evt_events.execute()], ignore_index=True)
    )

    graph = IdentityGraph(
        name="acct_graph",
        canonical_id="canonical_account_id",
        sources=[
            IdentitySource(
                source="accounts",
                id_field="account_id",
                match_key_field="account_id",
            ),
            IdentitySource(
                source="crm_activity",
                id_field="event_id",
                match_key_field="account_id",
            ),
        ],
    )
    mapping = resolve_identities(all_events, graph)
    enriched = enrich_events(all_events, mapping)

    entity = AnalyticsEntity(
        name="account",
        identity_graph="acct_graph",
        state_fields=[
            StateField(
                name="account_type",
                source="accounts",
                field="account_type",
                strategy="latest",
            ),
        ],
    )
    projected = project_analytics_entity(enriched, entity).execute()

    # Projected entity materialisation sees the extracted column: at
    # least one of the two active accounts resolves to "premium" and
    # the other to "free". No row maps to an inactive account.
    account_types = set()
    for v in projected["account_type"].tolist():
        if v is None:
            continue
        account_types.add(str(v))
    assert account_types == {"premium", "free"}
