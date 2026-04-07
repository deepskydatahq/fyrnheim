"""Identity resolution engine.

Builds canonical_id mappings from event streams by observing match keys
across multiple sources, then enriches events with the resolved canonical_id.
"""

from __future__ import annotations

import hashlib
import json

import ibis
import pandas as pd

from fyrnheim.core.identity import IdentityGraph


def resolve_identities(
    events: ibis.expr.types.Table,
    identity_graph: IdentityGraph,
) -> ibis.expr.types.Table:
    """Resolve identities from an event stream using an identity graph.

    For each IdentitySource in the graph, filters events by source name and
    event_type=='row_appeared', parses the payload JSON to extract the
    match_key_field value, and builds a mapping of (source, entity_id) to
    canonical_id.

    The canonical_id is a deterministic SHA-256 hash (first 16 hex chars)
    of the match_key_value.

    Args:
        events: Ibis table with columns: source, entity_id, ts, event_type, payload.
        identity_graph: The IdentityGraph defining sources and match keys.

    Returns:
        Ibis table with columns: source, entity_id, canonical_id.
    """
    events_df = events.execute()

    source_names = {s.source for s in identity_graph.sources}
    source_match_keys = {s.source: s.match_key_field for s in identity_graph.sources}

    # Filter to row_appeared events from graph sources
    mask = (events_df["source"].isin(source_names)) & (
        events_df["event_type"] == "row_appeared"
    )
    relevant = events_df[mask]

    rows: list[dict[str, str]] = []
    for _, row in relevant.iterrows():
        source = row["source"]
        entity_id = row["entity_id"]
        payload = row["payload"]
        match_key_field = source_match_keys[source]

        try:
            payload_data = json.loads(payload)
        except (json.JSONDecodeError, TypeError):
            continue

        match_key_value = payload_data.get(match_key_field)
        if match_key_value is None:
            continue

        canonical_id = hashlib.sha256(str(match_key_value).encode()).hexdigest()[:16]
        rows.append(
            {
                "source": source,
                "entity_id": entity_id,
                "canonical_id": canonical_id,
            }
        )

    if not rows:
        return ibis.memtable(
            pd.DataFrame(columns=["source", "entity_id", "canonical_id"])
        )

    mapping_df = pd.DataFrame(rows).drop_duplicates(
        subset=["source", "entity_id"], keep="first"
    )
    return ibis.memtable(mapping_df)


def enrich_events(
    events: ibis.expr.types.Table,
    id_mapping: ibis.expr.types.Table,
) -> ibis.expr.types.Table:
    """Enrich events with canonical_id from an identity mapping.

    Left joins events with the id_mapping on (source, entity_id).
    Events without a mapping get their entity_id as canonical_id (fallback).

    Args:
        events: Ibis table with columns: source, entity_id, ts, event_type, payload.
        id_mapping: Ibis table with columns: source, entity_id, canonical_id.

    Returns:
        Ibis table with columns: source, entity_id, ts, event_type, payload, canonical_id.
    """
    events_df = events.execute()
    mapping_df = id_mapping.execute()

    # Preserve any existing canonical_id from prior enrichments by coalescing
    # the new mapping with the existing column. Without this, a second
    # enrich_events call would produce canonical_id_x / canonical_id_y from
    # pandas' suffix logic and break downstream.
    had_existing = "canonical_id" in events_df.columns
    if had_existing:
        events_df = events_df.rename(columns={"canonical_id": "_existing_canonical_id"})

    merged = events_df.merge(mapping_df, on=["source", "entity_id"], how="left")

    if had_existing:
        # New mapping wins where present, fall back to existing, then entity_id
        merged["canonical_id"] = (
            merged["canonical_id"]
            .fillna(merged["_existing_canonical_id"])
            .fillna(merged["entity_id"])
        )
    else:
        merged["canonical_id"] = merged["canonical_id"].fillna(merged["entity_id"])

    result = merged[
        ["source", "entity_id", "ts", "event_type", "payload", "canonical_id"]
    ]
    return ibis.memtable(result)
