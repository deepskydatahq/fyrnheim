"""Identity resolution engine.

Builds canonical_id mappings from event streams by observing match keys
across multiple sources, then enriches events with the resolved canonical_id.
"""

from __future__ import annotations

import ibis

from fyrnheim.core.identity import IdentityGraph


@ibis.udf.scalar.builtin(name="sha256")
def _sha256(value: str) -> str:
    raise NotImplementedError


@ibis.udf.scalar.builtin(name="to_hex")
def _to_hex(value: bytes) -> str:
    raise NotImplementedError


def _json_scalar(payload: ibis.Value, field_name: str) -> ibis.Value:
    """Extract a scalar JSON payload field as a string expression.

    Ibis represents ``payload['field']`` as JSON. Casting to string keeps
    quoted JSON strings, so remove quotes to match the legacy ``json.loads``
    + ``str(value)`` behavior for identity match keys.
    """
    return payload.cast("json")[field_name].cast("string").replace('"', "")


def _canonical_id(match_key_value: ibis.Value, *, backend_name: str) -> ibis.Value:
    """Return Fyrnheim's deterministic canonical ID expression.

    DuckDB's ``sha256`` returns a lowercase hexadecimal string, matching the
    legacy Python ``hashlib.sha256(...).hexdigest()`` shape. BigQuery's
    ``SHA256`` returns bytes, so BigQuery uses ``TO_HEX(SHA256(...))``.
    """
    hashed = _sha256(match_key_value.cast("string"))
    if backend_name == "bigquery":
        hashed = _to_hex(hashed)  # type: ignore[assignment]
    return hashed.substr(0, 16)


def _empty_identity_mapping() -> ibis.expr.types.Table:
    """Return an empty identity mapping with the canonical schema."""
    return ibis.memtable(
        [],
        schema=ibis.schema(
            {
                "source": "string",
                "entity_id": "string",
                "canonical_id": "string",
            }
        ),
    )


def resolve_identities(
    events: ibis.expr.types.Table,
    identity_graph: IdentityGraph,
) -> ibis.expr.types.Table:
    """Resolve identities from an event stream using an identity graph.

    Builds an Ibis expression for each IdentitySource, unions those source
    mappings, and deduplicates by ``(source, entity_id)``. The large event
    stream stays in the backend; this function does not execute it locally.

    Args:
        events: Ibis table with columns: source, entity_id, ts, event_type, payload.
        identity_graph: The IdentityGraph defining sources and match keys.

    Returns:
        Ibis table with columns: source, entity_id, canonical_id.
    """
    source_mappings: list[ibis.expr.types.Table] = []
    payload = events["payload"]
    try:
        backend_name = events.get_backend().name
    except Exception:
        backend_name = ""

    for identity_source in identity_graph.sources:
        extracted = _json_scalar(payload, identity_source.match_key_field)
        match_key_value = extracted.coalesce(events["entity_id"].cast("string"))
        mapping = events.filter(
            (events["source"] == identity_source.source)
            & (events["event_type"] != "field_changed")
            & match_key_value.notnull()
        ).select(
            source=events["source"].cast("string"),
            entity_id=events["entity_id"].cast("string"),
            canonical_id=_canonical_id(match_key_value, backend_name=backend_name),
        )
        source_mappings.append(mapping)

    if not source_mappings:
        return _empty_identity_mapping()

    unioned = ibis.union(*source_mappings, distinct=False)
    return unioned.group_by(["source", "entity_id"]).agg(
        canonical_id=unioned["canonical_id"].first()
    )


def enrich_events(
    events: ibis.expr.types.Table,
    id_mapping: ibis.expr.types.Table,
) -> ibis.expr.types.Table:
    """Enrich events with canonical_id from an identity mapping.

    Left joins events with the id_mapping on (source, entity_id). Events
    without a mapping get their entity_id as canonical_id. If the events are
    already enriched by a previous identity graph, the new mapping wins where
    present, otherwise the existing canonical_id is preserved.

    Args:
        events: Ibis table with columns: source, entity_id, ts, event_type, payload.
        id_mapping: Ibis table with columns: source, entity_id, canonical_id.

    Returns:
        Ibis table with columns: source, entity_id, ts, event_type, payload, canonical_id.
    """
    had_existing = "canonical_id" in events.columns
    left = events
    if had_existing:
        left = left.rename(_existing_canonical_id="canonical_id")

    mapping = id_mapping.rename(_new_canonical_id="canonical_id")
    joined = left.left_join(mapping, ["source", "entity_id"])

    if had_existing:
        canonical_id = joined["_new_canonical_id"].coalesce(
            joined["_existing_canonical_id"], joined["entity_id"].cast("string")
        )
    else:
        canonical_id = joined["_new_canonical_id"].coalesce(
            joined["entity_id"].cast("string")
        )

    return joined.select(
        source=joined["source"],
        entity_id=joined["entity_id"],
        ts=joined["ts"],
        event_type=joined["event_type"],
        payload=joined["payload"],
        canonical_id=canonical_id,
    )
