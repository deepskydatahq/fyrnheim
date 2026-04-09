"""Tests for the identity resolution engine."""

from __future__ import annotations

import hashlib

import ibis
import pandas as pd
import pytest

from fyrnheim.core.identity import IdentityGraph, IdentitySource
from fyrnheim.engine.identity_engine import enrich_events, resolve_identities


@pytest.fixture()
def identity_graph() -> IdentityGraph:
    """Create a simple identity graph with two sources."""
    return IdentityGraph(
        name="user_graph",
        canonical_id="canonical_user_id",
        sources=[
            IdentitySource(
                source="crm", id_field="crm_id", match_key_field="email_hash"
            ),
            IdentitySource(
                source="billing",
                id_field="billing_id",
                match_key_field="email_hash",
            ),
        ],
    )


@pytest.fixture()
def events_table() -> ibis.expr.types.Table:
    """Create a sample events table with events from two sources."""
    df = pd.DataFrame(
        {
            "source": ["crm", "billing", "crm", "billing"],
            "entity_id": ["crm-1", "bill-1", "crm-2", "bill-2"],
            "ts": pd.to_datetime(
                [
                    "2026-01-01T00:00:00",
                    "2026-01-01T01:00:00",
                    "2026-01-02T00:00:00",
                    "2026-01-02T01:00:00",
                ]
            ),
            "event_type": [
                "row_appeared",
                "row_appeared",
                "row_appeared",
                "row_appeared",
            ],
            "payload": [
                '{"name": "alice", "email_hash": "abc123", "plan": "free"}',
                '{"amount": 100, "email_hash": "abc123", "currency": "USD"}',
                '{"name": "bob", "email_hash": "def456", "plan": "pro"}',
                '{"amount": 200, "email_hash": "xyz789", "currency": "EUR"}',
            ],
        }
    )
    return ibis.memtable(df)


class TestResolveIdentities:
    """Tests for resolve_identities function."""

    def test_returns_mapping_with_correct_columns(
        self, events_table: ibis.expr.types.Table, identity_graph: IdentityGraph
    ) -> None:
        """resolve_identities returns a mapping table with source, entity_id, canonical_id."""
        result = resolve_identities(events_table, identity_graph)
        result_df = result.execute()
        assert set(result_df.columns) == {"source", "entity_id", "canonical_id"}

    def test_same_match_key_gets_same_canonical_id(
        self, events_table: ibis.expr.types.Table, identity_graph: IdentityGraph
    ) -> None:
        """Two events from different sources with same match key value get the same canonical_id."""
        result = resolve_identities(events_table, identity_graph)
        result_df = result.execute()

        # crm-1 and bill-1 both have email_hash="abc123"
        crm1_cid = result_df[result_df["entity_id"] == "crm-1"][
            "canonical_id"
        ].iloc[0]
        bill1_cid = result_df[result_df["entity_id"] == "bill-1"][
            "canonical_id"
        ].iloc[0]
        assert crm1_cid == bill1_cid

    def test_different_match_keys_get_different_canonical_ids(
        self, events_table: ibis.expr.types.Table, identity_graph: IdentityGraph
    ) -> None:
        """Events from different match keys get different canonical_ids."""
        result = resolve_identities(events_table, identity_graph)
        result_df = result.execute()

        # crm-1 has email_hash="abc123", crm-2 has email_hash="def456"
        crm1_cid = result_df[result_df["entity_id"] == "crm-1"][
            "canonical_id"
        ].iloc[0]
        crm2_cid = result_df[result_df["entity_id"] == "crm-2"][
            "canonical_id"
        ].iloc[0]
        assert crm1_cid != crm2_cid

    def test_canonical_id_is_deterministic(
        self, events_table: ibis.expr.types.Table, identity_graph: IdentityGraph
    ) -> None:
        """canonical_id is deterministic: same match key always produces same canonical_id."""
        result1 = resolve_identities(events_table, identity_graph)
        result2 = resolve_identities(events_table, identity_graph)

        df1 = result1.execute().sort_values("entity_id").reset_index(drop=True)
        df2 = result2.execute().sort_values("entity_id").reset_index(drop=True)
        pd.testing.assert_frame_equal(df1, df2)

        # Verify the hash is sha256-based
        expected = hashlib.sha256(b"abc123").hexdigest()[:16]
        crm1_cid = df1[df1["entity_id"] == "crm-1"]["canonical_id"].iloc[0]
        assert crm1_cid == expected

    def test_only_row_appeared_events_are_used(
        self, identity_graph: IdentityGraph
    ) -> None:
        """Only row_appeared events are used for identity resolution."""
        df = pd.DataFrame(
            {
                "source": ["crm", "crm"],
                "entity_id": ["crm-1", "crm-1"],
                "ts": pd.to_datetime(
                    ["2026-01-01T00:00:00", "2026-01-01T01:00:00"]
                ),
                "event_type": ["row_appeared", "row_changed"],
                "payload": [
                    '{"email_hash": "abc123"}',
                    '{"email_hash": "abc123"}',
                ],
            }
        )
        events = ibis.memtable(df)
        result = resolve_identities(events, identity_graph)
        result_df = result.execute()
        assert len(result_df) == 1


class TestEnrichEvents:
    """Tests for enrich_events function."""

    def test_adds_canonical_id_column(
        self, events_table: ibis.expr.types.Table, identity_graph: IdentityGraph
    ) -> None:
        """enrich_events adds canonical_id column to event table."""
        mapping = resolve_identities(events_table, identity_graph)
        result = enrich_events(events_table, mapping)
        result_df = result.execute()
        assert "canonical_id" in result_df.columns

    def test_mapped_sources_get_resolved_canonical_id(
        self, events_table: ibis.expr.types.Table, identity_graph: IdentityGraph
    ) -> None:
        """Events from mapped sources get the resolved canonical_id."""
        mapping = resolve_identities(events_table, identity_graph)
        result = enrich_events(events_table, mapping)
        result_df = result.execute()

        expected_cid = hashlib.sha256(b"abc123").hexdigest()[:16]
        crm1_row = result_df[result_df["entity_id"] == "crm-1"]
        assert crm1_row["canonical_id"].iloc[0] == expected_cid

    def test_unmapped_sources_get_entity_id_as_fallback(self) -> None:
        """Events from unmapped sources get entity_id as canonical_id (fallback)."""
        events_df = pd.DataFrame(
            {
                "source": ["unknown_source"],
                "entity_id": ["unk-1"],
                "ts": pd.to_datetime(["2026-01-01T00:00:00"]),
                "event_type": ["row_appeared"],
                "payload": ['{"foo": "bar"}'],
            }
        )
        events = ibis.memtable(events_df)
        mapping = ibis.memtable(
            pd.DataFrame(columns=["source", "entity_id", "canonical_id"])
        )
        result = enrich_events(events, mapping)
        result_df = result.execute()
        assert result_df["canonical_id"].iloc[0] == "unk-1"

    def test_output_has_six_columns(
        self, events_table: ibis.expr.types.Table, identity_graph: IdentityGraph
    ) -> None:
        """Output has 6 columns: source, entity_id, ts, event_type, payload, canonical_id."""
        mapping = resolve_identities(events_table, identity_graph)
        result = enrich_events(events_table, mapping)
        result_df = result.execute()
        assert list(result_df.columns) == [
            "source",
            "entity_id",
            "ts",
            "event_type",
            "payload",
            "canonical_id",
        ]

    def test_second_enrich_call_preserves_canonical_id(self) -> None:
        """Calling enrich_events twice (multi-graph scenario) does not break.

        Regression: pandas merge would suffix canonical_id_x/canonical_id_y on
        the second call, causing KeyError: 'canonical_id'.
        """
        events_df = pd.DataFrame(
            {
                "source": ["crm", "billing"],
                "entity_id": ["crm-1", "bill-1"],
                "ts": pd.to_datetime(["2026-01-01", "2026-01-02"]),
                "event_type": ["row_appeared", "row_appeared"],
                "payload": ['{"email": "a@x.com"}', '{"phone": "555"}'],
            }
        )
        events = ibis.memtable(events_df)

        # First mapping covers crm
        mapping1 = ibis.memtable(
            pd.DataFrame(
                {
                    "source": ["crm"],
                    "entity_id": ["crm-1"],
                    "canonical_id": ["CAN-EMAIL"],
                }
            )
        )
        # Second mapping covers billing
        mapping2 = ibis.memtable(
            pd.DataFrame(
                {
                    "source": ["billing"],
                    "entity_id": ["bill-1"],
                    "canonical_id": ["CAN-PHONE"],
                }
            )
        )

        enriched = enrich_events(events, mapping1)
        enriched = enrich_events(enriched, mapping2)
        result_df = enriched.execute()

        assert "canonical_id" in result_df.columns
        assert "canonical_id_x" not in result_df.columns
        assert "canonical_id_y" not in result_df.columns

        crm_row = result_df[result_df["entity_id"] == "crm-1"].iloc[0]
        bill_row = result_df[result_df["entity_id"] == "bill-1"].iloc[0]
        # First-graph enrichment must be preserved through the second call
        assert crm_row["canonical_id"] == "CAN-EMAIL"
        assert bill_row["canonical_id"] == "CAN-PHONE"


# ---------------------------------------------------------------------------
# M051 regression tests (issues #91, #92)
# ---------------------------------------------------------------------------


class TestM051RegressionEntityIdFallback:
    """Issue #91: resolve_identities must fall back to entity_id when
    match_key_field == id_field (SnapshotDiff strips id_field from payload)."""

    def test_resolve_identities_falls_back_to_entity_id_when_match_key_equals_id_field(
        self,
    ) -> None:
        graph = IdentityGraph(
            name="g",
            canonical_id="canonical",
            sources=[
                IdentitySource(
                    source="users", id_field="user_id", match_key_field="user_id"
                ),
                IdentitySource(
                    source="other", id_field="other_id", match_key_field="user_id"
                ),
            ],
        )
        # Payload does NOT contain user_id (stripped by SnapshotDiff).
        events = ibis.memtable(
            pd.DataFrame(
                {
                    "source": ["users", "users"],
                    "entity_id": ["u1", "u2"],
                    "ts": ["2026-04-01", "2026-04-01"],
                    "event_type": ["row_appeared", "row_appeared"],
                    "payload": ['{"name": "alice"}', '{"name": "bob"}'],
                }
            )
        )
        result = resolve_identities(events, graph).execute()
        assert len(result) == 2
        expected_u1 = hashlib.sha256(b"u1").hexdigest()[:16]
        expected_u2 = hashlib.sha256(b"u2").hexdigest()[:16]
        row_u1 = result[result["entity_id"] == "u1"].iloc[0]
        row_u2 = result[result["entity_id"] == "u2"].iloc[0]
        assert row_u1["canonical_id"] == expected_u1
        assert row_u2["canonical_id"] == expected_u2


class TestM051RegressionEventSourceEvents:
    """Issue #92: resolve_identities must include EventSource events (any
    non-field_changed event_type), not just row_appeared."""

    def test_resolve_identities_includes_event_source_events(self) -> None:
        graph = IdentityGraph(
            name="g",
            canonical_id="canonical",
            sources=[
                IdentitySource(
                    source="ga4", id_field="event_id", match_key_field="user_pseudo_id"
                ),
                IdentitySource(
                    source="crm", id_field="crm_id", match_key_field="user_pseudo_id"
                ),
            ],
        )
        events = ibis.memtable(
            pd.DataFrame(
                {
                    "source": ["ga4", "ga4"],
                    "entity_id": ["e1", "e2"],
                    "ts": ["2026-04-01", "2026-04-01"],
                    "event_type": ["session_start", "form_submit"],
                    "payload": [
                        '{"user_pseudo_id": "pseudo-abc"}',
                        '{"user_pseudo_id": "pseudo-def"}',
                    ],
                }
            )
        )
        result = resolve_identities(events, graph).execute()
        assert len(result) == 2
        assert set(result["entity_id"]) == {"e1", "e2"}
        expected_abc = hashlib.sha256(b"pseudo-abc").hexdigest()[:16]
        assert (
            result[result["entity_id"] == "e1"].iloc[0]["canonical_id"]
            == expected_abc
        )

    def test_resolve_identities_still_excludes_field_changed_events(self) -> None:
        """Safeguard: field_changed events must NOT contribute to the mapping
        (their payload shape is {field_name, old_value, new_value})."""
        graph = IdentityGraph(
            name="g",
            canonical_id="canonical",
            sources=[
                IdentitySource(
                    source="crm", id_field="crm_id", match_key_field="email_hash"
                ),
                IdentitySource(
                    source="billing",
                    id_field="billing_id",
                    match_key_field="email_hash",
                ),
            ],
        )
        events = ibis.memtable(
            pd.DataFrame(
                {
                    "source": ["crm", "crm"],
                    "entity_id": ["c1", "c2"],
                    "ts": ["2026-04-01", "2026-04-01"],
                    "event_type": ["row_appeared", "field_changed"],
                    "payload": [
                        '{"email_hash": "abc"}',
                        '{"field_name": "email_hash", "old_value": "old", "new_value": "new"}',
                    ],
                }
            )
        )
        result = resolve_identities(events, graph).execute()
        # Only the row_appeared event contributes
        assert len(result) == 1
        assert result.iloc[0]["entity_id"] == "c1"
