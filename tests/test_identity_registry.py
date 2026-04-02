"""Tests for IdentityGraphRegistry and e2e identity resolution."""

from __future__ import annotations

import hashlib
import textwrap
from pathlib import Path

import ibis
import pandas as pd
import pytest

from fyrnheim.core.identity import IdentityGraph, IdentitySource
from fyrnheim.engine.identity_engine import enrich_events, resolve_identities
from fyrnheim.engine.identity_registry import IdentityGraphRegistry


class TestIdentityGraphRegistry:
    """Tests for IdentityGraphRegistry discovery and retrieval."""

    def test_discover_finds_identity_graph_variable(self, tmp_path: Path) -> None:
        """IdentityGraphRegistry.discover(dir) finds module-level 'identity_graph' variables."""
        graph_dir = tmp_path / "graphs"
        graph_dir.mkdir()
        (graph_dir / "user_graph.py").write_text(
            textwrap.dedent("""\
                from fyrnheim.core.identity import IdentityGraph, IdentitySource

                identity_graph = IdentityGraph(
                    name="user_graph",
                    canonical_id="canonical_user_id",
                    sources=[
                        IdentitySource(source="crm", id_field="crm_id", match_key_field="email_hash"),
                        IdentitySource(source="billing", id_field="billing_id", match_key_field="email_hash"),
                    ],
                )
            """)
        )

        registry = IdentityGraphRegistry()
        registry.discover(graph_dir)

        assert len(registry) == 1
        assert "user_graph" in registry

    def test_get_returns_identity_graph_by_name(self, tmp_path: Path) -> None:
        """IdentityGraphRegistry.get(name) returns the IdentityGraph by name."""
        graph_dir = tmp_path / "graphs"
        graph_dir.mkdir()
        (graph_dir / "user_graph.py").write_text(
            textwrap.dedent("""\
                from fyrnheim.core.identity import IdentityGraph, IdentitySource

                identity_graph = IdentityGraph(
                    name="user_graph",
                    canonical_id="canonical_user_id",
                    sources=[
                        IdentitySource(source="crm", id_field="crm_id", match_key_field="email_hash"),
                        IdentitySource(source="billing", id_field="billing_id", match_key_field="email_hash"),
                    ],
                )
            """)
        )

        registry = IdentityGraphRegistry()
        registry.discover(graph_dir)

        graph = registry.get("user_graph")
        assert isinstance(graph, IdentityGraph)
        assert graph.name == "user_graph"
        assert len(graph.sources) == 2

    def test_get_raises_key_error_for_unknown_name(self) -> None:
        """get() raises KeyError for unknown graph names."""
        registry = IdentityGraphRegistry()
        with pytest.raises(KeyError, match="not found"):
            registry.get("nonexistent")

    def test_discover_raises_on_missing_directory(self) -> None:
        """discover() raises FileNotFoundError for missing directory."""
        registry = IdentityGraphRegistry()
        with pytest.raises(FileNotFoundError):
            registry.discover(Path("/nonexistent/path"))

    def test_discover_raises_on_duplicate_name(self, tmp_path: Path) -> None:
        """discover() raises ValueError on duplicate graph names."""
        graph_dir = tmp_path / "graphs"
        graph_dir.mkdir()
        for fname in ("graph_a.py", "graph_b.py"):
            (graph_dir / fname).write_text(
                textwrap.dedent("""\
                    from fyrnheim.core.identity import IdentityGraph, IdentitySource

                    identity_graph = IdentityGraph(
                        name="duplicate_graph",
                        canonical_id="cid",
                        sources=[
                            IdentitySource(source="s1", id_field="id1", match_key_field="mk"),
                            IdentitySource(source="s2", id_field="id2", match_key_field="mk"),
                        ],
                    )
                """)
            )

        registry = IdentityGraphRegistry()
        with pytest.raises(ValueError, match="Duplicate"):
            registry.discover(graph_dir)


class TestE2eIdentityResolution:
    """End-to-end tests for multi-source identity resolution."""

    @pytest.fixture()
    def three_source_graph(self) -> IdentityGraph:
        """Identity graph linking crm, billing, and pageviews via email_hash."""
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
                IdentitySource(
                    source="pageviews",
                    id_field="session_id",
                    match_key_field="email_hash",
                ),
            ],
        )

    @pytest.fixture()
    def multi_source_events(self) -> ibis.expr.types.Table:
        """Events from 4 sources: crm, billing, pageviews, and support (not in graph)."""
        df = pd.DataFrame(
            {
                "source": [
                    "crm",
                    "billing",
                    "pageviews",
                    "crm",
                    "billing",
                    "support",
                    "support",
                ],
                "entity_id": [
                    "crm-1",
                    "bill-1",
                    "sess-1",
                    "crm-2",
                    "bill-2",
                    "ticket-1",
                    "ticket-2",
                ],
                "ts": pd.to_datetime(
                    [
                        "2026-01-01T00:00:00",
                        "2026-01-01T01:00:00",
                        "2026-01-01T02:00:00",
                        "2026-01-02T00:00:00",
                        "2026-01-02T01:00:00",
                        "2026-01-03T00:00:00",
                        "2026-01-03T01:00:00",
                    ]
                ),
                "event_type": [
                    "row_appeared",
                    "row_appeared",
                    "row_appeared",
                    "row_appeared",
                    "row_appeared",
                    "row_appeared",
                    "row_appeared",
                ],
                "payload": [
                    '{"name": "alice", "email_hash": "shared_hash_1", "plan": "free"}',
                    '{"amount": 100, "email_hash": "shared_hash_1", "currency": "USD"}',
                    '{"url": "/home", "email_hash": "shared_hash_1", "referrer": "google"}',
                    '{"name": "bob", "email_hash": "shared_hash_2", "plan": "pro"}',
                    '{"amount": 200, "email_hash": "shared_hash_2", "currency": "EUR"}',
                    '{"issue": "login", "email_hash": "support_hash"}',
                    '{"issue": "billing", "email_hash": "another_hash"}',
                ],
            }
        )
        return ibis.memtable(df)

    def test_three_sources_shared_email_get_same_canonical_id(
        self,
        three_source_graph: IdentityGraph,
        multi_source_events: ibis.expr.types.Table,
    ) -> None:
        """Events from 3 sources (crm, billing, pageviews) with shared email_hash get unified canonical_id."""
        mapping = resolve_identities(multi_source_events, three_source_graph)
        enriched = enrich_events(multi_source_events, mapping)
        result_df = enriched.execute()

        expected_cid_1 = hashlib.sha256(b"shared_hash_1").hexdigest()[:16]
        expected_cid_2 = hashlib.sha256(b"shared_hash_2").hexdigest()[:16]

        # crm-1, bill-1, sess-1 all share shared_hash_1
        crm1 = result_df[result_df["entity_id"] == "crm-1"]["canonical_id"].iloc[0]
        bill1 = result_df[result_df["entity_id"] == "bill-1"]["canonical_id"].iloc[0]
        sess1 = result_df[result_df["entity_id"] == "sess-1"]["canonical_id"].iloc[0]
        assert crm1 == bill1 == sess1 == expected_cid_1

        # crm-2, bill-2 share shared_hash_2
        crm2 = result_df[result_df["entity_id"] == "crm-2"]["canonical_id"].iloc[0]
        bill2 = result_df[result_df["entity_id"] == "bill-2"]["canonical_id"].iloc[0]
        assert crm2 == bill2 == expected_cid_2

        # Different groups have different canonical_ids
        assert crm1 != crm2

    def test_unmapped_source_retains_entity_id(
        self,
        three_source_graph: IdentityGraph,
        multi_source_events: ibis.expr.types.Table,
    ) -> None:
        """Events from a source not in the graph retain entity_id as canonical_id."""
        mapping = resolve_identities(multi_source_events, three_source_graph)
        enriched = enrich_events(multi_source_events, mapping)
        result_df = enriched.execute()

        # support source is not in the graph
        ticket1 = result_df[result_df["entity_id"] == "ticket-1"]
        ticket2 = result_df[result_df["entity_id"] == "ticket-2"]
        assert ticket1["canonical_id"].iloc[0] == "ticket-1"
        assert ticket2["canonical_id"].iloc[0] == "ticket-2"
