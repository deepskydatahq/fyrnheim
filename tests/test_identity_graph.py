"""Tests for IdentityGraph and IdentitySource top-level asset models."""

import pytest
from pydantic import ValidationError

from fyrnheim.core.identity import IdentityGraph, IdentitySource


class TestIdentitySource:
    """Tests for IdentitySource model."""

    def test_valid_identity_source(self) -> None:
        src = IdentitySource(
            source="crm",
            id_field="contact_id",
            match_key_field="email_hash",
        )
        assert src.source == "crm"
        assert src.id_field == "contact_id"
        assert src.match_key_field == "email_hash"

    def test_missing_match_key_field_raises(self) -> None:
        with pytest.raises(ValidationError):
            IdentitySource(source="crm", id_field="contact_id")  # type: ignore[call-arg]

    def test_empty_source_raises(self) -> None:
        with pytest.raises(ValidationError):
            IdentitySource(source="", id_field="contact_id", match_key_field="email_hash")

    def test_empty_id_field_raises(self) -> None:
        with pytest.raises(ValidationError):
            IdentitySource(source="crm", id_field="", match_key_field="email_hash")

    def test_empty_match_key_field_raises(self) -> None:
        with pytest.raises(ValidationError):
            IdentitySource(source="crm", id_field="contact_id", match_key_field="")


class TestIdentityGraph:
    """Tests for IdentityGraph model."""

    @pytest.fixture()
    def two_sources(self) -> list[IdentitySource]:
        return [
            IdentitySource(source="crm", id_field="contact_id", match_key_field="email_hash"),
            IdentitySource(source="web", id_field="visitor_id", match_key_field="email_hash"),
        ]

    def test_valid_identity_graph(self, two_sources: list[IdentitySource]) -> None:
        graph = IdentityGraph(
            name="customer",
            canonical_id="customer_id",
            sources=two_sources,
        )
        assert graph.name == "customer"
        assert graph.canonical_id == "customer_id"
        assert len(graph.sources) == 2
        assert graph.resolution_strategy == "match_key"

    def test_resolution_strategy_defaults_to_match_key(self, two_sources: list[IdentitySource]) -> None:
        graph = IdentityGraph(
            name="customer",
            canonical_id="customer_id",
            sources=two_sources,
        )
        assert graph.resolution_strategy == "match_key"

    def test_fewer_than_two_sources_raises(self) -> None:
        with pytest.raises(ValidationError):
            IdentityGraph(
                name="customer",
                canonical_id="customer_id",
                sources=[
                    IdentitySource(source="crm", id_field="contact_id", match_key_field="email_hash"),
                ],
            )

    def test_empty_sources_raises(self) -> None:
        with pytest.raises(ValidationError):
            IdentityGraph(
                name="customer",
                canonical_id="customer_id",
                sources=[],
            )

    def test_missing_name_raises(self, two_sources: list[IdentitySource]) -> None:
        with pytest.raises(ValidationError):
            IdentityGraph(
                name="",
                canonical_id="customer_id",
                sources=two_sources,
            )

    def test_missing_name_field_raises(self, two_sources: list[IdentitySource]) -> None:
        with pytest.raises(ValidationError):
            IdentityGraph(  # type: ignore[call-arg]
                canonical_id="customer_id",
                sources=two_sources,
            )

    def test_missing_canonical_id_raises(self, two_sources: list[IdentitySource]) -> None:
        with pytest.raises(ValidationError):
            IdentityGraph(
                name="customer",
                canonical_id="",
                sources=two_sources,
            )


class TestIdentityGraphExport:
    """Test that models are correctly exported from core package."""

    def test_import_from_core(self) -> None:
        from fyrnheim.core import IdentityGraph as IG, IdentitySource as IS

        assert IG is IdentityGraph
        assert IS is IdentitySource

    def test_import_from_top_level(self) -> None:
        from fyrnheim import IdentityGraph as IG, IdentitySource as IS

        assert IG is IdentityGraph
        assert IS is IdentitySource

    def test_no_clash_with_old_types(self) -> None:
        from fyrnheim.core import (
            IdentityGraph,
            IdentityGraphConfig,
            IdentityGraphSource,
            IdentitySource,
        )

        # All four types are distinct
        assert IdentityGraph is not IdentityGraphConfig
        assert IdentitySource is not IdentityGraphSource
