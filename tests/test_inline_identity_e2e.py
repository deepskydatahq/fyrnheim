"""End-to-end integration tests for inline identity graph sources.

Tests the full generate -> execute pipeline with inline TableSource
references in identity graphs.
"""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from fyrnheim import (
    ComputedColumn,
    DerivedSource,
    DimensionLayer,
    Entity,
    LayersConfig,
    PrepLayer,
    TableSource,
)
from fyrnheim._generate import generate
from fyrnheim.core.source import IdentityGraphConfig, IdentityGraphSource
from fyrnheim.engine.connection import create_connection
from fyrnheim.engine.executor import IbisExecutor
from fyrnheim.engine.resolution import extract_dependencies


class TestE2EInlineIdentityGraph:
    """Full pipeline e2e: inline sources through generate + execute."""

    @pytest.fixture()
    def two_inline_sources(self, tmp_path):
        """Two inline sources joined on email, producing unified person."""
        # Source A: CRM leads
        leads = pa.table({
            "lead_email": ["alice@co.com", "bob@co.com", "carol@co.com"],
            "lead_name": ["Alice Lead", "Bob Lead", "Carol Lead"],
            "lead_score": [90, 70, 85],
        })
        leads_path = tmp_path / "leads.parquet"
        pq.write_table(leads, leads_path)

        # Source B: Support tickets
        tickets = pa.table({
            "ticket_email": ["bob@co.com", "dave@co.com"],
            "ticket_name": ["Bob Ticket", "Dave Ticket"],
            "ticket_count": [5, 3],
        })
        tickets_path = tmp_path / "tickets.parquet"
        pq.write_table(tickets, tickets_path)

        person_entity = Entity(
            name="person",
            description="Unified person from CRM + support",
            source=DerivedSource(
                identity_graph="person_graph",
                identity_graph_config=IdentityGraphConfig(
                    match_key="email",
                    sources=[
                        IdentityGraphSource(
                            name="crm",
                            source=TableSource(
                                project="p", dataset="d", table="leads",
                                duckdb_path=str(leads_path),
                            ),
                            match_key_field="lead_email",
                            fields={"name": "lead_name"},
                        ),
                        IdentityGraphSource(
                            name="support",
                            source=TableSource(
                                project="p", dataset="d", table="tickets",
                                duckdb_path=str(tickets_path),
                            ),
                            match_key_field="ticket_email",
                            fields={"name": "ticket_name"},
                        ),
                    ],
                    priority=["crm", "support"],
                ),
            ),
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_person"),
                dimension=DimensionLayer(model_name="dim_person"),
            ),
        )

        generated_dir = tmp_path / "generated"
        generate(person_entity, output_dir=generated_dir)

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=generated_dir) as executor:
            result = executor.execute("person", entity=person_entity)
            df = executor.connection.table(result.target_name).to_pandas()

        return result, df, person_entity

    def test_two_inline_correct_output(self, two_inline_sources):
        """4 rows (3+2 with 1 overlap) with correct join result."""
        result, df, _ = two_inline_sources
        assert result.row_count == 4
        assert result.success is True
        emails = sorted(df["email"].tolist())
        assert emails == ["alice@co.com", "bob@co.com", "carol@co.com", "dave@co.com"]

    def test_two_inline_priority_coalesce(self, two_inline_sources):
        """Name uses CRM (priority 1) over support for overlap."""
        _, df, _ = two_inline_sources
        bob = df[df["email"] == "bob@co.com"].iloc[0]
        assert bob["name"] == "Bob Lead"

    def test_two_inline_source_flags(self, two_inline_sources):
        """Source flags correct for each record."""
        _, df, _ = two_inline_sources
        bob = df[df["email"] == "bob@co.com"].iloc[0]
        assert bool(bob["is_crm"]) is True
        assert bool(bob["is_support"]) is True
        alice = df[df["email"] == "alice@co.com"].iloc[0]
        assert bool(alice["is_crm"]) is True
        assert bool(alice["is_support"]) is False
        dave = df[df["email"] == "dave@co.com"].iloc[0]
        assert bool(dave["is_crm"]) is False
        assert bool(dave["is_support"]) is True


class TestE2EMixedInlineEntityRef:
    """Full pipeline e2e: mixed inline + entity-reference sources."""

    @pytest.fixture()
    def mixed_sources(self, tmp_path):
        """One entity-ref source and one inline source."""
        # Entity-ref parquet
        hubspot_data = pa.table({
            "person_id": ["h1", "h2", "h3"],
            "email": ["alice@ex.com", "bob@ex.com", "carol@ex.com"],
            "full_name": ["Alice Hub", "Bob Hub", "Carol Hub"],
        })
        hubspot_path = tmp_path / "hubspot.parquet"
        pq.write_table(hubspot_data, hubspot_path)

        # Inline parquet
        raw_events = pa.table({
            "event_email": ["bob@ex.com", "eve@ex.com"],
            "event_name": ["Bob Evt", "Eve Evt"],
        })
        events_path = tmp_path / "events.parquet"
        pq.write_table(raw_events, events_path)

        hubspot_entity = Entity(
            name="hubspot_contact",
            description="HubSpot contacts",
            source=TableSource(
                project="t", dataset="t", table="hubspot",
                duckdb_path=str(hubspot_path),
            ),
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_hubspot_contact"),
                dimension=DimensionLayer(model_name="dim_hubspot_contact"),
            ),
        )

        person_entity = Entity(
            name="person",
            description="Unified person",
            source=DerivedSource(
                identity_graph="person_graph",
                identity_graph_config=IdentityGraphConfig(
                    match_key="email",
                    sources=[
                        IdentityGraphSource(
                            name="hubspot",
                            entity="hubspot_contact",
                            match_key_field="email",
                            fields={"name": "full_name"},
                            id_field="person_id",
                        ),
                        IdentityGraphSource(
                            name="events",
                            source=TableSource(
                                project="p", dataset="d", table="events",
                                duckdb_path=str(events_path),
                            ),
                            match_key_field="event_email",
                            fields={"name": "event_name"},
                        ),
                    ],
                    priority=["hubspot", "events"],
                ),
            ),
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_person"),
                dimension=DimensionLayer(model_name="dim_person"),
            ),
        )

        generated_dir = tmp_path / "generated"
        generate(hubspot_entity, output_dir=generated_dir)
        generate(person_entity, output_dir=generated_dir)

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=generated_dir) as executor:
            executor.execute("hubspot_contact")
            result = executor.execute("person", entity=person_entity)
            df = executor.connection.table(result.target_name).to_pandas()

        return result, df, person_entity

    def test_mixed_correct_output(self, mixed_sources):
        """4 rows from 3 entity-ref + 2 inline with 1 overlap."""
        result, df, _ = mixed_sources
        assert result.row_count == 4
        assert result.success is True

    def test_mixed_priority_coalesce(self, mixed_sources):
        """HubSpot (entity-ref, first priority) wins over events (inline)."""
        _, df, _ = mixed_sources
        bob = df[df["email"] == "bob@ex.com"].iloc[0]
        assert bob["name"] == "Bob Hub"
        eve = df[df["email"] == "eve@ex.com"].iloc[0]
        assert eve["name"] == "Eve Evt"

    def test_mixed_entity_id_preserved(self, mixed_sources):
        """HubSpot ID preserved for entity-ref source."""
        _, df, _ = mixed_sources
        alice = df[df["email"] == "alice@ex.com"].iloc[0]
        assert alice["hubspot_id"] == "h1"


class TestE2EInlinePrepColumns:
    """Full pipeline e2e: inline sources with prep_columns transforms."""

    @pytest.fixture()
    def prep_result(self, tmp_path):
        """Inline source with prep_columns normalizing email case."""
        src_a = pa.table({
            "raw_email": ["ALICE@EX.COM", "BOB@EX.COM"],
            "name_a": ["Alice", "Bob"],
        })
        a_path = tmp_path / "src_a.parquet"
        pq.write_table(src_a, a_path)

        src_b = pa.table({
            "email_b": ["bob@ex.com", "carol@ex.com"],
            "name_b": ["Bob B", "Carol B"],
        })
        b_path = tmp_path / "src_b.parquet"
        pq.write_table(src_b, b_path)

        person_entity = Entity(
            name="person",
            description="Person with prep_columns",
            source=DerivedSource(
                identity_graph="person_graph",
                identity_graph_config=IdentityGraphConfig(
                    match_key="email",
                    sources=[
                        IdentityGraphSource(
                            name="src_a",
                            source=TableSource(
                                project="p", dataset="d", table="a",
                                duckdb_path=str(a_path),
                            ),
                            match_key_field="email",
                            fields={"name": "name_a"},
                            prep_columns=[
                                ComputedColumn(
                                    name="email",
                                    expression="t.raw_email.lower()",
                                    description="Lowercase email for matching",
                                ),
                            ],
                        ),
                        IdentityGraphSource(
                            name="src_b",
                            source=TableSource(
                                project="p", dataset="d", table="b",
                                duckdb_path=str(b_path),
                            ),
                            match_key_field="email_b",
                            fields={"name": "name_b"},
                        ),
                    ],
                    priority=["src_a", "src_b"],
                ),
            ),
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_person"),
                dimension=DimensionLayer(model_name="dim_person"),
            ),
        )

        generated_dir = tmp_path / "generated"
        generate(person_entity, output_dir=generated_dir)

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=generated_dir) as executor:
            result = executor.execute("person", entity=person_entity)
            df = executor.connection.table(result.target_name).to_pandas()

        return result, df

    def test_prep_columns_enable_join(self, prep_result):
        """prep_columns lowercase email enables bob to match across sources."""
        _, df = prep_result
        bob = df[df["email"] == "bob@ex.com"]
        assert len(bob) == 1
        assert bool(bob.iloc[0]["is_src_a"]) is True
        assert bool(bob.iloc[0]["is_src_b"]) is True

    def test_prep_columns_row_count(self, prep_result):
        """3 rows: alice, bob (overlapped), carol."""
        result, _ = prep_result
        assert result.row_count == 3

    def test_prep_columns_priority(self, prep_result):
        """src_a (first priority) name wins for overlapping bob."""
        _, df = prep_result
        bob = df[df["email"] == "bob@ex.com"].iloc[0]
        assert bob["name"] == "Bob"


class TestE2EInlineResolutionOrder:
    """Inline sources don't create false dependencies in resolution order."""

    def test_inline_sources_no_entity_dependency(self):
        """Entity with all-inline sources has no depends_on entries."""
        person_entity = Entity(
            name="person",
            description="All inline",
            source=DerivedSource(
                identity_graph="g",
                identity_graph_config=IdentityGraphConfig(
                    match_key="email",
                    sources=[
                        IdentityGraphSource(
                            name="a",
                            source=TableSource(
                                project="p", dataset="d", table="a",
                                duckdb_path="~/a.parquet",
                            ),
                            match_key_field="email",
                            fields={"name": "n"},
                        ),
                        IdentityGraphSource(
                            name="b",
                            source=TableSource(
                                project="p", dataset="d", table="b",
                                duckdb_path="~/b.parquet",
                            ),
                            match_key_field="email",
                            fields={"name": "n"},
                        ),
                    ],
                    priority=["a", "b"],
                ),
            ),
            layers=LayersConfig(prep=PrepLayer(model_name="prep_person")),
        )
        deps = extract_dependencies(person_entity)
        assert deps == []

    def test_mixed_inline_entity_only_entity_deps(self):
        """Mixed inline + entity-ref only produces entity-ref dependencies."""
        person_entity = Entity(
            name="person",
            description="Mixed",
            source=DerivedSource(
                identity_graph="g",
                identity_graph_config=IdentityGraphConfig(
                    match_key="email",
                    sources=[
                        IdentityGraphSource(
                            name="hubspot",
                            entity="hubspot_contact",
                            match_key_field="email",
                            fields={"name": "n"},
                        ),
                        IdentityGraphSource(
                            name="inline_src",
                            source=TableSource(
                                project="p", dataset="d", table="t",
                                duckdb_path="~/t.parquet",
                            ),
                            match_key_field="email",
                            fields={"name": "n"},
                        ),
                    ],
                    priority=["hubspot", "inline_src"],
                ),
            ),
            layers=LayersConfig(prep=PrepLayer(model_name="prep_person")),
        )
        deps = extract_dependencies(person_entity)
        assert deps == ["hubspot_contact"]
