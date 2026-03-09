"""Tests for executor handling of inline identity graph sources."""

import pytest
import pyarrow as pa
import pyarrow.parquet as pq

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


class TestExecutorInlineSources:
    """Executor correctly handles inline TableSource in identity graphs."""

    @pytest.fixture()
    def all_inline_result(self, tmp_path):
        """Two inline sources joined by identity graph."""
        # Create parquet files
        leads_table = pa.table({
            "lead_email": ["alice@ex.com", "bob@ex.com", "carol@ex.com"],
            "lead_name": ["Alice L", "Bob L", "Carol L"],
        })
        leads_path = tmp_path / "leads.parquet"
        pq.write_table(leads_table, leads_path)

        contacts_table = pa.table({
            "contact_email": ["bob@ex.com", "dave@ex.com"],
            "contact_name": ["Bob C", "Dave C"],
        })
        contacts_path = tmp_path / "contacts.parquet"
        pq.write_table(contacts_table, contacts_path)

        # Define entity with all-inline sources
        person_entity = Entity(
            name="person",
            description="Unified person from inline sources",
            source=DerivedSource(
                identity_graph="person_graph",
                identity_graph_config=IdentityGraphConfig(
                    match_key="email",
                    sources=[
                        IdentityGraphSource(
                            name="leads",
                            source=TableSource(
                                project="p", dataset="d", table="leads",
                                duckdb_path=str(leads_path),
                            ),
                            match_key_field="lead_email",
                            fields={"name": "lead_name"},
                        ),
                        IdentityGraphSource(
                            name="contacts",
                            source=TableSource(
                                project="p", dataset="d", table="contacts",
                                duckdb_path=str(contacts_path),
                            ),
                            match_key_field="contact_email",
                            fields={"name": "contact_name"},
                        ),
                    ],
                    priority=["leads", "contacts"],
                ),
            ),
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_person"),
                dimension=DimensionLayer(model_name="dim_person"),
            ),
        )

        # Generate and execute
        generated_dir = tmp_path / "generated"
        generate(person_entity, output_dir=generated_dir)

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=generated_dir) as executor:
            result = executor.execute("person", entity=person_entity)
            df = executor.connection.table(result.target_name).to_pandas()

        return result, df

    def test_all_inline_row_count(self, all_inline_result):
        """All-inline identity graph produces correct row count (4 = 3+2 minus 1 overlap)."""
        result, df = all_inline_result
        assert result.row_count == 4
        assert len(df) == 4

    def test_all_inline_priority_coalesce(self, all_inline_result):
        """Priority coalesce picks leads (first priority) for overlapping name."""
        _, df = all_inline_result
        bob = df[df["email"] == "bob@ex.com"].iloc[0]
        assert bob["name"] == "Bob L"

    def test_all_inline_source_flags(self, all_inline_result):
        """Source flags correctly set for inline sources."""
        _, df = all_inline_result
        bob = df[df["email"] == "bob@ex.com"].iloc[0]
        assert bob["is_leads"] == True
        assert bob["is_contacts"] == True
        alice = df[df["email"] == "alice@ex.com"].iloc[0]
        assert alice["is_leads"] == True
        assert alice["is_contacts"] == False

    @pytest.fixture()
    def mixed_result(self, tmp_path):
        """Mixed inline + entity-reference sources in identity graph."""
        # Create parquet files for entity-reference source
        hubspot_table = pa.table({
            "person_id": ["h1", "h2", "h3"],
            "email": ["alice@ex.com", "bob@ex.com", "carol@ex.com"],
            "full_name": ["Alice H", "Bob H", "Carol H"],
        })
        hubspot_path = tmp_path / "hubspot_person.parquet"
        pq.write_table(hubspot_table, hubspot_path)

        # Create parquet file for inline source
        raw_leads_table = pa.table({
            "lead_email": ["bob@ex.com", "dave@ex.com"],
            "lead_name": ["Bob R", "Dave R"],
        })
        raw_leads_path = tmp_path / "raw_leads.parquet"
        pq.write_table(raw_leads_table, raw_leads_path)

        # Entity-reference source entity
        hubspot_entity = Entity(
            name="hubspot_person",
            description="HubSpot contacts",
            source=TableSource(
                project="test", dataset="test", table="hubspot_person",
                duckdb_path=str(hubspot_path),
            ),
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_hubspot_person"),
                dimension=DimensionLayer(model_name="dim_hubspot_person"),
            ),
        )

        # Derived entity with mixed sources
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
                            entity="hubspot_person",
                            match_key_field="email",
                            fields={"name": "full_name"},
                            id_field="person_id",
                        ),
                        IdentityGraphSource(
                            name="raw_leads",
                            source=TableSource(
                                project="p", dataset="d", table="raw_leads",
                                duckdb_path=str(raw_leads_path),
                            ),
                            match_key_field="lead_email",
                            fields={"name": "lead_name"},
                        ),
                    ],
                    priority=["hubspot", "raw_leads"],
                ),
            ),
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_person"),
                dimension=DimensionLayer(model_name="dim_person"),
            ),
        )

        # Generate and execute
        generated_dir = tmp_path / "generated"
        generate(hubspot_entity, output_dir=generated_dir)
        generate(person_entity, output_dir=generated_dir)

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=generated_dir) as executor:
            # Execute dependency first
            executor.execute("hubspot_person")
            result = executor.execute("person", entity=person_entity)
            df = executor.connection.table(result.target_name).to_pandas()

        return result, df

    def test_mixed_row_count(self, mixed_result):
        """Mixed inline+entity produces correct row count (4 = 3+2 minus 1 overlap)."""
        result, df = mixed_result
        assert result.row_count == 4

    def test_mixed_priority_coalesce(self, mixed_result):
        """Priority coalesce picks hubspot (entity-ref, first priority) over raw_leads (inline)."""
        _, df = mixed_result
        bob = df[df["email"] == "bob@ex.com"].iloc[0]
        assert bob["name"] == "Bob H"
        dave = df[df["email"] == "dave@ex.com"].iloc[0]
        assert dave["name"] == "Dave R"

    def test_mixed_source_flags(self, mixed_result):
        """Source flags correctly set for mixed sources."""
        _, df = mixed_result
        bob = df[df["email"] == "bob@ex.com"].iloc[0]
        assert bob["is_hubspot"] == True
        assert bob["is_raw_leads"] == True

    @pytest.fixture()
    def prep_columns_result(self, tmp_path):
        """Inline source with prep_columns applying transforms before join."""
        leads_table = pa.table({
            "lead_email": ["Alice@EX.com", "Bob@EX.com"],
            "lead_name": ["Alice", "Bob"],
        })
        leads_path = tmp_path / "leads.parquet"
        pq.write_table(leads_table, leads_path)

        contacts_table = pa.table({
            "contact_email": ["bob@ex.com", "dave@ex.com"],
            "contact_name": ["Bob C", "Dave C"],
        })
        contacts_path = tmp_path / "contacts.parquet"
        pq.write_table(contacts_table, contacts_path)

        person_entity = Entity(
            name="person",
            description="Person with prep_columns",
            source=DerivedSource(
                identity_graph="person_graph",
                identity_graph_config=IdentityGraphConfig(
                    match_key="email",
                    sources=[
                        IdentityGraphSource(
                            name="leads",
                            source=TableSource(
                                project="p", dataset="d", table="leads",
                                duckdb_path=str(leads_path),
                            ),
                            match_key_field="email",
                            fields={"name": "lead_name"},
                            prep_columns=[
                                ComputedColumn(
                                    name="email",
                                    expression="t.lead_email.lower()",
                                ),
                            ],
                        ),
                        IdentityGraphSource(
                            name="contacts",
                            source=TableSource(
                                project="p", dataset="d", table="contacts",
                                duckdb_path=str(contacts_path),
                            ),
                            match_key_field="contact_email",
                            fields={"name": "contact_name"},
                        ),
                    ],
                    priority=["leads", "contacts"],
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

    def test_prep_columns_applied(self, prep_columns_result):
        """Prep columns lowercased email enabling correct join."""
        _, df = prep_columns_result
        # bob@ex.com should match between leads (lowered) and contacts
        bob = df[df["email"] == "bob@ex.com"]
        assert len(bob) == 1
        assert bob.iloc[0]["is_leads"] == True
        assert bob.iloc[0]["is_contacts"] == True

    def test_prep_columns_row_count(self, prep_columns_result):
        """Correct row count after prep_columns transform (3 = 2+2 minus 1 overlap)."""
        result, df = prep_columns_result
        assert result.row_count == 3
