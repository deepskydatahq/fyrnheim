"""E2E regression tests for IbisExecutor via create_connection on DuckDB.

Proves the full pipeline: entity defined -> code generated -> IbisExecutor
executes on DuckDB via the generic connection factory -> output verified.
This is the M004 regression gate.
"""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest

from fyrnheim import (
    ComputedColumn,
    DerivedSource,
    DimensionLayer,
    Entity,
    Field,
    LayersConfig,
    PrepLayer,
    SourceMapping,
    TableSource,
    UnionSource,
)
from fyrnheim.core.source import IdentityGraphConfig, IdentityGraphSource
from fyrnheim._generate import generate
from fyrnheim.engine.connection import create_connection
from fyrnheim.engine.executor import IbisExecutor

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def sample_parquet(tmp_path):
    """Create sample parquet data for E2E tests."""
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
        "amount_cents": [100, 2900, 9900, 0, 500],
        "plan": ["free", "pro", "enterprise", "free", "pro"],
    })
    path = tmp_path / "data" / "customers.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return path, len(df)


@pytest.fixture()
def entity_and_generated(sample_parquet, tmp_path):
    """Define entity, generate code, return (entity, generated_dir, parquet_path)."""
    parquet_path, row_count = sample_parquet
    generated_dir = tmp_path / "generated"

    entity = Entity(
        name="customers",
        description="E2E regression entity",
        source=TableSource(
            project="test", dataset="test", table="customers",
            duckdb_path=str(parquet_path),
        ),
        layers=LayersConfig(
            prep=PrepLayer(
                model_name="prep_customers",
                computed_columns=[
                    ComputedColumn(
                        name="amount_dollars",
                        expression="t.amount_cents / 100.0",
                        description="Amount in dollars",
                    ),
                ],
            ),
            dimension=DimensionLayer(
                model_name="dim_customers",
                computed_columns=[
                    ComputedColumn(
                        name="is_paying",
                        expression="t.plan != 'free'",
                        description="True if on a paid plan",
                    ),
                ],
            ),
        ),
    )

    generate(entity, output_dir=generated_dir)
    return entity, generated_dir, parquet_path, row_count


# ---------------------------------------------------------------------------
# E2E tests via generic IbisExecutor + create_connection
# ---------------------------------------------------------------------------


class TestE2EGenericIbisExecutor:
    """Full pipeline through generic IbisExecutor(create_connection(...))."""

    def test_generate_execute_verify(self, entity_and_generated):
        """Entity -> generate -> IbisExecutor.execute() on DuckDB -> verify output."""
        entity, generated_dir, _, row_count = entity_and_generated

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=generated_dir) as executor:
            result = executor.execute("customers")

            assert result.success is True
            assert result.entity_name == "customers"
            assert result.target_name == "dim_customers"
            assert result.row_count == row_count

    def test_output_schema_has_computed_columns(self, entity_and_generated):
        """Output table includes all original + computed columns."""
        _, generated_dir, _, _ = entity_and_generated

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=generated_dir) as executor:
            result = executor.execute("customers")
            df = executor.connection.table(result.target_name).to_pandas()

        assert "id" in df.columns
        assert "name" in df.columns
        assert "amount_dollars" in df.columns
        assert "is_paying" in df.columns

    def test_computed_values_correct(self, entity_and_generated):
        """Spot-check computed column values."""
        _, generated_dir, _, _ = entity_and_generated

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=generated_dir) as executor:
            executor.execute("customers")
            df = executor.connection.table("dim_customers").to_pandas()

        alice = df[df["id"] == 1].iloc[0]
        assert alice["amount_dollars"] == pytest.approx(1.0)
        assert not alice["is_paying"]

        bob = df[df["id"] == 2].iloc[0]
        assert bob["amount_dollars"] == pytest.approx(29.0)
        assert bob["is_paying"]

    def test_row_count_preserved(self, entity_and_generated):
        """No rows lost or duplicated through the pipeline."""
        _, generated_dir, _, row_count = entity_and_generated

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=generated_dir) as executor:
            result = executor.execute("customers")
            assert result.row_count == row_count

    def test_registered_source_path(self, entity_and_generated):
        """Pipeline works when source is pre-registered via register_parquet."""
        _, generated_dir, parquet_path, row_count = entity_and_generated

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=generated_dir) as executor:
            executor.register_parquet("source_customers", parquet_path)
            result = executor.execute("customers")
            assert result.success is True
            assert result.row_count == row_count


# ---------------------------------------------------------------------------
# Runner-level E2E via create_connection
# ---------------------------------------------------------------------------


class TestE2ERunnerWithConnectionFactory:
    """Test that runner.run() uses connection factory correctly."""

    def test_run_entity_duckdb(self, entity_and_generated):
        """run_entity() works end-to-end on DuckDB via connection factory."""
        from fyrnheim.engine.runner import run_entity

        entity, generated_dir, _, row_count = entity_and_generated
        data_dir = generated_dir.parent / "data"

        result = run_entity(
            entity, data_dir,
            backend="duckdb",
            generated_dir=generated_dir,
        )
        assert result.status == "success"
        assert result.row_count == row_count

    def test_run_full_pipeline_duckdb(self, sample_parquet, tmp_path):
        """run() discovers and executes entity on DuckDB."""
        from fyrnheim.engine.runner import run

        parquet_path, row_count = sample_parquet
        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()
        data_dir = parquet_path.parent
        generated_dir = tmp_path / "generated"

        entity_code = f"""\
from fyrnheim import Entity, LayersConfig, PrepLayer, TableSource

entity = Entity(
    name="customers",
    description="E2E runner test",
    source=TableSource(
        project="test", dataset="test", table="customers",
        duckdb_path="{parquet_path}",
    ),
    layers=LayersConfig(prep=PrepLayer(model_name="prep_customers")),
)
"""
        (entities_dir / "customers.py").write_text(entity_code)

        result = run(entities_dir, data_dir, backend="duckdb", generated_dir=generated_dir)
        assert result.ok is True
        assert len(result.entities) == 1
        assert result.entities[0].status == "success"
        assert result.entities[0].row_count == row_count


# ---------------------------------------------------------------------------
# Missing extras test
# ---------------------------------------------------------------------------


class TestMissingExtrasError:
    """Test that importing bigquery backend without extras gives helpful error."""

    def test_create_connection_bigquery_missing_extras(self):
        with patch.dict("sys.modules", {"ibis.backends.bigquery": None}):
            with pytest.raises(ImportError, match="BigQuery backend requires extra dependencies"):
                create_connection("bigquery", project_id="x", dataset_id="y")

    def test_error_message_includes_install_command(self):
        with patch.dict("sys.modules", {"ibis.backends.bigquery": None}):
            with pytest.raises(ImportError, match="pip install"):
                create_connection("bigquery", project_id="x", dataset_id="y")


# ---------------------------------------------------------------------------
# E2E tests for SourceMapping field_mappings
# ---------------------------------------------------------------------------


@pytest.fixture()
def source_parquet(tmp_path):
    """Parquet file with source-schema columns (not entity field names)."""
    df = pd.DataFrame({
        "id": [101, 102, 103],
        "subtotal": [1000, 2500, 500],
        "created": ["2024-01-01", "2024-02-15", "2024-03-10"],
    })
    path = tmp_path / "data" / "orders.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return path, len(df)


@pytest.fixture()
def mapped_entity_and_generated(source_parquet, tmp_path):
    """Entity with SourceMapping field_mappings -> generate -> return artifacts."""
    parquet_path, row_count = source_parquet
    generated_dir = tmp_path / "generated_mapped"

    entity = Entity(
        name="orders",
        description="Orders with field_mappings E2E",
        required_fields=[
            Field(name="transaction_id", type="STRING"),
            Field(name="amount_cents", type="INT64"),
            Field(name="created_at", type="STRING"),
        ],
        source=TableSource(
            project="test", dataset="test", table="orders",
            duckdb_path=str(parquet_path),
        ),
        layers=LayersConfig(
            prep=PrepLayer(model_name="prep_orders"),
            dimension=DimensionLayer(model_name="dim_orders"),
        ),
    )

    sm = SourceMapping(
        entity=entity,
        source=entity.source,
        field_mappings={
            "transaction_id": "id",
            "amount_cents": "subtotal",
            "created_at": "created",
        },
    )

    generate(entity, output_dir=generated_dir, source_mapping=sm)
    return entity, sm, generated_dir, parquet_path, row_count


class TestE2ESourceMappingFieldMappings:
    """E2E tests verifying SourceMapping.field_mappings rename source cols to entity names."""

    def test_output_has_entity_field_names(self, mapped_entity_and_generated):
        _, _, generated_dir, _, _ = mapped_entity_and_generated

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=generated_dir) as executor:
            result = executor.execute("orders")
            df = executor.connection.table(result.target_name).to_pandas()

        assert "transaction_id" in df.columns
        assert "amount_cents" in df.columns
        assert "created_at" in df.columns

    def test_source_column_names_absent(self, mapped_entity_and_generated):
        _, _, generated_dir, _, _ = mapped_entity_and_generated

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=generated_dir) as executor:
            result = executor.execute("orders")
            df = executor.connection.table(result.target_name).to_pandas()

        assert "id" not in df.columns
        assert "subtotal" not in df.columns
        assert "created" not in df.columns

    def test_row_count_preserved(self, mapped_entity_and_generated):
        _, _, generated_dir, _, row_count = mapped_entity_and_generated

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=generated_dir) as executor:
            result = executor.execute("orders")

        assert result.success is True
        assert result.row_count == row_count

    def test_field_values_preserved(self, mapped_entity_and_generated):
        _, _, generated_dir, _, _ = mapped_entity_and_generated

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=generated_dir) as executor:
            executor.execute("orders")
            df = executor.connection.table("dim_orders").to_pandas()

        row = df[df["transaction_id"] == 101].iloc[0]
        assert row["amount_cents"] == 1000

        row = df[df["transaction_id"] == 102].iloc[0]
        assert row["amount_cents"] == 2500

    def test_empty_field_mappings_backward_compatible(self, entity_and_generated):
        """Entity with no field_mappings (existing pattern) still executes correctly."""
        _, generated_dir, _, row_count = entity_and_generated

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=generated_dir) as executor:
            result = executor.execute("customers")

        assert result.success is True
        assert result.row_count == row_count


# ---------------------------------------------------------------------------
# E2E tests for UnionSource with per-source field normalization
# ---------------------------------------------------------------------------


class TestE2EUnionSourceFieldNormalization:
    """E2E tests for UnionSource with per-source field_mappings and literal_columns."""

    @pytest.fixture()
    def union_parquets(self, tmp_path):
        """Create two Parquet files with different column schemas."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        # Source 1: hubspot contacts (3 rows)
        hubspot_table = pa.table({
            "contact_id": [1, 2, 3],
            "contact_email": ["alice@example.com", "bob@example.com", "carol@example.com"],
            "contact_name": ["Alice", "Bob", "Carol"],
        })
        hubspot_path = tmp_path / "hubspot_contacts.parquet"
        pq.write_table(hubspot_table, hubspot_path)

        # Source 2: stripe customers (2 rows)
        stripe_table = pa.table({
            "customer_id": [101, 102],
            "email_address": ["dave@example.com", "eve@example.com"],
            "full_name": ["Dave", "Eve"],
        })
        stripe_path = tmp_path / "stripe_customers.parquet"
        pq.write_table(stripe_table, stripe_path)

        return hubspot_path, stripe_path

    def _make_entity(self, hubspot_path, stripe_path):
        """Build a UnionSource entity with field_mappings and literal_columns."""
        return Entity(
            name="contacts",
            description="Unified contacts",
            source=UnionSource(
                sources=[
                    TableSource(
                        project="test", dataset="test", table="hubspot_contacts",
                        duckdb_path=str(hubspot_path),
                        field_mappings={"contact_id": "id", "contact_email": "email", "contact_name": "name"},
                        literal_columns={"source_platform": "hubspot"},
                    ),
                    TableSource(
                        project="test", dataset="test", table="stripe_customers",
                        duckdb_path=str(stripe_path),
                        field_mappings={"customer_id": "id", "email_address": "email", "full_name": "name"},
                        literal_columns={"source_platform": "stripe"},
                    ),
                ]
            ),
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_contacts"),
            ),
        )

    def test_output_has_entity_field_names(self, tmp_path, union_parquets):
        """Two Parquet files with different column names produce unified table with entity field names."""
        hubspot_path, stripe_path = union_parquets
        entity = self._make_entity(hubspot_path, stripe_path)

        output_dir = tmp_path / "generated"
        generate(entity, output_dir)

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=output_dir) as executor:
            result = executor.execute("contacts")
            df = executor.connection.table(result.target_name).to_pandas()

        # Entity field names should be present
        assert "id" in df.columns
        assert "email" in df.columns
        assert "name" in df.columns
        # Source-native column names should be absent
        assert "contact_id" not in df.columns
        assert "contact_email" not in df.columns
        assert "customer_id" not in df.columns
        assert "email_address" not in df.columns

    def test_literal_columns_correct_values(self, tmp_path, union_parquets):
        """literal_columns produce correct values per source in output."""
        hubspot_path, stripe_path = union_parquets
        entity = self._make_entity(hubspot_path, stripe_path)

        output_dir = tmp_path / "generated"
        generate(entity, output_dir)

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=output_dir) as executor:
            result = executor.execute("contacts")
            df = executor.connection.table(result.target_name).to_pandas()

        assert "source_platform" in df.columns
        hubspot_rows = df[df["source_platform"] == "hubspot"]
        stripe_rows = df[df["source_platform"] == "stripe"]
        assert len(hubspot_rows) == 3
        assert len(stripe_rows) == 2

    def test_row_count_equals_sum(self, tmp_path, union_parquets):
        """Row count in output equals sum of rows from both input Parquet files."""
        hubspot_path, stripe_path = union_parquets
        entity = self._make_entity(hubspot_path, stripe_path)

        output_dir = tmp_path / "generated"
        generate(entity, output_dir)

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=output_dir) as executor:
            result = executor.execute("contacts")

        assert result.row_count == 5  # 3 hubspot + 2 stripe

    def test_output_columns_complete(self, tmp_path, union_parquets):
        """Output table has the expected columns: entity fields + literal columns."""
        hubspot_path, stripe_path = union_parquets
        entity = self._make_entity(hubspot_path, stripe_path)

        output_dir = tmp_path / "generated"
        generate(entity, output_dir)

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=output_dir) as executor:
            result = executor.execute("contacts")
            df = executor.connection.table(result.target_name).to_pandas()

        expected_cols = {"id", "email", "name", "source_platform"}
        assert expected_cols.issubset(set(df.columns))

    def test_plain_union_backward_compatible(self, tmp_path):
        """UnionSource with no field_mappings and no literal_columns still works."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        # Create two Parquet files with SAME column names
        table1 = pa.table({
            "id": [1, 2, 3],
            "email": ["a@ex.com", "b@ex.com", "c@ex.com"],
            "name": ["A", "B", "C"],
        })
        path1 = tmp_path / "source1.parquet"
        pq.write_table(table1, path1)

        table2 = pa.table({
            "id": [4, 5],
            "email": ["d@ex.com", "e@ex.com"],
            "name": ["D", "E"],
        })
        path2 = tmp_path / "source2.parquet"
        pq.write_table(table2, path2)

        entity = Entity(
            name="plain_contacts",
            description="Plain union without mappings",
            source=UnionSource(
                sources=[
                    TableSource(
                        project="test", dataset="test", table="source1",
                        duckdb_path=str(path1),
                    ),
                    TableSource(
                        project="test", dataset="test", table="source2",
                        duckdb_path=str(path2),
                    ),
                ]
            ),
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_plain_contacts"),
            ),
        )

        output_dir = tmp_path / "generated"
        generate(entity, output_dir)

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=output_dir) as executor:
            result = executor.execute("plain_contacts")
            df = executor.connection.table(result.target_name).to_pandas()

        assert len(df) == 5
        assert "id" in df.columns
        assert "email" in df.columns
        assert "name" in df.columns


# ---------------------------------------------------------------------------
# E2E tests for DerivedSource identity graph
# ---------------------------------------------------------------------------


class TestE2EIdentityGraphPerson:
    """E2E test: person-style entity with 2 source entities executes identity graph on DuckDB."""

    @pytest.fixture()
    def identity_graph_output(self, tmp_path):
        """Full identity graph pipeline: 2 source entities + 1 derived person entity."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        # --- Create parquet files ---
        hubspot_table = pa.table({
            "person_id": ["h1", "h2", "h3"],
            "email": ["alice@ex.com", "bob@ex.com", "carol@ex.com"],
            "full_name": ["Alice H", "Bob H", "Carol H"],
            "signup_date": ["2024-01-01", "2024-02-01", "2024-03-01"],
        })
        hubspot_path = tmp_path / "hubspot_person.parquet"
        pq.write_table(hubspot_table, hubspot_path)

        stripe_table = pa.table({
            "customer_id": ["s1", "s2"],
            "contact_email": ["bob@ex.com", "dave@ex.com"],
            "name": ["Bob S", "Dave S"],
            "created_at": ["2024-01-15", "2024-04-01"],
        })
        stripe_path = tmp_path / "stripe_person.parquet"
        pq.write_table(stripe_table, stripe_path)

        # --- Define entities ---
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

        stripe_entity = Entity(
            name="stripe_person",
            description="Stripe customers",
            source=TableSource(
                project="test", dataset="test", table="stripe_person",
                duckdb_path=str(stripe_path),
            ),
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_stripe_person"),
                dimension=DimensionLayer(model_name="dim_stripe_person"),
            ),
        )

        person_entity = Entity(
            name="person",
            description="Unified person entity",
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
                            name="stripe",
                            entity="stripe_person",
                            match_key_field="contact_email",
                            fields={"name": "name"},
                            id_field="customer_id",
                        ),
                    ],
                    priority=["hubspot", "stripe"],
                ),
            ),
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_person"),
                dimension=DimensionLayer(model_name="dim_person"),
            ),
        )

        # --- Generate code for all entities ---
        generated_dir = tmp_path / "generated"
        generate(hubspot_entity, output_dir=generated_dir)
        generate(stripe_entity, output_dir=generated_dir)
        generate(person_entity, output_dir=generated_dir)

        # --- Execute in dependency order ---
        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=generated_dir) as executor:
            executor.execute("hubspot_person")
            executor.execute("stripe_person")
            result = executor.execute("person", entity=person_entity)

            df = executor.connection.table(result.target_name).to_pandas()

        return result, df

    def test_row_count_and_schema(self, identity_graph_output):
        """AC1+AC5: 4 rows from 3+2 with 1 overlap, all expected columns present."""
        result, df = identity_graph_output
        assert result.row_count == 4
        assert len(df) == 4
        expected_cols = {"email", "name", "is_hubspot", "is_stripe", "hubspot_id", "stripe_id"}
        assert expected_cols.issubset(set(df.columns))

    def test_priority_coalesce_resolves_name(self, identity_graph_output):
        """AC2: Overlap record uses primary source value."""
        _, df = identity_graph_output
        bob = df[df["email"] == "bob@ex.com"].iloc[0]
        assert bob["name"] == "Bob H"
        alice = df[df["email"] == "alice@ex.com"].iloc[0]
        assert alice["name"] == "Alice H"
        dave = df[df["email"] == "dave@ex.com"].iloc[0]
        assert dave["name"] == "Dave S"

    def test_source_flags_correct(self, identity_graph_output):
        """AC3: is_hubspot and is_stripe flags correct."""
        _, df = identity_graph_output
        bob = df[df["email"] == "bob@ex.com"].iloc[0]
        assert bool(bob["is_hubspot"]) is True
        assert bool(bob["is_stripe"]) is True
        alice = df[df["email"] == "alice@ex.com"].iloc[0]
        assert bool(alice["is_hubspot"]) is True
        assert bool(alice["is_stripe"]) is False
        dave = df[df["email"] == "dave@ex.com"].iloc[0]
        assert bool(dave["is_hubspot"]) is False
        assert bool(dave["is_stripe"]) is True

    def test_source_ids_preserved(self, identity_graph_output):
        """AC4: Source IDs preserved, NULL when not from that source."""
        _, df = identity_graph_output
        bob = df[df["email"] == "bob@ex.com"].iloc[0]
        assert bob["hubspot_id"] == "h2"
        assert bob["stripe_id"] == "s1"
        alice = df[df["email"] == "alice@ex.com"].iloc[0]
        assert alice["hubspot_id"] == "h1"
        assert pd.isna(alice["stripe_id"])
        dave = df[df["email"] == "dave@ex.com"].iloc[0]
        assert pd.isna(dave["hubspot_id"])
        assert dave["stripe_id"] == "s2"
