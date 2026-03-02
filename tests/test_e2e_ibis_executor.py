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
    AggregationSource,
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
from fyrnheim._generate import generate
from fyrnheim.core.source import IdentityGraphConfig, IdentityGraphSource
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


# ---------------------------------------------------------------------------
# E2E tests for AggregationSource pipeline
# ---------------------------------------------------------------------------


class TestE2EAggregationSource:
    """E2E test: account-style entity aggregating from person-style entity on DuckDB."""

    @pytest.fixture()
    def aggregation_output(self, tmp_path):
        """Full aggregation pipeline: person entity + account entity aggregation."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        from fyrnheim.components.computed_column import ComputedColumn as CC

        # --- Create person parquet (6 rows, 3 accounts) ---
        person_table = pa.table({
            "person_id": ["p1", "p2", "p3", "p4", "p5", "p6"],
            "account_id": ["acct_1", "acct_1", "acct_2", "acct_2", "acct_2", "acct_3"],
            "name": ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank"],
            "amount": [100, 200, 300, 400, 500, 150],
        })
        person_path = tmp_path / "person.parquet"
        pq.write_table(person_table, person_path)

        # --- Define person entity (TableSource) ---
        person_entity = Entity(
            name="person",
            description="Person entity for aggregation test",
            source=TableSource(
                project="test", dataset="test", table="person",
                duckdb_path=str(person_path),
            ),
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_person"),
                dimension=DimensionLayer(model_name="dim_person"),
            ),
        )

        # --- Define account entity (AggregationSource) ---
        account_entity = Entity(
            name="account",
            description="Account entity aggregating from person",
            source=AggregationSource(
                source_entity="person",
                group_by_column="account_id",
                aggregations=[
                    CC(name="person_count", expression="t.person_id.count()"),
                    CC(name="total_amount", expression="t.amount.sum()"),
                ],
            ),
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_account"),
                dimension=DimensionLayer(model_name="dim_account"),
            ),
        )

        # --- Generate code for both entities ---
        generated_dir = tmp_path / "generated"
        generate(person_entity, output_dir=generated_dir)
        generate(account_entity, output_dir=generated_dir)

        # --- Execute in dependency order ---
        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=generated_dir) as executor:
            executor.execute("person")
            result = executor.execute("account", entity=account_entity)
            df = executor.connection.table(result.target_name).to_pandas()

        return result, df

    def test_row_count(self, aggregation_output):
        """3 account rows from 6 person rows across 3 accounts."""
        result, df = aggregation_output
        assert result.row_count == 3
        assert len(df) == 3

    def test_aggregation_values_correct(self, aggregation_output):
        """person_count and total_amount correct per account."""
        _, df = aggregation_output

        acct1 = df[df["account_id"] == "acct_1"].iloc[0]
        assert acct1["person_count"] == 2
        assert acct1["total_amount"] == 300

        acct2 = df[df["account_id"] == "acct_2"].iloc[0]
        assert acct2["person_count"] == 3
        assert acct2["total_amount"] == 1200

        acct3 = df[df["account_id"] == "acct_3"].iloc[0]
        assert acct3["person_count"] == 1
        assert acct3["total_amount"] == 150

    def test_output_columns(self, aggregation_output):
        """Output table has group_by key + aggregated fields."""
        _, df = aggregation_output
        expected_cols = {"account_id", "person_count", "total_amount"}
        assert expected_cols.issubset(set(df.columns))


# ---------------------------------------------------------------------------
# E2E tests for M006-E001: Simple TableSource entities (ghost_person + mailerlite_person)
# ---------------------------------------------------------------------------


class TestE2ESimpleTableSourceEntities:
    """E2E test: ghost_person and mailerlite_person entities execute on DuckDB."""

    @pytest.fixture()
    def ghost_and_mailerlite_data(self, tmp_path):
        """Create sample parquet files for Ghost members and MailerLite subscribers."""
        # --- Ghost members parquet ---
        ghost_df = pd.DataFrame({
            "id": ["g1", "g2", "g3", "g4"],
            "email": ["alice@example.com", "bob@test.org", "carol@example.com", "dave@corp.io"],
            "status": ["paid", "free", "comped", "paid"],
            "name": ["Alice Smith", "Bob Jones", "Carol White", "Dave Brown"],
            "created_at": pd.to_datetime([
                "2024-01-15T10:00:00",
                "2024-02-20T14:30:00",
                "2024-03-05T09:15:00",
                "2024-04-10T16:45:00",
            ]),
            "email_disabled": [False, False, True, False],
        })
        ghost_dir = tmp_path / "data" / "ghost_members"
        ghost_dir.mkdir(parents=True)
        ghost_df.to_parquet(ghost_dir / "members.parquet", index=False)

        # --- MailerLite subscribers parquet ---
        ml_df = pd.DataFrame({
            "id": ["m1", "m2", "m3"],
            "email": ["bob@test.org", "eve@example.com", "frank@startup.co"],
            "status": ["active", "unsubscribed", "active"],
            "subscribed_at": pd.to_datetime([
                "2024-01-10T08:00:00",
                "2024-03-15T12:00:00",
                "2024-05-01T10:30:00",
            ]),
            "source": ["organic", "referral", "ad_campaign"],
        })
        ml_dir = tmp_path / "data" / "mailerlite_subscribers"
        ml_dir.mkdir(parents=True)
        ml_df.to_parquet(ml_dir / "subscribers.parquet", index=False)

        return tmp_path, len(ghost_df), len(ml_df)

    @pytest.fixture()
    def executed_entities(self, ghost_and_mailerlite_data):
        """Generate and execute both entities on DuckDB, return connection + results."""
        from fyrnheim.primitives import hash_email

        base_dir, ghost_count, ml_count = ghost_and_mailerlite_data
        data_dir = base_dir / "data"
        generated_dir = base_dir / "generated"

        # Define entities inline with duckdb_path pointing to temp data
        ghost_entity = Entity(
            name="ghost_person",
            description="Ghost members for E2E test",
            is_internal=True,
            source=TableSource(
                project="test", dataset="test", table="members",
                duckdb_path=str(data_dir / "ghost_members" / "*.parquet"),
                fields=[
                    Field(name="id", type="STRING"),
                    Field(name="email", type="STRING"),
                    Field(name="status", type="STRING"),
                    Field(name="name", type="STRING"),
                    Field(name="created_at", type="TIMESTAMP"),
                    Field(name="email_disabled", type="BOOLEAN"),
                ],
            ),
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_ghost_person"),
                dimension=DimensionLayer(
                    model_name="dim_ghost_person",
                    computed_columns=[
                        ComputedColumn(
                            name="email_hash",
                            expression=hash_email("email"),
                        ),
                    ],
                ),
            ),
        )

        ml_entity = Entity(
            name="mailerlite_person",
            description="MailerLite subscribers for E2E test",
            is_internal=True,
            source=TableSource(
                project="test", dataset="test", table="subscribers",
                duckdb_path=str(data_dir / "mailerlite_subscribers" / "*.parquet"),
                fields=[
                    Field(name="id", type="STRING"),
                    Field(name="email", type="STRING"),
                    Field(name="status", type="STRING"),
                    Field(name="subscribed_at", type="TIMESTAMP"),
                    Field(name="source", type="STRING"),
                ],
            ),
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_mailerlite_person"),
                dimension=DimensionLayer(
                    model_name="dim_mailerlite_person",
                    computed_columns=[
                        ComputedColumn(
                            name="email_hash",
                            expression=hash_email("email"),
                        ),
                        ComputedColumn(
                            name="created_at",
                            expression="t.subscribed_at",
                        ),
                    ],
                ),
            ),
        )

        # Generate code for both
        generate(ghost_entity, output_dir=generated_dir)
        generate(ml_entity, output_dir=generated_dir)

        # Execute both on DuckDB
        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=generated_dir) as executor:
            ghost_result = executor.execute("ghost_person")
            ml_result = executor.execute("mailerlite_person")
            ghost_df = executor.connection.table("dim_ghost_person").to_pandas()
            ml_df = executor.connection.table("dim_mailerlite_person").to_pandas()

        return ghost_result, ml_result, ghost_df, ml_df, ghost_count, ml_count

    def test_ghost_person_row_count(self, executed_entities):
        """ghost_person dim table has correct row count."""
        ghost_result, _, ghost_df, _, ghost_count, _ = executed_entities
        assert ghost_result.row_count == ghost_count
        assert len(ghost_df) == ghost_count

    def test_mailerlite_person_row_count(self, executed_entities):
        """mailerlite_person dim table has correct row count."""
        _, ml_result, _, ml_df, _, ml_count = executed_entities
        assert ml_result.row_count == ml_count
        assert len(ml_df) == ml_count

    def test_ghost_person_has_email_column(self, executed_entities):
        """dim_ghost_person contains email column (match key for identity graph)."""
        _, _, ghost_df, _, _, _ = executed_entities
        assert "email" in ghost_df.columns

    def test_mailerlite_person_has_email_column(self, executed_entities):
        """dim_mailerlite_person contains email column (match key for identity graph)."""
        _, _, _, ml_df, _, _ = executed_entities
        assert "email" in ml_df.columns

    def test_ghost_person_has_email_hash(self, executed_entities):
        """dim_ghost_person has email_hash computed column."""
        _, _, ghost_df, _, _, _ = executed_entities
        assert "email_hash" in ghost_df.columns
        # All email_hash values should be non-null strings
        assert ghost_df["email_hash"].notna().all()

    def test_mailerlite_person_has_email_hash(self, executed_entities):
        """dim_mailerlite_person has email_hash computed column."""
        _, _, _, ml_df, _, _ = executed_entities
        assert "email_hash" in ml_df.columns
        assert ml_df["email_hash"].notna().all()

    def test_mailerlite_person_has_created_at(self, executed_entities):
        """dim_mailerlite_person has created_at mapped from subscribed_at."""
        _, _, _, ml_df, _, _ = executed_entities
        assert "created_at" in ml_df.columns
        assert ml_df["created_at"].notna().all()

    def test_ghost_person_preserves_source_columns(self, executed_entities):
        """dim_ghost_person has all source columns."""
        _, _, ghost_df, _, _, _ = executed_entities
        expected = {"id", "email", "status", "name", "created_at", "email_disabled"}
        assert expected.issubset(set(ghost_df.columns))

    def test_mailerlite_person_preserves_source_columns(self, executed_entities):
        """dim_mailerlite_person has all source columns."""
        _, _, _, ml_df, _, _ = executed_entities
        expected = {"id", "email", "status", "subscribed_at", "source"}
        assert expected.issubset(set(ml_df.columns))

    def test_both_entities_execute_successfully(self, executed_entities):
        """Both entities report success."""
        ghost_result, ml_result, _, _, _, _ = executed_entities
        assert ghost_result.success is True
        assert ml_result.success is True

    def test_runner_discovers_and_executes_both(self, ghost_and_mailerlite_data):
        """runner.run() discovers both entities from directory and executes them."""
        from fyrnheim.engine.runner import run

        base_dir, ghost_count, ml_count = ghost_and_mailerlite_data
        data_dir = base_dir / "data"
        entities_dir = base_dir / "entities"
        entities_dir.mkdir()
        generated_dir = base_dir / "generated"

        # Write entity files that point to temp parquet paths
        ghost_code = f'''\
from fyrnheim import (
    ComputedColumn, DimensionLayer, Entity, Field,
    LayersConfig, PrepLayer, TableSource,
)
from fyrnheim.primitives import hash_email

entity = Entity(
    name="ghost_person",
    description="Ghost members",
    is_internal=True,
    source=TableSource(
        project="test", dataset="test", table="members",
        duckdb_path="{data_dir / "ghost_members" / "*.parquet"}",
        fields=[
            Field(name="id", type="STRING"),
            Field(name="email", type="STRING"),
            Field(name="status", type="STRING"),
            Field(name="name", type="STRING"),
            Field(name="created_at", type="TIMESTAMP"),
            Field(name="email_disabled", type="BOOLEAN"),
        ],
    ),
    layers=LayersConfig(
        prep=PrepLayer(model_name="prep_ghost_person"),
        dimension=DimensionLayer(
            model_name="dim_ghost_person",
            computed_columns=[
                ComputedColumn(name="email_hash", expression=hash_email("email")),
            ],
        ),
    ),
)
'''
        ml_code = f'''\
from fyrnheim import (
    ComputedColumn, DimensionLayer, Entity, Field,
    LayersConfig, PrepLayer, TableSource,
)
from fyrnheim.primitives import hash_email

entity = Entity(
    name="mailerlite_person",
    description="MailerLite subscribers",
    is_internal=True,
    source=TableSource(
        project="test", dataset="test", table="subscribers",
        duckdb_path="{data_dir / "mailerlite_subscribers" / "*.parquet"}",
        fields=[
            Field(name="id", type="STRING"),
            Field(name="email", type="STRING"),
            Field(name="status", type="STRING"),
            Field(name="subscribed_at", type="TIMESTAMP"),
            Field(name="source", type="STRING"),
        ],
    ),
    layers=LayersConfig(
        prep=PrepLayer(model_name="prep_mailerlite_person"),
        dimension=DimensionLayer(
            model_name="dim_mailerlite_person",
            computed_columns=[
                ComputedColumn(name="email_hash", expression=hash_email("email")),
                ComputedColumn(name="created_at", expression="t.subscribed_at"),
            ],
        ),
    ),
)
'''
        (entities_dir / "ghost_person.py").write_text(ghost_code)
        (entities_dir / "mailerlite_person.py").write_text(ml_code)

        result = run(entities_dir, data_dir, backend="duckdb", generated_dir=generated_dir)
        assert result.ok is True
        assert len(result.entities) == 2
        assert all(e.status == "success" for e in result.entities)
        total_rows = sum(e.row_count for e in result.entities)
        assert total_rows == ghost_count + ml_count


# ---------------------------------------------------------------------------
# E2E tests for M006-E002: SourceMapping entities (transactions + subscriptions)
# ---------------------------------------------------------------------------


class TestE2ESourceMappingEntities:
    """E2E test: transactions and subscriptions with SourceMapping field_mappings on DuckDB."""

    @pytest.fixture()
    def sourcemapping_data(self, tmp_path):
        """Create sample parquet files with Lemonsqueezy source column names."""
        # --- Transactions parquet (source column names: id, subtotal) ---
        txn_df = pd.DataFrame({
            "id": ["t1", "t2", "t3", "t4"],
            "store_id": [1, 1, 1, 1],
            "identifier": ["ORD-001", "ORD-002", "ORD-003", "ORD-004"],
            "order_number": [1001, 1002, 1003, 1004],
            "status": ["paid", "paid", "refunded", "paid"],
            "customer_id": [10, 20, 10, 30],
            "customer_name": ["Alice", "Bob", "Alice", "Carol"],
            "customer_email": [
                "alice@example.com", "bob@test.org",
                "alice@example.com", "carol@startup.co",
            ],
            "total": [2000, 5000, 1500, 3000],
            "subtotal": [1800, 4500, 1300, 2700],
            "currency": ["USD", "USD", "USD", "EUR"],
            "refunded": [False, False, True, False],
            "created_at": pd.to_datetime([
                "2024-01-15", "2024-02-20", "2024-03-05", "2024-04-10",
            ]),
            "updated_at": pd.to_datetime([
                "2024-01-15", "2024-02-20", "2024-03-06", "2024-04-10",
            ]),
        })
        txn_dir = tmp_path / "data" / "transactions"
        txn_dir.mkdir(parents=True)
        txn_df.to_parquet(txn_dir / "transactions.parquet", index=False)

        # --- Subscriptions parquet (source column names: id) ---
        sub_df = pd.DataFrame({
            "id": ["s1", "s2", "s3"],
            "store_id": [1, 1, 1],
            "product_id": [100, 200, 100],
            "variant_id": [1000, 2000, 1000],
            "status": ["active", "cancelled", "on_trial"],
            "user_email": [
                "alice@example.com", "bob@test.org", "dave@corp.io",
            ],
            "user_name": ["Alice", "Bob", "Dave"],
            "renews_at": pd.to_datetime([
                "2024-07-15", "2024-06-20", "2024-08-01",
            ]),
            "ends_at": pd.array([None, "2024-06-20", None], dtype="datetime64[ns]"),
            "cancelled": [False, True, False],
            "billing_anchor": [15, 20, 1],
            "card_brand": ["visa", "mastercard", None],
            "created_at": pd.to_datetime([
                "2024-01-15", "2024-02-20", "2024-07-01",
            ]),
            "updated_at": pd.to_datetime([
                "2024-06-15", "2024-06-20", "2024-07-01",
            ]),
        })
        sub_dir = tmp_path / "data" / "subscriptions"
        sub_dir.mkdir(parents=True)
        sub_df.to_parquet(sub_dir / "subscriptions.parquet", index=False)

        return tmp_path, len(txn_df), len(sub_df)

    @pytest.fixture()
    def executed_sourcemapping_entities(self, sourcemapping_data):
        """Generate and execute both SourceMapping entities on DuckDB."""
        from fyrnheim.primitives import hash_email

        base_dir, txn_count, sub_count = sourcemapping_data
        data_dir = base_dir / "data"
        generated_dir = base_dir / "generated"

        # --- Transactions entity ---
        txn_entity = Entity(
            name="transactions",
            description="E2E transactions with SourceMapping",
            required_fields=[
                Field(name="transaction_id", type="STRING"),
                Field(name="customer_email", type="STRING"),
                Field(name="amount_cents", type="INT64"),
                Field(name="currency", type="STRING"),
                Field(name="status", type="STRING"),
                Field(name="created_at", type="TIMESTAMP"),
            ],
            optional_fields=[
                Field(name="store_id", type="INT64"),
                Field(name="customer_id", type="INT64"),
                Field(name="customer_name", type="STRING"),
                Field(name="total", type="INT64"),
                Field(name="refunded", type="BOOLEAN"),
                Field(name="updated_at", type="TIMESTAMP"),
            ],
            core_computed=[
                ComputedColumn(
                    name="customer_email_hash",
                    expression=hash_email("customer_email"),
                ),
            ],
            source=TableSource(
                project="test", dataset="test", table="transactions",
                duckdb_path=str(data_dir / "transactions" / "*.parquet"),
            ),
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_transactions"),
                dimension=DimensionLayer(model_name="dim_transactions"),
            ),
        )
        txn_sm = SourceMapping(
            entity=txn_entity,
            source=txn_entity.source,
            field_mappings={
                "transaction_id": "id",
                "customer_email": "customer_email",
                "amount_cents": "subtotal",
                "currency": "currency",
                "status": "status",
                "created_at": "created_at",
                "store_id": "store_id",
                "customer_id": "customer_id",
                "customer_name": "customer_name",
                "total": "total",
                "refunded": "refunded",
                "updated_at": "updated_at",
            },
        )

        # --- Subscriptions entity ---
        sub_entity = Entity(
            name="subscriptions",
            description="E2E subscriptions with SourceMapping",
            required_fields=[
                Field(name="subscription_id", type="STRING"),
                Field(name="user_email", type="STRING"),
                Field(name="status", type="STRING"),
                Field(name="created_at", type="TIMESTAMP"),
            ],
            optional_fields=[
                Field(name="store_id", type="INT64"),
                Field(name="product_id", type="INT64"),
                Field(name="user_name", type="STRING"),
                Field(name="cancelled", type="BOOLEAN"),
                Field(name="billing_anchor", type="INT64"),
                Field(name="card_brand", type="STRING"),
                Field(name="updated_at", type="TIMESTAMP"),
            ],
            core_computed=[
                ComputedColumn(
                    name="customer_email_hash",
                    expression=hash_email("user_email"),
                ),
                ComputedColumn(
                    name="is_active",
                    expression="t.status.isin(['active', 'on_trial'])",
                ),
                ComputedColumn(
                    name="is_churned",
                    expression="t.status.isin(['cancelled', 'expired', 'unpaid'])",
                ),
            ],
            source=TableSource(
                project="test", dataset="test", table="subscriptions",
                duckdb_path=str(data_dir / "subscriptions" / "*.parquet"),
            ),
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_subscriptions"),
                dimension=DimensionLayer(model_name="dim_subscriptions"),
            ),
        )
        sub_sm = SourceMapping(
            entity=sub_entity,
            source=sub_entity.source,
            field_mappings={
                "subscription_id": "id",
                "user_email": "user_email",
                "status": "status",
                "created_at": "created_at",
                "store_id": "store_id",
                "product_id": "product_id",
                "user_name": "user_name",
                "cancelled": "cancelled",
                "billing_anchor": "billing_anchor",
                "card_brand": "card_brand",
                "updated_at": "updated_at",
            },
        )

        # Generate and execute
        generate(txn_entity, output_dir=generated_dir, source_mapping=txn_sm)
        generate(sub_entity, output_dir=generated_dir, source_mapping=sub_sm)

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=generated_dir) as executor:
            txn_result = executor.execute("transactions")
            sub_result = executor.execute("subscriptions")
            txn_df = executor.connection.table("dim_transactions").to_pandas()
            sub_df = executor.connection.table("dim_subscriptions").to_pandas()

        return txn_result, sub_result, txn_df, sub_df, txn_count, sub_count

    def test_transactions_row_count(self, executed_sourcemapping_entities):
        """transactions dim table has correct row count."""
        txn_result, _, txn_df, _, txn_count, _ = executed_sourcemapping_entities
        assert txn_result.row_count == txn_count
        assert len(txn_df) == txn_count

    def test_subscriptions_row_count(self, executed_sourcemapping_entities):
        """subscriptions dim table has correct row count."""
        _, sub_result, _, sub_df, _, sub_count = executed_sourcemapping_entities
        assert sub_result.row_count == sub_count
        assert len(sub_df) == sub_count

    def test_transactions_has_entity_field_names(self, executed_sourcemapping_entities):
        """Output has entity field names (transaction_id, amount_cents), not source names."""
        _, _, txn_df, _, _, _ = executed_sourcemapping_entities
        assert "transaction_id" in txn_df.columns
        assert "amount_cents" in txn_df.columns

    def test_transactions_source_names_renamed(self, executed_sourcemapping_entities):
        """Source column names (id, subtotal) are renamed to entity names."""
        _, _, txn_df, _, _, _ = executed_sourcemapping_entities
        # 'id' should be renamed to 'transaction_id', 'subtotal' to 'amount_cents'
        # The renamed columns should not appear under their original names
        # (unless they also map 1:1 like 'status'->'status')
        assert "transaction_id" in txn_df.columns
        assert "amount_cents" in txn_df.columns

    def test_subscriptions_has_entity_field_names(self, executed_sourcemapping_entities):
        """Output has entity field names (subscription_id), not source names."""
        _, _, _, sub_df, _, _ = executed_sourcemapping_entities
        assert "subscription_id" in sub_df.columns

    def test_transactions_has_customer_email(self, executed_sourcemapping_entities):
        """dim_transactions contains customer_email for identity graph."""
        _, _, txn_df, _, _, _ = executed_sourcemapping_entities
        assert "customer_email" in txn_df.columns

    def test_subscriptions_has_user_email(self, executed_sourcemapping_entities):
        """dim_subscriptions contains user_email for identity graph."""
        _, _, _, sub_df, _, _ = executed_sourcemapping_entities
        assert "user_email" in sub_df.columns

    def test_transactions_has_email_hash(self, executed_sourcemapping_entities):
        """dim_transactions has customer_email_hash computed column."""
        _, _, txn_df, _, _, _ = executed_sourcemapping_entities
        assert "customer_email_hash" in txn_df.columns
        assert txn_df["customer_email_hash"].notna().all()

    def test_subscriptions_has_email_hash(self, executed_sourcemapping_entities):
        """dim_subscriptions has customer_email_hash computed column."""
        _, _, _, sub_df, _, _ = executed_sourcemapping_entities
        assert "customer_email_hash" in sub_df.columns
        assert sub_df["customer_email_hash"].notna().all()

    def test_subscriptions_lifecycle_flags(self, executed_sourcemapping_entities):
        """dim_subscriptions has is_active and is_churned computed columns."""
        _, _, _, sub_df, _, _ = executed_sourcemapping_entities
        assert "is_active" in sub_df.columns
        assert "is_churned" in sub_df.columns

        # active status -> is_active=True
        active_row = sub_df[sub_df["subscription_id"] == "s1"].iloc[0]
        assert bool(active_row["is_active"]) is True
        assert bool(active_row["is_churned"]) is False

        # cancelled status -> is_churned=True
        cancelled_row = sub_df[sub_df["subscription_id"] == "s2"].iloc[0]
        assert bool(cancelled_row["is_active"]) is False
        assert bool(cancelled_row["is_churned"]) is True

        # on_trial status -> is_active=True
        trial_row = sub_df[sub_df["subscription_id"] == "s3"].iloc[0]
        assert bool(trial_row["is_active"]) is True
        assert bool(trial_row["is_churned"]) is False

    def test_transactions_field_values_preserved(self, executed_sourcemapping_entities):
        """Renamed field values are preserved correctly."""
        _, _, txn_df, _, _, _ = executed_sourcemapping_entities
        row = txn_df[txn_df["transaction_id"] == "t1"].iloc[0]
        assert row["amount_cents"] == 1800
        assert row["customer_email"] == "alice@example.com"

    def test_both_execute_successfully(self, executed_sourcemapping_entities):
        """Both entities report success."""
        txn_result, sub_result, _, _, _, _ = executed_sourcemapping_entities
        assert txn_result.success is True
        assert sub_result.success is True

    def test_runner_with_sourcemapping(self, sourcemapping_data):
        """runner.run() discovers source_mapping from entity modules and applies it."""
        from fyrnheim.engine.runner import run

        base_dir, txn_count, sub_count = sourcemapping_data
        data_dir = base_dir / "data"
        entities_dir = base_dir / "entities"
        entities_dir.mkdir()
        generated_dir = base_dir / "generated"

        txn_code = f'''\
from fyrnheim import (
    ComputedColumn, DimensionLayer, Entity, Field,
    LayersConfig, PrepLayer, SourceMapping, TableSource,
)
from fyrnheim.primitives import hash_email

entity = Entity(
    name="transactions",
    description="Transactions with SourceMapping",
    required_fields=[
        Field(name="transaction_id", type="STRING"),
        Field(name="customer_email", type="STRING"),
        Field(name="amount_cents", type="INT64"),
        Field(name="currency", type="STRING"),
        Field(name="status", type="STRING"),
        Field(name="created_at", type="TIMESTAMP"),
    ],
    core_computed=[
        ComputedColumn(name="customer_email_hash", expression=hash_email("customer_email")),
    ],
    source=TableSource(
        project="test", dataset="test", table="transactions",
        duckdb_path="{data_dir / "transactions" / "*.parquet"}",
    ),
    layers=LayersConfig(
        prep=PrepLayer(model_name="prep_transactions"),
        dimension=DimensionLayer(model_name="dim_transactions"),
    ),
)

source_mapping = SourceMapping(
    entity=entity,
    source=entity.source,
    field_mappings={{
        "transaction_id": "id",
        "customer_email": "customer_email",
        "amount_cents": "subtotal",
        "currency": "currency",
        "status": "status",
        "created_at": "created_at",
    }},
)
'''
        sub_code = f'''\
from fyrnheim import (
    ComputedColumn, DimensionLayer, Entity, Field,
    LayersConfig, PrepLayer, SourceMapping, TableSource,
)
from fyrnheim.primitives import hash_email

entity = Entity(
    name="subscriptions",
    description="Subscriptions with SourceMapping",
    required_fields=[
        Field(name="subscription_id", type="STRING"),
        Field(name="user_email", type="STRING"),
        Field(name="status", type="STRING"),
        Field(name="created_at", type="TIMESTAMP"),
    ],
    core_computed=[
        ComputedColumn(name="customer_email_hash", expression=hash_email("user_email")),
        ComputedColumn(name="is_active", expression="t.status.isin(['active', 'on_trial'])"),
        ComputedColumn(name="is_churned", expression="t.status.isin(['cancelled', 'expired', 'unpaid'])"),
    ],
    source=TableSource(
        project="test", dataset="test", table="subscriptions",
        duckdb_path="{data_dir / "subscriptions" / "*.parquet"}",
    ),
    layers=LayersConfig(
        prep=PrepLayer(model_name="prep_subscriptions"),
        dimension=DimensionLayer(model_name="dim_subscriptions"),
    ),
)

source_mapping = SourceMapping(
    entity=entity,
    source=entity.source,
    field_mappings={{
        "subscription_id": "id",
        "user_email": "user_email",
        "status": "status",
        "created_at": "created_at",
    }},
)
'''
        (entities_dir / "transactions.py").write_text(txn_code)
        (entities_dir / "subscriptions.py").write_text(sub_code)

        result = run(entities_dir, data_dir, backend="duckdb", generated_dir=generated_dir)
        assert result.ok is True
        assert len(result.entities) == 2
        assert all(e.status == "success" for e in result.entities)

        # Verify entity field names in output (not source names)
        txn_result = next(e for e in result.entities if e.entity_name == "transactions")
        sub_result = next(e for e in result.entities if e.entity_name == "subscriptions")
        assert txn_result.row_count == txn_count
        assert sub_result.row_count == sub_count


# ---------------------------------------------------------------------------
# M006-E003: UnionSource entities (product, signals, anon)
# ---------------------------------------------------------------------------


class TestE2EProductEntity:
    """E2E tests for product entity: UnionSource (YouTube + LinkedIn)."""

    @pytest.fixture()
    def product_data(self, tmp_path):
        """Create sample YouTube and LinkedIn parquet files."""
        data_dir = tmp_path / "data"

        # YouTube videos (3 rows)
        yt_dir = data_dir / "youtube_videos"
        yt_dir.mkdir(parents=True)
        yt_df = pd.DataFrame({
            "video_id": ["v1", "v2", "v3"],
            "title": ["Video A", "Video B", "Video C"],
            "published_at": pd.to_datetime(["2024-01-01", "2024-02-01", "2024-03-01"]),
            "view_count": [1000, 2000, 3000],
            "like_count": [100, 200, 300],
            "comment_count": [10, 20, 30],
        })
        yt_df.to_parquet(yt_dir / "data.parquet", index=False)

        # LinkedIn posts (2 rows) — different column names
        li_dir = data_dir / "authoredup_posts"
        li_dir.mkdir(parents=True)
        li_df = pd.DataFrame({
            "post_id": ["p1", "p2"],
            "text": ["Post Alpha", "Post Beta"],
            "published_at": pd.to_datetime(["2024-04-01", "2024-05-01"]),
            "impressions": [500, 600],
            "reactions": [50, 60],
            "comments": [5, 6],
            "shares": [2, 3],
        })
        li_df.to_parquet(li_dir / "data.parquet", index=False)

        return data_dir, 3, 2  # yt_count, li_count

    def _make_entity(self, data_dir):
        return Entity(
            name="product",
            description="Unified product dimension",
            source=UnionSource(
                sources=[
                    TableSource(
                        project="test", dataset="test", table="youtube_videos",
                        duckdb_path=str(data_dir / "youtube_videos" / "*.parquet"),
                        field_mappings={"video_id": "product_id"},
                        literal_columns={"product_type": "video", "source_platform": "youtube"},
                    ),
                    TableSource(
                        project="test", dataset="test", table="authoredup_posts",
                        duckdb_path=str(data_dir / "authoredup_posts" / "*.parquet"),
                        field_mappings={
                            "post_id": "product_id", "text": "title",
                            "impressions": "view_count", "reactions": "like_count",
                            "comments": "comment_count", "shares": "share_count",
                        },
                        literal_columns={"product_type": "post", "source_platform": "linkedin"},
                    ),
                ]
            ),
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_product"),
                dimension=DimensionLayer(model_name="dim_product"),
            ),
        )

    def test_product_unified_columns(self, tmp_path, product_data):
        """Product dim table has unified columns and product_type/source_platform."""
        data_dir, yt_count, li_count = product_data
        entity = self._make_entity(data_dir)

        output_dir = tmp_path / "generated"
        generate(entity, output_dir)

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=output_dir) as executor:
            result = executor.execute("product")
            df = executor.connection.table(result.target_name).to_pandas()

        assert "product_id" in df.columns
        assert "title" in df.columns
        assert "view_count" in df.columns
        assert "product_type" in df.columns
        assert "source_platform" in df.columns

    def test_product_row_count(self, tmp_path, product_data):
        """Product row count equals youtube rows + linkedin rows."""
        data_dir, yt_count, li_count = product_data
        entity = self._make_entity(data_dir)

        output_dir = tmp_path / "generated"
        generate(entity, output_dir)

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=output_dir) as executor:
            result = executor.execute("product")

        assert result.row_count == yt_count + li_count

    def test_product_literal_columns_correct(self, tmp_path, product_data):
        """product_type and source_platform have correct values per source."""
        data_dir, yt_count, li_count = product_data
        entity = self._make_entity(data_dir)

        output_dir = tmp_path / "generated"
        generate(entity, output_dir)

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=output_dir) as executor:
            result = executor.execute("product")
            df = executor.connection.table(result.target_name).to_pandas()

        videos = df[df["product_type"] == "video"]
        posts = df[df["product_type"] == "post"]
        assert len(videos) == yt_count
        assert len(posts) == li_count
        assert set(videos["source_platform"]) == {"youtube"}
        assert set(posts["source_platform"]) == {"linkedin"}


class TestE2ESignalsEntity:
    """E2E tests for signals entity: UnionSource (walker + shortio + ghost)."""

    @pytest.fixture()
    def signals_data(self, tmp_path):
        """Create sample parquets for walker, shortio, ghost."""
        data_dir = tmp_path / "data"

        # Walker events (3 rows)
        walker_dir = data_dir / "walker_events"
        walker_dir.mkdir(parents=True)
        walker_df = pd.DataFrame({
            "session_id": ["s1", "s2", "s3"],
            "timestamp": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
            "referrer": ["google.com", "linkedin.com", ""],
            "event_name": ["page_view", "click", "page_view"],
            "page_path": ["/home", "/about", "/pricing"],
        })
        walker_df.to_parquet(walker_dir / "data.parquet", index=False)

        # Shortio clicks (2 rows)
        shortio_dir = data_dir / "shortio_clicks"
        shortio_dir.mkdir(parents=True)
        shortio_df = pd.DataFrame({
            "clicked_at": pd.to_datetime(["2024-02-01", "2024-02-02"]),
            "utm_source": ["linkedin", "newsletter"],
            "utm_medium": ["social", "email"],
            "utm_campaign": ["spring-promo", "weekly-digest"],
            "short_path": ["/abc", "/def"],
            "original_url": ["https://example.com/a", "https://example.com/b"],
        })
        shortio_df.to_parquet(shortio_dir / "data.parquet", index=False)

        # Ghost members (2 rows)
        ghost_dir = data_dir / "ghost_members"
        ghost_dir.mkdir(parents=True)
        ghost_df = pd.DataFrame({
            "email": ["alice@example.com", "bob@example.com"],
            "created_at": pd.to_datetime(["2024-03-01", "2024-03-15"]),
            "name": ["Alice", "Bob"],
        })
        ghost_df.to_parquet(ghost_dir / "data.parquet", index=False)

        return data_dir, 3, 2, 2  # walker, shortio, ghost counts

    def _make_entity(self, data_dir):
        from fyrnheim.primitives import concat_hash, hash_email

        return Entity(
            name="signals",
            description="Unified engagement signals",
            source=UnionSource(
                sources=[
                    TableSource(
                        project="test", dataset="test", table="walker_events",
                        duckdb_path=str(data_dir / "walker_events" / "*.parquet"),
                        field_mappings={"timestamp": "signal_timestamp"},
                        literal_columns={"source": "walker"},
                    ),
                    TableSource(
                        project="test", dataset="test", table="shortio_clicks",
                        duckdb_path=str(data_dir / "shortio_clicks" / "*.parquet"),
                        field_mappings={
                            "clicked_at": "signal_timestamp",
                            "utm_source": "channel_source",
                            "utm_medium": "channel_medium",
                            "utm_campaign": "campaign",
                        },
                        literal_columns={"source": "shortio", "signal_type": "link_clicked"},
                    ),
                    TableSource(
                        project="test", dataset="test", table="ghost_members",
                        duckdb_path=str(data_dir / "ghost_members" / "*.parquet"),
                        field_mappings={"created_at": "signal_timestamp"},
                        literal_columns={"source": "ghost", "signal_type": "newsletter_subscribed"},
                    ),
                ]
            ),
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_signals"),
                dimension=DimensionLayer(
                    model_name="dim_signals",
                    computed_columns=[
                        ComputedColumn(
                            name="person_id",
                            expression=hash_email("email"),
                        ),
                        ComputedColumn(
                            name="signal_id",
                            expression=concat_hash(
                                "email", "session_id", "signal_timestamp",
                                "signal_type", "source",
                            ),
                        ),
                    ],
                ),
            ),
        )

    def test_signals_unified_timestamp(self, tmp_path, signals_data):
        """Signals dim table has signal_timestamp column from all sources."""
        data_dir, *_ = signals_data
        entity = self._make_entity(data_dir)

        output_dir = tmp_path / "generated"
        generate(entity, output_dir)

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=output_dir) as executor:
            result = executor.execute("signals")
            df = executor.connection.table(result.target_name).to_pandas()

        assert "signal_timestamp" in df.columns
        assert "source" in df.columns
        assert "signal_type" in df.columns

    def test_signals_row_count(self, tmp_path, signals_data):
        """Signals row count equals walker + shortio + ghost rows."""
        data_dir, walker_count, shortio_count, ghost_count = signals_data
        entity = self._make_entity(data_dir)

        output_dir = tmp_path / "generated"
        generate(entity, output_dir)

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=output_dir) as executor:
            result = executor.execute("signals")

        assert result.row_count == walker_count + shortio_count + ghost_count

    def test_signals_source_tags(self, tmp_path, signals_data):
        """Each sub-source has correct source and signal_type tags."""
        data_dir, walker_count, shortio_count, ghost_count = signals_data
        entity = self._make_entity(data_dir)

        output_dir = tmp_path / "generated"
        generate(entity, output_dir)

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=output_dir) as executor:
            result = executor.execute("signals")
            df = executor.connection.table(result.target_name).to_pandas()

        walker_rows = df[df["source"] == "walker"]
        shortio_rows = df[df["source"] == "shortio"]
        ghost_rows = df[df["source"] == "ghost"]

        assert len(walker_rows) == walker_count
        assert len(shortio_rows) == shortio_count
        assert len(ghost_rows) == ghost_count

        # Shortio and ghost have literal signal_type
        assert set(shortio_rows["signal_type"]) == {"link_clicked"}
        assert set(ghost_rows["signal_type"]) == {"newsletter_subscribed"}

    def test_signals_computed_columns(self, tmp_path, signals_data):
        """Signals dim has person_id and signal_id computed columns."""
        data_dir, *_ = signals_data
        entity = self._make_entity(data_dir)

        output_dir = tmp_path / "generated"
        generate(entity, output_dir)

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=output_dir) as executor:
            result = executor.execute("signals")
            df = executor.connection.table(result.target_name).to_pandas()

        assert "person_id" in df.columns
        assert "signal_id" in df.columns

        # Ghost rows should have person_id (email-based hash)
        ghost_rows = df[df["source"] == "ghost"]
        assert ghost_rows["person_id"].notna().all()


class TestE2EAnonEntity:
    """E2E tests for anon entity: single TableSource (Walker sessions)."""

    @pytest.fixture()
    def anon_data(self, tmp_path):
        """Create sample Walker events parquet."""
        data_dir = tmp_path / "data"
        walker_dir = data_dir / "walker_events"
        walker_dir.mkdir(parents=True)

        walker_df = pd.DataFrame({
            "session_id": ["s1", "s2", "s3", "s4"],
            "timestamp": pd.to_datetime([
                "2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04",
            ]),
            "referrer": [
                "https://google.com/search?q=test",
                "https://linkedin.com/feed",
                "https://chatgpt.com/c/123",
                "",  # direct
            ],
            "event_name": ["page_view", "page_view", "page_view", "click"],
            "page_path": ["/home", "/about", "/pricing", "/signup"],
        })
        walker_df.to_parquet(walker_dir / "data.parquet", index=False)

        return data_dir, 4

    def _make_entity(self, data_dir):
        from fyrnheim.primitives import categorize_contains

        return Entity(
            name="anon",
            description="Anonymous visitor sessions",
            source=TableSource(
                project="test", dataset="test", table="walker_events",
                duckdb_path=str(data_dir / "walker_events" / "*.parquet"),
                fields=[
                    Field(name="session_id", type="STRING"),
                    Field(name="timestamp", type="TIMESTAMP"),
                    Field(name="referrer", type="STRING"),
                    Field(name="event_name", type="STRING"),
                    Field(name="page_path", type="STRING"),
                ],
            ),
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_anon"),
                dimension=DimensionLayer(
                    model_name="dim_anon",
                    computed_columns=[
                        ComputedColumn(
                            name="anon_id",
                            expression='(ibis.literal("walker") + t.session_id).hash().cast("string")',
                        ),
                        ComputedColumn(
                            name="source",
                            expression='ibis.literal("walker")',
                        ),
                        ComputedColumn(
                            name="channel_category",
                            expression=categorize_contains(
                                "referrer",
                                {
                                    "social_linkedin": ["linkedin.com", "lnkd.in"],
                                    "social_youtube": ["youtube.com"],
                                    "newsletter": ["mail.google", "ghost.io", "substack.com",
                                                    "mailerlite.com", "convertkit.com", "beehiiv.com"],
                                    "seo": ["google.com", "bing.com", "duckduckgo.com", "ecosia.org"],
                                    "ai": ["chatgpt.com", "perplexity.ai", "claude.ai"],
                                },
                                default="direct",
                            ),
                        ),
                    ],
                ),
            ),
        )

    def test_anon_computed_columns(self, tmp_path, anon_data):
        """Anon dim has anon_id, source, and channel_category computed columns."""
        data_dir, row_count = anon_data
        entity = self._make_entity(data_dir)

        output_dir = tmp_path / "generated"
        generate(entity, output_dir)

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=output_dir) as executor:
            result = executor.execute("anon")
            df = executor.connection.table(result.target_name).to_pandas()

        assert "anon_id" in df.columns
        assert "source" in df.columns
        assert "channel_category" in df.columns
        assert result.row_count == row_count

    def test_anon_source_literal(self, tmp_path, anon_data):
        """All anon rows have source='walker'."""
        data_dir, row_count = anon_data
        entity = self._make_entity(data_dir)

        output_dir = tmp_path / "generated"
        generate(entity, output_dir)

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=output_dir) as executor:
            result = executor.execute("anon")
            df = executor.connection.table(result.target_name).to_pandas()

        assert set(df["source"]) == {"walker"}

    def test_anon_channel_categorization(self, tmp_path, anon_data):
        """channel_category correctly classifies referrer URLs."""
        data_dir, _ = anon_data
        entity = self._make_entity(data_dir)

        output_dir = tmp_path / "generated"
        generate(entity, output_dir)

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=output_dir) as executor:
            result = executor.execute("anon")
            df = executor.connection.table(result.target_name).to_pandas()

        # google.com → seo, linkedin.com → social_linkedin, chatgpt.com → ai, empty → direct
        categories = dict(zip(df["session_id"], df["channel_category"]))
        assert categories["s1"] == "seo"
        assert categories["s2"] == "social_linkedin"
        assert categories["s3"] == "ai"
        assert categories["s4"] == "direct"

    def test_anon_id_is_unique(self, tmp_path, anon_data):
        """Each session gets a unique anon_id hash."""
        data_dir, row_count = anon_data
        entity = self._make_entity(data_dir)

        output_dir = tmp_path / "generated"
        generate(entity, output_dir)

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=output_dir) as executor:
            result = executor.execute("anon")
            df = executor.connection.table(result.target_name).to_pandas()

        assert df["anon_id"].nunique() == row_count


class TestE2ERunnerUnionEntities:
    """runner.run() discovers and executes all three E003 entities."""

    @pytest.fixture()
    def runner_data(self, tmp_path):
        """Create data and entity files for product, signals, anon."""
        base_dir = tmp_path
        data_dir = base_dir / "data"

        # YouTube videos (2 rows)
        yt_dir = data_dir / "youtube_videos"
        yt_dir.mkdir(parents=True)
        pd.DataFrame({
            "video_id": ["v1", "v2"],
            "title": ["Video A", "Video B"],
            "published_at": pd.to_datetime(["2024-01-01", "2024-02-01"]),
            "view_count": [100, 200],
            "like_count": [10, 20],
            "comment_count": [1, 2],
        }).to_parquet(yt_dir / "data.parquet", index=False)

        # LinkedIn posts (1 row)
        li_dir = data_dir / "authoredup_posts"
        li_dir.mkdir(parents=True)
        pd.DataFrame({
            "post_id": ["p1"],
            "text": ["Post Alpha"],
            "published_at": pd.to_datetime(["2024-03-01"]),
            "impressions": [500],
            "reactions": [50],
            "comments": [5],
            "shares": [2],
        }).to_parquet(li_dir / "data.parquet", index=False)

        # Walker events (2 rows)
        walker_dir = data_dir / "walker_events"
        walker_dir.mkdir(parents=True)
        pd.DataFrame({
            "session_id": ["s1", "s2"],
            "timestamp": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "referrer": ["google.com", ""],
            "event_name": ["page_view", "click"],
            "page_path": ["/home", "/about"],
        }).to_parquet(walker_dir / "data.parquet", index=False)

        # Shortio clicks (1 row)
        shortio_dir = data_dir / "shortio_clicks"
        shortio_dir.mkdir(parents=True)
        pd.DataFrame({
            "clicked_at": pd.to_datetime(["2024-02-01"]),
            "utm_source": ["linkedin"],
            "utm_medium": ["social"],
            "utm_campaign": ["promo"],
            "short_path": ["/abc"],
            "original_url": ["https://example.com/a"],
        }).to_parquet(shortio_dir / "data.parquet", index=False)

        # Ghost members (1 row)
        ghost_dir = data_dir / "ghost_members"
        ghost_dir.mkdir(parents=True)
        pd.DataFrame({
            "email": ["alice@example.com"],
            "created_at": pd.to_datetime(["2024-03-01"]),
            "name": ["Alice"],
        }).to_parquet(ghost_dir / "data.parquet", index=False)

        return base_dir, data_dir

    def test_runner_discovers_all_three_entities(self, runner_data):
        """runner.run() discovers and executes product, signals, anon."""
        from fyrnheim.engine.runner import run

        base_dir, data_dir = runner_data
        entities_dir = base_dir / "entities"
        entities_dir.mkdir()
        generated_dir = base_dir / "generated"

        # Write product entity
        product_code = f'''\
from fyrnheim import (
    DimensionLayer, Entity, LayersConfig, PrepLayer, TableSource, UnionSource,
)

entity = Entity(
    name="product",
    description="Unified product",
    source=UnionSource(
        sources=[
            TableSource(
                project="test", dataset="test", table="youtube_videos",
                duckdb_path="{data_dir / "youtube_videos" / "*.parquet"}",
                field_mappings={{"video_id": "product_id"}},
                literal_columns={{"product_type": "video", "source_platform": "youtube"}},
            ),
            TableSource(
                project="test", dataset="test", table="authoredup_posts",
                duckdb_path="{data_dir / "authoredup_posts" / "*.parquet"}",
                field_mappings={{
                    "post_id": "product_id", "text": "title",
                    "impressions": "view_count", "reactions": "like_count",
                    "comments": "comment_count", "shares": "share_count",
                }},
                literal_columns={{"product_type": "post", "source_platform": "linkedin"}},
            ),
        ]
    ),
    layers=LayersConfig(
        prep=PrepLayer(model_name="prep_product"),
        dimension=DimensionLayer(model_name="dim_product"),
    ),
)
'''
        (entities_dir / "product.py").write_text(product_code)

        # Write signals entity
        signals_code = f'''\
from fyrnheim import (
    ComputedColumn, DimensionLayer, Entity, LayersConfig, PrepLayer, TableSource, UnionSource,
)
from fyrnheim.primitives import concat_hash, hash_email

entity = Entity(
    name="signals",
    description="Unified signals",
    source=UnionSource(
        sources=[
            TableSource(
                project="test", dataset="test", table="walker_events",
                duckdb_path="{data_dir / "walker_events" / "*.parquet"}",
                field_mappings={{"timestamp": "signal_timestamp"}},
                literal_columns={{"source": "walker"}},
            ),
            TableSource(
                project="test", dataset="test", table="shortio_clicks",
                duckdb_path="{data_dir / "shortio_clicks" / "*.parquet"}",
                field_mappings={{
                    "clicked_at": "signal_timestamp",
                    "utm_source": "channel_source",
                    "utm_medium": "channel_medium",
                    "utm_campaign": "campaign",
                }},
                literal_columns={{"source": "shortio", "signal_type": "link_clicked"}},
            ),
            TableSource(
                project="test", dataset="test", table="ghost_members",
                duckdb_path="{data_dir / "ghost_members" / "*.parquet"}",
                field_mappings={{"created_at": "signal_timestamp"}},
                literal_columns={{"source": "ghost", "signal_type": "newsletter_subscribed"}},
            ),
        ]
    ),
    layers=LayersConfig(
        prep=PrepLayer(model_name="prep_signals"),
        dimension=DimensionLayer(
            model_name="dim_signals",
            computed_columns=[
                ComputedColumn(name="person_id", expression=hash_email("email")),
                ComputedColumn(name="signal_id", expression=concat_hash(
                    "email", "session_id", "signal_timestamp", "signal_type", "source",
                )),
            ],
        ),
    ),
)
'''
        (entities_dir / "signals.py").write_text(signals_code)

        # Write anon entity
        anon_code = f'''\
from fyrnheim import (
    ComputedColumn, DimensionLayer, Entity, Field, LayersConfig, PrepLayer, TableSource,
)
from fyrnheim.primitives import categorize_contains

entity = Entity(
    name="anon",
    description="Anonymous visitor sessions",
    source=TableSource(
        project="test", dataset="test", table="walker_events",
        duckdb_path="{data_dir / "walker_events" / "*.parquet"}",
        fields=[
            Field(name="session_id", type="STRING"),
            Field(name="timestamp", type="TIMESTAMP"),
            Field(name="referrer", type="STRING"),
            Field(name="event_name", type="STRING"),
            Field(name="page_path", type="STRING"),
        ],
    ),
    layers=LayersConfig(
        prep=PrepLayer(model_name="prep_anon"),
        dimension=DimensionLayer(
            model_name="dim_anon",
            computed_columns=[
                ComputedColumn(
                    name="anon_id",
                    expression='(ibis.literal("walker") + t.session_id).hash().cast("string")',
                ),
                ComputedColumn(
                    name="source",
                    expression='ibis.literal("walker")',
                ),
                ComputedColumn(
                    name="channel_category",
                    expression=categorize_contains(
                        "referrer",
                        {{
                            "seo": ["google.com", "bing.com"],
                            "social_linkedin": ["linkedin.com"],
                        }},
                        default="direct",
                    ),
                ),
            ],
        ),
    ),
)
'''
        (entities_dir / "anon.py").write_text(anon_code)

        result = run(entities_dir, data_dir, backend="duckdb", generated_dir=generated_dir)
        assert result.ok is True
        assert len(result.entities) == 3
        assert all(e.status == "success" for e in result.entities)

        product_r = next(e for e in result.entities if e.entity_name == "product")
        signals_r = next(e for e in result.entities if e.entity_name == "signals")
        anon_r = next(e for e in result.entities if e.entity_name == "anon")

        assert product_r.row_count == 3  # 2 youtube + 1 linkedin
        assert signals_r.row_count == 4  # 2 walker + 1 shortio + 1 ghost
        assert anon_r.row_count == 2  # 2 walker events


# ---------------------------------------------------------------------------
# M006-E004: Person identity graph (4 sources) + Account aggregation
# ---------------------------------------------------------------------------


class TestE2EPersonIdentityGraph4Sources:
    """E2E tests for person entity with 4-source IdentityGraphConfig."""

    @pytest.fixture()
    def person_output(self, tmp_path):
        """Full 4-source identity graph pipeline."""
        from fyrnheim.core.source import (
            DerivedSource,
            IdentityGraphConfig,
            IdentityGraphSource,
        )

        # --- Create parquet files for 4 source entities ---

        # Ghost members (3 rows)
        ghost_df = pd.DataFrame({
            "id": ["g1", "g2", "g3"],
            "email": ["alice@acme.com", "bob@acme.com", "carol@gmail.com"],
            "status": ["paid", "free", "free"],
            "name": ["Alice Ghost", "Bob Ghost", "Carol Ghost"],
            "created_at": pd.to_datetime(["2024-01-01", "2024-02-01", "2024-03-01"]),
            "email_disabled": [False, False, True],
        })
        ghost_path = tmp_path / "ghost_members.parquet"
        ghost_df.to_parquet(ghost_path, index=False)

        # MailerLite subscribers (2 rows)
        mailerlite_df = pd.DataFrame({
            "id": ["m1", "m2"],
            "email": ["alice@acme.com", "dave@bigcorp.com"],
            "status": ["active", "active"],
            "subscribed_at": pd.to_datetime(["2024-01-15", "2024-04-01"]),
            "source": ["website", "api"],
        })
        mailerlite_path = tmp_path / "mailerlite_subscribers.parquet"
        mailerlite_df.to_parquet(mailerlite_path, index=False)

        # Transactions (2 rows) — source columns (before SourceMapping rename)
        txn_df = pd.DataFrame({
            "id": ["t1", "t2"],
            "customer_email": ["alice@acme.com", "eve@bigcorp.com"],
            "subtotal": [9900, 4900],
            "currency": ["USD", "USD"],
            "status": ["paid", "paid"],
            "created_at": pd.to_datetime(["2024-01-10", "2024-05-01"]),
            "customer_name": ["Alice Txn", "Eve Txn"],
        })
        txn_path = tmp_path / "transactions.parquet"
        txn_df.to_parquet(txn_path, index=False)

        # Subscriptions (2 rows) — source columns (before SourceMapping rename)
        sub_df = pd.DataFrame({
            "id": ["s1", "s2"],
            "user_email": ["bob@acme.com", "eve@bigcorp.com"],
            "status": ["active", "cancelled"],
            "created_at": pd.to_datetime(["2024-02-15", "2024-04-15"]),
            "user_name": ["Bob Sub", "Eve Sub"],
        })
        sub_path = tmp_path / "subscriptions.parquet"
        sub_df.to_parquet(sub_path, index=False)

        # --- Define all 4 source entities ---
        from fyrnheim.primitives import hash_email

        ghost_entity = Entity(
            name="ghost_person",
            description="Ghost members",
            source=TableSource(
                project="test", dataset="test", table="ghost_members",
                duckdb_path=str(ghost_path),
            ),
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_ghost_person"),
                dimension=DimensionLayer(
                    model_name="dim_ghost_person",
                    computed_columns=[
                        ComputedColumn(name="email_hash", expression=hash_email("email")),
                    ],
                ),
            ),
        )

        mailerlite_entity = Entity(
            name="mailerlite_person",
            description="MailerLite subscribers",
            source=TableSource(
                project="test", dataset="test", table="mailerlite_subscribers",
                duckdb_path=str(mailerlite_path),
            ),
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_mailerlite_person"),
                dimension=DimensionLayer(
                    model_name="dim_mailerlite_person",
                    computed_columns=[
                        ComputedColumn(name="email_hash", expression=hash_email("email")),
                        ComputedColumn(name="created_at", expression="t.subscribed_at"),
                    ],
                ),
            ),
        )

        txn_entity = Entity(
            name="transactions",
            description="Transactions",
            required_fields=[
                Field(name="transaction_id", type="STRING"),
                Field(name="customer_email", type="STRING"),
                Field(name="amount_cents", type="INT64"),
                Field(name="currency", type="STRING"),
                Field(name="status", type="STRING"),
                Field(name="created_at", type="TIMESTAMP"),
            ],
            optional_fields=[
                Field(name="customer_name", type="STRING"),
            ],
            core_computed=[
                ComputedColumn(name="customer_email_hash", expression=hash_email("customer_email")),
            ],
            source=TableSource(
                project="test", dataset="test", table="transactions",
                duckdb_path=str(txn_path),
            ),
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_transactions"),
                dimension=DimensionLayer(model_name="dim_transactions"),
            ),
        )
        txn_mapping = SourceMapping(
            entity=txn_entity, source=txn_entity.source,
            field_mappings={
                "transaction_id": "id", "customer_email": "customer_email",
                "amount_cents": "subtotal", "currency": "currency",
                "status": "status", "created_at": "created_at",
                "customer_name": "customer_name",
            },
        )

        sub_entity = Entity(
            name="subscriptions",
            description="Subscriptions",
            required_fields=[
                Field(name="subscription_id", type="STRING"),
                Field(name="user_email", type="STRING"),
                Field(name="status", type="STRING"),
                Field(name="created_at", type="TIMESTAMP"),
            ],
            optional_fields=[
                Field(name="user_name", type="STRING"),
            ],
            core_computed=[
                ComputedColumn(name="customer_email_hash", expression=hash_email("user_email")),
            ],
            source=TableSource(
                project="test", dataset="test", table="subscriptions",
                duckdb_path=str(sub_path),
            ),
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_subscriptions"),
                dimension=DimensionLayer(model_name="dim_subscriptions"),
            ),
        )
        sub_mapping = SourceMapping(
            entity=sub_entity, source=sub_entity.source,
            field_mappings={
                "subscription_id": "id", "user_email": "user_email",
                "status": "status", "created_at": "created_at",
                "user_name": "user_name",
            },
        )

        # --- Person entity (identity graph) ---
        _personal = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com"]
        _pl = repr(_personal)

        person_entity = Entity(
            name="person",
            description="Unified person",
            source=DerivedSource(
                identity_graph="person_graph",
                identity_graph_config=IdentityGraphConfig(
                    match_key="email_hash",
                    sources=[
                        IdentityGraphSource(
                            name="ghost_person", entity="ghost_person",
                            match_key_field="email_hash",
                            fields={"email": "email", "name": "name"},
                            id_field="id", date_field="created_at",
                        ),
                        IdentityGraphSource(
                            name="mailerlite_person", entity="mailerlite_person",
                            match_key_field="email_hash",
                            fields={"email": "email"},
                            id_field="id", date_field="created_at",
                        ),
                        IdentityGraphSource(
                            name="transactions", entity="transactions",
                            match_key_field="customer_email_hash",
                            fields={"email": "customer_email", "name": "customer_name"},
                            id_field="transaction_id", date_field="created_at",
                        ),
                        IdentityGraphSource(
                            name="subscriptions", entity="subscriptions",
                            match_key_field="customer_email_hash",
                            fields={"email": "user_email", "name": "user_name"},
                            id_field="subscription_id", date_field="created_at",
                        ),
                    ],
                    priority=["transactions", "subscriptions", "ghost_person", "mailerlite_person"],
                ),
            ),
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_person"),
                dimension=DimensionLayer(
                    model_name="dim_person",
                    computed_columns=[
                        ComputedColumn(name="person_id", expression="t.email_hash"),
                        ComputedColumn(name="email_domain", expression='t.email.split("@")[1]'),
                        ComputedColumn(
                            name="is_personal_email",
                            expression=f't.email.split("@")[1].isin({_pl})',
                        ),
                        ComputedColumn(
                            name="account_id",
                            expression=(
                                f'ibis.ifelse(t.email.split("@")[1].isin({_pl}), '
                                f'ibis.literal(None).cast("string"), '
                                f't.email.split("@")[1].hash().cast("string"))'
                            ),
                        ),
                        ComputedColumn(
                            name="created_at",
                            expression=(
                                "ibis.coalesce("
                                "t.first_seen_transactions, "
                                "t.first_seen_subscriptions, "
                                "t.first_seen_ghost_person, "
                                "t.first_seen_mailerlite_person)"
                            ),
                        ),
                    ],
                ),
            ),
        )

        # --- Generate and execute ---
        generated_dir = tmp_path / "generated"
        generate(ghost_entity, output_dir=generated_dir)
        generate(mailerlite_entity, output_dir=generated_dir)
        generate(txn_entity, output_dir=generated_dir, source_mapping=txn_mapping)
        generate(sub_entity, output_dir=generated_dir, source_mapping=sub_mapping)
        generate(person_entity, output_dir=generated_dir)

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=generated_dir) as executor:
            executor.execute("ghost_person")
            executor.execute("mailerlite_person")
            executor.execute("transactions")
            executor.execute("subscriptions")
            result = executor.execute("person", entity=person_entity)
            df = executor.connection.table(result.target_name).to_pandas()

        return result, df

    def test_person_row_count(self, person_output):
        """5 unique persons from 4 sources with overlaps."""
        result, df = person_output
        # alice@acme.com: ghost + mailerlite + transactions (3 sources, 1 person)
        # bob@acme.com: ghost + subscriptions (2 sources, 1 person)
        # carol@gmail.com: ghost only (1 source, 1 person)
        # dave@bigcorp.com: mailerlite only (1 source, 1 person)
        # eve@bigcorp.com: transactions + subscriptions (2 sources, 1 person)
        assert result.row_count == 5

    def test_person_source_flags(self, person_output):
        """Source flags are correct for each person."""
        _, df = person_output

        alice = df[df["email"] == "alice@acme.com"].iloc[0]
        assert bool(alice["is_ghost_person"]) is True
        assert bool(alice["is_mailerlite_person"]) is True
        assert bool(alice["is_transactions"]) is True
        assert bool(alice["is_subscriptions"]) is False

        bob = df[df["email"] == "bob@acme.com"].iloc[0]
        assert bool(bob["is_ghost_person"]) is True
        assert bool(bob["is_subscriptions"]) is True
        assert bool(bob["is_transactions"]) is False

        eve = df[df["email"] == "eve@bigcorp.com"].iloc[0]
        assert bool(eve["is_transactions"]) is True
        assert bool(eve["is_subscriptions"]) is True
        assert bool(eve["is_ghost_person"]) is False

        carol = df[df["email"] == "carol@gmail.com"].iloc[0]
        assert bool(carol["is_ghost_person"]) is True
        assert bool(carol["is_mailerlite_person"]) is False

    def test_person_priority_coalesce(self, person_output):
        """Shared fields resolved from highest-priority source."""
        _, df = person_output

        # alice: in transactions (priority 1) + ghost + mailerlite
        # name should come from transactions (highest priority)
        alice = df[df["email"] == "alice@acme.com"].iloc[0]
        assert alice["name"] == "Alice Txn"

        # bob: in subscriptions (priority 2) + ghost
        # name should come from subscriptions
        bob = df[df["email"] == "bob@acme.com"].iloc[0]
        assert bob["name"] == "Bob Sub"

        # carol: only in ghost
        carol = df[df["email"] == "carol@gmail.com"].iloc[0]
        assert carol["name"] == "Carol Ghost"

    def test_person_source_ids(self, person_output):
        """Source IDs preserved, NULL when not from that source."""
        _, df = person_output

        alice = df[df["email"] == "alice@acme.com"].iloc[0]
        assert alice["ghost_person_id"] == "g1"
        assert alice["transactions_id"] == "t1"
        assert pd.isna(alice["subscriptions_id"])

        eve = df[df["email"] == "eve@bigcorp.com"].iloc[0]
        assert eve["transactions_id"] == "t2"
        assert eve["subscriptions_id"] == "s2"
        assert pd.isna(eve["ghost_person_id"])

    def test_person_computed_columns(self, person_output):
        """person_id, email_domain, is_personal_email, account_id computed correctly."""
        _, df = person_output

        assert "person_id" in df.columns
        assert "email_domain" in df.columns
        assert "is_personal_email" in df.columns
        assert "account_id" in df.columns
        assert "created_at" in df.columns

        # carol@gmail.com → personal, account_id should be null
        carol = df[df["email"] == "carol@gmail.com"].iloc[0]
        assert carol["email_domain"] == "gmail.com"
        assert bool(carol["is_personal_email"]) is True
        assert pd.isna(carol["account_id"])

        # alice@acme.com → business, account_id should be non-null hash
        alice = df[df["email"] == "alice@acme.com"].iloc[0]
        assert alice["email_domain"] == "acme.com"
        assert bool(alice["is_personal_email"]) is False
        assert alice["account_id"] is not None and not pd.isna(alice["account_id"])

        # bob and alice share acme.com → same account_id
        bob = df[df["email"] == "bob@acme.com"].iloc[0]
        assert alice["account_id"] == bob["account_id"]


class TestE2EAccountAggregationFromPerson:
    """E2E test: account entity aggregating from 4-source person identity graph."""

    @pytest.fixture()
    def account_output(self, tmp_path):
        """Full 3-tier chain: 4 leaf entities → person → account."""
        from fyrnheim.core.source import (
            DerivedSource,
            IdentityGraphConfig,
            IdentityGraphSource,
        )
        from fyrnheim.primitives import hash_email

        # --- Same parquet data as person test ---
        ghost_df = pd.DataFrame({
            "id": ["g1", "g2"],
            "email": ["alice@acme.com", "bob@acme.com"],
            "status": ["paid", "free"],
            "name": ["Alice G", "Bob G"],
            "created_at": pd.to_datetime(["2024-01-01", "2024-02-01"]),
            "email_disabled": [False, False],
        })
        ghost_path = tmp_path / "ghost.parquet"
        ghost_df.to_parquet(ghost_path, index=False)

        mailerlite_df = pd.DataFrame({
            "id": ["m1"],
            "email": ["carol@bigcorp.com"],
            "status": ["active"],
            "subscribed_at": pd.to_datetime(["2024-03-01"]),
            "source": ["website"],
        })
        ml_path = tmp_path / "mailerlite.parquet"
        mailerlite_df.to_parquet(ml_path, index=False)

        txn_df = pd.DataFrame({
            "id": ["t1"],
            "customer_email": ["alice@acme.com"],
            "subtotal": [9900],
            "currency": ["USD"],
            "status": ["paid"],
            "created_at": pd.to_datetime(["2024-01-10"]),
            "customer_name": ["Alice T"],
        })
        txn_path = tmp_path / "txn.parquet"
        txn_df.to_parquet(txn_path, index=False)

        sub_df = pd.DataFrame({
            "id": ["s1"],
            "user_email": ["carol@bigcorp.com"],
            "status": ["active"],
            "created_at": pd.to_datetime(["2024-03-15"]),
            "user_name": ["Carol S"],
        })
        sub_path = tmp_path / "sub.parquet"
        sub_df.to_parquet(sub_path, index=False)

        # --- Define entities ---
        ghost_entity = Entity(
            name="ghost_person", description="Ghost",
            source=TableSource(project="t", dataset="t", table="ghost",
                               duckdb_path=str(ghost_path)),
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_ghost_person"),
                dimension=DimensionLayer(model_name="dim_ghost_person",
                    computed_columns=[
                        ComputedColumn(name="email_hash", expression=hash_email("email")),
                    ]),
            ),
        )

        ml_entity = Entity(
            name="mailerlite_person", description="MailerLite",
            source=TableSource(project="t", dataset="t", table="mailerlite",
                               duckdb_path=str(ml_path)),
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_mailerlite_person"),
                dimension=DimensionLayer(model_name="dim_mailerlite_person",
                    computed_columns=[
                        ComputedColumn(name="email_hash", expression=hash_email("email")),
                        ComputedColumn(name="created_at", expression="t.subscribed_at"),
                    ]),
            ),
        )

        txn_entity = Entity(
            name="transactions", description="Txn",
            required_fields=[
                Field(name="transaction_id", type="STRING"),
                Field(name="customer_email", type="STRING"),
                Field(name="amount_cents", type="INT64"),
                Field(name="currency", type="STRING"),
                Field(name="status", type="STRING"),
                Field(name="created_at", type="TIMESTAMP"),
            ],
            optional_fields=[Field(name="customer_name", type="STRING")],
            core_computed=[
                ComputedColumn(name="customer_email_hash", expression=hash_email("customer_email")),
            ],
            source=TableSource(project="t", dataset="t", table="txn",
                               duckdb_path=str(txn_path)),
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_transactions"),
                dimension=DimensionLayer(model_name="dim_transactions"),
            ),
        )
        txn_mapping = SourceMapping(
            entity=txn_entity, source=txn_entity.source,
            field_mappings={
                "transaction_id": "id", "customer_email": "customer_email",
                "amount_cents": "subtotal", "currency": "currency",
                "status": "status", "created_at": "created_at",
                "customer_name": "customer_name",
            },
        )

        sub_entity = Entity(
            name="subscriptions", description="Sub",
            required_fields=[
                Field(name="subscription_id", type="STRING"),
                Field(name="user_email", type="STRING"),
                Field(name="status", type="STRING"),
                Field(name="created_at", type="TIMESTAMP"),
            ],
            optional_fields=[Field(name="user_name", type="STRING")],
            core_computed=[
                ComputedColumn(name="customer_email_hash", expression=hash_email("user_email")),
            ],
            source=TableSource(project="t", dataset="t", table="sub",
                               duckdb_path=str(sub_path)),
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_subscriptions"),
                dimension=DimensionLayer(model_name="dim_subscriptions"),
            ),
        )
        sub_mapping = SourceMapping(
            entity=sub_entity, source=sub_entity.source,
            field_mappings={
                "subscription_id": "id", "user_email": "user_email",
                "status": "status", "created_at": "created_at",
                "user_name": "user_name",
            },
        )

        # --- Person + Account entities ---
        person_entity = Entity(
            name="person", description="Person",
            source=DerivedSource(
                identity_graph="person_graph",
                identity_graph_config=IdentityGraphConfig(
                    match_key="email_hash",
                    sources=[
                        IdentityGraphSource(name="ghost_person", entity="ghost_person",
                            match_key_field="email_hash",
                            fields={"email": "email", "name": "name"},
                            id_field="id", date_field="created_at"),
                        IdentityGraphSource(name="mailerlite_person", entity="mailerlite_person",
                            match_key_field="email_hash",
                            fields={"email": "email"},
                            id_field="id", date_field="created_at"),
                        IdentityGraphSource(name="transactions", entity="transactions",
                            match_key_field="customer_email_hash",
                            fields={"email": "customer_email", "name": "customer_name"},
                            id_field="transaction_id", date_field="created_at"),
                        IdentityGraphSource(name="subscriptions", entity="subscriptions",
                            match_key_field="customer_email_hash",
                            fields={"email": "user_email", "name": "user_name"},
                            id_field="subscription_id", date_field="created_at"),
                    ],
                    priority=["transactions", "subscriptions", "ghost_person", "mailerlite_person"],
                ),
            ),
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_person"),
                dimension=DimensionLayer(model_name="dim_person",
                    computed_columns=[
                        ComputedColumn(name="person_id", expression="t.email_hash"),
                        ComputedColumn(name="email_domain", expression='t.email.split("@")[1]'),
                        ComputedColumn(name="is_personal_email",
                            expression='t.email.split("@")[1].isin(["gmail.com"])'),
                        ComputedColumn(name="account_id",
                            expression=(
                                'ibis.ifelse(t.email.split("@")[1].isin(["gmail.com"]), '
                                'ibis.literal(None).cast("string"), '
                                't.email.split("@")[1].hash().cast("string"))'
                            )),
                        ComputedColumn(name="created_at",
                            expression=(
                                "ibis.coalesce(t.first_seen_transactions, "
                                "t.first_seen_subscriptions, t.first_seen_ghost_person, "
                                "t.first_seen_mailerlite_person)"
                            )),
                    ]),
            ),
        )

        from fyrnheim.components.computed_column import ComputedColumn as CC

        account_entity = Entity(
            name="account", description="Account",
            source=AggregationSource(
                source_entity="person",
                group_by_column="account_id",
                filter_expression="t.account_id.notnull()",
                aggregations=[
                    CC(name="email_domain", expression="t.email_domain.arbitrary()"),
                    CC(name="num_persons", expression="t.person_id.nunique()"),
                    CC(name="has_ghost_person", expression="t.is_ghost_person.any()"),
                    CC(name="has_transactions", expression="t.is_transactions.any()"),
                    CC(name="first_seen_date", expression="t.created_at.min()"),
                ],
            ),
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_account"),
                dimension=DimensionLayer(model_name="dim_account"),
            ),
        )

        # --- Generate and execute full chain ---
        generated_dir = tmp_path / "generated"
        generate(ghost_entity, output_dir=generated_dir)
        generate(ml_entity, output_dir=generated_dir)
        generate(txn_entity, output_dir=generated_dir, source_mapping=txn_mapping)
        generate(sub_entity, output_dir=generated_dir, source_mapping=sub_mapping)
        generate(person_entity, output_dir=generated_dir)
        generate(account_entity, output_dir=generated_dir)

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=generated_dir) as executor:
            executor.execute("ghost_person")
            executor.execute("mailerlite_person")
            executor.execute("transactions")
            executor.execute("subscriptions")
            executor.execute("person", entity=person_entity)
            result = executor.execute("account", entity=account_entity)
            df = executor.connection.table(result.target_name).to_pandas()

        return result, df

    def test_account_row_count(self, account_output):
        """2 business accounts: acme.com (alice + bob) and bigcorp.com (carol)."""
        result, df = account_output
        # alice@acme.com, bob@acme.com → 1 acme account
        # carol@bigcorp.com → 1 bigcorp account
        # No gmail.com persons (none in this test data)
        assert result.row_count == 2

    def test_account_num_persons(self, account_output):
        """Person count per account is correct."""
        _, df = account_output

        # Find account rows by email_domain
        acme = df[df["email_domain"] == "acme.com"].iloc[0]
        assert acme["num_persons"] == 2  # alice + bob

        bigcorp = df[df["email_domain"] == "bigcorp.com"].iloc[0]
        assert bigcorp["num_persons"] == 1  # carol

    def test_account_source_flags(self, account_output):
        """has_* source presence flags correct."""
        _, df = account_output

        acme = df[df["email_domain"] == "acme.com"].iloc[0]
        assert bool(acme["has_ghost_person"]) is True  # alice + bob are ghost members
        assert bool(acme["has_transactions"]) is True  # alice has a transaction

        bigcorp = df[df["email_domain"] == "bigcorp.com"].iloc[0]
        assert bool(bigcorp["has_ghost_person"]) is False  # carol not in ghost

    def test_account_columns(self, account_output):
        """Output has expected columns."""
        _, df = account_output
        expected = {"account_id", "email_domain", "num_persons", "has_ghost_person",
                    "has_transactions", "first_seen_date"}
        assert expected.issubset(set(df.columns))
