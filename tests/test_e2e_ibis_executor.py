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
    DimensionLayer,
    Entity,
    LayersConfig,
    PrepLayer,
    TableSource,
)
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
