"""End-to-end tests for the snapshot layer pipeline.

Proves: entity with SnapshotLayer -> generate -> execute on DuckDB ->
verify dedup, surrogate keys, ds column, and multi-table output.
Uses duplicate input rows to prove deduplication works.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass

import pandas as pd
import pytest

from fyrnheim import (
    ComputedColumn,
    DimensionLayer,
    Entity,
    LayersConfig,
    PrepLayer,
    SnapshotLayer,
    TableSource,
)
from fyrnheim._generate import GenerateResult, generate
from fyrnheim.engine import DuckDBExecutor

# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------


@dataclass
class SnapshotPipelineResult:
    """Holds artifacts from a snapshot pipeline run."""

    generate_result: GenerateResult
    dim_df: pd.DataFrame
    snapshot_df: pd.DataFrame
    exec_result: object
    input_row_count: int


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def snapshot_parquet(tmp_path):
    """Create 5-row parquet with duplicate ids for dedup testing.

    id=1 appears twice (different updated_at, names)
    id=2 appears twice
    id=3 appears once
    Expected: dim has 5 rows, snapshot has 3 rows (deduped by id).
    """
    df = pd.DataFrame(
        {
            "id": [1, 1, 2, 2, 3],
            "email": [
                "alice@example.com",
                "alice@example.com",
                "bob@test.org",
                "bob@test.org",
                "carol@example.com",
            ],
            "name": [
                "Alice Old",
                "Alice New",
                "Bob Old",
                "Bob New",
                "Carol",
            ],
            "plan": ["free", "pro", "pro", "enterprise", "free"],
            "amount_cents": [0, 2900, 2900, 9900, 0],
            "updated_at": pd.to_datetime(
                [
                    "2025-01-01 10:00:00",
                    "2025-01-15 10:00:00",  # newer for id=1
                    "2025-02-01 10:00:00",
                    "2025-02-15 10:00:00",  # newer for id=2
                    "2025-03-01 10:00:00",
                ]
            ),
        }
    )
    path = tmp_path / "data" / "users.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return path, len(df)


@pytest.fixture()
def snapshot_pipeline(snapshot_parquet, tmp_path):
    """Run the full snapshot pipeline and return all artifacts."""
    parquet_path, input_row_count = snapshot_parquet
    generated_dir = tmp_path / "generated"

    entity = Entity(
        name="users",
        description="Users entity with snapshot for dedup testing",
        source=TableSource(
            project="test",
            dataset="test",
            table="users",
            duckdb_path=str(parquet_path),
        ),
        layers=LayersConfig(
            prep=PrepLayer(
                model_name="prep_users",
                computed_columns=[
                    ComputedColumn(
                        name="amount_dollars",
                        expression="t.amount_cents / 100.0",
                        description="Amount in dollars",
                    ),
                ],
            ),
            dimension=DimensionLayer(
                model_name="dim_users",
                computed_columns=[
                    ComputedColumn(
                        name="is_paying",
                        expression="t.plan != 'free'",
                        description="True if on a paid plan",
                    ),
                ],
            ),
            snapshot=SnapshotLayer(
                natural_key="id",
                deduplication_order_by="updated_at DESC",
            ),
        ),
    )

    gen_result = generate(entity, output_dir=generated_dir)

    with DuckDBExecutor(generated_dir=generated_dir) as executor:
        exec_result = executor.execute("users")

        dim_df = executor.connection.table("dim_users").to_pandas()
        snapshot_df = executor.connection.table("snapshot_users").to_pandas()

    return SnapshotPipelineResult(
        generate_result=gen_result,
        dim_df=dim_df,
        snapshot_df=snapshot_df,
        exec_result=exec_result,
        input_row_count=input_row_count,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSnapshotCodeGeneration:
    """Verify generated code has snapshot function."""

    def test_generated_code_is_valid_python(self, snapshot_pipeline):
        ast.parse(snapshot_pipeline.generate_result.code)

    def test_has_snapshot_function(self, snapshot_pipeline):
        code = snapshot_pipeline.generate_result.code
        tree = ast.parse(code)
        func_names = {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
        assert "snapshot_users" in func_names


class TestDimTable:
    """Verify dim table preserves all input rows."""

    def test_dim_table_created(self, snapshot_pipeline):
        assert len(snapshot_pipeline.dim_df) == snapshot_pipeline.input_row_count

    def test_dim_has_computed_columns(self, snapshot_pipeline):
        cols = set(snapshot_pipeline.dim_df.columns)
        assert "amount_dollars" in cols
        assert "is_paying" in cols


class TestSnapshotE2E:
    """Verify snapshot layer output."""

    def test_snapshot_table_created(self, snapshot_pipeline):
        assert len(snapshot_pipeline.snapshot_df) > 0

    def test_snapshot_has_ds_column(self, snapshot_pipeline):
        assert "ds" in snapshot_pipeline.snapshot_df.columns

    def test_snapshot_has_surrogate_key(self, snapshot_pipeline):
        assert "snapshot_key" in snapshot_pipeline.snapshot_df.columns
        # All keys should be unique
        keys = snapshot_pipeline.snapshot_df["snapshot_key"]
        assert keys.nunique() == len(keys)

    def test_snapshot_deduplicates_rows(self, snapshot_pipeline):
        # 5 input rows with 3 unique ids -> 3 snapshot rows
        assert len(snapshot_pipeline.snapshot_df) == 3

    def test_snapshot_keeps_latest_version(self, snapshot_pipeline):
        df = snapshot_pipeline.snapshot_df

        # id=1 should have "Alice New" (updated_at 2025-01-15 > 2025-01-01)
        alice = df[df["id"] == 1].iloc[0]
        assert alice["name"] == "Alice New"
        assert alice["plan"] == "pro"

        # id=2 should have "Bob New" (updated_at 2025-02-15 > 2025-02-01)
        bob = df[df["id"] == 2].iloc[0]
        assert bob["name"] == "Bob New"
        assert bob["plan"] == "enterprise"

    def test_execution_result_target_name(self, snapshot_pipeline):
        assert snapshot_pipeline.exec_result.target_name == "snapshot_users"

    def test_execution_result_snapshot_target_name(self, snapshot_pipeline):
        assert snapshot_pipeline.exec_result.snapshot_target_name == "snapshot_users"

    def test_execution_result_row_count(self, snapshot_pipeline):
        # ExecutionResult reports the snapshot table row count (3 deduped rows)
        assert snapshot_pipeline.exec_result.row_count == 3
