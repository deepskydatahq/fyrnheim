"""End-to-end tests for the activity layer pipeline.

Proves: entity with ActivityConfig -> generate -> execute on DuckDB ->
verify activity stream output with correct event counts and schema.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass

import pandas as pd
import pytest

from fyrnheim import (
    ActivityConfig,
    ActivityType,
    ComputedColumn,
    DimensionLayer,
    Entity,
    LayersConfig,
    PrepLayer,
    TableSource,
)
from fyrnheim._generate import GenerateResult, generate
from fyrnheim.engine import DuckDBExecutor
from fyrnheim.primitives import hash_email

# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------


@dataclass
class ActivityPipelineResult:
    """Holds artifacts from an activity pipeline run."""

    generate_result: GenerateResult
    dim_df: pd.DataFrame
    activity_df: pd.DataFrame
    input_row_count: int


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def activity_parquet(tmp_path):
    """Create 8-row parquet with mix of free/pro/enterprise plans.

    Plan distribution: free(3), pro(2), enterprise(3)
    Expected activities:
    - signup (row_appears): 8 events (all rows)
    - became_paying (status_becomes pro/enterprise): 5 events
    """
    df = pd.DataFrame(
        {
            "id": list(range(1, 9)),
            "email": [
                "alice@example.com",
                "bob@test.org",
                "carol@example.com",
                "dave@company.io",
                "eve@example.com",
                "frank@test.org",
                "grace@company.io",
                "henry@example.com",
            ],
            "name": [
                "Alice",
                "Bob",
                "Carol",
                "Dave",
                "Eve",
                "Frank",
                "Grace",
                "Henry",
            ],
            "plan": [
                "free",
                "pro",
                "enterprise",
                "free",
                "pro",
                "enterprise",
                "free",
                "enterprise",
            ],
            "created_at": pd.to_datetime(
                [
                    "2025-01-01",
                    "2025-02-01",
                    "2025-03-01",
                    "2025-04-01",
                    "2025-05-01",
                    "2025-06-01",
                    "2025-07-01",
                    "2025-08-01",
                ]
            ),
            "amount_cents": [0, 2900, 9900, 0, 2900, 9900, 0, 9900],
        }
    )
    path = tmp_path / "data" / "members.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return path, len(df)


@pytest.fixture()
def activity_pipeline(activity_parquet, tmp_path):
    """Run the activity pipeline and return all artifacts."""
    parquet_path, input_row_count = activity_parquet
    generated_dir = tmp_path / "generated"

    entity = Entity(
        name="members",
        description="Members entity with activity layer for testing",
        source=TableSource(
            project="test",
            dataset="test",
            table="members",
            duckdb_path=str(parquet_path),
        ),
        layers=LayersConfig(
            prep=PrepLayer(
                model_name="prep_members",
                computed_columns=[
                    ComputedColumn(
                        name="email_hash",
                        expression=hash_email("email"),
                        description="SHA256 hash of email",
                    ),
                ],
            ),
            dimension=DimensionLayer(model_name="dim_members"),
            activity=ActivityConfig(
                model_name="activity_members",
                types=[
                    ActivityType(
                        name="signup",
                        trigger="row_appears",
                        timestamp_field="created_at",
                    ),
                    ActivityType(
                        name="became_paying",
                        trigger="status_becomes",
                        timestamp_field="created_at",
                        field="plan",
                        values=["pro", "enterprise"],
                    ),
                ],
                entity_id_field="id",
                person_id_field="email_hash",
            ),
        ),
    )

    gen_result = generate(entity, output_dir=generated_dir)

    with DuckDBExecutor(generated_dir=generated_dir) as executor:
        executor.execute("members")

        dim_df = executor.connection.table("dim_members").to_pandas()
        activity_df = executor.connection.table("activity_members").to_pandas()

    return ActivityPipelineResult(
        generate_result=gen_result,
        dim_df=dim_df,
        activity_df=activity_df,
        input_row_count=input_row_count,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestActivityCodeGeneration:
    """Verify generated code has activity function."""

    def test_generated_code_is_valid_python(self, activity_pipeline):
        ast.parse(activity_pipeline.generate_result.code)

    def test_has_activity_function(self, activity_pipeline):
        code = activity_pipeline.generate_result.code
        tree = ast.parse(code)
        func_names = {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
        assert "activity_members" in func_names


class TestActivityStream:
    """Verify activity stream output."""

    def test_has_activity_columns(self, activity_pipeline):
        cols = set(activity_pipeline.activity_df.columns)
        assert "entity_id" in cols
        assert "identity" in cols
        assert "ts" in cols
        assert "activity_type" in cols

    def test_activity_types_present(self, activity_pipeline):
        types = set(activity_pipeline.activity_df["activity_type"].unique())
        assert "signup" in types
        assert "became_paying" in types

    def test_signup_count(self, activity_pipeline):
        # row_appears: one event per row = 8 events
        signups = activity_pipeline.activity_df[
            activity_pipeline.activity_df["activity_type"] == "signup"
        ]
        assert len(signups) == activity_pipeline.input_row_count

    def test_became_paying_count(self, activity_pipeline):
        # status_becomes pro/enterprise: 5 rows have pro or enterprise
        paying = activity_pipeline.activity_df[
            activity_pipeline.activity_df["activity_type"] == "became_paying"
        ]
        assert len(paying) == 5

    def test_total_activity_count(self, activity_pipeline):
        # 8 signups + 5 became_paying = 13 total
        assert len(activity_pipeline.activity_df) == 13

    def test_entity_id_is_string(self, activity_pipeline):
        # DuckDB returns StringDtype; check values are string-like
        sample = activity_pipeline.activity_df["entity_id"].iloc[0]
        assert isinstance(sample, str)

    def test_dim_table_unaffected(self, activity_pipeline):
        # Dim table should still have all input rows
        assert len(activity_pipeline.dim_df) == activity_pipeline.input_row_count
