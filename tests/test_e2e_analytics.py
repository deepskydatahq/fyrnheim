"""End-to-end tests for the analytics layer pipeline.

Proves: entity with AnalyticsLayer -> generate -> execute on DuckDB ->
verify date-grain aggregation with correct row counts and values.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass

import pandas as pd
import pytest

from fyrnheim import (
    AnalyticsLayer,
    AnalyticsMetric,
    ComputedColumn,
    DimensionLayer,
    Entity,
    LayersConfig,
    PrepLayer,
    TableSource,
)
from fyrnheim._generate import GenerateResult, generate
from fyrnheim.engine import IbisExecutor

# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------


@dataclass
class AnalyticsPipelineResult:
    """Holds artifacts from an analytics pipeline run."""

    generate_result: GenerateResult
    dim_df: pd.DataFrame
    analytics_df: pd.DataFrame
    input_row_count: int


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def analytics_parquet(tmp_path):
    """Create 20-row parquet: 5 rows per date across 4 dates, 3 plan types.

    Dates: 2025-01-01 to 2025-01-04
    Plans: free, pro, enterprise (cycling)
    Revenue: deterministic per row for verification
    """
    rows = []
    plans = ["free", "pro", "enterprise"]
    for day in range(4):
        for i in range(5):
            row_id = day * 5 + i + 1
            rows.append(
                {
                    "id": row_id,
                    "email": f"user{row_id}@example.com",
                    "name": f"User {row_id}",
                    "plan": plans[i % 3],
                    "created_at": pd.Timestamp(f"2025-01-0{day + 1} {10 + i}:00:00"),
                    "amount_cents": row_id * 100,
                }
            )

    df = pd.DataFrame(rows)
    path = tmp_path / "data" / "sales.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return path, len(df)


@pytest.fixture()
def analytics_pipeline(analytics_parquet, tmp_path):
    """Run the analytics pipeline and return all artifacts."""
    parquet_path, input_row_count = analytics_parquet
    generated_dir = tmp_path / "generated"

    entity = Entity(
        name="sales",
        description="Sales entity with analytics layer for testing",
        source=TableSource(
            project="test",
            dataset="test",
            table="sales",
            duckdb_path=str(parquet_path),
        ),
        layers=LayersConfig(
            prep=PrepLayer(
                model_name="prep_sales",
                computed_columns=[
                    ComputedColumn(
                        name="amount_dollars",
                        expression="t.amount_cents / 100.0",
                        description="Amount in dollars",
                    ),
                ],
            ),
            dimension=DimensionLayer(model_name="dim_sales"),
            analytics=AnalyticsLayer(
                model_name="analytics_sales",
                date_expression='t.created_at.cast("date")',
                metrics=[
                    AnalyticsMetric(
                        name="total_revenue",
                        expression="t.amount_cents.sum()",
                        metric_type="event",
                    ),
                    AnalyticsMetric(
                        name="order_count",
                        expression="t.amount_cents.count()",
                        metric_type="event",
                    ),
                ],
            ),
        ),
    )

    gen_result = generate(entity, output_dir=generated_dir)

    with IbisExecutor.duckdb(generated_dir=generated_dir) as executor:
        executor.execute("sales")

        dim_df = executor.connection.table("dim_sales").to_pandas()
        analytics_df = executor.connection.table("analytics_sales").to_pandas()

    return AnalyticsPipelineResult(
        generate_result=gen_result,
        dim_df=dim_df,
        analytics_df=analytics_df,
        input_row_count=input_row_count,
    )


@pytest.fixture()
def analytics_with_dimensions_pipeline(analytics_parquet, tmp_path):
    """Run analytics pipeline with dimension grouping (plan)."""
    parquet_path, input_row_count = analytics_parquet
    generated_dir = tmp_path / "generated"

    entity = Entity(
        name="sales",
        description="Sales with plan dimension for analytics",
        source=TableSource(
            project="test",
            dataset="test",
            table="sales",
            duckdb_path=str(parquet_path),
        ),
        layers=LayersConfig(
            prep=PrepLayer(model_name="prep_sales"),
            dimension=DimensionLayer(model_name="dim_sales"),
            analytics=AnalyticsLayer(
                model_name="analytics_sales",
                date_expression='t.created_at.cast("date")',
                metrics=[
                    AnalyticsMetric(
                        name="total_revenue",
                        expression="t.amount_cents.sum()",
                        metric_type="event",
                    ),
                    AnalyticsMetric(
                        name="order_count",
                        expression="t.amount_cents.count()",
                        metric_type="event",
                    ),
                ],
                dimensions=["plan"],
            ),
        ),
    )

    generate(entity, output_dir=generated_dir)

    with IbisExecutor.duckdb(generated_dir=generated_dir) as executor:
        executor.execute("sales")
        analytics_df = executor.connection.table("analytics_sales").to_pandas()

    return analytics_df


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestAnalyticsCodeGeneration:
    """Verify generated code has analytics function."""

    def test_generated_code_is_valid_python(self, analytics_pipeline):
        ast.parse(analytics_pipeline.generate_result.code)

    def test_has_analytics_function(self, analytics_pipeline):
        code = analytics_pipeline.generate_result.code
        tree = ast.parse(code)
        func_names = {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
        assert "analytics_sales" in func_names


class TestAnalyticsAggregation:
    """Verify analytics aggregation output."""

    def test_has_analytics_columns(self, analytics_pipeline):
        cols = set(analytics_pipeline.analytics_df.columns)
        assert "_date" in cols
        assert "total_revenue" in cols
        assert "order_count" in cols

    def test_date_grain(self, analytics_pipeline):
        # 4 unique dates (one per day)
        assert len(analytics_pipeline.analytics_df) == 4

    def test_order_count_per_day(self, analytics_pipeline):
        # Each day has exactly 5 orders
        df = analytics_pipeline.analytics_df.sort_values("_date")
        counts = df["order_count"].tolist()
        assert all(c == 5 for c in counts)

    def test_total_revenue_day_1(self, analytics_pipeline):
        # Day 1: ids 1-5, amounts 100,200,300,400,500 = 1500 cents
        df = analytics_pipeline.analytics_df.sort_values("_date")
        assert df.iloc[0]["total_revenue"] == 1500

    def test_total_revenue_day_4(self, analytics_pipeline):
        # Day 4: ids 16-20, amounts 1600+1700+1800+1900+2000 = 9000 cents
        df = analytics_pipeline.analytics_df.sort_values("_date")
        assert df.iloc[3]["total_revenue"] == 9000

    def test_total_revenue_is_positive(self, analytics_pipeline):
        assert (analytics_pipeline.analytics_df["total_revenue"] > 0).all()


class TestAnalyticsWithDimensions:
    """Verify analytics with dimension grouping."""

    def test_dimension_grouping(self, analytics_with_dimensions_pipeline):
        df = analytics_with_dimensions_pipeline
        assert "plan" in df.columns

    def test_row_count_with_dimensions(self, analytics_with_dimensions_pipeline):
        df = analytics_with_dimensions_pipeline
        # 4 dates x 3 plans = 12 rows
        assert len(df) == 12

    def test_order_count_per_group(self, analytics_with_dimensions_pipeline):
        df = analytics_with_dimensions_pipeline
        # Each date has 5 rows cycling through 3 plans:
        # free: 2 per day, pro: 2 per day, enterprise: 1 per day
        # (indices 0,3 are free; 1,4 are pro; 2 is enterprise)
        # Verify no group has more than 2 orders
        assert (df["order_count"] <= 2).all()
