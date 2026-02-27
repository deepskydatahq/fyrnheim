"""End-to-end test for the full 5-layer pipeline.

Proves: define entity with all 5 layers -> generate -> execute -> verify
all output tables (dim, snapshot, activity, analytics) + quality checks.
Uses a synthetic 100-row "orders" entity.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass

import pandas as pd
import pytest

from fyrnheim import (
    ActivityConfig,
    ActivityType,
    AnalyticsLayer,
    AnalyticsMetric,
    ComputedColumn,
    DimensionLayer,
    Entity,
    InRange,
    LayersConfig,
    NotNull,
    PrepLayer,
    QualityConfig,
    QualityRunner,
    SnapshotLayer,
    TableSource,
    Unique,
)
from fyrnheim._generate import GenerateResult, generate
from fyrnheim.engine import IbisExecutor
from fyrnheim.primitives import hash_email

# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------


@dataclass
class FullPipelineResult:
    """Holds all artifacts from a full pipeline run."""

    generate_result: GenerateResult
    dim_df: pd.DataFrame
    snapshot_df: pd.DataFrame
    activity_df: pd.DataFrame
    analytics_df: pd.DataFrame
    quality_results: list
    entity: Entity
    input_row_count: int
    exec_result: object


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def orders_parquet(tmp_path):
    """Create synthetic 100-row orders parquet.

    10 rows per day over 10 days (Jan 1-10, 2025).
    Status cycles: pending, active, inactive.
    Revenue: deterministic per row_id.
    """
    rows = []
    statuses = ["pending", "active", "inactive"]

    row_id = 1
    for day_offset in range(10):
        day = pd.Timestamp("2025-01-01") + pd.Timedelta(days=day_offset)
        for _ in range(10):
            email = f"user{((row_id - 1) % 30) + 1}@example.com"
            status = statuses[(row_id - 1) % 3]
            revenue = (row_id % 100) * 100
            rows.append(
                {
                    "id": row_id,
                    "email": email,
                    "name": f"User {row_id}",
                    "status": status,
                    "created_at": day + pd.Timedelta(hours=row_id % 24),
                    "updated_at": day + pd.Timedelta(hours=row_id % 24, minutes=30),
                    "revenue_cents": revenue,
                }
            )
            row_id += 1

    df = pd.DataFrame(rows)
    path = tmp_path / "data" / "orders.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return path, len(df)


@pytest.fixture()
def orders_entity(orders_parquet):
    """Build the full 5-layer orders entity."""
    parquet_path, _ = orders_parquet
    return Entity(
        name="orders",
        description="Synthetic orders for full-stack E2E test",
        source=TableSource(
            project="test",
            dataset="test",
            table="orders",
            duckdb_path=str(parquet_path),
        ),
        layers=LayersConfig(
            prep=PrepLayer(
                model_name="prep_orders",
                computed_columns=[
                    ComputedColumn(
                        name="email_hash",
                        expression=hash_email("email"),
                        description="SHA256 hash of email",
                    ),
                    ComputedColumn(
                        name="revenue_dollars",
                        expression="t.revenue_cents / 100.0",
                        description="Revenue in dollars",
                    ),
                ],
            ),
            dimension=DimensionLayer(
                model_name="dim_orders",
                computed_columns=[
                    ComputedColumn(
                        name="email_domain",
                        expression="t.email.split('@')[1]",
                        description="Email domain",
                    ),
                    ComputedColumn(
                        name="is_active",
                        expression="t.status == 'active'",
                        description="True if status is active",
                    ),
                ],
            ),
            snapshot=SnapshotLayer(
                natural_key="id",
                deduplication_order_by="updated_at DESC",
            ),
            activity=ActivityConfig(
                model_name="activity_orders",
                types=[
                    ActivityType(
                        name="order_created",
                        trigger="row_appears",
                        timestamp_field="created_at",
                    ),
                    ActivityType(
                        name="became_active",
                        trigger="status_becomes",
                        timestamp_field="updated_at",
                        field="status",
                        values=["active"],
                    ),
                ],
                entity_id_field="id",
                person_id_field="email_hash",
            ),
            analytics=AnalyticsLayer(
                model_name="analytics_orders",
                date_expression='t.created_at.cast("date")',
                metrics=[
                    AnalyticsMetric(
                        name="total_revenue",
                        expression="t.revenue_cents.sum()",
                        metric_type="event",
                    ),
                    AnalyticsMetric(
                        name="order_count",
                        expression="t.revenue_cents.count()",
                        metric_type="event",
                    ),
                ],
            ),
        ),
        quality=QualityConfig(
            primary_key="id",
            checks=[
                NotNull("email"),
                NotNull("id"),
                Unique("id"),
                InRange("revenue_cents", min=0),
            ],
        ),
    )


@pytest.fixture()
def full_pipeline(orders_parquet, orders_entity, tmp_path):
    """Run the full 5-layer pipeline and return all artifacts."""
    parquet_path, input_row_count = orders_parquet
    generated_dir = tmp_path / "generated"
    entity = orders_entity

    gen_result = generate(entity, output_dir=generated_dir)

    with IbisExecutor.duckdb(generated_dir=generated_dir) as executor:
        exec_result = executor.execute("orders")

        dim_df = executor.connection.table("dim_orders").to_pandas()
        snapshot_df = executor.connection.table("snapshot_orders").to_pandas()
        activity_df = executor.connection.table("activity_orders").to_pandas()
        analytics_df = executor.connection.table("analytics_orders").to_pandas()

        qr = QualityRunner(executor.connection)
        entity_result = qr.run_entity_checks(
            entity_name=entity.name,
            quality_config=entity.quality,
            primary_key=entity.quality.primary_key,
            table_name="dim_orders",
        )
        quality_results = entity_result.results

    return FullPipelineResult(
        generate_result=gen_result,
        dim_df=dim_df,
        snapshot_df=snapshot_df,
        activity_df=activity_df,
        analytics_df=analytics_df,
        quality_results=quality_results,
        entity=entity,
        input_row_count=input_row_count,
        exec_result=exec_result,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFullPipelineCodeGeneration:
    """Verify generated code has all layer functions."""

    def test_generated_code_is_valid_python(self, full_pipeline):
        ast.parse(full_pipeline.generate_result.code)

    def test_has_all_six_functions(self, full_pipeline):
        code = full_pipeline.generate_result.code
        tree = ast.parse(code)
        func_names = {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
        assert "source_orders" in func_names
        assert "prep_orders" in func_names
        assert "dim_orders" in func_names
        assert "snapshot_orders" in func_names
        assert "activity_orders" in func_names
        assert "analytics_orders" in func_names


class TestDimOrders:
    """Verify dimension layer output."""

    def test_row_count_matches_input(self, full_pipeline):
        assert len(full_pipeline.dim_df) == full_pipeline.input_row_count

    def test_has_computed_columns(self, full_pipeline):
        cols = set(full_pipeline.dim_df.columns)
        assert "email_hash" in cols
        assert "revenue_dollars" in cols
        assert "email_domain" in cols
        assert "is_active" in cols

    def test_email_domain_correct(self, full_pipeline):
        domains = full_pipeline.dim_df["email_domain"].unique()
        assert len(domains) == 1
        assert domains[0] == "example.com"

    def test_is_active_correct(self, full_pipeline):
        df = full_pipeline.dim_df
        active_rows = df[df["status"] == "active"]
        inactive_rows = df[df["status"] != "active"]
        assert active_rows["is_active"].all()
        assert not inactive_rows["is_active"].any()


class TestSnapshotOrders:
    """Verify snapshot layer output."""

    def test_has_snapshot_columns(self, full_pipeline):
        cols = set(full_pipeline.snapshot_df.columns)
        assert "ds" in cols
        assert "snapshot_key" in cols

    def test_deduplication_applied(self, full_pipeline):
        df = full_pipeline.snapshot_df
        dupes = df.groupby(["id", "ds"]).size()
        assert (dupes == 1).all()

    def test_row_count(self, full_pipeline):
        # 100 unique ids, 1 snapshot date -> 100 rows
        assert len(full_pipeline.snapshot_df) == full_pipeline.input_row_count

    def test_target_name_is_snapshot(self, full_pipeline):
        assert full_pipeline.exec_result.target_name == "snapshot_orders"


class TestActivityOrders:
    """Verify activity stream output."""

    def test_has_activity_columns(self, full_pipeline):
        cols = set(full_pipeline.activity_df.columns)
        assert "entity_id" in cols
        assert "identity" in cols
        assert "ts" in cols
        assert "activity_type" in cols

    def test_activity_types_present(self, full_pipeline):
        types = set(full_pipeline.activity_df["activity_type"].unique())
        assert "order_created" in types
        assert "became_active" in types

    def test_order_created_count(self, full_pipeline):
        created = full_pipeline.activity_df[
            full_pipeline.activity_df["activity_type"] == "order_created"
        ]
        assert len(created) == full_pipeline.input_row_count

    def test_became_active_count(self, full_pipeline):
        active = full_pipeline.activity_df[
            full_pipeline.activity_df["activity_type"] == "became_active"
        ]
        # (row_id - 1) % 3 == 1 => active: ids 2,5,8,...,98 = 33 rows
        expected_active = len([i for i in range(1, 101) if (i - 1) % 3 == 1])
        assert len(active) == expected_active

    def test_total_activity_count(self, full_pipeline):
        expected_active = len([i for i in range(1, 101) if (i - 1) % 3 == 1])
        assert len(full_pipeline.activity_df) == 100 + expected_active


class TestAnalyticsOrders:
    """Verify analytics aggregation output."""

    def test_has_analytics_columns(self, full_pipeline):
        cols = set(full_pipeline.analytics_df.columns)
        assert "_date" in cols
        assert "total_revenue" in cols
        assert "order_count" in cols

    def test_date_grain(self, full_pipeline):
        # 10 unique dates (one per day)
        assert len(full_pipeline.analytics_df) == 10

    def test_order_count_per_day(self, full_pipeline):
        counts = full_pipeline.analytics_df["order_count"].tolist()
        assert all(c == 10 for c in counts)

    def test_total_revenue_is_positive(self, full_pipeline):
        assert (full_pipeline.analytics_df["total_revenue"] > 0).all()


class TestQualityChecks:
    """Verify quality checks pass on clean data."""

    def test_all_checks_pass(self, full_pipeline):
        assert len(full_pipeline.quality_results) >= 2
        for check_result in full_pipeline.quality_results:
            assert check_result.passed is True, (
                f"Quality check failed: {check_result.check_name}"
            )
