"""End-to-end tests for the full typedata pipeline.

Proves: define entity -> generate Ibis code -> execute on DuckDB -> verify results.
This is the mission validation test for M001.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass

import pandas as pd
import pytest

from typedata import (
    ComputedColumn,
    DimensionLayer,
    Entity,
    InRange,
    LayersConfig,
    NotNull,
    PrepLayer,
    QualityConfig,
    QualityRunner,
    TableSource,
    Unique,
)
from typedata._generate import GenerateResult, generate
from typedata.engine import DuckDBExecutor
from typedata.primitives import date_trunc_month, hash_email

# ---------------------------------------------------------------------------
# Pipeline result container
# ---------------------------------------------------------------------------


@dataclass
class PipelineResult:
    """Holds all artifacts from a pipeline run for test inspection."""

    generate_result: GenerateResult
    result_df: pd.DataFrame
    quality_results: list
    entity: Entity
    input_row_count: int


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def sample_customers_parquet(tmp_path):
    """Create sample customer parquet file with 10 rows of known data."""
    df = pd.DataFrame(
        {
            "id": list(range(1, 11)),
            "email": [
                "alice@example.com",
                "bob@test.org",
                "carol@example.com",
                "dave@company.io",
                "eve@example.com",
                "frank@test.org",
                "grace@company.io",
                "henry@example.com",
                "ivy@test.org",
                "jack@company.io",
            ],
            "name": [
                "Alice Smith",
                "Bob Jones",
                "Carol White",
                "Dave Wilson",
                "Eve Taylor",
                "Frank Brown",
                "Grace Lee",
                "Henry Davis",
                "Ivy Miller",
                "Jack Moore",
            ],
            "created_at": pd.to_datetime(
                [
                    "2025-01-15",
                    "2025-02-20",
                    "2025-03-10",
                    "2025-04-05",
                    "2025-05-12",
                    "2025-06-18",
                    "2025-07-22",
                    "2025-08-30",
                    "2025-09-14",
                    "2025-10-01",
                ]
            ),
            "plan": [
                "free",
                "pro",
                "enterprise",
                "free",
                "pro",
                "enterprise",
                "free",
                "pro",
                "enterprise",
                "free",
            ],
            "amount_cents": [0, 2900, 9900, 0, 2900, 9900, 0, 2900, 9900, 0],
        }
    )
    path = tmp_path / "data" / "customers.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return path, len(df)


@pytest.fixture()
def e2e_pipeline(sample_customers_parquet, tmp_path):
    """Run the full pipeline and return all artifacts for inspection."""
    parquet_path, input_row_count = sample_customers_parquet
    generated_dir = tmp_path / "generated"

    entity = Entity(
        name="customers",
        description="Sample customers for e2e test",
        source=TableSource(
            project="test",
            dataset="test",
            table="customers",
            duckdb_path=str(parquet_path),
        ),
        layers=LayersConfig(
            prep=PrepLayer(
                model_name="prep_customers",
                computed_columns=[
                    ComputedColumn(
                        name="email_hash",
                        expression=hash_email("email"),
                        description="SHA256 hash of lowercase trimmed email",
                    ),
                    ComputedColumn(
                        name="amount_dollars",
                        expression="t.amount_cents / 100.0",
                        description="Monthly payment in dollars",
                    ),
                ],
            ),
            dimension=DimensionLayer(
                model_name="dim_customers",
                computed_columns=[
                    ComputedColumn(
                        name="email_domain",
                        expression="t.email.split('@')[1]",
                        description="Email domain extracted from address",
                    ),
                    ComputedColumn(
                        name="is_paying",
                        expression="t.plan != 'free'",
                        description="True if customer is on a paid plan",
                    ),
                    ComputedColumn(
                        name="signup_month",
                        expression=date_trunc_month("created_at"),
                        description="Signup month for cohort analysis",
                    ),
                ],
            ),
        ),
        quality=QualityConfig(
            primary_key="email_hash",
            checks=[
                NotNull("email"),
                NotNull("id"),
                Unique("email_hash"),
                InRange("amount_cents", min=0),
            ],
        ),
    )

    # Step 1: Generate
    gen_result = generate(entity, output_dir=generated_dir)

    # Step 2: Execute on DuckDB
    with DuckDBExecutor(generated_dir=generated_dir) as executor:
        exec_result = executor.execute("customers")
        result_df = executor.connection.table(exec_result.target_name).to_pandas()

        # Step 3: Quality checks
        qr = QualityRunner(executor.connection)
        entity_result = qr.run_entity_checks(
            entity_name=entity.name,
            quality_config=entity.quality,
            primary_key=entity.quality.primary_key,
            table_name=exec_result.target_name,
        )
        quality_results = entity_result.results

    return PipelineResult(
        generate_result=gen_result,
        result_df=result_df,
        quality_results=quality_results,
        entity=entity,
        input_row_count=input_row_count,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestEndToEnd:
    """Full pipeline: define -> generate -> execute -> verify."""

    def test_generate_produces_valid_python(self, e2e_pipeline):
        """Generated file exists, is .py, and parses as valid Python."""
        result = e2e_pipeline
        assert result.generate_result.output_path.exists()
        assert result.generate_result.output_path.suffix == ".py"
        ast.parse(result.generate_result.code)
        assert "customers" in result.generate_result.code

    def test_output_table_has_expected_schema(self, e2e_pipeline):
        """Output table contains all expected columns including computed ones."""
        columns = set(e2e_pipeline.result_df.columns)

        # Original columns
        for col in ["id", "email", "name", "created_at", "plan", "amount_cents"]:
            assert col in columns, f"Missing original column: {col}"

        # Computed columns from PrepLayer + DimensionLayer
        for col in ["email_hash", "amount_dollars", "email_domain", "is_paying", "signup_month"]:
            assert col in columns, f"Missing computed column: {col}"

    def test_output_row_count_matches_input(self, e2e_pipeline):
        """No rows lost or duplicated during transformation."""
        assert len(e2e_pipeline.result_df) == e2e_pipeline.input_row_count

    def test_computed_columns_have_correct_values(self, e2e_pipeline):
        """Spot-check computed column values for known input rows."""
        df = e2e_pipeline.result_df

        # Alice: free plan, 0 cents, example.com domain
        alice = df[df["id"] == 1].iloc[0]
        assert alice["email_domain"] == "example.com"
        assert alice["amount_dollars"] == pytest.approx(0.0)
        assert not alice["is_paying"]

        # Bob: pro plan, 2900 cents, test.org domain
        bob = df[df["id"] == 2].iloc[0]
        assert bob["email_domain"] == "test.org"
        assert bob["amount_dollars"] == pytest.approx(29.0)
        assert bob["is_paying"]

        # Carol: enterprise plan, 9900 cents
        carol = df[df["id"] == 3].iloc[0]
        assert carol["amount_dollars"] == pytest.approx(99.0)
        assert carol["is_paying"]

    def test_email_hash_is_populated(self, e2e_pipeline):
        """Email hash column has non-null values for all rows."""
        df = e2e_pipeline.result_df
        assert df["email_hash"].notna().all()
        assert df["email_hash"].nunique() == len(df)  # all unique

    def test_quality_checks_pass(self, e2e_pipeline):
        """Quality checks pass on clean data."""
        assert len(e2e_pipeline.quality_results) >= 2
        for check_result in e2e_pipeline.quality_results:
            assert check_result.passed is True, (
                f"Quality check failed: {check_result.check_name}"
            )

    def test_quality_check_detects_violations(self, tmp_path):
        """Quality checks correctly detect bad data (not vacuously true)."""
        # Create data with a NULL email to trigger NotNull failure
        df = pd.DataFrame(
            {
                "id": [1, 2],
                "email": ["valid@example.com", None],
                "name": ["Valid", "Bad"],
                "created_at": pd.to_datetime(["2025-01-01", "2025-01-02"]),
                "plan": ["free", "free"],
                "amount_cents": [0, 0],
            }
        )
        path = tmp_path / "bad_data" / "customers.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False)

        entity = Entity(
            name="customers",
            description="Bad data test",
            source=TableSource(
                project="test",
                dataset="test",
                table="customers",
                duckdb_path=str(path),
            ),
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_customers"),
                dimension=DimensionLayer(model_name="dim_customers"),
            ),
            quality=QualityConfig(checks=[NotNull("email")]),
        )

        generated_dir = tmp_path / "bad_generated"
        generate(entity, output_dir=generated_dir)

        with DuckDBExecutor(generated_dir=generated_dir) as executor:
            exec_result = executor.execute("customers")
            qr = QualityRunner(executor.connection)
            entity_result = qr.run_entity_checks(
                entity_name=entity.name,
                quality_config=entity.quality,
                primary_key="id",
                table_name=exec_result.target_name,
            )

        failed = [r for r in entity_result.results if not r.passed]
        assert len(failed) >= 1, "NotNull check should fail on NULL email"
