# Implementation Plan: typedata-gr1 — E2E Regression Test via IbisExecutor on DuckDB

## Task

M004-E004-S002: E2E regression test proving the full generate → executor.execute() → verify pipeline works on DuckDB through the multi-backend source function dispatch path.

## Status

Plan verified against codebase on 2026-02-25.

## Summary

Add one test class `TestMultiBackendE2E` to `tests/test_e2e_pipeline.py` with a single test method `test_source_fn_dispatch_e2e`. No production code changes.

## Verified Codebase Facts

- `sample_customers_parquet` fixture at test_e2e_pipeline.py:54, returns (path, 10)
- All imports already at file top (lines 15-29): Entity, TableSource, PrepLayer, DimensionLayer, ComputedColumn, LayersConfig, generate, DuckDBExecutor
- Source fallback path: executor.py:168-179 calls `source_fn(conn, backend)` when no registered source
- Backend branching in generated code: ibis_code_generator.py:96-106 generates `if backend == "duckdb"` / `elif backend == "bigquery"`
- ExecutionResult schema: executor.py:18-28 has target_name, row_count, columns, success fields
- Target name default: executor.py:122 returns `dim_{entity_name}` when no snapshot

## Implementation

### Step 1: Append test class to `tests/test_e2e_pipeline.py`

```python
class TestMultiBackendE2E:
    """M004 regression gate: source_fn(conn, backend) dispatch works E2E."""

    def test_source_fn_dispatch_e2e(self, sample_customers_parquet, tmp_path):
        """Entity defined -> code generated -> DuckDB executes via source_fn -> output verified."""
        parquet_path, input_row_count = sample_customers_parquet
        generated_dir = tmp_path / "generated"

        entity = Entity(
            name="customers",
            description="Multi-backend dispatch test",
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
                            name="amount_dollars",
                            expression="t.amount_cents / 100.0",
                            description="Dollars",
                        ),
                    ],
                ),
                dimension=DimensionLayer(
                    model_name="dim_customers",
                    computed_columns=[
                        ComputedColumn(
                            name="is_paying",
                            expression="t.plan != 'free'",
                            description="Paid plan flag",
                        ),
                    ],
                ),
            ),
        )

        # Generate -- produces source_fn with backend branching
        gen_result = generate(entity, output_dir=generated_dir)
        assert "def source_customers(conn" in gen_result.code
        assert 'backend == "duckdb"' in gen_result.code

        # Execute -- NO register_parquet, forces source_fn(conn, "duckdb") path
        with DuckDBExecutor(generated_dir=generated_dir) as executor:
            assert "source_customers" not in executor._registered_sources
            result = executor.execute("customers")

            assert result.success is True
            assert result.target_name == "dim_customers"
            assert result.row_count == input_row_count

            assert "amount_dollars" in result.columns
            assert "is_paying" in result.columns

            df = executor.connection.table("dim_customers").to_pandas()
            alice = df[df["id"] == 1].iloc[0]
            assert alice["amount_dollars"] == pytest.approx(0.0)
            assert not alice["is_paying"]

            bob = df[df["id"] == 2].iloc[0]
            assert bob["amount_dollars"] == pytest.approx(29.0)
            assert bob["is_paying"]
```

### Step 2: Verify

```bash
uv run pytest tests/test_e2e_pipeline.py::TestMultiBackendE2E -v
uv run pytest
uv run ruff check tests/test_e2e_pipeline.py
uv run mypy tests/test_e2e_pipeline.py
```

## Acceptance Criteria Mapping

| Criterion | Coverage |
|-----------|----------|
| E2E: entity → generate → execute on DuckDB → verify | Full (test_source_fn_dispatch_e2e) |
| All ~596 existing tests pass | Verified via full suite run |
| Missing extras guard test | Deferred to M004-E002-S001 (needs production guard code first) |

## Dependencies

Depends on typedata-11o (M004-E004-S001: BigQuery mock tests) — currently `open` status.

## What This Does NOT Do

- No IbisExecutor rename (M004-E001)
- No BigQuery extras guard test (M004-E002-S001)
- No production code changes
- No modifications to existing tests
