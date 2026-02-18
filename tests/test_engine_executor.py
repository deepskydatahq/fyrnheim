"""Tests for DuckDBExecutor and related engine components."""

from pathlib import Path

import pandas as pd
import pytest

from fyrnheim.engine import (
    DuckDBExecutor,
    ExecutionError,
    ExecutionResult,
    FyrnheimEngineError,
    SourceNotFoundError,
    TransformModuleError,
)
from fyrnheim.engine._loader import load_transform_module

# ---------------------------------------------------------------------------
# Helper: create sample parquet and generated transform modules
# ---------------------------------------------------------------------------


def _create_sample_parquet(tmp_path: Path) -> Path:
    """Create a minimal parquet file for testing."""
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Carol"],
        "amount_cents": [100, 200, 300],
    })
    path = tmp_path / "data" / "customers.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return path


def _create_transform_module(generated_dir: Path, entity_name: str, code: str) -> Path:
    """Write a generated transform module."""
    generated_dir.mkdir(parents=True, exist_ok=True)
    path = generated_dir / f"{entity_name}_transforms.py"
    path.write_text(code)
    return path


SIMPLE_TRANSFORM = """\
import os
import ibis

def source_customers(conn, backend):
    if backend == "duckdb":
        parquet_path = os.path.expanduser("{parquet_path}")
        return conn.read_parquet(parquet_path)
    raise ValueError(f"Unsupported backend: {{backend}}")

def prep_customers(source_customers):
    t = source_customers
    return t.mutate(amount_dollars=(t.amount_cents / 100.0))

def dim_customers(prep_customers):
    t = prep_customers
    return t
"""


# ---------------------------------------------------------------------------
# Error hierarchy tests
# ---------------------------------------------------------------------------


class TestErrorHierarchy:
    """Test that all engine errors inherit from FyrnheimEngineError."""

    def test_source_not_found(self):
        assert issubclass(SourceNotFoundError, FyrnheimEngineError)

    def test_transform_module_error(self):
        assert issubclass(TransformModuleError, FyrnheimEngineError)

    def test_execution_error(self):
        assert issubclass(ExecutionError, FyrnheimEngineError)


# ---------------------------------------------------------------------------
# Loader tests
# ---------------------------------------------------------------------------


class TestTransformModuleLoader:
    """Test _loader.load_transform_module."""

    def test_loads_valid_module(self, tmp_path):
        gen_dir = tmp_path / "generated"
        _create_transform_module(gen_dir, "test_entity", "x = 42\n")
        module = load_transform_module("test_entity", gen_dir)
        assert module.x == 42

    def test_raises_on_missing_file(self, tmp_path):
        gen_dir = tmp_path / "generated"
        gen_dir.mkdir()
        with pytest.raises(TransformModuleError, match="not found"):
            load_transform_module("missing", gen_dir)

    def test_raises_on_syntax_error(self, tmp_path):
        gen_dir = tmp_path / "generated"
        _create_transform_module(gen_dir, "bad", "def broken(\n")
        with pytest.raises(TransformModuleError, match="Failed to load"):
            load_transform_module("bad", gen_dir)


# ---------------------------------------------------------------------------
# DuckDBExecutor tests
# ---------------------------------------------------------------------------


class TestDuckDBExecutorLifecycle:
    """Test DuckDBExecutor connection lifecycle."""

    def test_creates_in_memory_connection(self):
        executor = DuckDBExecutor()
        assert executor.connection is not None
        executor.close()

    def test_context_manager(self):
        with DuckDBExecutor() as executor:
            assert executor.connection is not None

    def test_file_based_connection(self, tmp_path):
        db_path = tmp_path / "test.duckdb"
        with DuckDBExecutor(db_path=db_path) as executor:
            assert executor.connection is not None


class TestDuckDBExecutorRegisterParquet:
    """Test register_parquet method."""

    def test_register_parquet(self, tmp_path):
        parquet_path = _create_sample_parquet(tmp_path)
        with DuckDBExecutor() as executor:
            executor.register_parquet("source_customers", parquet_path)
            t = executor.connection.table("source_customers")
            assert t.count().execute() == 3

    def test_raises_on_missing_file(self, tmp_path):
        with DuckDBExecutor() as executor:
            with pytest.raises(SourceNotFoundError, match="Parquet file not found"):
                executor.register_parquet("x", tmp_path / "nope.parquet")


class TestDuckDBExecutorExecute:
    """Test execute method with generated transform modules."""

    def test_execute_pipeline(self, tmp_path):
        parquet_path = _create_sample_parquet(tmp_path)
        gen_dir = tmp_path / "generated"
        code = SIMPLE_TRANSFORM.format(parquet_path=str(parquet_path))
        _create_transform_module(gen_dir, "customers", code)

        with DuckDBExecutor(generated_dir=gen_dir) as executor:
            result = executor.execute("customers")
            assert isinstance(result, ExecutionResult)
            assert result.success is True
            assert result.entity_name == "customers"
            assert result.target_name == "dim_customers"
            assert result.row_count == 3
            assert "amount_dollars" in result.columns

    def test_execute_custom_target_name(self, tmp_path):
        parquet_path = _create_sample_parquet(tmp_path)
        gen_dir = tmp_path / "generated"
        code = SIMPLE_TRANSFORM.format(parquet_path=str(parquet_path))
        _create_transform_module(gen_dir, "customers", code)

        with DuckDBExecutor(generated_dir=gen_dir) as executor:
            result = executor.execute("customers", target_name="stg_customers")
            assert result.target_name == "stg_customers"

    def test_execute_override_generated_dir(self, tmp_path):
        parquet_path = _create_sample_parquet(tmp_path)
        gen_dir = tmp_path / "generated"
        code = SIMPLE_TRANSFORM.format(parquet_path=str(parquet_path))
        _create_transform_module(gen_dir, "customers", code)

        with DuckDBExecutor() as executor:
            result = executor.execute("customers", generated_dir=gen_dir)
            assert result.success is True

    def test_execute_no_generated_dir_raises(self):
        with DuckDBExecutor() as executor:
            with pytest.raises(ExecutionError, match="No generated_dir"):
                executor.execute("customers")

    def test_execute_missing_module_raises(self, tmp_path):
        gen_dir = tmp_path / "generated"
        gen_dir.mkdir()
        with DuckDBExecutor(generated_dir=gen_dir) as executor:
            with pytest.raises(TransformModuleError, match="not found"):
                executor.execute("missing_entity")

    def test_persisted_table_accessible(self, tmp_path):
        parquet_path = _create_sample_parquet(tmp_path)
        gen_dir = tmp_path / "generated"
        code = SIMPLE_TRANSFORM.format(parquet_path=str(parquet_path))
        _create_transform_module(gen_dir, "customers", code)

        with DuckDBExecutor(generated_dir=gen_dir) as executor:
            executor.execute("customers")
            t = executor.connection.table("dim_customers")
            df = t.to_pandas()
            assert len(df) == 3
            assert "amount_dollars" in df.columns
            assert df["amount_dollars"].iloc[0] == pytest.approx(1.0)

    def test_execute_overwrite_on_rerun(self, tmp_path):
        parquet_path = _create_sample_parquet(tmp_path)
        gen_dir = tmp_path / "generated"
        code = SIMPLE_TRANSFORM.format(parquet_path=str(parquet_path))
        _create_transform_module(gen_dir, "customers", code)

        with DuckDBExecutor(generated_dir=gen_dir) as executor:
            result1 = executor.execute("customers")
            result2 = executor.execute("customers")
            assert result1.row_count == result2.row_count

    def test_execute_with_registered_source(self, tmp_path):
        """Test execution where source comes from register_parquet."""
        parquet_path = _create_sample_parquet(tmp_path)
        gen_dir = tmp_path / "generated"

        # Module that uses registered source, no source function
        code = """\
import ibis

def prep_items(source_items):
    return source_items

def dim_items(prep_items):
    return prep_items
"""
        _create_transform_module(gen_dir, "items", code)

        with DuckDBExecutor(generated_dir=gen_dir) as executor:
            executor.register_parquet("source_items", parquet_path)
            result = executor.execute("items")
            assert result.success is True
            assert result.row_count == 3


    def test_execute_source_fn_fallback(self, tmp_path):
        """source_fn in module is used when no source is registered."""
        parquet_path = _create_sample_parquet(tmp_path)
        gen_dir = tmp_path / "generated"
        code = SIMPLE_TRANSFORM.format(parquet_path=str(parquet_path))
        _create_transform_module(gen_dir, "customers", code)

        with DuckDBExecutor(generated_dir=gen_dir) as executor:
            # Deliberately do NOT register_parquet -- forces source_fn path
            assert "source_customers" not in executor._registered_sources
            result = executor.execute("customers")
            assert result.success is True
            assert result.row_count == 3

    def test_execute_no_source_raises(self, tmp_path):
        """Raises ExecutionError when no registered source and no source_fn."""
        gen_dir = tmp_path / "generated"
        code = """\
import ibis

def prep_widgets(source_widgets):
    return source_widgets

def dim_widgets(prep_widgets):
    return prep_widgets
"""
        _create_transform_module(gen_dir, "widgets", code)

        with DuckDBExecutor(generated_dir=gen_dir) as executor:
            with pytest.raises(ExecutionError, match="No source function or registered source"):
                executor.execute("widgets")


class TestExecutionResultDataclass:
    """Test ExecutionResult frozen dataclass."""

    def test_frozen(self):
        result = ExecutionResult(
            entity_name="x", target_name="dim_x", row_count=10,
            columns=["a", "b"], success=True,
        )
        with pytest.raises(AttributeError):
            result.row_count = 0  # type: ignore[misc]

    def test_fields(self):
        result = ExecutionResult(
            entity_name="x", target_name="dim_x", row_count=10,
            columns=["a", "b"], success=True,
        )
        assert result.entity_name == "x"
        assert result.error is None
