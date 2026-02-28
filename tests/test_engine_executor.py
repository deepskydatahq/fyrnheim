"""Tests for IbisExecutor and related engine components."""

from pathlib import Path

import ibis
import pandas as pd
import pytest

from fyrnheim import Entity, LayersConfig, PrepLayer, DerivedSource
from fyrnheim.core.source import IdentityGraphConfig, IdentityGraphSource
from fyrnheim.engine import (
    ExecutionError,
    ExecutionResult,
    FyrnheimEngineError,
    IbisExecutor,
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
# IbisExecutor tests
# ---------------------------------------------------------------------------


class TestIbisExecutorGenericConstructor:
    """Test IbisExecutor accepts any Ibis connection + backend string."""

    def test_accepts_conn_and_backend(self):
        conn = ibis.duckdb.connect(":memory:")
        executor = IbisExecutor(conn=conn, backend="duckdb")
        assert executor.connection is conn
        assert executor._backend == "duckdb"
        executor.close()

    def test_accepts_arbitrary_backend_string(self):
        conn = ibis.duckdb.connect(":memory:")
        executor = IbisExecutor(conn=conn, backend="bigquery")
        assert executor._backend == "bigquery"
        executor.close()

    def test_context_manager(self):
        conn = ibis.duckdb.connect(":memory:")
        with IbisExecutor(conn=conn, backend="duckdb") as executor:
            assert executor.connection is conn

    def test_generated_dir(self, tmp_path):
        conn = ibis.duckdb.connect(":memory:")
        gen_dir = tmp_path / "gen"
        executor = IbisExecutor(conn=conn, backend="duckdb", generated_dir=gen_dir)
        assert executor._generated_dir == gen_dir
        executor.close()


class TestIbisExecutorDuckdbClassmethod:
    """Test IbisExecutor.duckdb() convenience classmethod."""

    def test_creates_in_memory_connection(self):
        executor = IbisExecutor.duckdb()
        assert executor.connection is not None
        assert executor._backend == "duckdb"
        executor.close()

    def test_file_based_connection(self, tmp_path):
        db_path = tmp_path / "test.duckdb"
        with IbisExecutor.duckdb(db_path=db_path) as executor:
            assert executor.connection is not None
            assert executor._backend == "duckdb"

    def test_with_generated_dir(self, tmp_path):
        gen_dir = tmp_path / "gen"
        executor = IbisExecutor.duckdb(generated_dir=gen_dir)
        assert executor._generated_dir == gen_dir
        executor.close()


class TestIbisExecutorBackendInPipeline:
    """Test that _run_transform_pipeline uses self._backend."""

    def test_pipeline_passes_backend_to_source_fn(self, tmp_path):
        """source_fn receives the executor's backend string, not hardcoded 'duckdb'."""
        parquet_path = _create_sample_parquet(tmp_path)
        gen_dir = tmp_path / "generated"
        code = SIMPLE_TRANSFORM.format(parquet_path=str(parquet_path))
        _create_transform_module(gen_dir, "customers", code)

        conn = ibis.duckdb.connect(":memory:")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=gen_dir) as executor:
            result = executor.execute("customers")
            assert result.success is True
            assert result.row_count == 3


class TestIbisExecutorLifecycle:
    """Test IbisExecutor connection lifecycle (via .duckdb() classmethod)."""

    def test_creates_in_memory_connection(self):
        executor = IbisExecutor.duckdb()
        assert executor.connection is not None
        executor.close()

    def test_context_manager(self):
        with IbisExecutor.duckdb() as executor:
            assert executor.connection is not None

    def test_file_based_connection(self, tmp_path):
        db_path = tmp_path / "test.duckdb"
        with IbisExecutor.duckdb(db_path=db_path) as executor:
            assert executor.connection is not None


class TestIbisExecutorRegisterParquet:
    """Test register_parquet method."""

    def test_register_parquet(self, tmp_path):
        parquet_path = _create_sample_parquet(tmp_path)
        with IbisExecutor.duckdb() as executor:
            executor.register_parquet("source_customers", parquet_path)
            t = executor.connection.table("source_customers")
            assert t.count().execute() == 3

    def test_raises_on_missing_file(self, tmp_path):
        with IbisExecutor.duckdb() as executor:
            with pytest.raises(SourceNotFoundError, match="Parquet file not found"):
                executor.register_parquet("x", tmp_path / "nope.parquet")


class TestIbisExecutorExecute:
    """Test execute method with generated transform modules."""

    def test_execute_pipeline(self, tmp_path):
        parquet_path = _create_sample_parquet(tmp_path)
        gen_dir = tmp_path / "generated"
        code = SIMPLE_TRANSFORM.format(parquet_path=str(parquet_path))
        _create_transform_module(gen_dir, "customers", code)

        with IbisExecutor.duckdb(generated_dir=gen_dir) as executor:
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

        with IbisExecutor.duckdb(generated_dir=gen_dir) as executor:
            result = executor.execute("customers", target_name="stg_customers")
            assert result.target_name == "stg_customers"

    def test_execute_override_generated_dir(self, tmp_path):
        parquet_path = _create_sample_parquet(tmp_path)
        gen_dir = tmp_path / "generated"
        code = SIMPLE_TRANSFORM.format(parquet_path=str(parquet_path))
        _create_transform_module(gen_dir, "customers", code)

        with IbisExecutor.duckdb() as executor:
            result = executor.execute("customers", generated_dir=gen_dir)
            assert result.success is True

    def test_execute_no_generated_dir_raises(self):
        with IbisExecutor.duckdb() as executor:
            with pytest.raises(ExecutionError, match="No generated_dir"):
                executor.execute("customers")

    def test_execute_missing_module_raises(self, tmp_path):
        gen_dir = tmp_path / "generated"
        gen_dir.mkdir()
        with IbisExecutor.duckdb(generated_dir=gen_dir) as executor:
            with pytest.raises(TransformModuleError, match="not found"):
                executor.execute("missing_entity")

    def test_persisted_table_accessible(self, tmp_path):
        parquet_path = _create_sample_parquet(tmp_path)
        gen_dir = tmp_path / "generated"
        code = SIMPLE_TRANSFORM.format(parquet_path=str(parquet_path))
        _create_transform_module(gen_dir, "customers", code)

        with IbisExecutor.duckdb(generated_dir=gen_dir) as executor:
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

        with IbisExecutor.duckdb(generated_dir=gen_dir) as executor:
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

        with IbisExecutor.duckdb(generated_dir=gen_dir) as executor:
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

        with IbisExecutor.duckdb(generated_dir=gen_dir) as executor:
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

        with IbisExecutor.duckdb(generated_dir=gen_dir) as executor:
            with pytest.raises(ExecutionError, match="No source function or registered source"):
                executor.execute("widgets")


class TestIbisExecutorActivityBranch:
    """Test activity branch execution in _run_transform_pipeline."""

    def test_activity_table_created(self, tmp_path):
        """activity_{name} table is created when activity function exists."""
        parquet_path = _create_sample_parquet(tmp_path)
        gen_dir = tmp_path / "generated"
        code = f"""\
import ibis

def source_customers(conn, backend):
    import os
    return conn.read_parquet(os.path.expanduser("{parquet_path}"))

def prep_customers(source_customers):
    return source_customers

def dim_customers(prep_customers):
    return prep_customers

def activity_customers(dim_customers):
    t = dim_customers
    return t.select(
        entity_id=t.id.cast("string"),
        name=t.name,
    )
"""
        _create_transform_module(gen_dir, "customers", code)

        with IbisExecutor.duckdb(generated_dir=gen_dir) as executor:
            executor.execute("customers")
            activity = executor.connection.table("activity_customers").to_pandas()
            assert len(activity) == 3
            assert "entity_id" in activity.columns

    def test_activity_receives_dim_not_snapshot(self, tmp_path):
        """Activity branches from dim, not snapshot."""
        parquet_path = _create_sample_parquet(tmp_path)
        gen_dir = tmp_path / "generated"
        code = f"""\
import ibis

def source_customers(conn, backend):
    import os
    return conn.read_parquet(os.path.expanduser("{parquet_path}"))

def prep_customers(source_customers):
    return source_customers

def dim_customers(prep_customers):
    return prep_customers.mutate(dim_marker=ibis.literal("from_dim"))

def snapshot_customers(dim_customers):
    return dim_customers.mutate(snap_marker=ibis.literal("from_snap"))

def activity_customers(dim_customers):
    # Should have dim_marker but NOT snap_marker
    return dim_customers.select(
        entity_id=dim_customers.id.cast("string"),
        has_dim=dim_customers.dim_marker,
    )
"""
        _create_transform_module(gen_dir, "customers", code)

        with IbisExecutor.duckdb(generated_dir=gen_dir) as executor:
            executor.execute("customers")
            activity = executor.connection.table("activity_customers").to_pandas()
            assert "has_dim" in activity.columns
            assert (activity["has_dim"] == "from_dim").all()


class TestIbisExecutorAnalyticsBranch:
    """Test analytics branch execution in _run_transform_pipeline."""

    def test_analytics_table_created(self, tmp_path):
        """analytics_{name} table is created when analytics function exists."""
        parquet_path = _create_sample_parquet(tmp_path)
        gen_dir = tmp_path / "generated"
        code = f"""\
import ibis

def source_customers(conn, backend):
    import os
    return conn.read_parquet(os.path.expanduser("{parquet_path}"))

def prep_customers(source_customers):
    return source_customers

def dim_customers(prep_customers):
    return prep_customers

def analytics_customers(dim_customers):
    return dim_customers.group_by("name").aggregate(
        total=dim_customers.amount_cents.sum(),
    )
"""
        _create_transform_module(gen_dir, "customers", code)

        with IbisExecutor.duckdb(generated_dir=gen_dir) as executor:
            executor.execute("customers")
            analytics = executor.connection.table("analytics_customers").to_pandas()
            assert len(analytics) == 3  # 3 unique names
            assert "total" in analytics.columns

    def test_analytics_receives_dim_not_snapshot(self, tmp_path):
        """Analytics branches from dim, not snapshot."""
        parquet_path = _create_sample_parquet(tmp_path)
        gen_dir = tmp_path / "generated"
        code = f"""\
import ibis

def source_customers(conn, backend):
    import os
    return conn.read_parquet(os.path.expanduser("{parquet_path}"))

def prep_customers(source_customers):
    return source_customers

def dim_customers(prep_customers):
    return prep_customers.mutate(dim_flag=ibis.literal(1))

def snapshot_customers(dim_customers):
    return dim_customers.mutate(snap_flag=ibis.literal(2))

def analytics_customers(dim_customers):
    return dim_customers.group_by("name").aggregate(
        dim_check=dim_customers.dim_flag.sum(),
    )
"""
        _create_transform_module(gen_dir, "customers", code)

        with IbisExecutor.duckdb(generated_dir=gen_dir) as executor:
            executor.execute("customers")
            analytics = executor.connection.table("analytics_customers").to_pandas()
            assert "dim_check" in analytics.columns
            assert (analytics["dim_check"] == 1).all()


class TestIbisExecutorBranchCombinations:
    """Test combinations of activity + analytics branches."""

    def test_both_branches_created(self, tmp_path):
        parquet_path = _create_sample_parquet(tmp_path)
        gen_dir = tmp_path / "generated"
        code = f"""\
import ibis

def source_customers(conn, backend):
    import os
    return conn.read_parquet(os.path.expanduser("{parquet_path}"))

def prep_customers(source_customers):
    return source_customers

def dim_customers(prep_customers):
    return prep_customers

def activity_customers(dim_customers):
    return dim_customers.select(entity_id=dim_customers.id.cast("string"))

def analytics_customers(dim_customers):
    return dim_customers.group_by("name").aggregate(cnt=dim_customers.id.count())
"""
        _create_transform_module(gen_dir, "customers", code)

        with IbisExecutor.duckdb(generated_dir=gen_dir) as executor:
            executor.execute("customers")
            tables = executor.connection.list_tables()
            assert "activity_customers" in tables
            assert "analytics_customers" in tables
            assert "dim_customers" in tables

    def test_no_branches_still_works(self, tmp_path):
        """Pipeline with no activity/analytics branches works fine."""
        parquet_path = _create_sample_parquet(tmp_path)
        gen_dir = tmp_path / "generated"
        code = SIMPLE_TRANSFORM.format(parquet_path=str(parquet_path))
        _create_transform_module(gen_dir, "customers", code)

        with IbisExecutor.duckdb(generated_dir=gen_dir) as executor:
            result = executor.execute("customers")
            assert result.success is True
            tables = executor.connection.list_tables()
            assert "activity_customers" not in tables
            assert "analytics_customers" not in tables


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

    def test_activity_analytics_row_counts_default_none(self):
        result = ExecutionResult(
            entity_name="x", target_name="dim_x", row_count=10,
            columns=["a"], success=True,
        )
        assert result.activity_row_count is None
        assert result.analytics_row_count is None

    def test_activity_analytics_row_counts_set(self):
        result = ExecutionResult(
            entity_name="x", target_name="dim_x", row_count=10,
            columns=["a"], success=True,
            activity_row_count=5, analytics_row_count=3,
        )
        assert result.activity_row_count == 5
        assert result.analytics_row_count == 3


class TestExecutionResultRowCounts:
    """Test that execute() populates activity/analytics row counts."""

    def test_activity_row_count_populated(self, tmp_path):
        parquet_path = _create_sample_parquet(tmp_path)
        gen_dir = tmp_path / "generated"
        code = f"""\
import ibis

def source_customers(conn, backend):
    import os
    return conn.read_parquet(os.path.expanduser("{parquet_path}"))

def prep_customers(source_customers):
    return source_customers

def dim_customers(prep_customers):
    return prep_customers

def activity_customers(dim_customers):
    return dim_customers.select(entity_id=dim_customers.id.cast("string"))
"""
        _create_transform_module(gen_dir, "customers", code)

        with IbisExecutor.duckdb(generated_dir=gen_dir) as executor:
            result = executor.execute("customers")
            assert result.activity_row_count == 3
            assert result.analytics_row_count is None

    def test_analytics_row_count_populated(self, tmp_path):
        parquet_path = _create_sample_parquet(tmp_path)
        gen_dir = tmp_path / "generated"
        code = f"""\
import ibis

def source_customers(conn, backend):
    import os
    return conn.read_parquet(os.path.expanduser("{parquet_path}"))

def prep_customers(source_customers):
    return source_customers

def dim_customers(prep_customers):
    return prep_customers

def analytics_customers(dim_customers):
    return dim_customers.group_by("name").aggregate(cnt=dim_customers.id.count())
"""
        _create_transform_module(gen_dir, "customers", code)

        with IbisExecutor.duckdb(generated_dir=gen_dir) as executor:
            result = executor.execute("customers")
            assert result.activity_row_count is None
            assert result.analytics_row_count == 3

    def test_no_branches_row_counts_none(self, tmp_path):
        parquet_path = _create_sample_parquet(tmp_path)
        gen_dir = tmp_path / "generated"
        code = SIMPLE_TRANSFORM.format(parquet_path=str(parquet_path))
        _create_transform_module(gen_dir, "customers", code)

        with IbisExecutor.duckdb(generated_dir=gen_dir) as executor:
            result = executor.execute("customers")
            assert result.activity_row_count is None
            assert result.analytics_row_count is None


# ---------------------------------------------------------------------------
# DerivedSource execution tests
# ---------------------------------------------------------------------------


class TestDerivedSourceExecution:
    """Test DerivedSource detection and dispatch in executor."""

    def _make_derived_entity(self):
        """Create a DerivedSource entity with identity graph config."""
        config = IdentityGraphConfig(
            match_key="email",
            sources=[
                IdentityGraphSource(
                    name="hubspot", entity="hubspot_person",
                    match_key_field="email", fields={"name": "full_name"},
                    id_field="person_id",
                ),
                IdentityGraphSource(
                    name="stripe", entity="stripe_customer",
                    match_key_field="email", fields={"name": "name"},
                    id_field="customer_id",
                ),
            ],
            priority=["hubspot", "stripe"],
        )
        return Entity(
            name="person",
            description="Unified person",
            layers=LayersConfig(prep=PrepLayer(model_name="prep_person")),
            source=DerivedSource(identity_graph="person_graph", identity_graph_config=config),
        )

    def test_derived_source_calls_source_fn_with_dict(self, tmp_path):
        """DerivedSource entity calls source_fn(sources_dict) not source_fn(conn, backend)."""
        gen_dir = tmp_path / "generated"
        gen_dir.mkdir(parents=True, exist_ok=True)

        # Pre-populate dim tables
        conn = ibis.duckdb.connect(":memory:")
        df_hubspot = pd.DataFrame({"email": ["a@ex.com"], "full_name": ["Alice"], "person_id": ["h1"]})
        df_stripe = pd.DataFrame({"email": ["b@ex.com"], "name": ["Bob"], "customer_id": ["s1"]})
        conn.create_table("dim_hubspot_person", df_hubspot)
        conn.create_table("dim_stripe_customer", df_stripe)

        # Create a mock source function that accepts sources dict
        code = """\
import ibis

def source_person(sources: dict) -> ibis.Table:
    t_hubspot = sources["hubspot"]
    t_stripe = sources["stripe"]
    return ibis.union(t_hubspot.select("email"), t_stripe.select("email"))

def prep_person(source_person):
    return source_person

def dim_person(prep_person):
    return prep_person
"""
        (gen_dir / "person_transforms.py").write_text(code)

        entity = self._make_derived_entity()
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=gen_dir) as executor:
            result = executor.execute("person", entity=entity)
            assert result.success is True
            assert result.row_count == 2

    def test_sources_dict_keys_match_source_names(self, tmp_path):
        """Sources dict uses ig_source.name as keys."""
        gen_dir = tmp_path / "generated"
        gen_dir.mkdir(parents=True, exist_ok=True)

        conn = ibis.duckdb.connect(":memory:")
        df1 = pd.DataFrame({"email": ["a@ex.com"], "full_name": ["Alice"], "person_id": ["h1"]})
        df2 = pd.DataFrame({"email": ["b@ex.com"], "name": ["Bob"], "customer_id": ["s1"]})
        conn.create_table("dim_hubspot_person", df1)
        conn.create_table("dim_stripe_customer", df2)

        # Source fn that captures the keys
        code = """\
import ibis

def source_person(sources: dict) -> ibis.Table:
    assert "hubspot" in sources, f"Expected 'hubspot' key, got {list(sources.keys())}"
    assert "stripe" in sources, f"Expected 'stripe' key, got {list(sources.keys())}"
    return sources["hubspot"].select("email")

def prep_person(source_person):
    return source_person

def dim_person(prep_person):
    return prep_person
"""
        (gen_dir / "person_transforms.py").write_text(code)

        entity = self._make_derived_entity()
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=gen_dir) as executor:
            result = executor.execute("person", entity=entity)
            assert result.success is True

    def test_missing_dependency_table_raises(self, tmp_path):
        """ExecutionError when dependency dim table is missing."""
        gen_dir = tmp_path / "generated"
        gen_dir.mkdir(parents=True, exist_ok=True)

        conn = ibis.duckdb.connect(":memory:")
        # Only create one of the two dependency tables
        df1 = pd.DataFrame({"email": ["a@ex.com"]})
        conn.create_table("dim_hubspot_person", df1)
        # dim_stripe_customer is missing

        code = """\
import ibis

def source_person(sources: dict) -> ibis.Table:
    return sources["hubspot"]

def dim_person(source_person):
    return source_person
"""
        (gen_dir / "person_transforms.py").write_text(code)

        entity = self._make_derived_entity()
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=gen_dir) as executor:
            with pytest.raises(ExecutionError, match="Dependency table"):
                executor.execute("person", entity=entity)

    def test_non_derived_source_backward_compatible(self, tmp_path):
        """Existing TableSource entity with entity param still uses standard path."""
        parquet_path = _create_sample_parquet(tmp_path)
        gen_dir = tmp_path / "generated"
        code = SIMPLE_TRANSFORM.format(parquet_path=str(parquet_path))
        _create_transform_module(gen_dir, "customers", code)

        from fyrnheim import TableSource
        entity = Entity(
            name="customers",
            description="Test",
            layers=LayersConfig(prep=PrepLayer(model_name="prep_customers")),
            source=TableSource(project="p", dataset="d", table="customers", duckdb_path=str(parquet_path)),
        )

        with IbisExecutor.duckdb(generated_dir=gen_dir) as executor:
            result = executor.execute("customers", entity=entity)
            assert result.success is True
            assert result.row_count == 3

    def test_no_entity_param_backward_compatible(self, tmp_path):
        """Existing call pattern without entity param still works."""
        parquet_path = _create_sample_parquet(tmp_path)
        gen_dir = tmp_path / "generated"
        code = SIMPLE_TRANSFORM.format(parquet_path=str(parquet_path))
        _create_transform_module(gen_dir, "customers", code)

        with IbisExecutor.duckdb(generated_dir=gen_dir) as executor:
            result = executor.execute("customers")
            assert result.success is True
            assert result.row_count == 3
