"""Tests for snapshot layer code generation and runtime."""

import ast

from fyrnheim import (
    DimensionLayer,
    Entity,
    LayersConfig,
    PrepLayer,
    SnapshotLayer,
    TableSource,
)
from fyrnheim.engine.snapshot import _parse_dedup_order
from fyrnheim.generators import IbisCodeGenerator


def _make_snapshot_entity(
    name="subscriptions",
    snapshot=None,
    has_prep=True,
    has_dimension=True,
):
    """Build an Entity with a snapshot layer for testing."""
    layers_kwargs = {}
    if has_prep:
        layers_kwargs["prep"] = PrepLayer(model_name=f"prep_{name}")
    if has_dimension:
        layers_kwargs["dimension"] = DimensionLayer(model_name=f"dim_{name}")
    layers_kwargs["snapshot"] = snapshot or SnapshotLayer()

    return Entity(
        name=name,
        description=f"Test entity {name}",
        layers=LayersConfig(**layers_kwargs),
        source=TableSource(project="p", dataset="d", table=name, duckdb_path=f"data/{name}/*.parquet"),
    )


# ---------------------------------------------------------------------------
# SnapshotLayer config tests
# ---------------------------------------------------------------------------


class TestSnapshotLayerConfig:
    """Test SnapshotLayer natural_key and include_validity_range fields."""

    def test_natural_key_default(self):
        layer = SnapshotLayer()
        assert layer.natural_key == "id"

    def test_natural_key_string(self):
        layer = SnapshotLayer(natural_key="subscription_id")
        assert layer.natural_key == "subscription_id"

    def test_natural_key_list(self):
        layer = SnapshotLayer(natural_key=["org_id", "user_id"])
        assert layer.natural_key == ["org_id", "user_id"]

    def test_include_validity_range_default(self):
        layer = SnapshotLayer()
        assert layer.include_validity_range is False

    def test_include_validity_range_true(self):
        layer = SnapshotLayer(include_validity_range=True)
        assert layer.include_validity_range is True


# ---------------------------------------------------------------------------
# Generator output tests (code generation correctness)
# ---------------------------------------------------------------------------


class TestSnapshotCodeGeneration:
    """Test _generate_snapshot_function produces correct code."""

    def test_calls_apply_snapshot(self):
        entity = _make_snapshot_entity()
        gen = IbisCodeGenerator(entity)
        code = gen.generate_module()
        assert "from fyrnheim.engine.snapshot import apply_snapshot" in code
        assert "apply_snapshot(" in code

    def test_passes_natural_key_string(self):
        entity = _make_snapshot_entity(
            snapshot=SnapshotLayer(natural_key="subscription_id"),
        )
        gen = IbisCodeGenerator(entity)
        code = gen._generate_snapshot_function()
        assert 'natural_key="subscription_id"' in code

    def test_passes_natural_key_list(self):
        entity = _make_snapshot_entity(
            snapshot=SnapshotLayer(natural_key=["org_id", "user_id"]),
        )
        gen = IbisCodeGenerator(entity)
        code = gen._generate_snapshot_function()
        assert "natural_key=['org_id', 'user_id']" in code

    def test_passes_date_column(self):
        entity = _make_snapshot_entity(
            snapshot=SnapshotLayer(date_column="snapshot_date"),
        )
        gen = IbisCodeGenerator(entity)
        code = gen._generate_snapshot_function()
        assert 'date_column="snapshot_date"' in code

    def test_parses_dedup_order_desc(self):
        entity = _make_snapshot_entity(
            snapshot=SnapshotLayer(deduplication_order_by="updated_at DESC"),
        )
        gen = IbisCodeGenerator(entity)
        code = gen._generate_snapshot_function()
        assert 'dedup_order_by="updated_at"' in code
        assert "dedup_descending=True" in code

    def test_parses_dedup_order_asc(self):
        entity = _make_snapshot_entity(
            snapshot=SnapshotLayer(deduplication_order_by="created_at ASC"),
        )
        gen = IbisCodeGenerator(entity)
        code = gen._generate_snapshot_function()
        assert 'dedup_order_by="created_at"' in code
        assert "dedup_descending=False" in code

    def test_includes_validity_range_true(self):
        entity = _make_snapshot_entity(
            snapshot=SnapshotLayer(include_validity_range=True),
        )
        gen = IbisCodeGenerator(entity)
        code = gen._generate_snapshot_function()
        assert "include_validity_range=True" in code

    def test_includes_validity_range_false(self):
        entity = _make_snapshot_entity(
            snapshot=SnapshotLayer(include_validity_range=False),
        )
        gen = IbisCodeGenerator(entity)
        code = gen._generate_snapshot_function()
        assert "include_validity_range=False" in code

    def test_input_from_dimension(self):
        entity = _make_snapshot_entity(has_prep=True, has_dimension=True)
        gen = IbisCodeGenerator(entity)
        code = gen._generate_snapshot_function()
        assert "dim_subscriptions: ibis.Table" in code

    def test_input_from_prep_when_no_dimension(self):
        entity = _make_snapshot_entity(has_prep=True, has_dimension=False)
        gen = IbisCodeGenerator(entity)
        code = gen._generate_snapshot_function()
        assert "prep_subscriptions: ibis.Table" in code

    def test_input_from_source_when_no_prep_or_dimension(self):
        entity = _make_snapshot_entity(has_prep=False, has_dimension=False)
        gen = IbisCodeGenerator(entity)
        code = gen._generate_snapshot_function()
        assert "source_subscriptions: ibis.Table" in code

    def test_function_name(self):
        entity = _make_snapshot_entity()
        gen = IbisCodeGenerator(entity)
        code = gen._generate_snapshot_function()
        assert "def snapshot_subscriptions(" in code

    def test_code_is_valid_python(self):
        entity = _make_snapshot_entity()
        gen = IbisCodeGenerator(entity)
        code = gen.generate_module()
        ast.parse(code)

    def test_full_module_has_snapshot_function(self):
        entity = _make_snapshot_entity()
        gen = IbisCodeGenerator(entity)
        code = gen.generate_module()
        tree = ast.parse(code)
        func_names = [n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]
        assert "snapshot_subscriptions" in func_names


# ---------------------------------------------------------------------------
# _parse_dedup_order helper tests
# ---------------------------------------------------------------------------


class TestParseDedupOrder:
    """Test _parse_dedup_order helper."""

    def test_with_desc(self):
        col, desc = _parse_dedup_order("updated_at DESC")
        assert col == "updated_at"
        assert desc is True

    def test_with_asc(self):
        col, desc = _parse_dedup_order("created_at ASC")
        assert col == "created_at"
        assert desc is False

    def test_no_direction(self):
        col, desc = _parse_dedup_order("updated_at")
        assert col == "updated_at"
        assert desc is True  # default to DESC

    def test_whitespace_handling(self):
        col, desc = _parse_dedup_order("  updated_at   DESC  ")
        assert col == "updated_at"
        assert desc is True
