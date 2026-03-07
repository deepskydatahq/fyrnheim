"""Unit tests for fyrnheim.testing module (EntityTest framework).

Tests the EntityTest base class, given(), run(), and TestResult
with minimal inline entity definitions.
"""

from __future__ import annotations

import pytest

from fyrnheim import (
    ComputedColumn,
    DimensionLayer,
    Entity,
    Field,
    LayersConfig,
    PrepLayer,
    TableSource,
)
from fyrnheim.testing import EntityTest, TestResult


# ---------------------------------------------------------------------------
# Minimal test entities (inline, simpler than scaffold)
# ---------------------------------------------------------------------------

_simple_entity = Entity(
    name="items",
    description="Minimal prep-only entity for testing",
    source=TableSource(
        project="test",
        dataset="raw",
        table="items",
        duckdb_path="items.parquet",
    ),
    layers=LayersConfig(
        prep=PrepLayer(
            model_name="prep_items",
            computed_columns=[
                ComputedColumn(
                    name="price_dollars",
                    expression="t.price_cents / 100.0",
                    description="Price in dollars",
                ),
            ],
        ),
    ),
)

_entity_with_dimension = Entity(
    name="products",
    description="Entity with prep and dimension layers",
    source=TableSource(
        project="test",
        dataset="raw",
        table="products",
        duckdb_path="products.parquet",
    ),
    layers=LayersConfig(
        prep=PrepLayer(
            model_name="prep_products",
            computed_columns=[
                ComputedColumn(
                    name="price_dollars",
                    expression="t.price_cents / 100.0",
                    description="Price in dollars",
                ),
            ],
        ),
        dimension=DimensionLayer(
            model_name="dim_products",
            computed_columns=[
                ComputedColumn(
                    name="is_expensive",
                    expression="t.price_dollars > 50.0",
                    description="True if price over $50",
                ),
            ],
        ),
    ),
)


# ---------------------------------------------------------------------------
# Test classes
# ---------------------------------------------------------------------------


class TestRowCount(EntityTest):
    """Test that EntityTest with a simple prep-only entity produces correct row count."""

    entity = _simple_entity

    def test_single_row(self) -> None:
        result = self.given(
            {"source_items": [{"id": 1, "name": "Widget", "price_cents": 999}]}
        ).run()
        assert result.row_count == 1

    def test_multiple_rows(self) -> None:
        result = self.given(
            {
                "source_items": [
                    {"id": 1, "name": "Widget", "price_cents": 999},
                    {"id": 2, "name": "Gadget", "price_cents": 2499},
                    {"id": 3, "name": "Doohickey", "price_cents": 50},
                ]
            }
        ).run()
        assert result.row_count == 3


class TestColumns(EntityTest):
    """Test that TestResult.columns includes expected computed columns."""

    entity = _entity_with_dimension

    def test_columns_include_computed(self) -> None:
        result = self.given(
            {
                "source_products": [
                    {"id": 1, "name": "Laptop", "price_cents": 99900},
                ]
            }
        ).run()
        assert "price_dollars" in result.columns
        assert "is_expensive" in result.columns
        assert "id" in result.columns
        assert "name" in result.columns


class TestColumnAccess(EntityTest):
    """Test that TestResult.column() returns correct values."""

    entity = _simple_entity

    def test_column_values(self) -> None:
        result = self.given(
            {
                "source_items": [
                    {"id": 1, "name": "Widget", "price_cents": 1000},
                    {"id": 2, "name": "Gadget", "price_cents": 2500},
                ]
            }
        ).run()
        assert result.column("name") == ["Widget", "Gadget"]
        assert result.column("price_dollars") == [10.0, 25.0]

    def test_column_not_found_raises(self) -> None:
        result = self.given(
            {"source_items": [{"id": 1, "name": "Widget", "price_cents": 100}]}
        ).run()
        with pytest.raises(KeyError, match="nonexistent"):
            result.column("nonexistent")


class TestToDicts(EntityTest):
    """Test that TestResult.to_dicts() returns expected row data."""

    entity = _simple_entity

    def test_to_dicts_structure(self) -> None:
        result = self.given(
            {
                "source_items": [
                    {"id": 1, "name": "Widget", "price_cents": 500},
                ]
            }
        ).run()
        dicts = result.to_dicts()
        assert len(dicts) == 1
        row = dicts[0]
        assert row["id"] == 1
        assert row["name"] == "Widget"
        assert row["price_cents"] == 500
        assert row["price_dollars"] == 5.0

    def test_to_dicts_multiple_rows(self) -> None:
        result = self.given(
            {
                "source_items": [
                    {"id": 1, "name": "A", "price_cents": 100},
                    {"id": 2, "name": "B", "price_cents": 200},
                ]
            }
        ).run()
        dicts = result.to_dicts()
        assert len(dicts) == 2
        names = [d["name"] for d in dicts]
        assert "A" in names
        assert "B" in names


class TestMissingGivenData:
    """Test that run() with missing given() data raises clear error."""

    def test_empty_tables_raises(self) -> None:
        class EmptyTest(EntityTest):
            entity = _simple_entity

        t = EmptyTest()
        with pytest.raises(ValueError, match="No fixture data"):
            t.given({}).run()

    def test_no_entity_raises(self) -> None:
        class NoEntityTest(EntityTest):
            pass

        t = NoEntityTest()
        with pytest.raises(ValueError, match="must set 'entity'"):
            t.given({"source_items": [{"id": 1}]})


class TestMultipleTables(EntityTest):
    """Test that given() with multiple tables works for entities with dependencies.

    While this entity only uses one source table, this test verifies
    that multiple tables can be registered in the DuckDB instance
    and the entity still executes correctly.
    """

    entity = _simple_entity

    def test_extra_tables_dont_break_execution(self) -> None:
        result = self.given(
            {
                "source_items": [
                    {"id": 1, "name": "Widget", "price_cents": 100},
                ],
                "other_table": [
                    {"key": "a", "value": 42},
                ],
            }
        ).run()
        assert result.row_count == 1
        assert result.column("name") == ["Widget"]
