"""Tests for dependency resolution and execution ordering."""

from pathlib import Path

import pytest

from fyrnheim import (
    AggregationSource,
    DerivedSource,
    Entity,
    LayersConfig,
    PrepLayer,
    TableSource,
)
from fyrnheim.engine import (
    CircularDependencyError,
    EntityInfo,
    EntityRegistry,
    resolve_execution_order,
)
from fyrnheim.engine.resolution import _extract_dependencies


def _write_entity_file(directory: Path, filename: str, entity_code: str) -> Path:
    path = directory / filename
    path.write_text(entity_code)
    return path


def _make_info(name: str, entity: Entity) -> EntityInfo:
    """Create an EntityInfo for testing."""
    layers = [
        ln
        for ln in ["prep", "dimension", "snapshot", "activity", "analytics"]
        if entity.has_layer(ln)
    ]
    return EntityInfo(name=name, entity=entity, path=Path(f"{name}.py"), layers=layers)


def _leaf_entity(name: str) -> Entity:
    """Create a leaf entity with TableSource (no dependencies)."""
    return Entity(
        name=name,
        description=f"Entity {name}",
        layers=LayersConfig(prep=PrepLayer(model_name=f"prep_{name}")),
        source=TableSource(project="p", dataset="d", table=name),
    )


def _agg_entity(name: str, source_entity: str, depends_on: list[str] | None = None) -> Entity:
    """Create an entity with AggregationSource."""
    return Entity(
        name=name,
        description=f"Entity {name}",
        layers=LayersConfig(prep=PrepLayer(model_name=f"prep_{name}")),
        source=AggregationSource(
            source_entity=source_entity,
            group_by_column="id",
            depends_on=depends_on or [],
        ),
    )


def _derived_entity(name: str, depends_on: list[str]) -> Entity:
    """Create an entity with DerivedSource."""
    return Entity(
        name=name,
        description=f"Entity {name}",
        layers=LayersConfig(prep=PrepLayer(model_name=f"prep_{name}")),
        source=DerivedSource(identity_graph="ig", depends_on=depends_on),
    )


class TestExtractDependencies:
    """Test _extract_dependencies helper."""

    def test_table_source_no_deps(self):
        entity = _leaf_entity("orders")
        assert _extract_dependencies(entity) == []

    def test_aggregation_source_has_source_entity(self):
        entity = _agg_entity("account", source_entity="person")
        deps = _extract_dependencies(entity)
        assert "person" in deps

    def test_aggregation_source_merges_depends_on(self):
        entity = _agg_entity("account", source_entity="person", depends_on=["extra"])
        deps = _extract_dependencies(entity)
        assert "person" in deps
        assert "extra" in deps

    def test_derived_source_uses_depends_on(self):
        entity = _derived_entity("person", depends_on=["txns", "subs"])
        deps = _extract_dependencies(entity)
        assert "txns" in deps
        assert "subs" in deps

    def test_derived_source_empty_depends_on(self):
        entity = Entity(
            name="x",
            description="test",
            layers=LayersConfig(prep=PrepLayer(model_name="prep_x")),
            source=DerivedSource(identity_graph="ig"),
        )
        assert _extract_dependencies(entity) == []

    def test_no_source(self):
        entity = Entity(
            name="x",
            description="test",
            layers=LayersConfig(prep=PrepLayer(model_name="prep_x")),
            required_fields=[],
        )
        assert _extract_dependencies(entity) == []


class TestResolveExecutionOrder:
    """Test resolve_execution_order function."""

    def _build_registry(self, entities: list[Entity]) -> EntityRegistry:
        """Build a registry from in-memory entities (without file discovery)."""
        registry = EntityRegistry()
        for entity in entities:
            info = _make_info(entity.name, entity)
            registry._entities[entity.name] = info
        return registry

    def test_leaf_entities_all_present(self):
        a = _leaf_entity("a")
        b = _leaf_entity("b")
        c = _leaf_entity("c")
        registry = self._build_registry([a, b, c])
        order = resolve_execution_order(registry)
        names = [e.name for e in order]
        assert set(names) == {"a", "b", "c"}

    def test_aggregation_dependency_order(self):
        person = _leaf_entity("person")
        account = _agg_entity("account", source_entity="person")
        registry = self._build_registry([account, person])
        order = resolve_execution_order(registry)
        names = [e.name for e in order]
        assert names.index("person") < names.index("account")

    def test_derived_source_dependency_order(self):
        txns = _leaf_entity("transactions")
        subs = _leaf_entity("subscriptions")
        person = _derived_entity("person", depends_on=["transactions", "subscriptions"])
        registry = self._build_registry([person, txns, subs])
        order = resolve_execution_order(registry)
        names = [e.name for e in order]
        assert names.index("transactions") < names.index("person")
        assert names.index("subscriptions") < names.index("person")

    def test_circular_dependency_raises(self):
        a = _agg_entity("a", source_entity="b")
        b = _agg_entity("b", source_entity="a")
        registry = self._build_registry([a, b])
        with pytest.raises(CircularDependencyError, match="Circular dependency"):
            resolve_execution_order(registry)

    def test_self_referential_raises(self):
        a = _agg_entity("a", source_entity="a")
        registry = self._build_registry([a])
        with pytest.raises(CircularDependencyError, match="Circular dependency"):
            resolve_execution_order(registry)

    def test_diamond_dependency(self):
        a = _leaf_entity("a")
        b = _agg_entity("b", source_entity="a")
        c = _agg_entity("c", source_entity="a")
        d = _derived_entity("d", depends_on=["b", "c"])
        registry = self._build_registry([d, c, b, a])
        order = resolve_execution_order(registry)
        names = [e.name for e in order]
        assert names.index("a") < names.index("b")
        assert names.index("a") < names.index("c")
        assert names.index("b") < names.index("d")
        assert names.index("c") < names.index("d")

    def test_empty_registry(self):
        registry = EntityRegistry()
        order = resolve_execution_order(registry)
        assert order == []

    def test_external_dependency_filtered(self):
        """Entities referencing non-registered deps still resolve."""
        a = _agg_entity("a", source_entity="external")
        registry = self._build_registry([a])
        order = resolve_execution_order(registry)
        names = [e.name for e in order]
        assert names == ["a"]


class TestDependsOnFields:
    """Test the new depends_on fields on source types."""

    def test_derived_source_depends_on_default(self):
        ds = DerivedSource(identity_graph="ig")
        assert ds.depends_on == []

    def test_derived_source_depends_on_set(self):
        ds = DerivedSource(identity_graph="ig", depends_on=["a", "b"])
        assert ds.depends_on == ["a", "b"]

    def test_aggregation_source_depends_on_default(self):
        a = AggregationSource(source_entity="x", group_by_column="id")
        assert a.depends_on == []

    def test_aggregation_source_depends_on_set(self):
        a = AggregationSource(
            source_entity="x", group_by_column="id", depends_on=["extra"]
        )
        assert a.depends_on == ["extra"]
