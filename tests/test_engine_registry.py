"""Tests for EntityRegistry and EntityInfo discovery."""

from pathlib import Path

import pytest

from typedata.engine import EntityInfo, EntityRegistry


def _write_entity_file(directory: Path, filename: str, entity_code: str) -> Path:
    """Write a Python file that defines an entity."""
    path = directory / filename
    path.write_text(entity_code)
    return path


ENTITY_TEMPLATE = """\
from typedata import Entity, LayersConfig, PrepLayer, TableSource

entity = Entity(
    name="{name}",
    description="Test entity {name}",
    layers=LayersConfig(prep=PrepLayer(model_name="prep_{name}")),
    source=TableSource(project="p", dataset="d", table="{name}"),
)
"""


class TestEntityRegistryDiscover:
    """Test EntityRegistry.discover() method."""

    def test_discovers_entity_files(self, tmp_path):
        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()
        _write_entity_file(entities_dir, "orders.py", ENTITY_TEMPLATE.format(name="orders"))
        _write_entity_file(entities_dir, "users.py", ENTITY_TEMPLATE.format(name="users"))

        registry = EntityRegistry()
        registry.discover(entities_dir)
        assert len(registry) == 2
        assert registry.get("orders") is not None
        assert registry.get("users") is not None

    def test_skips_init_and_dotfiles(self, tmp_path):
        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()
        _write_entity_file(entities_dir, "__init__.py", "# init")
        _write_entity_file(entities_dir, ".hidden.py", "# hidden")
        _write_entity_file(entities_dir, "orders.py", ENTITY_TEMPLATE.format(name="orders"))

        registry = EntityRegistry()
        registry.discover(entities_dir)
        assert len(registry) == 1

    def test_skips_files_without_entity(self, tmp_path):
        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()
        _write_entity_file(entities_dir, "helpers.py", "x = 42\n")
        _write_entity_file(entities_dir, "orders.py", ENTITY_TEMPLATE.format(name="orders"))

        registry = EntityRegistry()
        registry.discover(entities_dir)
        assert len(registry) == 1

    def test_skips_non_entity_attribute(self, tmp_path):
        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()
        _write_entity_file(entities_dir, "bad.py", 'entity = "not an Entity"\n')
        _write_entity_file(entities_dir, "orders.py", ENTITY_TEMPLATE.format(name="orders"))

        registry = EntityRegistry()
        registry.discover(entities_dir)
        assert len(registry) == 1
        assert "orders" in registry

    def test_uses_entity_name_not_filename(self, tmp_path):
        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()
        _write_entity_file(
            entities_dir, "my_weird_file.py", ENTITY_TEMPLATE.format(name="actual_name")
        )

        registry = EntityRegistry()
        registry.discover(entities_dir)
        assert "actual_name" in registry
        assert registry.get("my_weird_file") is None

    def test_raises_on_duplicate_name(self, tmp_path):
        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()
        _write_entity_file(entities_dir, "a.py", ENTITY_TEMPLATE.format(name="orders"))
        _write_entity_file(entities_dir, "b.py", ENTITY_TEMPLATE.format(name="orders"))

        registry = EntityRegistry()
        with pytest.raises(ValueError, match="Duplicate entity name 'orders'"):
            registry.discover(entities_dir)

    def test_raises_on_missing_directory(self, tmp_path):
        registry = EntityRegistry()
        with pytest.raises(FileNotFoundError, match="Entities directory not found"):
            registry.discover(tmp_path / "does_not_exist")

    def test_raises_on_import_error(self, tmp_path):
        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()
        _write_entity_file(entities_dir, "bad.py", "import nonexistent_module_xyz_123\n")

        registry = EntityRegistry()
        with pytest.raises(ModuleNotFoundError):
            registry.discover(entities_dir)

    def test_entity_info_has_correct_layers(self, tmp_path):
        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()
        code = """\
from typedata import Entity, LayersConfig, PrepLayer, DimensionLayer, TableSource

entity = Entity(
    name="users",
    description="Test entity",
    layers=LayersConfig(
        prep=PrepLayer(model_name="prep_users"),
        dimension=DimensionLayer(model_name="dim_users"),
    ),
    source=TableSource(project="p", dataset="d", table="users"),
)
"""
        _write_entity_file(entities_dir, "users.py", code)

        registry = EntityRegistry()
        registry.discover(entities_dir)
        info = registry.get("users")
        assert info is not None
        assert info.layers == ["prep", "dimension"]

    def test_entity_info_fields(self, tmp_path):
        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()
        _write_entity_file(entities_dir, "orders.py", ENTITY_TEMPLATE.format(name="orders"))

        registry = EntityRegistry()
        registry.discover(entities_dir)
        info = registry.get("orders")
        assert isinstance(info, EntityInfo)
        assert info.name == "orders"
        assert info.path == entities_dir / "orders.py"
        assert "prep" in info.layers


class TestEntityRegistryProtocol:
    """Test EntityRegistry dunder methods."""

    def test_contains(self, tmp_path):
        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()
        _write_entity_file(entities_dir, "orders.py", ENTITY_TEMPLATE.format(name="orders"))

        registry = EntityRegistry()
        registry.discover(entities_dir)
        assert "orders" in registry
        assert "missing" not in registry

    def test_iter(self, tmp_path):
        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()
        _write_entity_file(entities_dir, "a.py", ENTITY_TEMPLATE.format(name="alpha"))
        _write_entity_file(entities_dir, "b.py", ENTITY_TEMPLATE.format(name="beta"))

        registry = EntityRegistry()
        registry.discover(entities_dir)
        names = list(registry)
        assert "alpha" in names
        assert "beta" in names

    def test_len(self, tmp_path):
        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()
        _write_entity_file(entities_dir, "a.py", ENTITY_TEMPLATE.format(name="alpha"))

        registry = EntityRegistry()
        assert len(registry) == 0
        registry.discover(entities_dir)
        assert len(registry) == 1

    def test_items(self, tmp_path):
        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()
        _write_entity_file(entities_dir, "orders.py", ENTITY_TEMPLATE.format(name="orders"))

        registry = EntityRegistry()
        registry.discover(entities_dir)
        items = dict(registry.items())
        assert "orders" in items
        assert isinstance(items["orders"], EntityInfo)

    def test_get_returns_none_for_missing(self, tmp_path):
        registry = EntityRegistry()
        assert registry.get("nonexistent") is None

    def test_discover_accumulates_across_calls(self, tmp_path):
        dir_a = tmp_path / "a"
        dir_a.mkdir()
        _write_entity_file(dir_a, "x.py", ENTITY_TEMPLATE.format(name="alpha"))

        dir_b = tmp_path / "b"
        dir_b.mkdir()
        _write_entity_file(dir_b, "y.py", ENTITY_TEMPLATE.format(name="beta"))

        registry = EntityRegistry()
        registry.discover(dir_a)
        registry.discover(dir_b)
        assert len(registry) == 2
        assert "alpha" in registry
        assert "beta" in registry
