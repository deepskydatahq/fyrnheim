"""Tests for the EntityModelRegistry."""

import textwrap

import pytest

from fyrnheim.engine.entity_model_registry import EntityModelRegistry


@pytest.fixture
def models_dir(tmp_path):
    """Create a temporary directory with entity model Python files."""
    d = tmp_path / "entity_models"
    d.mkdir()
    return d


def _write_model_file(models_dir, filename, content):
    """Write a Python file with entity model definitions."""
    (models_dir / filename).write_text(textwrap.dedent(content))


class TestDiscover:
    """EntityModelRegistry.discover finds module-level entity_model variables."""

    def test_discovers_single_entity_model(self, models_dir):
        _write_model_file(models_dir, "user.py", """\
            from fyrnheim.core.entity_model import EntityModel, StateField

            entity_model = EntityModel(
                name="user",
                identity_graph="user_graph",
                state_fields=[
                    StateField(name="name", source="crm", field="name", strategy="latest"),
                ],
            )
        """)
        registry = EntityModelRegistry()
        registry.discover(models_dir)
        assert len(registry) == 1
        assert "user" in registry

    def test_discovers_entity_models_list(self, models_dir):
        _write_model_file(models_dir, "models.py", """\
            from fyrnheim.core.entity_model import EntityModel, StateField

            entity_models = [
                EntityModel(
                    name="user",
                    identity_graph="user_graph",
                    state_fields=[
                        StateField(name="name", source="crm", field="name", strategy="latest"),
                    ],
                ),
                EntityModel(
                    name="account",
                    identity_graph="account_graph",
                    state_fields=[
                        StateField(name="plan", source="billing", field="plan", strategy="latest"),
                    ],
                ),
            ]
        """)
        registry = EntityModelRegistry()
        registry.discover(models_dir)
        assert len(registry) == 2
        assert "user" in registry
        assert "account" in registry

    def test_ignores_non_entity_model_variables(self, models_dir):
        _write_model_file(models_dir, "other.py", """\
            entity_model = "not an entity model"
            some_var = 42
        """)
        registry = EntityModelRegistry()
        registry.discover(models_dir)
        assert len(registry) == 0

    def test_raises_on_missing_directory(self):
        registry = EntityModelRegistry()
        with pytest.raises(FileNotFoundError):
            registry.discover("/nonexistent/path")


class TestGet:
    """EntityModelRegistry.get returns the EntityModel by name."""

    def test_get_existing(self, models_dir):
        _write_model_file(models_dir, "user.py", """\
            from fyrnheim.core.entity_model import EntityModel, StateField

            entity_model = EntityModel(
                name="user",
                identity_graph="user_graph",
                state_fields=[
                    StateField(name="name", source="crm", field="name", strategy="latest"),
                ],
            )
        """)
        registry = EntityModelRegistry()
        registry.discover(models_dir)
        model = registry.get("user")
        assert model.name == "user"

    def test_get_missing_raises_key_error(self):
        registry = EntityModelRegistry()
        with pytest.raises(KeyError, match="not found"):
            registry.get("nonexistent")


class TestDuplicateNames:
    """EntityModelRegistry raises ValueError on duplicate names."""

    def test_duplicate_in_same_discover(self, models_dir):
        _write_model_file(models_dir, "a.py", """\
            from fyrnheim.core.entity_model import EntityModel, StateField

            entity_model = EntityModel(
                name="user",
                identity_graph="user_graph",
                state_fields=[
                    StateField(name="name", source="crm", field="name", strategy="latest"),
                ],
            )
        """)
        _write_model_file(models_dir, "b.py", """\
            from fyrnheim.core.entity_model import EntityModel, StateField

            entity_model = EntityModel(
                name="user",
                identity_graph="user_graph",
                state_fields=[
                    StateField(name="plan", source="billing", field="plan", strategy="latest"),
                ],
            )
        """)
        registry = EntityModelRegistry()
        with pytest.raises(ValueError, match="Duplicate entity model name"):
            registry.discover(models_dir)
