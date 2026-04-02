"""EntityModelRegistry for discovering EntityModel instances from Python files."""

from __future__ import annotations

import importlib.util
import sys
from collections.abc import ItemsView, Iterator
from pathlib import Path

from fyrnheim.core.entity_model import EntityModel


class EntityModelRegistry:
    """Registry for discovering entity models from Python files.

    Scans directories for .py files containing module-level ``entity_model``
    (single EntityModel) or ``entity_models`` (list of EntityModel) variables.
    """

    def __init__(self) -> None:
        self._models: dict[str, EntityModel] = {}

    def discover(self, models_dir: Path | str) -> None:
        """Discover entity models in a directory.

        Scans for *.py files, dynamically imports them, and extracts
        module-level ``entity_model`` (single) or ``entity_models`` (list)
        variables that are EntityModel instances.

        Accumulates models across multiple calls. Raises ValueError
        on duplicate entity model names.
        """
        models_dir = Path(models_dir)
        if not models_dir.exists():
            raise FileNotFoundError(
                f"Entity models directory not found: {models_dir}"
            )

        # Add parent to sys.path for import resolution
        parent = str(models_dir.parent.resolve())
        if parent not in sys.path:
            sys.path.insert(0, parent)

        # Scan for .py files, excluding __init__.py and dotfiles
        py_files = sorted(
            f
            for f in models_dir.glob("*.py")
            if f.name != "__init__.py" and not f.name.startswith(".")
        )

        for py_file in py_files:
            module_name = f"_fyrnheim_entity_model_{py_file.stem}"
            spec = importlib.util.spec_from_file_location(module_name, py_file)
            if spec is None or spec.loader is None:
                raise ImportError(f"Could not create import spec for {py_file}")

            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            # Check for single 'entity_model' variable
            if hasattr(module, "entity_model"):
                obj = module.entity_model
                if isinstance(obj, EntityModel):
                    self._register(obj, py_file)

            # Check for 'entity_models' list variable
            if hasattr(module, "entity_models"):
                obj = module.entity_models
                if isinstance(obj, list):
                    for item in obj:
                        if isinstance(item, EntityModel):
                            self._register(item, py_file)

    def _register(self, model: EntityModel, source_file: Path) -> None:
        """Register a single EntityModel, raising on duplicates."""
        if model.name in self._models:
            raise ValueError(
                f"Duplicate entity model name '{model.name}': "
                f"already registered, also found in {source_file}"
            )
        self._models[model.name] = model

    def get(self, name: str) -> EntityModel:
        """Get an EntityModel by name.

        Raises:
            KeyError: If no model with that name exists.
        """
        if name not in self._models:
            raise KeyError(f"Entity model '{name}' not found")
        return self._models[name]

    def all(self) -> list[EntityModel]:
        """Return all registered entity models."""
        return list(self._models.values())

    def items(self) -> ItemsView[str, EntityModel]:
        """Return (name, EntityModel) pairs."""
        return self._models.items()

    def __iter__(self) -> Iterator[str]:
        return iter(self._models)

    def __len__(self) -> int:
        return len(self._models)

    def __contains__(self, name: str) -> bool:
        return name in self._models
