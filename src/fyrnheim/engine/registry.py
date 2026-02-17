"""Entity discovery via dynamic module loading."""

from __future__ import annotations

import importlib.util
import sys
from collections.abc import ItemsView, Iterator
from pathlib import Path

from pydantic import BaseModel

from fyrnheim.core.entity import Entity


class EntityInfo(BaseModel):
    """Information about a discovered entity."""

    model_config = {"arbitrary_types_allowed": True}

    name: str
    entity: Entity
    path: Path
    layers: list[str]


class EntityRegistry:
    """Registry for discovering entity definitions from Python files."""

    def __init__(self) -> None:
        self._entities: dict[str, EntityInfo] = {}

    def discover(self, entities_dir: Path | str) -> None:
        """Discover entity definitions in a directory.

        Scans for *.py files, dynamically imports them, and extracts
        module-level ``entity = Entity(...)`` instances.

        Accumulates entities across multiple calls. Raises ValueError
        on duplicate entity names. Raises immediately on import errors.
        """
        entities_dir = Path(entities_dir)
        if not entities_dir.exists():
            raise FileNotFoundError(f"Entities directory not found: {entities_dir}")

        # Add parent to sys.path for import resolution
        entities_parent = str(entities_dir.parent.resolve())
        if entities_parent not in sys.path:
            sys.path.insert(0, entities_parent)

        # Scan for .py files, excluding __init__.py and dotfiles
        entity_files = sorted(
            f
            for f in entities_dir.glob("*.py")
            if f.name != "__init__.py" and not f.name.startswith(".")
        )

        for entity_file in entity_files:
            module_name = f"_fyrnheim_entity_{entity_file.stem}"
            spec = importlib.util.spec_from_file_location(module_name, entity_file)
            if spec is None or spec.loader is None:
                raise ImportError(f"Could not create import spec for {entity_file}")

            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            if not hasattr(module, "entity"):
                continue
            entity_obj = module.entity
            if not isinstance(entity_obj, Entity):
                continue

            name = entity_obj.name

            if name in self._entities:
                existing_path = self._entities[name].path
                raise ValueError(
                    f"Duplicate entity name '{name}': "
                    f"defined in {existing_path} and {entity_file}"
                )

            layer_names = ["prep", "dimension", "snapshot", "activity", "analytics"]
            layers = [ln for ln in layer_names if entity_obj.has_layer(ln)]

            self._entities[name] = EntityInfo(
                name=name,
                entity=entity_obj,
                path=entity_file,
                layers=layers,
            )

    def get(self, name: str) -> EntityInfo | None:
        """Get entity info by name, or None if not found."""
        return self._entities.get(name)

    def items(self) -> ItemsView[str, EntityInfo]:
        """Return (name, EntityInfo) pairs."""
        return self._entities.items()

    def __iter__(self) -> Iterator[str]:
        return iter(self._entities)

    def __len__(self) -> int:
        return len(self._entities)

    def __contains__(self, name: str) -> bool:
        return name in self._entities
