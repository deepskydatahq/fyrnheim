"""AnalyticsEntityRegistry for discovering AnalyticsEntity instances from Python files."""

from __future__ import annotations

import importlib.util
import sys
from collections.abc import ItemsView, Iterator
from pathlib import Path

from fyrnheim.core.analytics_entity import AnalyticsEntity


class AnalyticsEntityRegistry:
    """Registry for discovering analytics entities from Python files.

    Scans directories for .py files containing module-level ``analytics_entity``
    (single AnalyticsEntity) or ``analytics_entities`` (list of AnalyticsEntity)
    variables.
    """

    def __init__(self) -> None:
        self._entities: dict[str, AnalyticsEntity] = {}

    def discover(self, entities_dir: Path | str) -> None:
        """Discover analytics entities in a directory.

        Scans for *.py files, dynamically imports them, and extracts
        module-level ``analytics_entity`` (single) or ``analytics_entities``
        (list) variables that are AnalyticsEntity instances.

        Accumulates entities across multiple calls. Raises ValueError
        on duplicate entity names.
        """
        entities_dir = Path(entities_dir)
        if not entities_dir.exists():
            raise FileNotFoundError(
                f"Analytics entities directory not found: {entities_dir}"
            )

        # Add parent to sys.path for import resolution
        parent = str(entities_dir.parent.resolve())
        if parent not in sys.path:
            sys.path.insert(0, parent)

        # Scan for .py files, excluding __init__.py and dotfiles
        py_files = sorted(
            f
            for f in entities_dir.glob("*.py")
            if f.name != "__init__.py" and not f.name.startswith(".")
        )

        for py_file in py_files:
            module_name = f"_fyrnheim_analytics_entity_{py_file.stem}"
            spec = importlib.util.spec_from_file_location(module_name, py_file)
            if spec is None or spec.loader is None:
                raise ImportError(f"Could not create import spec for {py_file}")

            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            # Check for single 'analytics_entity' variable
            if hasattr(module, "analytics_entity"):
                obj = module.analytics_entity
                if isinstance(obj, AnalyticsEntity):
                    self._register(obj, py_file)

            # Check for 'analytics_entities' list variable
            if hasattr(module, "analytics_entities"):
                obj = module.analytics_entities
                if isinstance(obj, list):
                    for item in obj:
                        if isinstance(item, AnalyticsEntity):
                            self._register(item, py_file)

    def _register(self, entity: AnalyticsEntity, source_file: Path) -> None:
        """Register a single AnalyticsEntity, raising on duplicates."""
        if entity.name in self._entities:
            raise ValueError(
                f"Duplicate analytics entity name '{entity.name}': "
                f"already registered, also found in {source_file}"
            )
        self._entities[entity.name] = entity

    def get(self, name: str) -> AnalyticsEntity:
        """Get an AnalyticsEntity by name.

        Raises:
            KeyError: If no entity with that name exists.
        """
        if name not in self._entities:
            raise KeyError(f"Analytics entity '{name}' not found")
        return self._entities[name]

    def all(self) -> list[AnalyticsEntity]:
        """Return all registered analytics entities."""
        return list(self._entities.values())

    def items(self) -> ItemsView[str, AnalyticsEntity]:
        """Return (name, AnalyticsEntity) pairs."""
        return self._entities.items()

    def __iter__(self) -> Iterator[str]:
        return iter(self._entities)

    def __len__(self) -> int:
        return len(self._entities)

    def __contains__(self, name: str) -> bool:
        return name in self._entities
