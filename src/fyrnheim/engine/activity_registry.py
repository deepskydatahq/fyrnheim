"""ActivityRegistry for discovering ActivityDefinition instances from Python files."""

from __future__ import annotations

import importlib.util
import sys
from collections.abc import ItemsView, Iterator
from pathlib import Path

from fyrnheim.core.activity import ActivityDefinition


class ActivityRegistry:
    """Registry for discovering activity definitions from Python files.

    Scans directories for .py files containing module-level ``activity``
    (single ActivityDefinition) or ``activities`` (list of ActivityDefinition)
    variables.
    """

    def __init__(self) -> None:
        self._definitions: dict[str, ActivityDefinition] = {}

    def discover(self, activities_dir: Path | str) -> None:
        """Discover activity definitions in a directory.

        Scans for *.py files, dynamically imports them, and extracts
        module-level ``activity`` (single) or ``activities`` (list)
        variables that are ActivityDefinition instances.

        Accumulates definitions across multiple calls. Raises ValueError
        on duplicate activity names.
        """
        activities_dir = Path(activities_dir)
        if not activities_dir.exists():
            raise FileNotFoundError(
                f"Activities directory not found: {activities_dir}"
            )

        # Add parent to sys.path for import resolution
        parent = str(activities_dir.parent.resolve())
        if parent not in sys.path:
            sys.path.insert(0, parent)

        # Scan for .py files, excluding __init__.py and dotfiles
        py_files = sorted(
            f
            for f in activities_dir.glob("*.py")
            if f.name != "__init__.py" and not f.name.startswith(".")
        )

        for py_file in py_files:
            module_name = f"_fyrnheim_activity_{py_file.stem}"
            spec = importlib.util.spec_from_file_location(module_name, py_file)
            if spec is None or spec.loader is None:
                raise ImportError(f"Could not create import spec for {py_file}")

            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            # Check for single 'activity' variable
            if hasattr(module, "activity"):
                obj = module.activity
                if isinstance(obj, ActivityDefinition):
                    self._register(obj, py_file)

            # Check for 'activities' list variable
            if hasattr(module, "activities"):
                obj = module.activities
                if isinstance(obj, list):
                    for item in obj:
                        if isinstance(item, ActivityDefinition):
                            self._register(item, py_file)

    def _register(self, defn: ActivityDefinition, source_file: Path) -> None:
        """Register a single ActivityDefinition, raising on duplicates."""
        if defn.name in self._definitions:
            raise ValueError(
                f"Duplicate activity name '{defn.name}': "
                f"already registered, also found in {source_file}"
            )
        self._definitions[defn.name] = defn

    def get(self, name: str) -> ActivityDefinition:
        """Get an ActivityDefinition by name.

        Raises:
            KeyError: If no definition with that name exists.
        """
        if name not in self._definitions:
            raise KeyError(f"Activity definition '{name}' not found")
        return self._definitions[name]

    def all(self) -> list[ActivityDefinition]:
        """Return all registered activity definitions."""
        return list(self._definitions.values())

    def items(self) -> ItemsView[str, ActivityDefinition]:
        """Return (name, ActivityDefinition) pairs."""
        return self._definitions.items()

    def __iter__(self) -> Iterator[str]:
        return iter(self._definitions)

    def __len__(self) -> int:
        return len(self._definitions)

    def __contains__(self, name: str) -> bool:
        return name in self._definitions
