"""AnalyticsModelRegistry for discovering StreamAnalyticsModel instances from Python files."""

from __future__ import annotations

import importlib.util
import sys
from collections.abc import ItemsView, Iterator
from pathlib import Path

from fyrnheim.core.analytics_model import StreamAnalyticsModel


class AnalyticsModelRegistry:
    """Registry for discovering analytics models from Python files.

    Scans directories for .py files containing module-level ``analytics_model``
    (single StreamAnalyticsModel) or ``analytics_models`` (list of
    StreamAnalyticsModel) variables.
    """

    def __init__(self) -> None:
        self._models: dict[str, StreamAnalyticsModel] = {}

    def discover(self, models_dir: Path | str) -> None:
        """Discover analytics models in a directory.

        Scans for *.py files, dynamically imports them, and extracts
        module-level ``analytics_model`` (single) or ``analytics_models``
        (list) variables that are StreamAnalyticsModel instances.

        Accumulates models across multiple calls. Raises ValueError
        on duplicate analytics model names.
        """
        models_dir = Path(models_dir)
        if not models_dir.exists():
            raise FileNotFoundError(
                f"Analytics models directory not found: {models_dir}"
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
            module_name = f"_fyrnheim_analytics_model_{py_file.stem}"
            spec = importlib.util.spec_from_file_location(module_name, py_file)
            if spec is None or spec.loader is None:
                raise ImportError(f"Could not create import spec for {py_file}")

            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            # Check for single 'analytics_model' variable
            if hasattr(module, "analytics_model"):
                obj = module.analytics_model
                if isinstance(obj, StreamAnalyticsModel):
                    self._register(obj, py_file)

            # Check for 'analytics_models' list variable
            if hasattr(module, "analytics_models"):
                obj = module.analytics_models
                if isinstance(obj, list):
                    for item in obj:
                        if isinstance(item, StreamAnalyticsModel):
                            self._register(item, py_file)

    def _register(
        self, model: StreamAnalyticsModel, source_file: Path
    ) -> None:
        """Register a single StreamAnalyticsModel, raising on duplicates."""
        if model.name in self._models:
            raise ValueError(
                f"Duplicate analytics model name '{model.name}': "
                f"already registered, also found in {source_file}"
            )
        self._models[model.name] = model

    def get(self, name: str) -> StreamAnalyticsModel:
        """Get a StreamAnalyticsModel by name.

        Raises:
            KeyError: If no model with that name exists.
        """
        if name not in self._models:
            raise KeyError(f"Analytics model '{name}' not found")
        return self._models[name]

    def all(self) -> list[StreamAnalyticsModel]:
        """Return all registered analytics models."""
        return list(self._models.values())

    def items(self) -> ItemsView[str, StreamAnalyticsModel]:
        """Return (name, StreamAnalyticsModel) pairs."""
        return self._models.items()

    def __iter__(self) -> Iterator[str]:
        return iter(self._models)

    def __len__(self) -> int:
        return len(self._models)

    def __contains__(self, name: str) -> bool:
        return name in self._models
