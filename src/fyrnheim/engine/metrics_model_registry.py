"""MetricsModelRegistry for discovering MetricsModel instances from Python files."""

from __future__ import annotations

import importlib.util
import sys
from collections.abc import ItemsView, Iterator
from pathlib import Path

from fyrnheim.core.metrics_model import MetricsModel


class MetricsModelRegistry:
    """Registry for discovering metrics models from Python files.

    Scans directories for .py files containing module-level ``metrics_model``
    (single MetricsModel) or ``metrics_models`` (list of MetricsModel)
    variables.
    """

    def __init__(self) -> None:
        self._models: dict[str, MetricsModel] = {}

    def discover(self, models_dir: Path | str) -> None:
        """Discover metrics models in a directory.

        Scans for *.py files, dynamically imports them, and extracts
        module-level ``metrics_model`` (single) or ``metrics_models``
        (list) variables that are MetricsModel instances.

        Accumulates models across multiple calls. Raises ValueError
        on duplicate metrics model names.
        """
        models_dir = Path(models_dir)
        if not models_dir.exists():
            raise FileNotFoundError(
                f"Metrics models directory not found: {models_dir}"
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
            module_name = f"_fyrnheim_metrics_model_{py_file.stem}"
            spec = importlib.util.spec_from_file_location(module_name, py_file)
            if spec is None or spec.loader is None:
                raise ImportError(f"Could not create import spec for {py_file}")

            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            # Check for single 'metrics_model' variable
            if hasattr(module, "metrics_model"):
                obj = module.metrics_model
                if isinstance(obj, MetricsModel):
                    self._register(obj, py_file)

            # Check for 'metrics_models' list variable
            if hasattr(module, "metrics_models"):
                obj = module.metrics_models
                if isinstance(obj, list):
                    for item in obj:
                        if isinstance(item, MetricsModel):
                            self._register(item, py_file)

    def _register(self, model: MetricsModel, source_file: Path) -> None:
        """Register a single MetricsModel, raising on duplicates."""
        if model.name in self._models:
            raise ValueError(
                f"Duplicate metrics model name '{model.name}': "
                f"already registered, also found in {source_file}"
            )
        self._models[model.name] = model

    def get(self, name: str) -> MetricsModel:
        """Get a MetricsModel by name.

        Raises:
            KeyError: If no model with that name exists.
        """
        if name not in self._models:
            raise KeyError(f"Metrics model '{name}' not found")
        return self._models[name]

    def all(self) -> list[MetricsModel]:
        """Return all registered metrics models."""
        return list(self._models.values())

    def items(self) -> ItemsView[str, MetricsModel]:
        """Return (name, MetricsModel) pairs."""
        return self._models.items()

    def __iter__(self) -> Iterator[str]:
        return iter(self._models)

    def __len__(self) -> int:
        return len(self._models)

    def __contains__(self, name: str) -> bool:
        return name in self._models
