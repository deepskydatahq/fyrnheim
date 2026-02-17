"""Internal: transform module loading via importlib."""

from __future__ import annotations

import importlib.util
import types
from pathlib import Path

from typedata.engine.errors import TransformModuleError


def load_transform_module(entity_name: str, generated_dir: Path) -> types.ModuleType:
    """Load a generated transform module for an entity.

    Args:
        entity_name: Name of the entity (used to derive the filename).
        generated_dir: Directory containing generated transform files.

    Returns:
        The loaded Python module.

    Raises:
        TransformModuleError: If the module file is missing or cannot be loaded.
    """
    module_path = generated_dir / f"{entity_name}_transforms.py"
    if not module_path.exists():
        raise TransformModuleError(
            f"Generated transform module not found: {module_path}"
        )

    module_name = f"_typedata_generated_{entity_name}_transforms"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise TransformModuleError(
            f"Could not create import spec for {module_path}"
        )

    try:
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    except Exception as e:
        raise TransformModuleError(
            f"Failed to load transform module {module_path}: {e}"
        ) from e

    return module
