"""IdentityGraphRegistry for discovering IdentityGraph instances from Python files."""

from __future__ import annotations

import importlib.util
import sys
from collections.abc import ItemsView, Iterator
from pathlib import Path

from fyrnheim.core.identity import IdentityGraph


class IdentityGraphRegistry:
    """Registry for discovering identity graphs from Python files.

    Scans directories for .py files containing module-level ``identity_graph``
    variables that are IdentityGraph instances.
    """

    def __init__(self) -> None:
        self._graphs: dict[str, IdentityGraph] = {}

    def discover(self, directory: Path | str) -> None:
        """Discover identity graphs in a directory.

        Scans for *.py files, dynamically imports them, and extracts
        module-level ``identity_graph`` variables that are IdentityGraph
        instances.

        Accumulates graphs across multiple calls. Raises ValueError
        on duplicate graph names.
        """
        directory = Path(directory)
        if not directory.exists():
            raise FileNotFoundError(
                f"Directory not found: {directory}"
            )

        parent = str(directory.parent.resolve())
        if parent not in sys.path:
            sys.path.insert(0, parent)

        py_files = sorted(
            f
            for f in directory.glob("*.py")
            if f.name != "__init__.py" and not f.name.startswith(".")
        )

        for py_file in py_files:
            module_name = f"_fyrnheim_identity_{py_file.stem}"
            spec = importlib.util.spec_from_file_location(module_name, py_file)
            if spec is None or spec.loader is None:
                raise ImportError(f"Could not create import spec for {py_file}")

            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)

            if hasattr(module, "identity_graph"):
                obj = module.identity_graph
                if isinstance(obj, IdentityGraph):
                    self._register(obj, py_file)

    def _register(self, graph: IdentityGraph, source_file: Path) -> None:
        """Register a single IdentityGraph, raising on duplicates."""
        if graph.name in self._graphs:
            raise ValueError(
                f"Duplicate identity graph name '{graph.name}': "
                f"already registered, also found in {source_file}"
            )
        self._graphs[graph.name] = graph

    def get(self, name: str) -> IdentityGraph:
        """Get an IdentityGraph by name.

        Raises:
            KeyError: If no graph with that name exists.
        """
        if name not in self._graphs:
            raise KeyError(f"Identity graph '{name}' not found")
        return self._graphs[name]

    def all(self) -> list[IdentityGraph]:
        """Return all registered identity graphs."""
        return list(self._graphs.values())

    def items(self) -> ItemsView[str, IdentityGraph]:
        """Return (name, IdentityGraph) pairs."""
        return self._graphs.items()

    def __iter__(self) -> Iterator[str]:
        return iter(self._graphs)

    def __len__(self) -> int:
        return len(self._graphs)

    def __contains__(self, name: str) -> bool:
        return name in self._graphs
