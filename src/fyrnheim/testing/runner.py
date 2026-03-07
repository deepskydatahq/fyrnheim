"""fyrnheim.testing.runner -- Test discovery and execution.

Discovers test_*.py files, finds EntityTest subclasses, and runs
test_ methods with pass/fail tracking.
"""

from __future__ import annotations

import importlib.util
import inspect
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class TestOutcome:
    """Result of running a single test method."""

    __test__ = False  # Prevent pytest collection

    test_name: str
    passed: bool
    error: str | None = None
    duration_seconds: float = 0.0


def discover_tests(path: Path) -> list[tuple[type, str]]:
    """Find EntityTest subclasses and their test_ methods in test_*.py files.

    Args:
        path: Directory to scan for test_*.py files.

    Returns:
        List of (class, method_name) tuples.
    """
    from fyrnheim.testing import EntityTest

    results: list[tuple[type, str]] = []

    if not path.is_dir():
        return results

    test_files = sorted(path.glob("test_*.py"))

    for test_file in test_files:
        module = _import_test_file(test_file)
        if module is None:
            continue

        for _name, obj in inspect.getmembers(module, inspect.isclass):
            if (
                issubclass(obj, EntityTest)
                and obj is not EntityTest
                and hasattr(obj, "entity")
            ):
                for attr_name in sorted(dir(obj)):
                    if attr_name.startswith("test_"):
                        method = getattr(obj, attr_name, None)
                        if callable(method):
                            results.append((obj, attr_name))

    return results


def run_tests(
    tests: list[tuple[type, str]],
    entity_filter: str | None = None,
) -> list[TestOutcome]:
    """Execute test methods and return outcomes.

    Args:
        tests: List of (class, method_name) tuples from discover_tests().
        entity_filter: If set, only run tests where the entity name matches.

    Returns:
        List of TestOutcome for each executed test.
    """
    outcomes: list[TestOutcome] = []

    for cls, method_name in tests:
        # Apply entity filter
        if entity_filter is not None:
            entity = getattr(cls, "entity", None)
            if entity is None or getattr(entity, "name", None) != entity_filter:
                continue

        test_name = f"{cls.__name__}.{method_name}"
        start = time.monotonic()

        try:
            instance = cls()
            method = getattr(instance, method_name)
            method()
            duration = time.monotonic() - start
            outcomes.append(
                TestOutcome(
                    test_name=test_name,
                    passed=True,
                    duration_seconds=duration,
                )
            )
        except Exception as exc:
            duration = time.monotonic() - start
            outcomes.append(
                TestOutcome(
                    test_name=test_name,
                    passed=False,
                    error=str(exc),
                    duration_seconds=duration,
                )
            )

    return outcomes


def _import_test_file(file_path: Path) -> Any | None:
    """Import a Python file as a module, returning None on failure."""
    module_name = f"_fyrnheim_test_{file_path.stem}_{id(file_path)}"
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        del sys.modules[module_name]
        return None
    return module
