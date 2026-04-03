"""Tests for the ActivityRegistry."""

from __future__ import annotations

from pathlib import Path

import pytest

from fyrnheim.engine.activity_registry import ActivityRegistry


@pytest.fixture
def tmp_activities_dir(tmp_path: Path) -> Path:
    """Create a temporary directory for activity definition files."""
    d = tmp_path / "activities"
    d.mkdir()
    return d


class TestActivityRegistryDiscover:
    def test_discovers_single_activity_variable(self, tmp_activities_dir: Path):
        (tmp_activities_dir / "signup.py").write_text(
            """\
from fyrnheim.core.activity import ActivityDefinition, RowAppeared

activity = ActivityDefinition(
    name="signup",
    source="customers",
    trigger=RowAppeared(),
    entity_id_field="id",
)
"""
        )

        registry = ActivityRegistry()
        registry.discover(tmp_activities_dir)

        assert len(registry) == 1
        defn = registry.get("signup")
        assert defn.name == "signup"
        assert defn.source == "customers"

    def test_discovers_activities_list_variable(self, tmp_activities_dir: Path):
        (tmp_activities_dir / "customer_activities.py").write_text(
            """\
from fyrnheim.core.activity import (
    ActivityDefinition, RowAppeared, FieldChanged,
)

activities = [
    ActivityDefinition(
        name="signup",
        source="customers",
        trigger=RowAppeared(),
        entity_id_field="id",
    ),
    ActivityDefinition(
        name="plan_changed",
        source="customers",
        trigger=FieldChanged(field="plan"),
        entity_id_field="id",
    ),
]
"""
        )

        registry = ActivityRegistry()
        registry.discover(tmp_activities_dir)

        assert len(registry) == 2
        assert "signup" in registry
        assert "plan_changed" in registry

    def test_get_returns_definition_by_name(self, tmp_activities_dir: Path):
        (tmp_activities_dir / "signup.py").write_text(
            """\
from fyrnheim.core.activity import ActivityDefinition, RowAppeared

activity = ActivityDefinition(
    name="signup",
    source="customers",
    trigger=RowAppeared(),
    entity_id_field="id",
)
"""
        )

        registry = ActivityRegistry()
        registry.discover(tmp_activities_dir)

        defn = registry.get("signup")
        assert defn.name == "signup"

    def test_get_raises_key_error_for_missing(self, tmp_activities_dir: Path):
        registry = ActivityRegistry()
        registry.discover(tmp_activities_dir)

        with pytest.raises(KeyError, match="not found"):
            registry.get("nonexistent")

    def test_raises_value_error_on_duplicate_names(self, tmp_activities_dir: Path):
        (tmp_activities_dir / "a.py").write_text(
            """\
from fyrnheim.core.activity import ActivityDefinition, RowAppeared

activity = ActivityDefinition(
    name="signup",
    source="customers",
    trigger=RowAppeared(),
    entity_id_field="id",
)
"""
        )
        (tmp_activities_dir / "b.py").write_text(
            """\
from fyrnheim.core.activity import ActivityDefinition, RowAppeared

activity = ActivityDefinition(
    name="signup",
    source="orders",
    trigger=RowAppeared(),
    entity_id_field="id",
)
"""
        )

        registry = ActivityRegistry()
        with pytest.raises(ValueError, match="Duplicate activity name 'signup'"):
            registry.discover(tmp_activities_dir)

    def test_ignores_non_activity_variables(self, tmp_activities_dir: Path):
        (tmp_activities_dir / "misc.py").write_text(
            """\
activity = "not an activity definition"
activities = [1, 2, 3]
"""
        )

        registry = ActivityRegistry()
        registry.discover(tmp_activities_dir)

        assert len(registry) == 0

    def test_ignores_init_files(self, tmp_activities_dir: Path):
        (tmp_activities_dir / "__init__.py").write_text(
            """\
from fyrnheim.core.activity import ActivityDefinition, RowAppeared

activity = ActivityDefinition(
    name="should_not_find",
    source="customers",
    trigger=RowAppeared(),
    entity_id_field="id",
)
"""
        )

        registry = ActivityRegistry()
        registry.discover(tmp_activities_dir)

        assert len(registry) == 0

    def test_raises_on_missing_directory(self):
        registry = ActivityRegistry()
        with pytest.raises(FileNotFoundError):
            registry.discover(Path("/nonexistent/path"))

    def test_accumulates_across_multiple_discovers(self, tmp_path: Path):
        dir1 = tmp_path / "dir1"
        dir1.mkdir()
        dir2 = tmp_path / "dir2"
        dir2.mkdir()

        (dir1 / "signup.py").write_text(
            """\
from fyrnheim.core.activity import ActivityDefinition, RowAppeared

activity = ActivityDefinition(
    name="signup",
    source="customers",
    trigger=RowAppeared(),
    entity_id_field="id",
)
"""
        )
        (dir2 / "churned.py").write_text(
            """\
from fyrnheim.core.activity import ActivityDefinition, RowDisappeared

activity = ActivityDefinition(
    name="churned",
    source="customers",
    trigger=RowDisappeared(),
    entity_id_field="id",
)
"""
        )

        registry = ActivityRegistry()
        registry.discover(dir1)
        registry.discover(dir2)

        assert len(registry) == 2
        assert "signup" in registry
        assert "churned" in registry
