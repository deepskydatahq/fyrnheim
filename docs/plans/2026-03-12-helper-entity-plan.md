# HelperEntity Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a `HelperEntity` subclass of `Entity` for intermediate computation steps, with prep-only layer restriction and orphan validation in the runner.

**Architecture:** `HelperEntity` is a thin subclass in `core/entity.py` with a Pydantic model validator restricting layers to prep-only and forcing `is_internal=True`. The runner validates that every `HelperEntity` is referenced by at least one other entity's dependency chain. The CLI `list` command shows a `[helper]` marker.

**Tech Stack:** Pydantic (model validators), pytest

---

### Task 1: HelperEntity subclass — layer restriction

**Files:**
- Modify: `src/fyrnheim/core/entity.py`
- Test: `tests/test_helper_entity.py`

**Step 1: Write the failing tests**

Create `tests/test_helper_entity.py`:

```python
"""Tests for HelperEntity subclass."""

import pytest
from pydantic import ValidationError

from fyrnheim.core.entity import HelperEntity, LayersConfig
from fyrnheim.core.layer import DimensionLayer, PrepLayer, SnapshotLayer
from fyrnheim.core.source import TableSource


def _source():
    return TableSource(project="p", dataset="d", table="t")


class TestHelperEntityLayers:
    """HelperEntity restricts layers to prep-only."""

    def test_prep_only_valid(self):
        h = HelperEntity(
            name="identity_map",
            description="Mapping table",
            layers=LayersConfig(prep=PrepLayer(model_name="prep_identity_map")),
            source=_source(),
        )
        assert h.layers.prep is not None

    def test_dimension_rejected(self):
        with pytest.raises(ValidationError, match="does not support dimension layer"):
            HelperEntity(
                name="identity_map",
                description="Mapping table",
                layers=LayersConfig(
                    dimension=DimensionLayer(model_name="dim_identity_map"),
                ),
                source=_source(),
            )

    def test_snapshot_rejected(self):
        with pytest.raises(ValidationError, match="does not support snapshot layer"):
            HelperEntity(
                name="identity_map",
                description="Mapping table",
                layers=LayersConfig(
                    snapshot=SnapshotLayer(),
                ),
                source=_source(),
            )

    def test_prep_plus_dimension_rejected(self):
        with pytest.raises(ValidationError, match="does not support dimension layer"):
            HelperEntity(
                name="identity_map",
                description="Mapping table",
                layers=LayersConfig(
                    prep=PrepLayer(model_name="prep_identity_map"),
                    dimension=DimensionLayer(model_name="dim_identity_map"),
                ),
                source=_source(),
            )
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_helper_entity.py -v`
Expected: FAIL — `ImportError: cannot import name 'HelperEntity'`

**Step 3: Write minimal implementation**

In `src/fyrnheim/core/entity.py`, add after the `Entity` class:

```python
class HelperEntity(Entity):
    """Intermediate computation entity with restricted layers.

    Must be depended on by at least one other entity.
    Only prep layer is allowed.
    """

    @model_validator(mode="after")
    def _restrict_layers(self) -> HelperEntity:
        layers = self.layers
        if layers.dimension is not None:
            raise ValueError("HelperEntity does not support dimension layer")
        if layers.snapshot is not None:
            raise ValueError("HelperEntity does not support snapshot layer")
        if layers.activity is not None:
            raise ValueError("HelperEntity does not support activity layer")
        if layers.analytics is not None:
            raise ValueError("HelperEntity does not support analytics layer")
        return self

    def model_post_init(self, __context: object) -> None:
        super().model_post_init(__context)
        object.__setattr__(self, "is_internal", True)
```

**Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_helper_entity.py -v`
Expected: PASS (4 tests)

**Step 5: Commit**

```bash
git add tests/test_helper_entity.py src/fyrnheim/core/entity.py
git commit -m "feat: add HelperEntity with prep-only layer restriction"
```

---

### Task 2: HelperEntity — is_internal always True

**Files:**
- Modify: `tests/test_helper_entity.py`
- (No new production code — already implemented in Task 1)

**Step 1: Write the failing tests**

Add to `tests/test_helper_entity.py`:

```python
class TestHelperEntityInternal:
    """HelperEntity.is_internal is always True."""

    def test_is_internal_default(self):
        h = HelperEntity(
            name="identity_map",
            description="Mapping table",
            layers=LayersConfig(prep=PrepLayer(model_name="prep_identity_map")),
            source=_source(),
        )
        assert h.is_internal is True

    def test_is_internal_overridden_to_false(self):
        """Even if user explicitly passes is_internal=False, it stays True."""
        h = HelperEntity(
            name="identity_map",
            description="Mapping table",
            layers=LayersConfig(prep=PrepLayer(model_name="prep_identity_map")),
            source=_source(),
            is_internal=False,
        )
        assert h.is_internal is True

    def test_is_subclass_of_entity(self):
        from fyrnheim.core.entity import Entity

        h = HelperEntity(
            name="identity_map",
            description="Mapping table",
            layers=LayersConfig(prep=PrepLayer(model_name="prep_identity_map")),
            source=_source(),
        )
        assert isinstance(h, Entity)
```

**Step 2: Run tests to verify they pass**

Run: `uv run pytest tests/test_helper_entity.py::TestHelperEntityInternal -v`
Expected: PASS (3 tests — the implementation from Task 1 already handles this)

**Step 3: Commit**

```bash
git add tests/test_helper_entity.py
git commit -m "test: add is_internal and subclass tests for HelperEntity"
```

---

### Task 3: Export HelperEntity from public API

**Files:**
- Modify: `src/fyrnheim/core/__init__.py`
- Modify: `src/fyrnheim/__init__.py`

**Step 1: Write the failing test**

Add to `tests/test_helper_entity.py`:

```python
class TestHelperEntityExport:
    """HelperEntity is importable from top-level package."""

    def test_import_from_fyrnheim(self):
        from fyrnheim import HelperEntity as HE

        assert HE is HelperEntity
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_helper_entity.py::TestHelperEntityExport -v`
Expected: FAIL — `ImportError: cannot import name 'HelperEntity' from 'fyrnheim'`

**Step 3: Write minimal implementation**

In `src/fyrnheim/core/__init__.py`, add `HelperEntity` to the import from `.entity` and to `__all__`.

In `src/fyrnheim/__init__.py`, add:
```python
from fyrnheim.core import (
    HelperEntity as HelperEntity,
    # ... existing imports ...
)
```
And add `"HelperEntity"` to `__all__`.

Also add `HelperEntity.model_rebuild()` after `Entity.model_rebuild()`.

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_helper_entity.py::TestHelperEntityExport -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/fyrnheim/core/__init__.py src/fyrnheim/__init__.py tests/test_helper_entity.py
git commit -m "feat: export HelperEntity from fyrnheim package"
```

---

### Task 4: Runner validation — orphaned helper entities

**Files:**
- Modify: `src/fyrnheim/engine/runner.py`
- Modify: `src/fyrnheim/engine/resolution.py`
- Test: `tests/test_helper_entity.py`

**Step 1: Write the failing tests**

Add to `tests/test_helper_entity.py`:

```python
from fyrnheim.core.source import DerivedSource, AggregationSource
from fyrnheim.engine.resolution import extract_dependencies


def _helper(name="identity_map"):
    return HelperEntity(
        name=name,
        description="Mapping table",
        layers=LayersConfig(prep=PrepLayer(model_name=f"prep_{name}")),
        source=_source(),
    )


def _entity(name, source=None):
    return Entity(
        name=name,
        description="Test entity",
        layers=LayersConfig(prep=PrepLayer(model_name=f"prep_{name}")),
        source=source or _source(),
    )


class TestHelperEntityOrphanValidation:
    """Runner rejects orphaned HelperEntities."""

    def test_orphaned_helper_raises(self):
        from fyrnheim.engine.runner import validate_helper_entities

        entities = [
            _helper("identity_map"),
            _entity("account"),
        ]
        with pytest.raises(ValueError, match="not referenced"):
            validate_helper_entities(entities)

    def test_referenced_helper_passes(self):
        from fyrnheim.engine.runner import validate_helper_entities

        entities = [
            _helper("identity_map"),
            _entity(
                "account",
                source=DerivedSource(
                    identity_graph="person_graph",
                    depends_on=["identity_map"],
                ),
            ),
        ]
        validate_helper_entities(entities)  # Should not raise

    def test_no_helpers_passes(self):
        from fyrnheim.engine.runner import validate_helper_entities

        entities = [
            _entity("account"),
            _entity("touchpoints"),
        ]
        validate_helper_entities(entities)  # Should not raise

    def test_helper_referenced_via_aggregation_source(self):
        from fyrnheim.engine.runner import validate_helper_entities

        entities = [
            _helper("intermediate"),
            _entity(
                "summary",
                source=AggregationSource(
                    source_entity="intermediate",
                    group_by_column="account_id",
                ),
            ),
        ]
        validate_helper_entities(entities)  # Should not raise

    def test_multiple_orphaned_helpers_listed(self):
        from fyrnheim.engine.runner import validate_helper_entities

        entities = [
            _helper("helper_a"),
            _helper("helper_b"),
            _entity("account"),
        ]
        with pytest.raises(ValueError, match="helper_a|helper_b"):
            validate_helper_entities(entities)
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_helper_entity.py::TestHelperEntityOrphanValidation -v`
Expected: FAIL — `ImportError: cannot import name 'validate_helper_entities'`

**Step 3: Write minimal implementation**

In `src/fyrnheim/engine/runner.py`, add a new function (import `HelperEntity` and `extract_dependencies`):

```python
from fyrnheim.core.entity import Entity, HelperEntity
from fyrnheim.engine.resolution import extract_dependencies


def validate_helper_entities(entities: list[Entity]) -> None:
    """Validate all HelperEntities are referenced by at least one other entity.

    Raises ValueError if any HelperEntity is orphaned (not in any
    other entity's dependency chain).
    """
    helper_names = {e.name for e in entities if isinstance(e, HelperEntity)}
    if not helper_names:
        return

    referenced: set[str] = set()
    for e in entities:
        referenced.update(extract_dependencies(e))

    orphaned = helper_names - referenced
    if orphaned:
        raise ValueError(
            f"HelperEntity(s) {orphaned} not referenced by any other entity. "
            "Helper entities must be depended on."
        )
```

**Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_helper_entity.py::TestHelperEntityOrphanValidation -v`
Expected: PASS (5 tests)

**Step 5: Commit**

```bash
git add src/fyrnheim/engine/runner.py tests/test_helper_entity.py
git commit -m "feat: add validate_helper_entities for orphan detection"
```

---

### Task 5: Wire validation into runner.run()

**Files:**
- Modify: `src/fyrnheim/engine/runner.py`

**Step 1: Wire the call**

In `src/fyrnheim/engine/runner.py`, in the `run()` function, add `validate_helper_entities()` call between entity discovery (line ~355) and dependency resolution (line ~366):

```python
    # Phase 1: Discover
    log.info("Discovering entities in %s", entities_dir)
    registry = EntityRegistry()
    registry.discover(entities_dir)

    # ... existing empty check ...

    # Phase 1.5: Validate helper entities
    all_entities = [info.entity for _name, info in registry.items()]
    validate_helper_entities(all_entities)

    # Phase 2: Resolve dependency order
    sorted_entities = resolve_execution_order(registry)
```

**Step 2: Run full test suite to verify nothing breaks**

Run: `uv run pytest -v`
Expected: All existing tests PASS

**Step 3: Commit**

```bash
git add src/fyrnheim/engine/runner.py
git commit -m "feat: wire validate_helper_entities into runner pipeline"
```

---

### Task 6: CLI list [helper] marker

**Files:**
- Modify: `src/fyrnheim/cli.py:522-524`
- Test: `tests/test_cli_list.py`

**Step 1: Write the failing test**

Add to `tests/test_cli_list.py`:

```python
def test_list_shows_helper_marker(tmp_path, cli_runner):
    """HelperEntity shows [helper] marker in list output."""
    entity_file = tmp_path / "identity_map.py"
    entity_file.write_text(
        "from fyrnheim import HelperEntity, LayersConfig, PrepLayer, TableSource\n"
        "entity = HelperEntity(\n"
        "    name='identity_map',\n"
        "    description='Mapping table',\n"
        "    layers=LayersConfig(prep=PrepLayer(model_name='prep_identity_map')),\n"
        "    source=TableSource(project='p', dataset='d', table='t'),\n"
        ")\n"
    )
    result = cli_runner.invoke(main, ["list", "--entities-dir", str(tmp_path)])
    assert result.exit_code == 0
    assert "[helper]" in result.output
    assert "identity_map" in result.output
```

Check the test file for the existing `cli_runner` fixture and `main` import — match those patterns.

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_cli_list.py::test_list_shows_helper_marker -v`
Expected: FAIL — output contains `identity_map` but no `[helper]`

**Step 3: Write minimal implementation**

In `src/fyrnheim/cli.py`, modify the list command's entity display loop (around line 522-524):

```python
    for _name, info in registry.items():
        layers_str = ", ".join(info.layers)
        from fyrnheim.core.entity import HelperEntity
        marker = " [helper]" if isinstance(info.entity, HelperEntity) else ""
        click.echo(f"  {info.name:<20s} {layers_str:<30s} {info.path}{marker}")
```

Move the import to the top of the function body (alongside the other local imports).

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_cli_list.py -v`
Expected: All list tests PASS

**Step 5: Commit**

```bash
git add src/fyrnheim/cli.py tests/test_cli_list.py
git commit -m "feat: show [helper] marker in fyr list output"
```

---

### Task 7: Registry discovers HelperEntity instances

**Files:**
- Test: `tests/test_engine_registry.py`

**Step 1: Write the test**

Add to `tests/test_engine_registry.py`:

```python
def test_discovers_helper_entity(tmp_path):
    """Registry discovers HelperEntity instances from entity files."""
    entity_file = tmp_path / "identity_map.py"
    entity_file.write_text(
        "from fyrnheim import HelperEntity, LayersConfig, PrepLayer, TableSource\n"
        "entity = HelperEntity(\n"
        "    name='identity_map',\n"
        "    description='Mapping table',\n"
        "    layers=LayersConfig(prep=PrepLayer(model_name='prep_identity_map')),\n"
        "    source=TableSource(project='p', dataset='d', table='t'),\n"
        ")\n"
    )
    registry = EntityRegistry()
    registry.discover(tmp_path)
    assert "identity_map" in registry
    info = registry["identity_map"]
    from fyrnheim.core.entity import HelperEntity
    assert isinstance(info.entity, HelperEntity)
```

**Step 2: Run test**

Run: `uv run pytest tests/test_engine_registry.py::test_discovers_helper_entity -v`
Expected: PASS (HelperEntity is a subclass of Entity, so `isinstance(entity, Entity)` check in registry should already work)

**Step 3: Commit**

```bash
git add tests/test_engine_registry.py
git commit -m "test: verify registry discovers HelperEntity instances"
```

---

### Task 8: Final verification

**Step 1: Run full test suite**

Run: `uv run pytest -v`
Expected: All tests PASS

**Step 2: Run linter**

Run: `uv run ruff check src/ tests/`
Expected: No errors

**Step 3: Run type checker**

Run: `uv run mypy src/`
Expected: No errors

**Step 4: Fix any issues found**

**Step 5: Commit any fixes**

```bash
git commit -m "chore: fix lint/type issues"
```
