# Design: M001-E004-S001 -- Extract entity discovery and dependency resolution

**Date:** 2026-02-17
**Story:** M001-E004-S001
**Status:** ready

## Source Analysis

Three files from `timo-data-stack/metadata/`:

- **entity_registry.py** (225 lines) -- `EntityRegistry` class discovers entity definitions from a directory by scanning `.py` files, dynamically importing them, and extracting the module-level `entity = Entity(...)` variable. Also tracks generated code availability and layer enumeration.
- **dependency_resolver.py** (300 lines) -- `DependencyResolver` class builds a dependency graph from discovered entities and computes topological execution order using `graphlib.TopologicalSorter`. Has smart resolution rules for dependency names (entity names, layer-prefixed names, `source:` prefix notation).
- **entity_dependency_parser.py** (218 lines) -- `EntityDependencyParser` extracts a visualization-friendly dependency graph (nodes + edges). Uses `DerivedSource.identity_graph` to resolve transitive dependencies through identity graph configs.

### What creates inter-entity dependencies?

Two source types create cross-entity edges:

| Source type | Dependency mechanism | Example |
|---|---|---|
| `DerivedSource` | References an identity graph config that lists source dimension tables from other entities | `person` depends on `_ghost_person`, `_mailerlite_person`, `transactions`, `subscriptions` via `person_identity_graph` |
| `AggregationSource` | References another entity by name via `source_entity` field | `account` depends on `person` via `source_entity="person"` |

`BigQuerySource`, `EventAggregationSource`, `UnionSource` are leaf sources (no entity dependencies).

## Decision 1: How does entity discovery work?

**Keep the `entity = Entity(...)` module-level convention.** This is clean and works well.

### Mechanism

1. Scan a directory for `*.py` files (excluding `__init__.py` and files starting with `.`)
2. For each file, use `importlib.util.spec_from_file_location()` + `exec_module()` to dynamically load the module
3. Check for a module-level attribute named `entity` that is an instance of `Entity`
4. Extract the entity name from `Entity.name` (NOT from the filename, unlike timo-data-stack which uses `entity_file.stem`)

### Changes from timo-data-stack

| Aspect | timo-data-stack | fyrnheim |
|---|---|---|
| Entity name source | `entity_file.stem` (filename) | `entity.name` (from the Entity definition) |
| Layer enumeration | Hardcoded check of `entity.layers.prep`, `.dimension`, `.snapshot`, `.activity` | Use `Entity.has_layer()` method; iterate over all layer names dynamically |
| Generated code tracking | Checks for `{entity_name}_transforms.py` in generated dir | **Remove.** Generated code is a separate concern (E004-S003 run function). Discovery should only find and validate entity definitions. |
| sys.path manipulation | Inserts parent directory into `sys.path` | Keep. Required for dynamic import to resolve entity module dependencies (e.g., entity files that import from `fyrnheim`). |
| `EntityInfo.module` | References the generated transforms module | **Remove.** Not the registry's responsibility. |
| `EntityInfo.generated_path` | Path to generated transforms file | **Remove.** |
| `EntityInfo.has_generated_code` | Boolean flag | **Remove.** |
| Model name helpers | `get_model_names()`, `get_entity_for_model()`, `get_entity_name_for_model()` | **Remove.** These are timo-specific (layer name prefixes like `prep_`, `dim_`, `snapshot_`). The execution engine can handle model naming. |
| Validation | `validate()` checks generated code exists | **Remove.** Validation of generated code belongs in the executor story. |

### EntityInfo (simplified)

```python
class EntityInfo(BaseModel):
    """Information about a discovered entity."""
    model_config = {"arbitrary_types_allowed": True}

    name: str              # From entity.name
    entity: Entity         # The Entity instance
    path: Path             # Path to the .py definition file
    layers: list[str]      # Layer names configured on this entity
```

**Rationale for `name` from `Entity.name`:** The filename convention (`transactions.py` -> entity name `transactions`) is fragile. If the file is renamed, the entity name silently changes. Using `entity.name` makes the name explicit and authoritative. The filename becomes just a file organization detail.

### Discovery API

```python
class EntityRegistry:
    def __init__(self) -> None: ...
    def discover(self, entities_dir: Path | str) -> None: ...
    def get(self, name: str) -> EntityInfo | None: ...
    def items(self) -> ItemsView[str, EntityInfo]: ...
    def __iter__(self) -> Iterator[str]: ...
    def __len__(self) -> int: ...
    def __contains__(self, name: str) -> bool: ...
```

**Note:** `discover()` takes the directory as a parameter rather than a constructor argument. This allows a single registry to discover from multiple directories if needed (useful for testing, or projects that organize entities in subdirectories). Each call to `discover()` accumulates entities; duplicate names raise an error.

## Decision 2: How does dependency resolution work?

**Use `DerivedSource` and `AggregationSource` to extract inter-entity edges, then topological sort via `graphlib.TopologicalSorter`.**

### Dependency extraction (what creates edges)

The dependency graph is built from entity source types:

```python
def _extract_dependencies(entity: Entity) -> list[str]:
    """Return list of entity names this entity depends on."""
    source = entity.source
    if source is None:
        return []
    if isinstance(source, AggregationSource):
        return [source.source_entity]
    if isinstance(source, DerivedSource):
        # DerivedSource depends on entities via identity graph.
        # For now: return empty (identity graph resolution is a future story).
        # The dependency can be declared explicitly via a new field.
        return []
    return []  # TableSource, EventAggregationSource, UnionSource are leaf sources
```

### The DerivedSource dependency problem

In timo-data-stack, `DerivedSource` dependencies are resolved by loading the identity graph YAML config file and inspecting its source table references. This creates a coupling to identity graph internals.

**Decision: Add an explicit `depends_on` field to `DerivedSource`.** The entity definition should declare its dependencies explicitly rather than requiring the registry to parse identity graph configs.

```python
class DerivedSource(BaseModel):
    """Source for derived entities created via identity graph resolution."""
    model_config = ConfigDict(frozen=True)
    identity_graph: str = PydanticField(min_length=1)
    depends_on: list[str] = PydanticField(default_factory=list)
```

This gives:
```python
# person.py
entity = Entity(
    name="person",
    source=DerivedSource(
        identity_graph="person_identity_graph",
        depends_on=["_ghost_person", "_mailerlite_person", "transactions", "subscriptions"],
    ),
    ...
)
```

**Rationale:**
- Explicit is better than implicit (Zen of Python)
- No need to parse identity graph configs at discovery time
- Makes the dependency graph statically analyzable from entity definitions alone
- The identity graph loader can validate consistency separately

### Also add `depends_on` to `AggregationSource`

`AggregationSource` already has `source_entity` which is an implicit dependency. Keep that AND also support an optional `depends_on` list for additional cross-entity dependencies:

```python
class AggregationSource(BaseModel):
    source_entity: str
    group_by_column: str
    filter_expression: str | None = None
    fields: list[Field] | None = None
    depends_on: list[str] = PydanticField(default_factory=list)
```

The dependency extractor merges `[source_entity] + depends_on`.

### Topological sort

```python
from graphlib import CycleError, TopologicalSorter

def resolve_execution_order(registry: EntityRegistry) -> list[EntityInfo]:
    """Return entities in dependency order (dependencies first)."""
    graph: dict[str, set[str]] = {}

    for name, info in registry.items():
        deps = _extract_dependencies(info.entity)
        graph[name] = set(deps)

    ts = TopologicalSorter(graph)
    try:
        order = list(ts.static_order())
    except CycleError as e:
        raise CircularDependencyError(str(e)) from e

    return [registry.get(name) for name in order if registry.get(name) is not None]
```

**Key difference from timo-data-stack:** The timo-data-stack `DependencyResolver` operates at the model/layer level (`prep_orders`, `snapshot_customers`, `dim_person`). Fyrnheim operates at the entity level. Intra-entity layer ordering (source -> snapshot -> prep -> dimension -> activity -> analytics) is a separate concern handled by the executor, not the registry.

### Why entity-level, not layer-level?

In timo-data-stack, the dependency resolver builds a graph of individual layer models (`prep_orders` depends on `snapshot_orders` depends on `source_orders`). This is over-engineered for fyrnheim:

1. **Intra-entity layer ordering is fixed.** An entity's layers always execute in the same order: source -> snapshot -> prep -> dimension -> activity -> analytics. There is no need to topologically sort these -- it is a linear pipeline defined by the framework.
2. **Cross-entity dependencies are entity-to-entity.** When `account` depends on `person`, it means "all of person's layers must complete before any of account's layers start." This is the only edge the registry needs.
3. **Simpler mental model.** Users think in entities, not layer-models. "Account depends on Person" is clearer than "dim_account depends on snapshot_person which depends on source_person."

## Decision 3: Class-based or functional API?

**Class-based `EntityRegistry` for discovery, free function for resolution.**

### Registry: class

```python
registry = EntityRegistry()
registry.discover("src/entities")
info = registry.get("person")
```

**Rationale:** Discovery is stateful (accumulates entities). A class naturally encapsulates this state. It also provides `__len__`, `__iter__`, `__contains__` for ergonomic use.

### Resolution: free function

```python
execution_order = resolve_execution_order(registry)
```

**Rationale:** Resolution is a pure computation over the registry's data. It has no state of its own. Making it a function keeps the API surface minimal and makes it easy to test (pass any registry). It also avoids the timo-data-stack pattern where `DependencyResolver` wraps `EntityRegistry` -- that extra class adds indirection without value.

**Do NOT create a separate `DependencyResolver` class.** The timo-data-stack's `DependencyResolver` exists because it does smart layer-level resolution (entity name -> best layer, `source:` prefix handling, intra-entity layer chaining). Since fyrnheim resolves at the entity level, all of that complexity vanishes. A single function suffices.

## Decision 4: Where does it live in the fyrnheim package?

```
fyrnheim/
  engine/
    __init__.py          # Re-exports: EntityRegistry, EntityInfo, resolve_execution_order
    registry.py          # EntityRegistry, EntityInfo
    resolution.py        # resolve_execution_order, CircularDependencyError
```

### Import paths

```python
from fyrnheim.engine import EntityRegistry, EntityInfo, resolve_execution_order
```

**Rationale:** The story acceptance criteria say "EntityRegistry importable from fyrnheim.engine". The `engine` sub-package will also hold the executor (E004-S002) and run function (E004-S003), so discovery and resolution belong here as the first building blocks.

### Why not `fyrnheim.engine.registry`?

The public import path is `fyrnheim.engine`. The internal module split (`registry.py`, `resolution.py`) is an implementation detail. Users should not need to know which submodule contains which class. The `engine/__init__.py` re-exports everything.

## Decision 5: How to handle circular dependencies?

**Raise `CircularDependencyError` with a clear message showing the cycle.**

```python
class CircularDependencyError(Exception):
    """Raised when entities form a circular dependency."""
    pass
```

### Detection

`graphlib.TopologicalSorter.static_order()` raises `graphlib.CycleError` when a cycle exists. We catch it and wrap it:

```python
try:
    order = list(ts.static_order())
except CycleError as e:
    # CycleError message includes the cycle path
    raise CircularDependencyError(
        f"Circular dependency detected among entities: {e}"
    ) from e
```

### Why a custom exception?

- `graphlib.CycleError` is an implementation detail. Users should not need to import `graphlib` to catch dependency errors.
- The custom exception allows the executor to catch it and provide an actionable message ("Entity A depends on B which depends on A. Break the cycle by...").
- Acceptance criteria explicitly require "Circular dependency raises clear error."

### What about self-referential entities?

An entity whose `AggregationSource.source_entity` points to itself would create a self-loop. `TopologicalSorter` catches this as a cycle. No special handling needed.

## Summary: What gets built

### New files

| File | Contents |
|---|---|
| `fyrnheim/engine/__init__.py` | Re-exports `EntityRegistry`, `EntityInfo`, `resolve_execution_order`, `CircularDependencyError` |
| `fyrnheim/engine/registry.py` | `EntityInfo` (Pydantic model), `EntityRegistry` class with `discover()`, `get()`, `items()`, `__iter__`, `__len__`, `__contains__` |
| `fyrnheim/engine/resolution.py` | `resolve_execution_order()` function, `CircularDependencyError` exception, `_extract_dependencies()` helper |

### Modifications to existing types (future, noted here)

| Type | Change | Story |
|---|---|---|
| `DerivedSource` | Add `depends_on: list[str]` field | Should be done in this story since resolution depends on it |
| `AggregationSource` | Dependency extraction uses existing `source_entity` field | No change needed |

### Lines of code estimate

- `registry.py`: ~80 lines (EntityInfo ~15, EntityRegistry ~65)
- `resolution.py`: ~50 lines (function ~30, exception ~5, helper ~15)
- `__init__.py`: ~10 lines
- Tests: ~150 lines (discovery, dependency resolution, circular detection, error cases)

Total: ~290 lines (vs ~740 lines across the three timo-data-stack source files). The reduction comes from removing layer-level resolution, generated code tracking, model name helpers, YAML config integration, and identity graph loading.

## Risks and Open Questions

1. **Dynamic import safety** -- `importlib.util.exec_module()` executes arbitrary Python code at discovery time. This is the same approach timo-data-stack uses and is standard for plugin-style discovery (pytest, Django, etc.). Risk is minimal since entity files are authored by the project owner, but worth noting.

2. **DerivedSource.depends_on maintenance** -- Adding explicit `depends_on` to `DerivedSource` means the dependency list can drift from the actual identity graph configuration. This is a tradeoff: explicit declaration vs. automatic extraction. We choose explicit because it eliminates the coupling to identity graph internals and makes the dependency graph statically analyzable. A future linting story could validate consistency.

3. **Entity name collisions** -- If `discover()` is called on multiple directories and two files define entities with the same `entity.name`, we need a clear error. Design: raise `ValueError(f"Duplicate entity name '{name}': defined in {existing_path} and {new_path}")`.

4. **Import errors during discovery** -- If an entity file has import errors (e.g., missing dependency), the current timo-data-stack code prints a warning and skips it. Fyrnheim should collect these errors and either (a) raise them all at the end, or (b) return them alongside successful discoveries. Decision: **Raise immediately.** Silent skipping hides real problems. If a file exists in the entities directory, it should load. If it cannot, that is a bug the user needs to fix, not a warning to ignore.

5. **`resolve_execution_order` for partial graphs** -- The function resolves ALL discovered entities. A future story may want to resolve a subset (e.g., "just run `account` and its transitive dependencies"). This can be added later as `resolve_execution_order(registry, targets=["account"])` without changing the current design.

---

## Implementation Plan

### Prerequisites

This story depends on M001-E002-S004 (public API exports), which means `Entity`, `DerivedSource`, `AggregationSource`, `LayersConfig`, and all core types are importable from `fyrnheim`. The `src/fyrnheim/engine/` directory exists as an empty sub-package from M001-E001-S001.

### Step 1: Add `depends_on` field to `DerivedSource`

**File:** `src/fyrnheim/core/source.py`

Add a `depends_on: list[str]` field to `DerivedSource`. This is required before resolution can work, because `DerivedSource` dependencies are not extractable from the `identity_graph` string alone.

```python
class DerivedSource(BaseModel):
    """Source for derived entities created via identity graph resolution."""
    model_config = ConfigDict(frozen=True)

    identity_graph: str = PydanticField(min_length=1)
    depends_on: list[str] = PydanticField(default_factory=list)

    @field_validator("identity_graph")
    @classmethod
    def validate_identity_graph(cls, v: str) -> str:
        if not isinstance(v, str) or not v:
            raise ValueError("identity_graph must be a non-empty string")
        return v
```

The `frozen=True` config means `depends_on` must be set at construction time, which is the intended usage. The field defaults to an empty list so existing `DerivedSource(identity_graph="...")` calls do not break.

Also add `depends_on` to `AggregationSource` for optional additional cross-entity dependencies:

```python
class AggregationSource(BaseModel):
    """Source for entities aggregated from other entities."""
    source_entity: str
    group_by_column: str
    filter_expression: str | None = None
    fields: list[Field] | None = None
    depends_on: list[str] = PydanticField(default_factory=list)
```

**Update re-exports** if `depends_on` changes the public API surface -- it does not; `DerivedSource` and `AggregationSource` are already exported. No changes to `__init__.py` files needed.

**Verification:** Write a quick test that `DerivedSource(identity_graph="x", depends_on=["a", "b"])` constructs without error, and that the default `depends_on` is `[]`.

### Step 2: Create `src/fyrnheim/engine/registry.py`

**File:** `src/fyrnheim/engine/registry.py` (~80 lines)

This file contains two classes: `EntityInfo` and `EntityRegistry`.

#### EntityInfo

```python
from pathlib import Path
from pydantic import BaseModel
from fyrnheim.core.entity import Entity  # or wherever Entity lives after E002-S004

class EntityInfo(BaseModel):
    """Information about a discovered entity."""
    model_config = {"arbitrary_types_allowed": True}

    name: str
    entity: Entity
    path: Path
    layers: list[str]
```

Key differences from timo-data-stack `EntityInfo`:
- No `generated_path`, `module`, `has_generated_code` fields (those are executor concerns)
- No `get_model_names()` method (timo-specific layer prefix logic removed)
- `name` comes from `entity.name`, not from the filename
- `path` is the definition file path (renamed from `definition_path` for brevity)

#### EntityRegistry

```python
import importlib.util
import sys
from pathlib import Path
from collections.abc import ItemsView, Iterator

class EntityRegistry:
    """Registry for discovering entity definitions from Python files."""

    def __init__(self) -> None:
        self._entities: dict[str, EntityInfo] = {}

    def discover(self, entities_dir: Path | str) -> None:
        """Discover entity definitions in a directory.

        Scans for *.py files, dynamically imports them, and extracts
        module-level `entity = Entity(...)` instances.

        Accumulates entities across multiple calls. Raises ValueError
        on duplicate entity names. Raises immediately on import errors.
        """
        entities_dir = Path(entities_dir)
        if not entities_dir.exists():
            raise FileNotFoundError(f"Entities directory not found: {entities_dir}")

        # Add parent to sys.path for import resolution
        entities_parent = entities_dir.parent.resolve()
        if str(entities_parent) not in sys.path:
            sys.path.insert(0, str(entities_parent))

        # Scan for .py files, excluding __init__.py and dotfiles
        entity_files = sorted(
            f for f in entities_dir.glob("*.py")
            if f.name != "__init__.py" and not f.name.startswith(".")
        )

        for entity_file in entity_files:
            # Dynamic import
            module_name = f"_fyrnheim_entity_{entity_file.stem}"
            spec = importlib.util.spec_from_file_location(module_name, entity_file)
            if spec is None or spec.loader is None:
                raise ImportError(f"Could not create import spec for {entity_file}")

            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)  # Raises on import error -- intentional

            # Check for entity attribute
            if not hasattr(module, "entity"):
                continue  # File has no entity definition; skip silently
            entity_obj = module.entity
            if not isinstance(entity_obj, Entity):
                continue  # entity attribute is not an Entity instance; skip

            # Extract name from Entity.name (NOT filename)
            name = entity_obj.name

            # Check for duplicates
            if name in self._entities:
                existing_path = self._entities[name].path
                raise ValueError(
                    f"Duplicate entity name '{name}': defined in {existing_path} and {entity_file}"
                )

            # Enumerate layers dynamically using Entity.has_layer()
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
        """Iterate over entity names."""
        return iter(self._entities)

    def __len__(self) -> int:
        return len(self._entities)

    def __contains__(self, name: str) -> bool:
        return name in self._entities
```

Key design decisions baked in:
- `discover()` takes a directory parameter (not constructor arg) so a single registry can discover from multiple directories
- `discover()` raises `FileNotFoundError` if the directory does not exist (fail-fast, unlike timo-data-stack which silently returns)
- Import errors propagate immediately (no silent `print()` + `continue` like timo-data-stack)
- Files without `entity` attribute or with non-Entity `entity` are silently skipped (these are helper modules, not bugs)
- Entity name from `entity.name`, not from filename
- Layer enumeration uses `Entity.has_layer()` method, iterating over the known layer names dynamically
- Sorted glob results for deterministic discovery order

### Step 3: Create `src/fyrnheim/engine/resolution.py`

**File:** `src/fyrnheim/engine/resolution.py` (~50 lines)

```python
"""Dependency resolution for entity execution ordering."""

from __future__ import annotations

from graphlib import CycleError, TopologicalSorter
from typing import TYPE_CHECKING

from fyrnheim.core.source import AggregationSource, DerivedSource

if TYPE_CHECKING:
    from fyrnheim.engine.registry import EntityInfo, EntityRegistry
    from fyrnheim.core.entity import Entity


class CircularDependencyError(Exception):
    """Raised when entities form a circular dependency."""
    pass


def _extract_dependencies(entity: Entity) -> list[str]:
    """Return entity names this entity depends on.

    Dependency edges are determined by the entity's source type:
    - AggregationSource: depends on source_entity + explicit depends_on
    - DerivedSource: depends on explicit depends_on list
    - All other source types (TableSource, EventAggregationSource,
      UnionSource): leaf sources with no entity dependencies.
    """
    source = entity.source
    if source is None:
        return []
    if isinstance(source, AggregationSource):
        return [source.source_entity] + list(source.depends_on)
    if isinstance(source, DerivedSource):
        return list(source.depends_on)
    return []


def resolve_execution_order(registry: EntityRegistry) -> list[EntityInfo]:
    """Return entities sorted in dependency order (dependencies first).

    Uses graphlib.TopologicalSorter to compute a valid execution order
    where every entity's dependencies appear before it in the list.

    Args:
        registry: EntityRegistry with discovered entities.

    Returns:
        List of EntityInfo in execution order.

    Raises:
        CircularDependencyError: If entities form a dependency cycle.
    """
    graph: dict[str, set[str]] = {}

    for name, info in registry.items():
        deps = _extract_dependencies(info.entity)
        graph[name] = set(deps)

    ts = TopologicalSorter(graph)
    try:
        order = list(ts.static_order())
    except CycleError as e:
        raise CircularDependencyError(
            f"Circular dependency detected among entities: {e}"
        ) from e

    # Filter to entities that exist in the registry (deps may reference
    # entities not in this registry -- TopologicalSorter includes them
    # as nodes but they have no EntityInfo).
    return [registry.get(name) for name in order if registry.get(name) is not None]
```

Key points:
- `_extract_dependencies()` is a private helper. It checks `isinstance` on the source to determine which dependency fields to read. This is simple, explicit, and extensible (add a new source type check when needed).
- `AggregationSource` merges `[source_entity] + depends_on` -- the `source_entity` is always a dependency, plus any additional explicit ones.
- `DerivedSource` uses only `depends_on` -- there is no implicit dependency to extract.
- `CircularDependencyError` wraps `graphlib.CycleError` so callers do not need to know about `graphlib`.
- The function filters the topological order to only return entities present in the registry. `TopologicalSorter` will include dependency names as nodes even if they are not keys in the graph dict, but those have no `EntityInfo`.

### Step 4: Update `src/fyrnheim/engine/__init__.py`

**File:** `src/fyrnheim/engine/__init__.py`

Replace the empty `__init__.py` with re-exports:

```python
"""fyrnheim.engine -- Entity discovery, dependency resolution, and execution."""

from fyrnheim.engine.registry import EntityInfo, EntityRegistry
from fyrnheim.engine.resolution import CircularDependencyError, resolve_execution_order

__all__ = [
    "CircularDependencyError",
    "EntityInfo",
    "EntityRegistry",
    "resolve_execution_order",
]
```

This enables the acceptance-criteria import: `from fyrnheim.engine import EntityRegistry`.

### Step 5: Write tests

**File:** `tests/test_engine_registry.py` (~80 lines)

Tests for `EntityRegistry` and `EntityInfo`:

1. **test_discover_finds_entity_files** -- Create a temp directory with two `.py` files each exporting `entity = Entity(...)`. Call `registry.discover(tmp_dir)`. Assert `len(registry) == 2` and both entities are retrievable via `registry.get()`.

2. **test_discover_skips_init_and_dotfiles** -- Create `__init__.py` and `.hidden.py` in the temp dir. Assert they are not discovered.

3. **test_discover_skips_files_without_entity** -- Create a `.py` file that has no `entity` variable. Assert it is skipped without error.

4. **test_discover_skips_non_entity_attribute** -- Create a `.py` file where `entity = "not an Entity"`. Assert it is skipped.

5. **test_discover_uses_entity_name_not_filename** -- Create `my_file.py` with `entity = Entity(name="actual_name", ...)`. Assert the registered name is `"actual_name"`, not `"my_file"`.

6. **test_discover_raises_on_duplicate_name** -- Two files define entities with the same `name`. Assert `ValueError` with "Duplicate entity name" in the message.

7. **test_discover_raises_on_missing_directory** -- Call `discover()` on a non-existent path. Assert `FileNotFoundError`.

8. **test_discover_raises_on_import_error** -- Create a `.py` file with a syntax error. Assert the import error propagates.

9. **test_entity_info_has_correct_layers** -- Create an entity with `prep` and `dimension` layers. Assert `info.layers == ["prep", "dimension"]`.

10. **test_registry_contains_and_iter** -- Test `__contains__`, `__iter__`, `__len__`, `items()`.

11. **test_discover_accumulates_across_calls** -- Call `discover()` on two different directories. Assert entities from both are present.

**File:** `tests/test_engine_resolution.py` (~70 lines)

Tests for `resolve_execution_order` and dependency extraction:

1. **test_leaf_entities_resolve_in_any_order** -- Three entities with `TableSource` (no deps). Assert all three appear in the output list (order among independent entities is unspecified).

2. **test_aggregation_source_creates_dependency** -- Entity `account` has `AggregationSource(source_entity="person")`. Assert `person` appears before `account` in execution order.

3. **test_derived_source_depends_on** -- Entity `person` has `DerivedSource(identity_graph="ig", depends_on=["transactions", "subscriptions"])`. Assert `transactions` and `subscriptions` appear before `person`.

4. **test_circular_dependency_raises** -- Entity A depends on B, B depends on A. Assert `CircularDependencyError` is raised.

5. **test_self_referential_raises** -- Entity A has `AggregationSource(source_entity="a")` (self-loop). Assert `CircularDependencyError`.

6. **test_diamond_dependency** -- D depends on B and C; B depends on A; C depends on A. Assert A appears first, D appears last, B and C in between.

7. **test_aggregation_source_merges_depends_on** -- `AggregationSource(source_entity="person", depends_on=["extra"])`. Assert both `person` and `extra` are treated as dependencies.

8. **test_entities_with_no_source** -- Entity with `required_fields` but no `source`. Assert it appears in the output with no dependencies.

#### Test fixtures strategy

Tests will use `tmp_path` (pytest fixture) to create temporary entity files. A helper function creates minimal entity `.py` files:

```python
def write_entity_file(directory: Path, filename: str, entity_code: str) -> Path:
    """Write a Python file that defines an entity."""
    path = directory / filename
    path.write_text(entity_code)
    return path
```

Entity code will import from `fyrnheim` to create real `Entity` instances. This requires `fyrnheim` to be installed (editable mode). The entity definitions will be minimal -- just `name`, `description`, `layers` with one layer, and optionally `source`.

### Implementation Checklist

1. [ ] Add `depends_on: list[str] = PydanticField(default_factory=list)` to `DerivedSource` in `src/fyrnheim/core/source.py`
2. [ ] Add `depends_on: list[str] = PydanticField(default_factory=list)` to `AggregationSource` in `src/fyrnheim/core/source.py`
3. [ ] Create `src/fyrnheim/engine/registry.py` with `EntityInfo` and `EntityRegistry`
4. [ ] Create `src/fyrnheim/engine/resolution.py` with `CircularDependencyError`, `_extract_dependencies`, `resolve_execution_order`
5. [ ] Update `src/fyrnheim/engine/__init__.py` with re-exports
6. [ ] Create `tests/test_engine_registry.py` with discovery tests
7. [ ] Create `tests/test_engine_resolution.py` with dependency resolution tests
8. [ ] Run `pytest tests/test_engine_registry.py tests/test_engine_resolution.py` -- all pass
9. [ ] Run `ruff check src/fyrnheim/engine/` -- no lint errors
10. [ ] Run `mypy src/fyrnheim/engine/` -- no type errors

### Acceptance Criteria Mapping

| Criterion | Satisfied by |
|---|---|
| EntityRegistry importable from fyrnheim.engine | Step 4: `__init__.py` re-exports |
| EntityRegistry.discover(entities_dir) finds all .py files exporting Entity instances | Step 2: `discover()` method with glob + dynamic import |
| Dependency resolution: entity with DerivedSource sorts after its source entity | Step 3: `_extract_dependencies` reads `DerivedSource.depends_on`, `resolve_execution_order` uses TopologicalSorter |
| Circular dependency raises clear error | Step 3: `CircularDependencyError` wrapping `CycleError` |
| EntityInfo provides name, entity, layers list, and path information | Step 2: `EntityInfo` Pydantic model with those four fields |
