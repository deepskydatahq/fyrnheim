# Design: IdentityGraphConfig model and DerivedSource wiring

**Task:** typedata-8nf (M005-E003-S001)
**Date:** 2026-02-28
**Status:** brainstorm complete

---

## 1. Context

DerivedSource currently holds a bare `identity_graph: str` name and a manual `depends_on: list[str]`. There is no structured configuration for *what* an identity graph actually does -- which sources to join, on what key, which fields to coalesce, in what priority order.

Downstream stories (S002: codegen, S003: executor multi-input) need a typed config object they can read to generate cascading FULL OUTER JOIN chains, PriorityCoalesce `.fillna()` chains, and auto-columns (is_{source}, {source}_id, first_seen_{source}).

This story introduces that typed config without changing any runtime behavior. Pure model addition.

## 2. Key design decisions

### Q1: Should DerivedSource stay frozen?

**Decision: Keep frozen. Use a `model_validator(mode='after')` to compute `depends_on` at construction time.**

Frozen models are good -- they prevent accidental mutation after construction. The conflict is only apparent: we need `depends_on` to be auto-derived from `identity_graph_config.sources`, but this can be done during validation (before the freeze takes effect) rather than via post-construction mutation.

Pydantic's `model_validator(mode='after')` runs after field assignment but before the frozen check is enforced for external callers. We can use `model.__dict__` update inside the validator since Pydantic validators run in a privileged context before the object is fully frozen.

**Implementation pattern:**
```python
@model_validator(mode="after")
def _derive_depends_on(self) -> DerivedSource:
    if self.identity_graph_config is not None and not self.depends_on:
        entity_names = [s.entity for s in self.identity_graph_config.sources]
        object.__setattr__(self, "depends_on", entity_names)
    return self
```

Using `object.__setattr__` bypasses Pydantic's frozen check inside the validator. This is the standard Pydantic pattern for computed fields on frozen models.

**Why not remove frozen:** Frozen provides immutability guarantees that callers depend on (existing test `test_frozen` asserts this). Removing it would be a regression.

### Q2: Should IdentityGraphSource use SourcePriority enum or just ordering?

**Decision: Use the `priority: list[str]` ordering on IdentityGraphConfig. Do NOT use SourcePriority enum.**

`SourcePriority` is a fixed 4-level int enum (PRIMARY=1 through QUATERNARY=4). It has zero usage in actual logic -- only test coverage proving it exists. An explicit ordered list `priority: list[str]` is:

- More flexible (supports N sources, not just 4)
- More readable (`priority: ["hubspot", "stripe"]` vs assigning enum values per source)
- Explicit about ordering (list position = priority rank)
- What S002 codegen expects: it iterates `priority` to build `.fillna()` chains

The `SourcePriority` enum is a legacy artifact. It should remain in types.py for backward compatibility but is not used by this new config.

### Q3: Should priority be a separate list or implicit from source ordering?

**Decision: Explicit `priority: list[str]` on IdentityGraphConfig.**

Rationale:
- Source definition order and coalesce priority order are logically separate concerns. A source might be listed first for readability but be lowest priority for field resolution.
- Making priority explicit avoids "read the source order to understand behavior" confusion.
- S002's codegen iterates `config.priority` to determine `.fillna()` chain order -- it needs an explicit ordered list of source names.
- Validation ensures `set(priority) == set(source names)`, so they stay in sync.

### Q4: Where to place the new models?

**Decision: In `source.py`, alongside existing source models.**

Both `IdentityGraphSource` and `IdentityGraphConfig` are source configuration. They describe how data sources combine, not types or layers. Placing them in `source.py` keeps the import graph simple and follows the existing pattern where all `*Source` models live.

### Q5: Should `_extract_dependencies` change?

**Decision: No change needed to `resolution.py`.**

Since `DerivedSource.depends_on` is auto-populated by the model validator when `identity_graph_config` is present, `_extract_dependencies` already returns the right thing: `list(source.depends_on)`. The dependency derivation happens at model construction time, not at resolution time. This keeps resolution.py simple and unchanged.

### Q6: Should explicit depends_on merge with config-derived depends_on?

**Decision: Yes, union them. Explicit depends_on may reference entities not in the identity graph (e.g., a lookup table).**

The model validator should union the explicit list with the config-derived list, deduplicating:

```python
if self.identity_graph_config is not None:
    config_entities = [s.entity for s in self.identity_graph_config.sources]
    merged = list(dict.fromkeys(list(self.depends_on) + config_entities))
    object.__setattr__(self, "depends_on", merged)
```

Using `dict.fromkeys` preserves insertion order while deduplicating.

## 3. Model definitions

### IdentityGraphSource

```python
class IdentityGraphSource(BaseModel):
    """Configuration for one source in an identity graph.

    Each source represents an upstream entity whose records will be
    joined into the unified identity graph via the match key.
    """
    model_config = ConfigDict(frozen=True)

    name: str = PydanticField(min_length=1)
    entity: str = PydanticField(min_length=1)
    match_key_field: str = PydanticField(min_length=1)
    fields: dict[str, str] = PydanticField(default_factory=dict)
    id_field: str | None = None
    date_field: str | None = None
```

**Field semantics:**

| Field | Type | Purpose |
|-------|------|---------|
| `name` | str | Label for this source (e.g., "hubspot"). Used as key in priority list, source flag names, codegen variable names. |
| `entity` | str | Entity name to read from (e.g., "hubspot_person"). Drives depends_on derivation. |
| `match_key_field` | str | Column name in this source that maps to the unified match key (e.g., "email"). |
| `fields` | dict[str, str] | Mapping of `{unified_field_name: source_column_name}`. Empty dict means no field coalescing from this source. |
| `id_field` | str \| None | Source column to preserve as `{name}_id` in output. None means no source ID column. |
| `date_field` | str \| None | Source column to use for `first_seen_{name}` in output. None means no first-seen column. |

### IdentityGraphConfig

```python
class IdentityGraphConfig(BaseModel):
    """Configuration for an identity graph that merges multiple sources.

    Defines how to join multiple entity sources on a common match key
    and resolve field conflicts via priority ordering.
    """
    model_config = ConfigDict(frozen=True)

    match_key: str = PydanticField(min_length=1)
    sources: list[IdentityGraphSource] = PydanticField(min_length=2)
    priority: list[str]

    @field_validator("priority")
    @classmethod
    def validate_priority_not_empty(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("priority must not be empty")
        return v

    @model_validator(mode="after")
    def validate_priority_matches_sources(self) -> IdentityGraphConfig:
        source_names = {s.name for s in self.sources}
        priority_names = set(self.priority)
        if source_names != priority_names:
            missing_from_priority = source_names - priority_names
            extra_in_priority = priority_names - source_names
            parts = []
            if missing_from_priority:
                parts.append(f"sources not in priority: {missing_from_priority}")
            if extra_in_priority:
                parts.append(f"priority names not in sources: {extra_in_priority}")
            raise ValueError(
                f"priority must contain exactly the source names. {'; '.join(parts)}"
            )
        return self
```

**Validations:**
1. `sources` has `min_length=2` via Pydantic field constraint -- identity graphs need 2+ inputs
2. `priority` names must exactly match source names (set equality)
3. `match_key` must be non-empty

### DerivedSource changes

```python
class DerivedSource(BaseModel):
    """Source for derived entities created via identity graph resolution."""
    model_config = ConfigDict(frozen=True)

    identity_graph: str = PydanticField(min_length=1)
    depends_on: list[str] = PydanticField(default_factory=list)
    identity_graph_config: IdentityGraphConfig | None = None

    @field_validator("identity_graph")
    @classmethod
    def validate_identity_graph(cls, v: str) -> str:
        if not isinstance(v, str) or not v:
            raise ValueError("identity_graph must be a non-empty string")
        return v

    @model_validator(mode="after")
    def _derive_depends_on(self) -> DerivedSource:
        """Auto-populate depends_on from identity_graph_config sources."""
        if self.identity_graph_config is not None:
            config_entities = [s.entity for s in self.identity_graph_config.sources]
            merged = list(dict.fromkeys(list(self.depends_on) + config_entities))
            object.__setattr__(self, "depends_on", merged)
        return self
```

**Key points:**
- `identity_graph_config` is optional, defaults to None -- fully backward compatible
- Existing `DerivedSource(identity_graph="person_graph")` works unchanged
- When config is provided, `depends_on` is auto-derived (union of explicit + config-derived)
- `frozen=True` stays; `object.__setattr__` bypasses freeze inside validator

## 4. Usage example

```python
config = IdentityGraphConfig(
    match_key="email",
    sources=[
        IdentityGraphSource(
            name="hubspot",
            entity="hubspot_person",
            match_key_field="contact_email",
            fields={"first_name": "firstname", "last_name": "lastname"},
            id_field="contact_id",
            date_field="created_at",
        ),
        IdentityGraphSource(
            name="stripe",
            entity="stripe_customer",
            match_key_field="email",
            fields={"first_name": "name"},
            id_field="customer_id",
        ),
    ],
    priority=["hubspot", "stripe"],
)

ds = DerivedSource(
    identity_graph="person_graph",
    identity_graph_config=config,
)

assert ds.depends_on == ["hubspot_person", "stripe_customer"]
assert ds.identity_graph == "person_graph"
```

## 5. Files changed

| File | Change |
|------|--------|
| `src/fyrnheim/core/source.py` | Add `IdentityGraphSource`, `IdentityGraphConfig`; update `DerivedSource` with optional `identity_graph_config` and model validator |
| `src/fyrnheim/core/__init__.py` | Export `IdentityGraphSource`, `IdentityGraphConfig` |
| `src/fyrnheim/__init__.py` | Export `IdentityGraphSource`, `IdentityGraphConfig`; add to `__all__` |
| `src/fyrnheim/engine/resolution.py` | **No changes** -- depends_on auto-derivation happens at model construction |
| `tests/test_core_source.py` | Add `TestIdentityGraphSource`, `TestIdentityGraphConfig` classes; extend `TestDerivedSource` with config tests |
| `tests/test_engine_resolution.py` | Add test for derived entity with identity_graph_config auto-derived depends_on |

## 6. Test plan

### IdentityGraphSource tests
- Minimal creation (name, entity, match_key_field only)
- Full creation (all fields populated)
- Empty name rejected
- Empty entity rejected
- Empty match_key_field rejected
- Default fields is empty dict
- Default id_field is None
- Default date_field is None
- Frozen (cannot mutate after construction)

### IdentityGraphConfig tests
- Valid creation with 2 sources
- Valid creation with 3+ sources
- Fewer than 2 sources rejected (min_length=2)
- Priority matches source names (valid)
- Priority missing a source name (rejected)
- Priority has extra name not in sources (rejected)
- Empty priority rejected
- Empty match_key rejected
- Frozen

### DerivedSource with config tests
- Config is None by default (backward compat)
- Existing `DerivedSource(identity_graph="x")` works unchanged
- With config, depends_on auto-populated from source entities
- With config AND explicit depends_on, lists are merged (union)
- Duplicate entity names in explicit + config are deduplicated
- Still frozen after construction
- resolution._extract_dependencies returns correct deps for config-bearing DerivedSource

## 7. Simplification review

### What would I remove?

1. **id_field and date_field on IdentityGraphSource** -- These are only used by S002 codegen for auto-generated columns. They could be deferred. **Verdict: KEEP.** They are in the acceptance criteria and S002 depends on them. Removing means a schema change later. Better to define the full model now even if codegen comes in S002.

2. **SourcePriority enum** -- It is unused in any logic. This story does not use it. **Verdict: OUT OF SCOPE.** Do not remove it in this story (separate cleanup task). Do not use it in this story either.

3. **DerivedEntitySource** -- There is a separate `DerivedEntitySource` class that also has `identity_graph` and `fields`. It looks like a parallel concept. **Verdict: OUT OF SCOPE.** This story does not touch it. A future cleanup may consolidate or deprecate it.

4. **Separate priority list vs implicit source ordering** -- Could we remove `priority` and just use source list order? **Verdict: KEEP EXPLICIT.** The acceptance criteria specifically require `priority: list[str]` and validation that priority names match source names. Also, separating definition order from resolution order is better API design.

5. **fields as dict[str, str] vs list[Field]** -- Could we reuse the existing `Field` model? **Verdict: KEEP dict[str, str].** The `fields` here is a simple mapping `{unified_name: source_column}`, not a full field definition with type/nullable/description. Using the full `Field` model would be over-engineering. The codegen needs `{unified: source}` pairs, nothing more.

### Final verdict: APPROVED

Every field in the design is either required by the acceptance criteria or needed by downstream stories (S002, S003). No unnecessary complexity. The model is frozen, minimal, and uses standard Pydantic patterns.

## 8. Risks and mitigations

| Risk | Mitigation |
|------|------------|
| `object.__setattr__` on frozen model is fragile across Pydantic versions | Well-documented Pydantic pattern; tested explicitly; alternative is removing frozen (worse) |
| Source name uniqueness not validated | Could add a validator on `IdentityGraphConfig.sources` checking name uniqueness. Low risk for MVP but worth adding. |
| `identity_graph: str` becomes redundant when `identity_graph_config` is present | Keep both: the string is the graph's logical name, the config is its definition. They serve different purposes. Codegen uses the config; logging/error messages use the name. |

**Recommended addition:** Add a validator on `IdentityGraphConfig` ensuring source names are unique:

```python
@field_validator("sources")
@classmethod
def validate_unique_source_names(cls, v: list[IdentityGraphSource]) -> list[IdentityGraphSource]:
    names = [s.name for s in v]
    if len(names) != len(set(names)):
        dupes = [n for n in names if names.count(n) > 1]
        raise ValueError(f"Duplicate source names: {set(dupes)}")
    return v
```

This prevents subtle bugs where two sources have the same name but different configs.
