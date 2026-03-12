# HelperEntity Design (M023)

## Goal

Provide a first-class `HelperEntity` type for intermediate computation steps (e.g., identity resolution mapping tables) with explicit constraints to prevent proliferation.

## Design

`HelperEntity` is a subclass of `Entity` with two constraints:

### 1. Must be depended on

At run time, the runner validates that every HelperEntity is referenced by at least one other entity's dependency chain (via `depends_on`, DerivedSource, AggregationSource, or IdentityGraphConfig entity references). Orphaned helpers fail the pipeline with a clear error message.

### 2. Prep-only layers

HelperEntity restricts `LayersConfig` to only allow the `prep` layer. Attempting to set `dimension`, `snapshot`, `activity`, or `analytics` raises a ValidationError at definition time. Helpers are intermediate computation steps, not business entities.

### Automatic behaviors

- `is_internal` is always `True` (set in `model_post_init`, user can't override)
- Shows in `fyr list` with a `[helper]` marker
- Excluded from `fyr docs` catalog (inherited from `is_internal=True`)
- No auto-cleanup or count limits

## Implementation

### Entity subclass

Location: `src/fyrnheim/core/entity.py`

```python
class HelperEntity(Entity):
    """Intermediate computation entity with restricted layers.

    Must be depended on by at least one other entity.
    Only prep layer is allowed.
    """

    @model_validator(mode="after")
    def _restrict_layers(self) -> "HelperEntity":
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

    def model_post_init(self, __context):
        super().model_post_init(__context)
        object.__setattr__(self, "is_internal", True)
```

### Runner validation

Location: `src/fyrnheim/engine/runner.py`

After entity discovery, before execution:

```python
def _validate_helper_entities(entities: list[Entity]) -> None:
    helper_names = {e.name for e in entities if isinstance(e, HelperEntity)}
    if not helper_names:
        return

    # Collect all dependency references from non-helper entities
    referenced = set()
    for e in entities:
        # Check depends_on, DerivedSource refs, AggregationSource refs, etc.
        referenced.update(_collect_entity_refs(e))

    orphaned = helper_names - referenced
    if orphaned:
        raise ValueError(
            f"HelperEntity(s) {orphaned} not referenced by any other entity. "
            "Helper entities must be depended on."
        )
```

## What doesn't change

- Codegen: HelperEntity generates the same transform modules as Entity (source + prep)
- Executor: HelperEntity tables are persisted normally (as prep_{name} or dim_{name})
- Existing entities: no changes, fully backward compatible

## Use case: Zeffy identity resolution

```
HelperEntity: _identity_map (prep only)
  - DerivedSource joining touchpoints + merge_ids on amplitude_id
  - Prep: propagate org_id through merged links
  - Output: amplitude_id → organization_id mapping

Entity: account
  - IdentityGraphConfig joining organizations + _identity_map on organization_id
  - DimensionLayer: account_name, created_at, referral_source
```
