# Design: M001-E001-S003 -- Extract activity and analytics layer config classes

**Story:** M001-E001-S003
**Date:** 2026-02-17
**Status:** ready
**Source files:**
- `/home/tmo/roadtothebeach/tmo/timo-data-stack/metadata/core/activity.py` (43 lines)
- `/home/tmo/roadtothebeach/tmo/timo-data-stack/metadata/core/analytics.py` (90 lines)

**Target location:** `src/typedata/core/activity.py` and `src/typedata/core/analytics.py`

---

## 1. Source analysis

### activity.py (43 lines, 2 classes)

| Class | Fields | Deps | Notes |
|-------|--------|------|-------|
| `ActivityType` | `name`, `trigger` (Literal), `timestamp_field`, `values`, `field` | pydantic, stdlib | Pure config. No dbt refs. |
| `ActivityConfig` | `model_name`, `types: list[ActivityType]`, `entity_id_field`, `person_id_field`, `anon_id_field` | pydantic, stdlib | Validator ensures at least one type. `model_post_init` ensures person or anon ID. `identity_field` property. |

**External dependencies:** `pydantic.BaseModel`, `pydantic.field_validator`, `typing.Literal`. Zero internal imports.

**dbt references:** None. This file is already dbt-free. The old dbt-based `ActivityLayer` class lives in `layer.py`, not here. `ActivityConfig` is the newer Ibis-based replacement. This is confirmed by `entity.py`'s `LayersConfig`, which has both `activity: ActivityLayer` (legacy dbt) and `activities: ActivityConfig` (new Ibis).

**Consumers in timo-data-stack:**
- `metadata/core/entity.py` -- `LayersConfig.activities` field
- `entities/entities/signals.py` -- creates an `ActivityConfig` instance
- `metadata/generators/ibis_code_generator.py` -- reads config to generate Ibis code
- `metadata/generators/activities_generator.py` -- uses `ActivitiesLayerConfig` (a different, dbt-based config from `entity.py`), NOT `ActivityConfig`

### analytics.py (90 lines, 5 classes)

| Class | Fields | Deps | Notes |
|-------|--------|------|-------|
| `AnalyticsMetric` | `name`, `expression`, `metric_type` (Literal), `description` | pydantic | Single metric definition. Expression is an Ibis string. |
| `AnalyticsLayer` | `model_name`, `date_expression`, `metrics: list[AnalyticsMetric]`, `dimensions` | pydantic | Layer-level config. Validator ensures at least one metric. |
| `AnalyticsSource` | `entity`, `layer` | pydantic | Reference pointer to an entity's analytics layer. |
| `ComputedMetric` | `name`, `expression`, `description` | pydantic | Cross-entity computed metric (ratios, averages). |
| `AnalyticsModel` | `name`, `description`, `grain`, `sources: list[AnalyticsSource]`, `computed_metrics` | pydantic | Combines multiple entity analytics layers into a wide table. |

**External dependencies:** `pydantic.BaseModel`, `pydantic.field_validator`, `pydantic.Field`, `typing.Literal`. Zero internal imports.

**dbt references:** None. Expressions are Ibis-style strings (e.g., `t.published_at.date()`, `(t.source_platform == 'youtube').sum()`).

**Consumers in timo-data-stack:**
- `metadata/core/entity.py` -- `LayersConfig.analytics` field uses `AnalyticsLayer`
- `entities/entities/signals.py` -- creates `AnalyticsLayer` with 14 metrics
- `metadata/entities/product.py` -- creates `AnalyticsLayer` with 9 metrics
- `entities/analytics/daily.py` -- creates `AnalyticsModel` combining 3 entity sources with `ComputedMetric` instances
- `metadata/generators/duckdb_generator.py` -- reads analytics config to generate Ibis code
- Multiple test files validate analytics behavior

---

## 2. Design decisions

### Decision 1: Copy both files nearly verbatim

**Rationale:** Both files are pure Pydantic models with zero internal dependencies. They import only from `pydantic` and `typing`. There is nothing to strip, refactor, or abstract. The code is clean, well-typed, and already generic.

**Action:** Copy with import path changes only (`metadata.core.*` references in any docstrings become `typedata.core.*`). No actual import statements reference internal modules, so no code changes are needed beyond the module-level docstring if desired.

### Decision 2: No dbt stripping needed

**Rationale:** Neither `activity.py` nor `analytics.py` contain any dbt references. The `ActivityConfig` class was specifically designed as the Ibis-based replacement for the legacy dbt `ActivityLayer` (which lives in `layer.py`). The expressions in `AnalyticsMetric` are Ibis expression strings, not SQL. These files are already dbt-free.

The legacy dbt-based `ActivityLayer` class in `layer.py` is NOT part of this story's scope. It will be evaluated separately in the layer extraction story (M001-E002-S001).

### Decision 3: All analytics classes are generic enough for the library

**Rationale:** The five analytics classes form a clean, composable pattern:

```
AnalyticsMetric  (leaf: single metric definition)
    |
AnalyticsLayer   (entity-level: groups metrics + date grain)
    |
AnalyticsSource  (pointer: references an entity's analytics layer)
    |
ComputedMetric   (cross-entity: derived metrics from combined data)
    |
AnalyticsModel   (top-level: combines sources into wide table)
```

This hierarchy is entirely domain-agnostic. The only domain-specific content is the Ibis expression strings written by users, which is the intended usage pattern. The class structure itself has no timo-data-stack-specific assumptions.

`AnalyticsModel` with its `sources` + `computed_metrics` pattern is the equivalent of a dbt mart model, but expressed purely in Python config. This is a core typedata concept.

### Decision 4: Keep all 7 classes, modify nothing

**Classes to extract (unchanged):**

From `activity.py`:
1. **`ActivityType`** -- keep as-is. The `trigger` Literal values (`row_appears`, `status_becomes`, `field_changes`) are generic activity stream patterns, not timo-specific.
2. **`ActivityConfig`** -- keep as-is. The `model_post_init` validation (require person or anon ID) is a sound generic constraint for any activity stream.

From `analytics.py`:
3. **`AnalyticsMetric`** -- keep as-is. Clean metric definition.
4. **`AnalyticsLayer`** -- keep as-is. Clean layer config.
5. **`AnalyticsSource`** -- keep as-is. Clean entity reference.
6. **`ComputedMetric`** -- keep as-is. Clean cross-entity metric.
7. **`AnalyticsModel`** -- keep as-is. Clean top-level combiner.

### Decision 5: Minor API improvements to consider (deferred)

These are observations for future stories, NOT for this extraction:

- **`ActivityType.trigger`** could become an `enum` instead of `Literal` for extensibility, but changing it now would break the interface contract. Defer.
- **`AnalyticsMetric.expression`** is typed as `str` but actually holds Ibis expression code. A future story could introduce an `IbisExpression` type alias for documentation clarity. Defer.
- **`ComputedMetric`** and `AnalyticsMetric` share `name`/`expression`/`description` fields. A shared base class could reduce duplication, but it would change the class hierarchy and is not worth the risk for this extraction. Defer.

---

## 3. Implementation plan

### File: `src/typedata/core/activity.py`

```python
"""Activity layer configuration for entities."""

from typing import Literal

from pydantic import BaseModel, field_validator


class ActivityType(BaseModel):
    """Defines one activity type derived from an entity."""

    name: str
    trigger: Literal["row_appears", "status_becomes", "field_changes"]
    timestamp_field: str
    values: list[str] | None = None
    field: str | None = None


class ActivityConfig(BaseModel):
    """Activity layer configuration for an entity."""

    model_name: str
    types: list[ActivityType]
    entity_id_field: str
    person_id_field: str | None = None
    anon_id_field: str | None = None

    @field_validator("types")
    @classmethod
    def validate_types(cls, v: list) -> list:
        if not v:
            raise ValueError("ActivityConfig requires at least one activity type")
        return v

    def model_post_init(self, __context) -> None:
        if self.person_id_field is None and self.anon_id_field is None:
            raise ValueError("ActivityConfig requires person_id_field or anon_id_field")

    @property
    def identity_field(self) -> str:
        result = self.person_id_field or self.anon_id_field
        assert result is not None
        return result
```

Identical to source. Zero changes.

### File: `src/typedata/core/analytics.py`

```python
"""Analytics layer components for date-grain metric aggregation."""

from typing import Literal

from pydantic import BaseModel, field_validator
from pydantic import Field as PydanticField


class AnalyticsMetric(BaseModel):
    ...  # identical to source


class AnalyticsLayer(BaseModel):
    ...  # identical to source


class AnalyticsSource(BaseModel):
    ...  # identical to source


class ComputedMetric(BaseModel):
    ...  # identical to source


class AnalyticsModel(BaseModel):
    ...  # identical to source
```

Identical to source. Zero changes.

### Exports in `src/typedata/core/__init__.py`

Add to the existing `__init__.py` (which will be created by S001 and may already have S002 exports):

```python
from .activity import ActivityConfig, ActivityType
from .analytics import (
    AnalyticsLayer,
    AnalyticsMetric,
    AnalyticsModel,
    AnalyticsSource,
    ComputedMetric,
)
```

---

## 4. Testing plan

Four tests matching acceptance criteria:

1. **Import test -- activity:** `ActivityConfig, ActivityType` importable from `typedata.core.activity`
2. **Import test -- analytics:** `AnalyticsLayer, AnalyticsMetric, AnalyticsModel, AnalyticsSource, ComputedMetric` importable from `typedata.core.analytics`
3. **Validation test -- activity:** `ActivityConfig` requires at least one `ActivityType`, requires `person_id_field` or `anon_id_field`, and `identity_field` property returns the correct value
4. **Validation test -- analytics:** `AnalyticsLayer` requires at least one metric, `AnalyticsMetric` validates `min_length=1` on `name` and `expression`

---

## 5. Risks and open questions

**Risks:** None. Both files are zero-dependency leaf modules with no coupling. This is a straightforward copy.

**Open question:** The `ActivityConfig` in `entity.py` coexists with a legacy `ActivityLayer` (dbt-based) in `layer.py`. In typedata, only `ActivityConfig` should exist. The legacy `ActivityLayer` should NOT be extracted. This is already the plan per the layer config story (M001-E002-S001), but worth noting explicitly: typedata has `ActivityConfig` for activity streams, not `ActivityLayer`.

---

## 6. Dependencies

- **Depends on:** M001-E001-S001 (package structure must exist)
- **Parallel with:** M001-E001-S002 (no shared files), M001-E001-S004 (no shared files)
- **Depended on by:** M001-E002-S001 (layer config needs activity/analytics imports), M001-E002-S002 (entity LayersConfig references these)

---

## 7. Implementation plan (detailed)

### Summary

Extract 7 Pydantic config classes from two source files in timo-data-stack into the typedata library. Both source files (`activity.py` with 2 classes, `analytics.py` with 5 classes) are pure Pydantic models with zero internal dependencies -- they import only from `pydantic` and `typing`. Neither file contains any dbt references; all expressions are Ibis-style strings. **These are verbatim copies with zero code changes.**

### Acceptance criteria

1. `ActivityConfig` and `ActivityType` are importable from `typedata.core.activity`
2. `AnalyticsLayer`, `AnalyticsMetric`, `AnalyticsModel`, `AnalyticsSource`, and `ComputedMetric` are importable from `typedata.core.analytics`
3. `ActivityConfig` validates with correct activity type enum values (trigger Literal, at least one type, requires person or anon ID)
4. `AnalyticsLayer` validates with metrics list (at least one required) and `AnalyticsMetric` enforces `min_length=1` on `name` and `expression`

### Implementation tasks

**Task 1: Create `src/typedata/core/activity.py`**
- Target: `/home/tmo/roadtothebeach/tmo/typedata/src/typedata/core/activity.py`
- Source: `/home/tmo/roadtothebeach/tmo/timo-data-stack/metadata/core/activity.py` (43 lines)
- Action: Verbatim copy. Zero modifications. Contains `ActivityType` and `ActivityConfig`.
- Classes: 2 (`ActivityType`, `ActivityConfig`)
- Dependencies: `pydantic.BaseModel`, `pydantic.field_validator`, `typing.Literal`

**Task 2: Create `src/typedata/core/analytics.py`**
- Target: `/home/tmo/roadtothebeach/tmo/typedata/src/typedata/core/analytics.py`
- Source: `/home/tmo/roadtothebeach/tmo/timo-data-stack/metadata/core/analytics.py` (90 lines)
- Action: Verbatim copy. Zero modifications. Contains all 5 analytics classes.
- Classes: 5 (`AnalyticsMetric`, `AnalyticsLayer`, `AnalyticsSource`, `ComputedMetric`, `AnalyticsModel`)
- Dependencies: `pydantic.BaseModel`, `pydantic.field_validator`, `pydantic.Field`, `typing.Literal`

**Task 3: Update `src/typedata/core/__init__.py`**
- Target: `/home/tmo/roadtothebeach/tmo/typedata/src/typedata/core/__init__.py`
- Action: Add re-exports for all 7 classes from the two new modules.
- Append:
  ```python
  from .activity import ActivityConfig, ActivityType
  from .analytics import (
      AnalyticsLayer,
      AnalyticsMetric,
      AnalyticsModel,
      AnalyticsSource,
      ComputedMetric,
  )
  ```

### Test plan

Test file: `/home/tmo/roadtothebeach/tmo/typedata/tests/core/test_activity_analytics.py`

**Test 1: `test_activity_types_importable`**
- Import `ActivityConfig` and `ActivityType` from `typedata.core.activity`
- Verify both are subclasses of `pydantic.BaseModel`

**Test 2: `test_analytics_classes_importable`**
- Import all 5 analytics classes from `typedata.core.analytics`
- Verify each is a subclass of `pydantic.BaseModel`

**Test 3: `test_activity_config_validation`**
- Construct a valid `ActivityConfig` with one `ActivityType` (trigger=`"row_appears"`) and `person_id_field` set -- assert succeeds
- Construct with empty `types` list -- assert `ValidationError` raised
- Construct without `person_id_field` or `anon_id_field` -- assert `ValueError` raised
- Construct with `anon_id_field` only -- assert `identity_field` property returns the anon ID field
- Construct with `person_id_field` set -- assert `identity_field` returns the person ID field

**Test 4: `test_analytics_layer_validation`**
- Construct a valid `AnalyticsLayer` with one `AnalyticsMetric` -- assert succeeds
- Construct with empty `metrics` list -- assert `ValidationError` raised
- Construct `AnalyticsMetric` with empty `name` -- assert `ValidationError` raised (min_length=1)
- Construct `AnalyticsMetric` with empty `expression` -- assert `ValidationError` raised (min_length=1)

**Test 5: `test_analytics_model_composition`**
- Construct an `AnalyticsModel` with `AnalyticsSource` and `ComputedMetric` instances
- Verify `sources` and `computed_metrics` lists are correctly populated
- Verify `AnalyticsSource` defaults `layer` to `"analytics"`

### Notes

- Both source files are **verbatim copies** -- the implementation task is a direct file copy with no modifications whatsoever. This is explicitly confirmed in the design doc (Decisions 1, 2, 4).
- The legacy dbt-based `ActivityLayer` from `layer.py` is NOT in scope. Only the Ibis-based `ActivityConfig` is extracted.
- Depends on M001-E001-S001 (package structure) for directory and `__init__.py` to exist.
