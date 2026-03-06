---
title: Quality Checks
description: Declarative data quality rules that run after transformations.
---

Fyrnheim includes declarative data quality rules that run after transformations. Define checks alongside your entity and they execute automatically during `fyr run`.

## Configuring Quality Checks

```python
from fyrnheim import QualityConfig, NotNull, Unique, InRange

quality = QualityConfig(
    primary_key="email_hash",
    checks=[
        NotNull("email"),
        Unique("email_hash"),
        InRange("amount_cents", min=0),
    ],
)
```

Attach the `QualityConfig` to your entity:

```python
entity = Entity(
    name="customers",
    source=TableSource(...),
    layers=LayersConfig(...),
    quality=quality,
)
```

## Built-in Checks

| Check | Description |
|-------|-------------|
| `NotNull(column)` | Column must not contain null values |
| `NotEmpty(column)` | Column must not contain empty strings |
| `Unique(column)` | Column values must be unique |
| `InRange(column, min, max)` | Column values must fall within a numeric range |
| `InSet(column, values)` | Column values must be one of the allowed values |
| `MatchesPattern(column, pattern)` | Column values must match a regex pattern |
| `ForeignKey(column, ref_entity, ref_column)` | Column values must exist in a referenced entity |
| `MaxAge(column, max_hours)` | Timestamps must be within a maximum age |
| `CustomSQL(expression)` | Arbitrary SQL/Ibis expression that must evaluate to true |

## Running Quality Checks

Quality checks run automatically as part of `fyr run`. You can also run them programmatically:

```python
from fyrnheim import QualityRunner

runner = QualityRunner()
results = runner.run(entity, table)
```

The runner returns `EntityResult` containing a list of `CheckResult` objects, each indicating whether the check passed or failed along with details.
