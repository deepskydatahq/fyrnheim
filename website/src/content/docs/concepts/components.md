---
title: Components
description: Multi-column patterns that generate related fields from a single config.
---

Components are multi-column patterns that generate related fields from a single configuration. Instead of writing multiple computed columns by hand, a component produces a set of related fields automatically.

## LifecycleFlags

Produces `is_active`, `is_churned`, `is_at_risk` (and more) from a status column.

```python
from fyrnheim import LifecycleFlags

flags = LifecycleFlags(
    status_column="status",
    active_states=["active"],
    churned_states=["cancelled"],
)
```

Use lifecycle flags in a DimensionLayer to add customer lifecycle columns without writing each one manually.

## TimeBasedMetrics

Computes tenure, recency, and other time-based metrics from date columns.

```python
from fyrnheim import TimeBasedMetrics

metrics = TimeBasedMetrics(
    created_at_column="created_at",
    last_active_column="last_login_at",
)
```

## ComputedColumn

The building block for all transformations. A computed column has a name and an expression (either a string or a primitive function call).

```python
from fyrnheim import ComputedColumn
from fyrnheim.primitives import hash_email

ComputedColumn(name="email_hash", expression=hash_email("email"))
ComputedColumn(name="amount_dollars", expression="t.amount_cents / 100.0")
```

## DataQualityChecks

A component that bundles common quality check patterns for reuse across entities.

## Measure

Define reusable metric definitions that can be referenced across analytics layers.
