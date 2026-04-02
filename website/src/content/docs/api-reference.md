---
title: API Reference
description: Public types and functions exported by the fyrnheim package.
---

All types below are importable from `fyrnheim.core`:

```python
from fyrnheim.core import StateSource, ActivityDefinition, EntityModel, ...
```

## Sources

| Type | Description |
|------|-------------|
| `StateSource` | Declare a state-shaped data source (CRM exports, dimension tables) |
| `EventSource` | Declare an event-shaped data source (page views, transactions) |
| `Field` | Declare a field with name and type on a source |
| `SourceTransforms` | Container for source-level transforms (renames, casts) |
| `Rename` | Rename a column in the source |
| `TypeCast` | Cast a column to a different type |
| `Divide` | Divide a column by a value |
| `Multiply` | Multiply a column by a value |

## Activities

| Type | Description |
|------|-------------|
| `ActivityDefinition` | Interpret raw changes into named business events |
| `RowAppeared` | Trigger: fire when a new row appears in a state source |
| `FieldChanged` | Trigger: fire when a field value changes (with optional from/to filters) |
| `RowDisappeared` | Trigger: fire when a row disappears from a state source |
| `EventOccurred` | Trigger: fire when an event source row matches a type |

## Identity

| Type | Description |
|------|-------------|
| `IdentityGraph` | Map raw source IDs to canonical entity IDs |
| `IdentitySource` | A single source in an identity graph with match key mapping |

## Entity Models

| Type | Description |
|------|-------------|
| `EntityModel` | Project current entity state from the enriched activity stream |
| `StateField` | Declare a field with source, resolution strategy, and priority |

## Analytics

| Type | Description |
|------|-------------|
| `StreamAnalyticsModel` | Time-grain metric aggregation over the activity stream |
| `StreamMetric` | A single metric with expression, filter, and type |

## Components

| Type | Description |
|------|-------------|
| `ComputedColumn` | A computed column with name and expression |

## Primitives

Reusable Python functions for hashing, dates, categorization, and more. See [Primitives](/concepts/primitives/) for the full list.

```python
from fyrnheim.primitives import hash_email, date_trunc_month, categorize
```

## Engine

| Type | Description |
|------|-------------|
| `generate` | Generate transformation code from definitions |
| `run` | Run all pipelines |
| `run_entity` | Run a single entity pipeline |
| `create_connection` | Create an Ibis backend connection |
| `EntityRegistry` | Registry for discovering and managing entities |
