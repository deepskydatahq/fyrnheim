---
title: API Reference
description: Complete listing of all public exports from the fyrnheim package.
---

All symbols below are importable from the top-level `fyrnheim` package:

```python
from fyrnheim import Entity, TableSource, PrepLayer, ...
```

## Core

| Export | Description |
|--------|-------------|
| `AnalyticsEntity` | Pydantic model combining state fields, measures, and computed fields into a single entity |
| `Measure` | Activity-derived measure definition (count, sum, latest) |
| `StateField` | Defines how a field is projected from a source (latest, first, coalesce) |
| `Source` | Base type for all source types |
| `Field` | Declares a required field with name and type on an entity |
| `MaterializationType` | Enum for table materialization strategy (table, view, incremental) |
| `IncrementalStrategy` | Enum for incremental load strategy |
| `SourcePriority` | Priority level for field resolution in identity graphs |

## Sources

| Export | Description |
|--------|-------------|
| `BaseTableSource` | Base class for table-backed sources |
| `TableSource` | Read from a single warehouse table or local parquet file |
| `DerivedSource` | Build identity graphs by joining multiple entities on a shared key |
| `DerivedEntitySource` | Reference another entity as a source for derived joins |
| `AggregationSource` | Aggregate from another entity with GROUP BY |
| `EventAggregationSource` | Aggregate raw event streams from a table |
| `IdentityGraphConfig` | Configuration for identity graph resolution in DerivedSource |
| `IdentityGraphSource` | A single source in an identity graph with field mappings |
| `UnionSource` | Combine multiple sources into a common schema |
| `SourceTransforms` | Container for source-level transforms (casts, renames, etc.) |
| `TypeCast` | Cast a column to a different type |
| `Rename` | Rename a column in the source |
| `Divide` | Divide a column by a value |
| `Multiply` | Multiply a column by a value |

## Layers

| Export | Description |
|--------|-------------|
| `PrepLayer` | Clean raw data: type casts, renames, computed columns |
| `DimensionLayer` | Add business logic columns |
| `SnapshotLayer` | Track changes over time (daily snapshots, SCD) |
| `ActivityConfig` | Detect events from state changes |
| `ActivityType` | Define an activity type with trigger and timestamp |
| `AnalyticsLayer` | Date-grain metric aggregation |
| `AnalyticsMetric` | A single metric in an analytics layer |
| `MetricsModel` | Configuration for snapshot-based metrics (e.g., daily deltas) |
| `MetricField` | A single field in a MetricsModel with aggregation type |

## Source Mapping

| Export | Description |
|--------|-------------|
| `SourceMapping` | Map entity field names to source column names |

## Components

| Export | Description |
|--------|-------------|
| `ComputedColumn` | A computed column with name and expression |
| `LifecycleFlags` | Multi-column component producing is_active, is_churned, etc. |
| `TimeBasedMetrics` | Multi-column component for tenure and recency |
| `DataQualityChecks` | Bundled quality check patterns for reuse |

## Quality

| Export | Description |
|--------|-------------|
| `QualityConfig` | Container for quality checks on an entity |
| `QualityCheck` | Base class for all quality checks |
| `NotNull` | Check that a column has no null values |
| `NotEmpty` | Check that a column has no empty strings |
| `InRange` | Check that values fall within a numeric range |
| `InSet` | Check that values are in an allowed set |
| `MatchesPattern` | Check that values match a regex pattern |
| `ForeignKey` | Check referential integrity against another entity |
| `Unique` | Check that column values are unique |
| `MaxAge` | Check that timestamps are within a maximum age |
| `CustomSQL` | Arbitrary expression that must evaluate to true |
| `QualityRunner` | Programmatic runner for quality checks |
| `CheckResult` | Result of a single quality check |
| `EntityResult` | Result of all quality checks for an entity |

## Primitives: Hashing

| Export | Description |
|--------|-------------|
| `hash_email` | SHA-256 hash of a lowered, trimmed email |
| `hash_id` | SHA-256 hash of a column value |
| `hash_md5` | MD5 hash of a column value |
| `hash_sha256` | SHA-256 hash of a column value |
| `concat_hash` | Concatenate columns and hash the result |

## Primitives: Dates

| Export | Description |
|--------|-------------|
| `date_diff_days` | Number of days between two date columns |
| `date_trunc_month` | Truncate a date to the first of the month |
| `date_trunc_quarter` | Truncate a date to the first of the quarter |
| `date_trunc_year` | Truncate a date to the first of the year |
| `days_since` | Number of days between a date column and today |
| `extract_year` | Extract year from a date |
| `extract_month` | Extract month from a date |
| `extract_day` | Extract day from a date |
| `earliest_date` | The earliest (minimum) date across columns |
| `latest_date` | The latest (maximum) date across columns |

## Primitives: Categorization

| Export | Description |
|--------|-------------|
| `categorize` | Map column values to categories via a dictionary |
| `categorize_contains` | Categorize by substring matching |
| `lifecycle_flag` | Produce a boolean flag based on column value |
| `boolean_to_int` | Convert a boolean column to 0/1 |

## Primitives: JSON

| Export | Description |
|--------|-------------|
| `to_json_struct` | Parse a JSON string column into a struct |
| `json_extract_scalar` | Extract a scalar value from JSON |
| `json_value` | Extract a value from JSON |

## Primitives: Aggregations

| Export | Description |
|--------|-------------|
| `sum_` | Sum of a column |
| `count_` | Count of a column |
| `count_distinct` | Count of distinct values |
| `avg_` | Average of a column |
| `min_` | Minimum value |
| `max_` | Maximum value |
| `row_number_by` | Row number within a partition |
| `cumulative_sum` | Cumulative sum |
| `lag_value` | Previous row's value |
| `lead_value` | Next row's value |
| `first_value` | First value in a window |
| `last_value` | Last value in a window |
| `any_value` | Any non-null value |

## Primitives: Strings

| Export | Description |
|--------|-------------|
| `extract_email_domain` | Extract the domain from an email address |
| `is_personal_email_domain` | Check if an email domain is a personal provider |
| `account_id_from_domain` | Derive an account ID from an email domain |

## Primitives: Time

| Export | Description |
|--------|-------------|
| `parse_iso8601_duration` | Parse an ISO 8601 duration string |

## Engine

| Export | Description |
|--------|-------------|
| `generate` | Generate transformation code from entity definitions |
| `GenerateResult` | Result of code generation |
| `IbisCodeGenerator` | Low-level code generator for Ibis transforms |
| `run` | Run all entity pipelines |
| `run_entity` | Run a single entity pipeline |
| `RunResult` | Result of running all entity pipelines |
| `EntityRunResult` | Result of running a single entity pipeline |
| `IbisExecutor` | Low-level executor for running Ibis transforms |
| `ExecutionResult` | Result of a single execution step |
| `create_connection` | Create an Ibis backend connection |
| `EntityRegistry` | Registry for discovering and managing entities |
| `EntityInfo` | Metadata about a registered entity |

## Engine: Errors

| Export | Description |
|--------|-------------|
| `ExecutionError` | Error during pipeline execution |
| `SourceNotFoundError` | Source table or file not found |
| `TransformModuleError` | Error loading a generated transform module |
| `FyrnheimEngineError` | Base error class for engine errors |
| `CircularDependencyError` | Circular dependency detected in entity graph |
