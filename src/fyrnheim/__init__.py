"""fyrnheim -- Define typed Python entities, generate transformations, run anywhere."""

from typing import Any

from fyrnheim.components import (
    ComputedColumn as ComputedColumn,
    DataQualityChecks as DataQualityChecks,
    LifecycleFlags as LifecycleFlags,
    Measure as Measure,
    TimeBasedMetrics as TimeBasedMetrics,
)
from fyrnheim.core import (
    ActivityConfig as ActivityConfig,
    ActivityDefinition as ActivityDefinition,
    ActivityType as ActivityType,
    AggregationSource as AggregationSource,
    AnalyticsLayer as AnalyticsLayer,
    AnalyticsMetric as AnalyticsMetric,
    AnalyticsModel as AnalyticsModel,
    AnalyticsSource as AnalyticsSource,
    BaseTableSource as BaseTableSource,
    ComputedMetric as ComputedMetric,
    DerivedEntitySource as DerivedEntitySource,
    DerivedSource as DerivedSource,
    DimensionLayer as DimensionLayer,
    Divide as Divide,
    Entity as Entity,
    EntityModel as EntityModel,
    EventAggregationSource as EventAggregationSource,
    EventOccurred as EventOccurred,
    EventSource as EventSource,
    Field as Field,
    FieldChanged as FieldChanged,
    HelperEntity as HelperEntity,
    IdentityGraph as IdentityGraph,
    IdentityGraphConfig as IdentityGraphConfig,
    IdentityGraphSource as IdentityGraphSource,
    IdentitySource as IdentitySource,
    IncrementalStrategy as IncrementalStrategy,
    LayersConfig as LayersConfig,
    MaterializationType as MaterializationType,
    Multiply as Multiply,
    PrepLayer as PrepLayer,
    Rename as Rename,
    RowAppeared as RowAppeared,
    RowDisappeared as RowDisappeared,
    SnapshotLayer as SnapshotLayer,
    Source as Source,
    SourceMapping as SourceMapping,
    SourcePriority as SourcePriority,
    SourceTransforms as SourceTransforms,
    StateField as StateField,
    StateSource as StateSource,
    StreamAnalyticsModel as StreamAnalyticsModel,
    StreamMetric as StreamMetric,
    TableSource as TableSource,
    TypeCast as TypeCast,
    UnionSource as UnionSource,
)
from fyrnheim.primitives import (
    account_id_from_domain as account_id_from_domain,
    any_value as any_value,
    avg_ as avg_,
    boolean_to_int as boolean_to_int,
    categorize as categorize,
    categorize_contains as categorize_contains,
    concat_hash as concat_hash,
    count_ as count_,
    count_distinct as count_distinct,
    cumulative_sum as cumulative_sum,
    date_diff_days as date_diff_days,
    date_trunc_month as date_trunc_month,
    date_trunc_quarter as date_trunc_quarter,
    date_trunc_year as date_trunc_year,
    days_since as days_since,
    earliest_date as earliest_date,
    extract_day as extract_day,
    extract_email_domain as extract_email_domain,
    extract_month as extract_month,
    extract_year as extract_year,
    first_value as first_value,
    hash_email as hash_email,
    hash_id as hash_id,
    hash_md5 as hash_md5,
    hash_sha256 as hash_sha256,
    is_personal_email_domain as is_personal_email_domain,
    json_extract_scalar as json_extract_scalar,
    json_value as json_value,
    lag_value as lag_value,
    last_value as last_value,
    latest_date as latest_date,
    lead_value as lead_value,
    lifecycle_flag as lifecycle_flag,
    max_ as max_,
    min_ as min_,
    parse_iso8601_duration as parse_iso8601_duration,
    row_number_by as row_number_by,
    sum_ as sum_,
    to_json_struct as to_json_struct,
)
from fyrnheim.quality import (
    CheckResult as CheckResult,
    CustomSQL as CustomSQL,
    EntityResult as EntityResult,
    ForeignKey as ForeignKey,
    InRange as InRange,
    InSet as InSet,
    MatchesPattern as MatchesPattern,
    MaxAge as MaxAge,
    NotEmpty as NotEmpty,
    NotNull as NotNull,
    QualityCheck as QualityCheck,
    QualityConfig as QualityConfig,
    QualityRunner as QualityRunner,
    Unique as Unique,
)

# Resolve forward references (order matters: Entity first, then SourceMapping)
Entity.model_rebuild()
HelperEntity.model_rebuild()
SourceMapping.model_rebuild()

__version__ = "0.1.0"

# Lazy imports for generator/engine symbols that require ibis
_LAZY_IMPORTS = {
    "generate": "fyrnheim._generate",
    "GenerateResult": "fyrnheim._generate",
    "IbisCodeGenerator": "fyrnheim.generators",
    "run": "fyrnheim.engine.runner",
    "run_entity": "fyrnheim.engine.runner",
    "RunResult": "fyrnheim.engine.runner",
    "EntityRunResult": "fyrnheim.engine.runner",
    "IbisExecutor": "fyrnheim.engine.executor",
    "ExecutionResult": "fyrnheim.engine.executor",
    "create_connection": "fyrnheim.engine.connection",
    # Engine: Errors
    "ExecutionError": "fyrnheim.engine.errors",
    "SourceNotFoundError": "fyrnheim.engine.errors",
    "TransformModuleError": "fyrnheim.engine.errors",
    "FyrnheimEngineError": "fyrnheim.engine.errors",
    # Engine: Resolution
    "CircularDependencyError": "fyrnheim.engine.resolution",
    # Engine: Registry
    "EntityRegistry": "fyrnheim.engine.registry",
    "EntityInfo": "fyrnheim.engine.registry",
}


def __getattr__(name: str) -> Any:  # noqa: N807
    if name in _LAZY_IMPORTS:
        module_path = _LAZY_IMPORTS[name]
        import importlib

        mod = importlib.import_module(module_path)
        val = getattr(mod, name)
        globals()[name] = val
        return val
    raise AttributeError(f"module 'fyrnheim' has no attribute {name!r}")


__all__ = [
    # Core
    "Entity",
    "EntityModel",
    "HelperEntity",
    "LayersConfig",
    "Source",
    "Field",
    # Types/Enums
    "MaterializationType",
    "IncrementalStrategy",
    "SourcePriority",
    # Sources
    "BaseTableSource",
    "TableSource",
    "DerivedSource",
    "DerivedEntitySource",
    "AggregationSource",
    "EventAggregationSource",
    "EventSource",
    "IdentityGraph",
    "IdentityGraphConfig",
    "IdentityGraphSource",
    "IdentitySource",
    "UnionSource",
    "StateSource",
    "StateField",
    "SourceTransforms",
    "TypeCast",
    "Rename",
    "Divide",
    "Multiply",
    # Layers
    "PrepLayer",
    "DimensionLayer",
    "SnapshotLayer",
    "ActivityConfig",
    "ActivityDefinition",
    "ActivityType",
    "RowAppeared",
    "FieldChanged",
    "RowDisappeared",
    "EventOccurred",
    "AnalyticsLayer",
    "AnalyticsMetric",
    "AnalyticsModel",
    "AnalyticsSource",
    "ComputedMetric",
    # Stream Analytics (top-level)
    "StreamAnalyticsModel",
    "StreamMetric",
    # Source Mapping
    "SourceMapping",
    # Components
    "ComputedColumn",
    "Measure",
    "LifecycleFlags",
    "TimeBasedMetrics",
    "DataQualityChecks",
    # Quality
    "QualityConfig",
    "QualityCheck",
    "NotNull",
    "NotEmpty",
    "InRange",
    "InSet",
    "MatchesPattern",
    "ForeignKey",
    "Unique",
    "MaxAge",
    "CustomSQL",
    "QualityRunner",
    "CheckResult",
    "EntityResult",
    # Primitives: Hashing
    "concat_hash",
    "hash_email",
    "hash_id",
    "hash_md5",
    "hash_sha256",
    # Primitives: Dates
    "date_diff_days",
    "date_trunc_month",
    "date_trunc_quarter",
    "date_trunc_year",
    "days_since",
    "extract_year",
    "extract_month",
    "extract_day",
    "earliest_date",
    "latest_date",
    # Primitives: Categorization
    "categorize",
    "categorize_contains",
    "lifecycle_flag",
    "boolean_to_int",
    # Primitives: JSON
    "to_json_struct",
    "json_extract_scalar",
    "json_value",
    # Primitives: Aggregations
    "sum_",
    "count_",
    "count_distinct",
    "avg_",
    "min_",
    "max_",
    "row_number_by",
    "cumulative_sum",
    "lag_value",
    "lead_value",
    "first_value",
    "last_value",
    "any_value",
    # Primitives: Strings
    "extract_email_domain",
    "is_personal_email_domain",
    "account_id_from_domain",
    # Primitives: Time
    "parse_iso8601_duration",
    # Code generation (lazy imports)
    "generate",
    "GenerateResult",
    "IbisCodeGenerator",
    # Execution engine (lazy imports)
    "run",
    "run_entity",
    "RunResult",
    "EntityRunResult",
    "IbisExecutor",
    "ExecutionResult",
    "create_connection",
    # Engine: Errors (lazy imports)
    "ExecutionError",
    "SourceNotFoundError",
    "TransformModuleError",
    "FyrnheimEngineError",
    "CircularDependencyError",
    # Engine: Registry (lazy imports)
    "EntityRegistry",
    "EntityInfo",
]
