"""typedata.engine -- Entity discovery, dependency resolution, and execution."""

from typedata.engine.errors import (
    ExecutionError as ExecutionError,
    SourceNotFoundError as SourceNotFoundError,
    TransformModuleError as TransformModuleError,
    TypedataEngineError as TypedataEngineError,
)
from typedata.engine.executor import (
    DuckDBExecutor as DuckDBExecutor,
    ExecutionResult as ExecutionResult,
)
from typedata.engine.registry import EntityInfo as EntityInfo, EntityRegistry as EntityRegistry
from typedata.engine.resolution import (
    CircularDependencyError as CircularDependencyError,
    resolve_execution_order as resolve_execution_order,
)
from typedata.engine.runner import (
    EntityRunResult as EntityRunResult,
    RunResult as RunResult,
    run as run,
    run_entity as run_entity,
)

__all__ = [
    "CircularDependencyError",
    "DuckDBExecutor",
    "EntityInfo",
    "EntityRegistry",
    "EntityRunResult",
    "ExecutionError",
    "ExecutionResult",
    "RunResult",
    "SourceNotFoundError",
    "TransformModuleError",
    "TypedataEngineError",
    "resolve_execution_order",
    "run",
    "run_entity",
]
