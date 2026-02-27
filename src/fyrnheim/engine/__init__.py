"""fyrnheim.engine -- Entity discovery, dependency resolution, and execution."""

from fyrnheim.engine.connection import (
    SUPPORTED_BACKENDS as SUPPORTED_BACKENDS,
    create_connection as create_connection,
)
from fyrnheim.engine.errors import (
    ExecutionError as ExecutionError,
    FyrnheimEngineError as FyrnheimEngineError,
    SourceNotFoundError as SourceNotFoundError,
    TransformModuleError as TransformModuleError,
)
from fyrnheim.engine.executor import (
    ExecutionResult as ExecutionResult,
    IbisExecutor as IbisExecutor,
)
from fyrnheim.engine.registry import EntityInfo as EntityInfo, EntityRegistry as EntityRegistry
from fyrnheim.engine.resolution import (
    CircularDependencyError as CircularDependencyError,
    resolve_execution_order as resolve_execution_order,
)
from fyrnheim.engine.runner import (
    EntityRunResult as EntityRunResult,
    RunResult as RunResult,
    run as run,
    run_entity as run_entity,
)

__all__ = [
    "CircularDependencyError",
    "IbisExecutor",
    "create_connection",
    "EntityInfo",
    "EntityRegistry",
    "EntityRunResult",
    "ExecutionError",
    "ExecutionResult",
    "RunResult",
    "SourceNotFoundError",
    "TransformModuleError",
    "FyrnheimEngineError",
    "resolve_execution_order",
    "run",
    "run_entity",
]
