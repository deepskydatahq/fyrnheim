"""Exception hierarchy for the typedata execution engine."""


class TypedataEngineError(Exception):
    """Base exception for engine errors."""


class SourceNotFoundError(TypedataEngineError):
    """Raised when a source table or parquet file cannot be found."""


class TransformModuleError(TypedataEngineError):
    """Raised when a generated transform module cannot be loaded."""


class ExecutionError(TypedataEngineError):
    """Raised when transform execution or result persistence fails."""
