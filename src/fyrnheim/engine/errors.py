"""Exception hierarchy for the fyrnheim execution engine."""


class FyrnheimEngineError(Exception):
    """Base exception for engine errors."""


class SourceNotFoundError(FyrnheimEngineError):
    """Raised when a source table or parquet file cannot be found."""


class TransformModuleError(FyrnheimEngineError):
    """Raised when a generated transform module cannot be loaded."""


class ExecutionError(FyrnheimEngineError):
    """Raised when transform execution or result persistence fails."""
