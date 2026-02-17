"""Core type definitions and enums."""

from enum import Enum, StrEnum


class MaterializationType(StrEnum):
    """Materialization strategies for transformed entities."""
    TABLE = "table"
    VIEW = "view"
    INCREMENTAL = "incremental"
    EPHEMERAL = "ephemeral"


class IncrementalStrategy(StrEnum):
    """Strategies for incremental materialization."""
    MERGE = "merge"
    APPEND = "append"
    DELETE_INSERT = "delete+insert"


class SourcePriority(int, Enum):
    """Priority levels for identity graph field resolution."""
    PRIMARY = 1
    SECONDARY = 2
    TERTIARY = 3
    QUATERNARY = 4
