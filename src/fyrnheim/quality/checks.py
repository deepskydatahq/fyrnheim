"""Quality check definitions."""

from abc import ABC, abstractmethod
from collections.abc import Sequence

from pydantic import BaseModel, Field as PydanticField


class QualityCheck(ABC):
    """Base class for all quality checks."""

    @abstractmethod
    def get_where_clause(self) -> str:
        """Return SQL WHERE clause that matches failing rows."""
        pass

    @property
    @abstractmethod
    def display_name(self) -> str:
        """Human-readable name for display."""
        pass

    @property
    def columns_to_show(self) -> list[str]:
        """Columns to include in failure output. Override in subclasses."""
        return []

    @property
    def requires_special_query(self) -> bool:
        """Whether this check requires a special query instead of a simple WHERE clause."""
        return False


class NotNull(QualityCheck, BaseModel):
    """Check that columns are not null."""

    columns: tuple[str, ...]

    def __init__(self, *columns: str):
        if not columns:
            raise ValueError("At least one column must be specified")
        super().__init__(columns=columns)

    def get_where_clause(self) -> str:
        return " OR ".join(f"{col} IS NULL" for col in self.columns)

    @property
    def display_name(self) -> str:
        return f"NotNull: {', '.join(self.columns)}"

    @property
    def columns_to_show(self) -> list[str]:
        return list(self.columns)


class NotEmpty(QualityCheck, BaseModel):
    """Check that string columns are not null or empty."""

    columns: tuple[str, ...]

    def __init__(self, *columns: str):
        if not columns:
            raise ValueError("At least one column must be specified")
        super().__init__(columns=columns)

    def get_where_clause(self) -> str:
        conditions = [f"({col} IS NULL OR TRIM({col}) = '')" for col in self.columns]
        return " OR ".join(conditions)

    @property
    def display_name(self) -> str:
        return f"NotEmpty: {', '.join(self.columns)}"

    @property
    def columns_to_show(self) -> list[str]:
        return list(self.columns)


class InRange(QualityCheck, BaseModel):
    """Check that values are within a range."""

    column: str
    min: int | float | str | None = None
    max: int | float | str | None = None

    def __init__(
        self,
        column: str,
        min: int | float | str | None = None,
        max: int | float | str | None = None,
    ):
        if min is None and max is None:
            raise ValueError("At least one of min or max must be specified")
        super().__init__(column=column, min=min, max=max)

    def get_where_clause(self) -> str:
        conditions = []
        if self.min is not None:
            conditions.append(f"{self.column} >= {self._format_value(self.min)}")
        if self.max is not None:
            conditions.append(f"{self.column} <= {self._format_value(self.max)}")
        return f"NOT ({' AND '.join(conditions)})"

    def _format_value(self, value: int | float | str) -> str:
        if isinstance(value, str):
            return value  # SQL expression like CURRENT_DATE()
        return str(value)

    @property
    def display_name(self) -> str:
        parts = []
        if self.min is not None:
            parts.append(f"{self.column} >= {self.min}")
        if self.max is not None:
            parts.append(f"{self.column} <= {self.max}")
        return f"InRange: {' AND '.join(parts)}"

    @property
    def columns_to_show(self) -> list[str]:
        return [self.column]


class InSet(QualityCheck, BaseModel):
    """Check that values are in an allowed set."""

    column: str
    values: Sequence[str]

    def __init__(self, column: str, values: Sequence[str]):
        super().__init__(column=column, values=values)

    def get_where_clause(self) -> str:
        escaped = [v.replace("'", "\\'") for v in self.values]
        quoted = ", ".join(f"'{v}'" for v in escaped)
        return f"{self.column} NOT IN ({quoted})"

    @property
    def display_name(self) -> str:
        return f"InSet: {self.column} in [{', '.join(self.values[:3])}{'...' if len(self.values) > 3 else ''}]"

    @property
    def columns_to_show(self) -> list[str]:
        return [self.column]


class MatchesPattern(QualityCheck, BaseModel):
    """Check that values match a regex pattern.

    Uses Ibis re_search() for portable regex matching across backends.
    """

    column: str
    pattern: str

    def __init__(self, column: str, pattern: str):
        super().__init__(column=column, pattern=pattern)

    def get_where_clause(self) -> str:
        raise NotImplementedError(
            "MatchesPattern uses Ibis expressions for portable regex. "
            "Use requires_special_query path."
        )

    @property
    def requires_special_query(self) -> bool:
        return True

    @property
    def display_name(self) -> str:
        return f"MatchesPattern: {self.column}"

    @property
    def columns_to_show(self) -> list[str]:
        return [self.column]


class ForeignKey(QualityCheck, BaseModel):
    """Check that foreign key values exist in referenced table."""

    column: str
    references: str  # Format: "entity.column"
    ref_table: str | None = None

    def __init__(self, column: str, references: str, ref_table: str | None = None):
        if "." not in references:
            raise ValueError("references must be in format 'entity.column'")
        super().__init__(column=column, references=references, ref_table=ref_table)

    def get_where_clause(self) -> str:
        return f"{self.column} IS NOT NULL AND {self.column} NOT IN (SELECT {self._ref_column} FROM {self._resolved_ref_table})"

    @property
    def _resolved_ref_table(self) -> str:
        return self.ref_table or self.references.split(".")[0]

    @property
    def _ref_column(self) -> str:
        return self.references.split(".")[1]

    @property
    def display_name(self) -> str:
        return f"ForeignKey: {self.column} -> {self.references.split('.')[0]}"

    @property
    def columns_to_show(self) -> list[str]:
        return [self.column]


class Unique(QualityCheck, BaseModel):
    """Check that column(s) are unique."""

    columns: tuple[str, ...]

    def __init__(self, *columns: str):
        if not columns:
            raise ValueError("At least one column must be specified")
        super().__init__(columns=columns)

    def get_where_clause(self) -> str:
        raise NotImplementedError(
            "Unique check requires a GROUP BY query and cannot be expressed as a simple WHERE clause."
        )

    @property
    def requires_special_query(self) -> bool:
        return True

    @property
    def display_name(self) -> str:
        return f"Unique: {', '.join(self.columns)}"

    @property
    def columns_to_show(self) -> list[str]:
        return list(self.columns)


class MaxAge(QualityCheck, BaseModel):
    """Check that most recent value is within N days."""

    column: str
    days: int

    def __init__(self, column: str, days: int):
        super().__init__(column=column, days=days)

    def get_where_clause(self) -> str:
        raise NotImplementedError(
            "MaxAge check requires a MAX() aggregation and cannot be expressed as a simple WHERE clause."
        )

    @property
    def requires_special_query(self) -> bool:
        return True

    @property
    def display_name(self) -> str:
        return f"MaxAge: {self.column} within {self.days} days"

    @property
    def columns_to_show(self) -> list[str]:
        return [self.column]


class CustomSQL(QualityCheck, BaseModel):
    """Custom SQL check with user-defined predicate.

    Note: The SQL predicate is passed through as-is and may contain
    backend-specific SQL syntax. This check is inherently non-portable
    across different database backends. Users are responsible for ensuring
    the SQL is compatible with their target backend.
    """

    name: str
    sql: str
    description: str = ""

    def get_where_clause(self) -> str:
        return f"NOT ({self.sql})"

    @property
    def display_name(self) -> str:
        return f"Custom: {self.name}"


class QualityConfig(BaseModel):
    """Configuration for entity quality checks."""

    model_config = {"arbitrary_types_allowed": True}

    checks: list[QualityCheck] = PydanticField(default_factory=list)
    primary_key: str = "id"  # Primary key column for sample failure output
