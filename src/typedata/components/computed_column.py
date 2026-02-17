"""Base computed column configuration."""

from pydantic import BaseModel, Field as PydanticField, field_validator


class ComputedColumn(BaseModel):
    """Defines a computed column in a transformation layer."""

    name: str = PydanticField(min_length=1)
    expression: str = PydanticField(min_length=1)
    description: str | None = None

    @field_validator("expression")
    @classmethod
    def strip_expression(cls, v: str) -> str:
        return v.strip()

    def to_sql(self, indent: int = 4) -> str:
        """Generate SQL with proper indentation.

        Args:
            indent: Number of spaces for indentation

        Returns:
            Formatted SQL string
        """
        spaces = " " * indent
        return f"{spaces}{self.expression} AS {self.name}"

    def __repr__(self) -> str:
        """String representation for debugging."""
        desc = f" ({self.description})" if self.description else ""
        return f"ComputedColumn(name='{self.name}'{desc})"
