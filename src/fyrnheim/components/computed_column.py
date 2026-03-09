"""Base computed column configuration."""

from pydantic import BaseModel, Field as PydanticField, field_validator, model_validator


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


class CaseColumn(ComputedColumn):
    """Computed column that generates ibis.cases() expressions from structured when/then pairs.

    Instead of writing raw expression strings, users provide structured cases
    and an optional default value. The expression is auto-generated.

    Example:
        CaseColumn(
            name='tier',
            cases=[('t.score >= 90', 'high'), ('t.score >= 50', 'medium')],
            default='low',
        )
        # Generates: ibis.cases((t.score >= 90, 'high'), (t.score >= 50, 'medium')).else_('low')
    """

    cases: list[tuple[str, str]]
    default: str | None = None
    # expression is auto-generated; provide a placeholder so pydantic doesn't reject it
    expression: str = PydanticField(default="__placeholder__")

    @model_validator(mode="after")
    def build_expression(self) -> "CaseColumn":
        """Build the ibis.cases() expression from structured cases and default."""
        case_parts = ", ".join(
            f"({condition}, {_quote_value(value)})" for condition, value in self.cases
        )
        expr = f"ibis.cases({case_parts})"
        if self.default is not None:
            expr += f".else_({_quote_value(self.default)})"
        self.expression = expr
        return self

    def __repr__(self) -> str:
        """String representation for debugging."""
        desc = f" ({self.description})" if self.description else ""
        return f"CaseColumn(name='{self.name}', cases={len(self.cases)}{desc})"


def _quote_value(value: str) -> str:
    """Quote a value as a string literal for ibis expression generation.

    If the value already looks like a code expression (contains dots, parens,
    or starts with a known prefix), return it as-is. Otherwise wrap in quotes.
    """
    # If it looks like a code expression, don't quote it
    if any(c in value for c in (".", "(", ")", "+", "-", "*", "/")):
        return value
    return f"'{value}'"
