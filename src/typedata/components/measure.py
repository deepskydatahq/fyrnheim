"""Measure component for defining aggregation expressions."""

from pydantic import BaseModel, Field as PydanticField, field_validator


class Measure(BaseModel):
    """Defines an aggregation measure for an entity.

    Measures are aggregation expressions that compute metrics over groups of rows.
    They're declared at the entity level so tenants know what metrics are available.

    Examples:
        Measure(name="total_revenue", expression=sum_("amount_cents"))
        Measure(name="transaction_count", expression=count_())
        Measure(name="avg_order_value", expression=avg_("amount_cents"))
    """

    name: str = PydanticField(min_length=1)
    expression: str = PydanticField(min_length=1)
    description: str | None = None

    @field_validator("expression")
    @classmethod
    def strip_expression(cls, v: str) -> str:
        return v.strip()

    def __repr__(self) -> str:
        """String representation for debugging."""
        desc = f" ({self.description})" if self.description else ""
        return f"Measure(name='{self.name}'{desc})"
