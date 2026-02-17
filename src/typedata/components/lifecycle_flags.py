"""Lifecycle flags component - generates is_active, is_churned, is_at_risk."""

from pydantic import BaseModel

from ..primitives import lifecycle_flag
from .computed_column import ComputedColumn


class LifecycleFlags(BaseModel):
    """Standard lifecycle flags for any dimension with a status column."""

    status_column: str
    active_states: list[str]
    churned_states: list[str]
    at_risk_states: list[str] | None = None

    def to_computed_columns(self) -> list[ComputedColumn]:
        """Generate is_active, is_churned, is_at_risk columns.

        Returns:
            List of ComputedColumn objects
        """
        columns = [
            ComputedColumn(
                name="is_active",
                description=f"True if {self.status_column} indicates active state",
                expression=lifecycle_flag(self.status_column, self.active_states),
            ),
            ComputedColumn(
                name="is_churned",
                description=f"True if {self.status_column} indicates churned state",
                expression=lifecycle_flag(self.status_column, self.churned_states),
            ),
        ]

        if self.at_risk_states:
            columns.append(
                ComputedColumn(
                    name="is_at_risk",
                    description=f"True if {self.status_column} indicates at-risk state",
                    expression=lifecycle_flag(self.status_column, self.at_risk_states),
                )
            )

        return columns

    def __repr__(self) -> str:
        """String representation."""
        return f"LifecycleFlags(status_column='{self.status_column}', active={len(self.active_states)}, churned={len(self.churned_states)})"
