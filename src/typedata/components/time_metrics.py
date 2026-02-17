"""Time-based metrics component."""

from pydantic import BaseModel, Field as PydanticField

from ..primitives import date_diff_days, date_trunc_month, extract_year
from .computed_column import ComputedColumn


class TimeBasedMetrics(BaseModel):
    """Standard time-based computed columns."""

    created_at_col: str = PydanticField(min_length=1)
    updated_at_col: str | None = None

    def to_computed_columns(self) -> list[ComputedColumn]:
        """Generate time-based columns.

        Returns:
            List of ComputedColumn objects
        """
        cols = [
            ComputedColumn(
                name="days_since_created",
                description="Days since creation",
                expression=date_diff_days(self.created_at_col),
            ),
            ComputedColumn(
                name="created_month",
                description="Month of creation for cohort analysis",
                expression=date_trunc_month(self.created_at_col),
            ),
            ComputedColumn(
                name="created_year",
                description="Year of creation",
                expression=extract_year(self.created_at_col),
            ),
        ]

        if self.updated_at_col:
            cols.extend(
                [
                    ComputedColumn(
                        name="days_since_updated",
                        description="Days since last update",
                        expression=date_diff_days(self.updated_at_col),
                    ),
                    ComputedColumn(
                        name="days_between_created_and_updated",
                        description="Days from creation to last update",
                        expression=date_diff_days(self.created_at_col, self.updated_at_col),
                    ),
                ]
            )

        return cols
