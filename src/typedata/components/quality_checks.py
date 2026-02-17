"""Data quality check component."""

from pydantic import BaseModel, Field as PydanticField

from .computed_column import ComputedColumn


class DataQualityChecks(BaseModel):
    """Standard data quality flag columns."""

    checks: dict[str, str] = PydanticField(default_factory=dict)
    prefix: str = "has_"

    def to_computed_columns(self) -> list[ComputedColumn]:
        """Generate quality check flag columns.

        Returns:
            List of ComputedColumn objects for quality flags

        Example:
            >>> checks = DataQualityChecks(checks={
            ...     "missing_email": "email IS NULL",
            ...     "future_date": "created_at > CURRENT_TIMESTAMP()",
            ... })
            >>> columns = checks.to_computed_columns()
            >>> # Generates: has_missing_email, has_future_date
        """
        return [
            ComputedColumn(
                name=f"{self.prefix}{check_name}",
                description=f"Quality flag: {check_name}",
                expression=sql_condition,
            )
            for check_name, sql_condition in self.checks.items()
        ]
