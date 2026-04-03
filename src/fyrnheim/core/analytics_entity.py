"""AnalyticsEntity and Measure types for combined state + activity-derived measures."""

from typing import Literal

from pydantic import BaseModel, Field, model_validator

from fyrnheim.components.computed_column import ComputedColumn
from fyrnheim.quality.checks import QualityCheck


class Measure(BaseModel):
    """Defines an activity-derived measure (count, sum, latest)."""

    name: str = Field(min_length=1)
    activity: str = Field(min_length=1)  # activity name to match event_type
    aggregation: Literal["count", "sum", "latest"]
    field: str | None = None  # payload field for sum/latest

    @model_validator(mode="after")
    def _validate_field_for_sum_latest(self) -> "Measure":
        if self.aggregation in ("sum", "latest") and not self.field:
            raise ValueError(f"'{self.aggregation}' aggregation requires a 'field'")
        return self


class StateField(BaseModel):
    """Defines how a single field is projected from the activity stream.

    Same definition as entity_model.StateField -- kept here for AnalyticsEntity usage.
    """

    name: str = Field(min_length=1)
    source: str = Field(min_length=1)
    field: str = Field(min_length=1)
    strategy: Literal["latest", "first", "coalesce"]
    priority: list[str] | None = None

    @model_validator(mode="after")
    def _validate_coalesce_priority(self) -> "StateField":
        if self.strategy == "coalesce" and not self.priority:
            raise ValueError("coalesce strategy requires a priority list")
        return self


class AnalyticsEntity(BaseModel):
    """Combines state field projection with activity-derived measures in one asset.

    One row per entity with state fields + measures + computed fields.
    At least one of state_fields or measures must be non-empty.
    """

    model_config = {"arbitrary_types_allowed": True}

    name: str = Field(min_length=1)
    identity_graph: str | None = None
    state_fields: list[StateField] = Field(default_factory=list)
    measures: list[Measure] = Field(default_factory=list)
    computed_fields: list[ComputedColumn] = Field(default_factory=list)
    quality_checks: list[QualityCheck] = Field(default_factory=list)

    @model_validator(mode="after")
    def _require_at_least_one_field_or_measure(self) -> "AnalyticsEntity":
        if not self.state_fields and not self.measures:
            raise ValueError(
                "AnalyticsEntity requires at least one state_field or measure"
            )
        return self
