"""MetricsModel and MetricField types for aggregating metric deltas and event counts."""

from typing import Literal

from pydantic import BaseModel, Field, model_validator


class MetricField(BaseModel):
    """A single metric field with its aggregation strategy.

    For state-based aggregations (sum_delta, last_value, max_value):
        field_name is the name of the field that changed in the payload.

    For event-based aggregations (count, count_distinct):
        field_name is the event_type to match (e.g. "blog_post_opened").
        For count_distinct, distinct_field names the payload field to count
        distinct values of.
    """

    field_name: str = Field(min_length=1)
    aggregation: Literal["sum_delta", "last_value", "max_value", "count", "count_distinct"]
    distinct_field: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _validate_distinct_field(self) -> "MetricField":
        if self.aggregation == "count_distinct" and not self.distinct_field:
            raise ValueError("distinct_field is required when aggregation is 'count_distinct'")
        if self.aggregation != "count_distinct" and self.distinct_field is not None:
            raise ValueError("distinct_field is only valid when aggregation is 'count_distinct'")
        return self


class MetricsModel(BaseModel):
    """Model for aggregating numeric field changes into time-grain metric tables."""

    name: str = Field(min_length=1)
    sources: list[str] = Field(min_length=1)
    grain: Literal["hourly", "daily", "weekly", "monthly"]
    metric_fields: list[MetricField] = Field(min_length=1)
    dimensions: list[str] = Field(default_factory=list)
