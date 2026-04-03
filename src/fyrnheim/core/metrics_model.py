"""MetricsModel and MetricField types for aggregating metric deltas."""

from typing import Literal

from pydantic import BaseModel, Field


class MetricField(BaseModel):
    """A single metric field with its aggregation strategy."""

    field_name: str = Field(min_length=1)
    aggregation: Literal["sum_delta", "last_value", "max_value"]


class MetricsModel(BaseModel):
    """Model for aggregating numeric field changes into time-grain metric tables."""

    name: str = Field(min_length=1)
    source: str = Field(min_length=1)
    grain: Literal["hourly", "daily", "weekly", "monthly"]
    metric_fields: list[MetricField] = Field(min_length=1)
    dimensions: list[str] = Field(default_factory=list)
