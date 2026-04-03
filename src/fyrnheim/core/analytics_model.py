"""Stream analytics model -- top-level asset for time-grain metrics."""

from typing import Literal

from pydantic import BaseModel, Field as PydanticField, field_validator


class StreamMetric(BaseModel):
    """A single metric in a stream analytics model.

    Attributes:
        name: Metric column name in output table
        expression: Ibis aggregation expression string
        metric_type: One of 'count', 'sum', or 'snapshot'
        event_filter: Optional filter expression to restrict events
    """

    name: str = PydanticField(min_length=1)
    expression: str = PydanticField(min_length=1)
    metric_type: Literal["count", "sum", "snapshot"]
    event_filter: str | None = None


class StreamAnalyticsModel(BaseModel):
    """Top-level analytics model that aggregates an enriched activity stream.

    Produces time-grain metrics grouped by date and optional dimensions.

    Attributes:
        name: Model name (e.g., 'daily_metrics')
        identity_graph: Name of the identity graph to use
        date_grain: Time grain for aggregation
        metrics: At least one StreamMetric
        dimensions: Optional grouping dimensions beyond date
    """

    name: str = PydanticField(min_length=1)
    identity_graph: str | None = None
    date_grain: Literal["daily", "weekly", "monthly"]
    metrics: list[StreamMetric] = PydanticField(min_length=1)
    dimensions: list[str] = PydanticField(default_factory=list)

    @field_validator("metrics")
    @classmethod
    def validate_metrics_not_empty(cls, v: list[StreamMetric]) -> list[StreamMetric]:
        if not v:
            raise ValueError("At least one metric is required")
        return v
