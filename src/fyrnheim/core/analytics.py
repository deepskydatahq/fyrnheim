"""Analytics layer components for date-grain metric aggregation."""

from typing import Literal

from pydantic import BaseModel, Field as PydanticField, field_validator


class AnalyticsMetric(BaseModel):
    """A single metric definition for analytics aggregation.

    Attributes:
        name: Column name in output table
        expression: Ibis aggregation expression
        metric_type: "snapshot" (cumulative) or "event" (per-period)
        description: Optional metric description
    """

    name: str = PydanticField(min_length=1)
    expression: str = PydanticField(min_length=1)
    metric_type: Literal["snapshot", "event"]
    description: str | None = None


class AnalyticsLayer(BaseModel):
    """Layer that defines metrics an entity contributes at date grain.

    Attributes:
        model_name: Output model name (e.g., "analytics_product")
        date_expression: Ibis expression to extract date
        metrics: List of metrics to aggregate
        dimensions: Optional additional dimensions beyond date
    """

    model_name: str
    date_expression: str
    metrics: list[AnalyticsMetric]
    dimensions: list[str] = PydanticField(default_factory=list)

    @field_validator("metrics")
    @classmethod
    def validate_metrics(cls, v: list) -> list:
        if not v:
            raise ValueError("At least one metric required")
        return v


class AnalyticsSource(BaseModel):
    """Reference to an entity's analytics layer.

    Attributes:
        entity: Entity name (e.g., "product")
        layer: Layer name (always "analytics" for now)
    """

    entity: str
    layer: str = "analytics"


class ComputedMetric(BaseModel):
    """A metric computed from combined analytics data.

    Attributes:
        name: Column name in output
        expression: SQL/Ibis expression using combined columns
        description: Optional description
    """

    name: str
    expression: str
    description: str | None = None


class AnalyticsModel(BaseModel):
    """Combines multiple AnalyticsLayers into a final wide table.

    Attributes:
        name: Model name (e.g., "analytics_daily")
        description: Model description
        grain: Aggregation grain (e.g., "date")
        sources: List of entity analytics layers to combine
        computed_metrics: Metrics computed from combined data
    """

    name: str
    description: str
    grain: str
    sources: list[AnalyticsSource]
    computed_metrics: list[ComputedMetric] = PydanticField(default_factory=list)
