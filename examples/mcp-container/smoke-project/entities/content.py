"""Minimal Fyrnheim entities for MCP catalog smoke testing."""

from fyrnheim.core import EventSource, MetricField, MetricsModel

content_events = EventSource(
    name="content_events",
    project="smoke",
    dataset="analytics",
    table="content_events",
    entity_id_field="content_id",
    timestamp_field="event_ts",
    event_type_field="event_type",
)

content_metrics = MetricsModel(
    name="content_metrics_daily",
    sources=["content_events"],
    grain="daily",
    metric_fields=[
        MetricField(field_name="view", aggregation="count"),
        MetricField(field_name="click", aggregation="count"),
    ],
    dimensions=["channel"],
)
