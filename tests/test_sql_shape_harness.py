"""SQLGlot structural tests for warehouse-native Ibis expressions."""

from __future__ import annotations

import ibis
from sqlglot import exp

from fyrnheim.core.activity import ActivityDefinition, EventOccurred, RowAppeared
from fyrnheim.core.analytics_entity import AnalyticsEntity, Measure, StateField
from fyrnheim.core.identity import IdentityGraph, IdentitySource
from fyrnheim.core.metrics_model import MetricField, MetricsModel
from fyrnheim.core.source import EventSource
from fyrnheim.engine.activity_engine import apply_activity_definitions
from fyrnheim.engine.analytics_entity_engine import project_analytics_entity
from fyrnheim.engine.diff_engine import diff_snapshots
from fyrnheim.engine.event_source_loader import build_event_source_event_table
from fyrnheim.engine.identity_engine import enrich_events, resolve_identities
from fyrnheim.engine.metrics_engine import aggregate_metrics
from tests.sql_shape import compile_bigquery_shape


def _event_stream_table() -> ibis.expr.types.Table:
    return ibis.table(
        {
            "source": "string",
            "entity_id": "string",
            "ts": "string",
            "event_type": "string",
            "payload": "string",
        },
        name="events",
    )


def test_sqlglot_helper_parses_eventsource_payload_struct() -> None:
    table = ibis.table(
        {"user_id": "string", "viewed_at": "string", "page": "string"},
        name="page_views_raw",
    )
    source = EventSource(
        name="page_views",
        project="p",
        dataset="d",
        table="page_views_raw",
        entity_id_field="user_id",
        timestamp_field="viewed_at",
        event_type="page_view",
    )

    shape = compile_bigquery_shape(build_event_source_event_table(table, source))

    shape.assert_has(exp.Struct)
    assert "payload" in shape.normalized
    assert "page_view" in shape.normalized


def test_sqlglot_shape_covers_activity_union_all() -> None:
    events = _event_stream_table()
    definitions = [
        ActivityDefinition(
            name="crm_signup",
            source="crm",
            trigger=RowAppeared(),
            entity_id_field="entity_id",
        ),
        ActivityDefinition(
            name="purchase",
            source="web",
            trigger=EventOccurred(event_type="checkout"),
            entity_id_field="entity_id",
            include_fields=["amount"],
        ),
    ]

    shape = compile_bigquery_shape(apply_activity_definitions(events, definitions))

    unions = shape.find_all(exp.Union)
    assert unions
    assert any(union.args.get("distinct") is False for union in unions)
    shape.assert_has(exp.Struct)


def test_sqlglot_shape_covers_identity_join_and_coalesce() -> None:
    events = _event_stream_table()
    graph = IdentityGraph(
        name="person",
        canonical_id="person_id",
        sources=[
            IdentitySource(source="crm", id_field="id", match_key_field="email"),
            IdentitySource(source="billing", id_field="id", match_key_field="email"),
        ],
    )

    mapping = resolve_identities(events, graph)
    shape = compile_bigquery_shape(enrich_events(events, mapping))

    shape.assert_has(exp.Join)
    shape.assert_has(exp.Coalesce)
    shape.assert_function("SHA256")


def test_sqlglot_shape_covers_metrics_group_by() -> None:
    events = _event_stream_table()
    model = MetricsModel(
        name="engagement",
        sources=["web"],
        grain="daily",
        dimensions=["entity_id"],
        metric_fields=[
            MetricField(field_name="score", aggregation="sum_delta"),
            MetricField(field_name="signup", aggregation="count"),
            MetricField(
                field_name="signup",
                aggregation="count_distinct",
                distinct_field="session_id",
            ),
        ],
    )

    shape = compile_bigquery_shape(aggregate_metrics(events, model))

    shape.assert_has(exp.Group)
    shape.assert_has(exp.Count)
    shape.assert_has(exp.Sum)
    assert "DISTINCT" in shape.normalized.upper()


def test_sqlglot_shape_covers_statesource_diff_union_and_joins() -> None:
    current = ibis.table(
        {"id": "int64", "name": "string", "plan": "string"},
        name="current_accounts",
    )
    previous = ibis.table(
        {"id": "int64", "name": "string", "plan": "string"},
        name="previous_accounts",
    )

    shape = compile_bigquery_shape(
        diff_snapshots(
            current,
            previous,
            source_name="accounts",
            id_field="id",
            snapshot_date="2026-01-02",
        )
    )

    assert shape.count(exp.Union) >= 1
    assert shape.count(exp.Join) >= 2
    shape.assert_has(exp.Struct)
    assert "field_changed" in shape.normalized


def test_sqlglot_shape_covers_analytics_entity_projection() -> None:
    events = ibis.table(
        {
            "source": "string",
            "entity_id": "string",
            "canonical_id": "string",
            "ts": "string",
            "event_type": "string",
            "payload": "string",
        },
        name="enriched_events",
    )
    entity = AnalyticsEntity(
        name="accounts",
        state_fields=[
            StateField(name="company_name", source="crm", field="name", strategy="latest"),
        ],
        measures=[
            Measure(name="login_count", activity="login", aggregation="count"),
            Measure(name="purchase_total", activity="purchase", aggregation="sum", field="amount"),
            Measure(name="last_purchase", activity="purchase", aggregation="latest", field="amount"),
        ],
    )

    shape = compile_bigquery_shape(project_analytics_entity(events, entity))

    shape.assert_has(exp.Group)
    shape.assert_has(exp.Sum)
    assert "canonical_id" in shape.normalized
    assert "company_name" in shape.normalized
    shape.assert_function("COUNTIF")
