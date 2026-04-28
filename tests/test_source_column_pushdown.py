"""Tests for source column-pushdown analysis."""

from __future__ import annotations

from fyrnheim.components.computed_column import ComputedColumn
from fyrnheim.core.activity import ActivityDefinition, FieldChanged, RowAppeared
from fyrnheim.core.analytics_entity import AnalyticsEntity, StateField
from fyrnheim.core.identity import IdentityGraph, IdentitySource
from fyrnheim.core.metrics_model import MetricField, MetricsModel
from fyrnheim.core.source import (
    EventSource,
    Field,
    Join,
    Rename,
    SourceTransforms,
    StateSource,
)
from fyrnheim.engine.source_column_pushdown import (
    collect_required_source_columns,
    expression_column_references,
    required_raw_columns_for_source,
)


def test_expression_column_references_extracts_simple_t_references() -> None:
    assert expression_column_references("(t.email != '') & t['active']") == {
        "email",
        "active",
    }


def test_required_columns_cover_downstream_assets() -> None:
    customers = StateSource(
        name="customers",
        project="p",
        dataset="d",
        table="customers",
        id_field="customer_id",
    )
    events = EventSource(
        name="events",
        project="p",
        dataset="d",
        table="events",
        entity_id_field="user_id",
        timestamp_field="occurred_at",
        event_type_field="kind",
    )
    assets = {
        "sources": [customers, events],
        "activities": [
            ActivityDefinition(
                name="became_paid",
                source="customers",
                trigger=FieldChanged(field="plan"),
                entity_id_field="customer_id",
                include_fields=["plan", "email"],
            ),
            ActivityDefinition(
                name="signup",
                source="events",
                trigger=RowAppeared(),
                entity_id_field="user_id",
                include_fields=["campaign"],
            ),
        ],
        "identity_graphs": [
            IdentityGraph(
                name="ids",
                canonical_id="person_id",
                sources=[
                    IdentitySource(
                        source="customers",
                        id_field="customer_id",
                        match_key_field="email",
                    ),
                    IdentitySource(
                        source="events",
                        id_field="user_id",
                        match_key_field="anonymous_id",
                    ),
                ],
            )
        ],
        "analytics_entities": [
            AnalyticsEntity(
                name="customer",
                state_fields=[
                    StateField(
                        name="country",
                        source="customers",
                        field="country",
                        strategy="latest",
                    )
                ],
            )
        ],
        "metrics_models": [
            MetricsModel(
                name="engagement",
                sources=["customers", "events"],
                grain="daily",
                metric_fields=[
                    MetricField(field_name="plan", aggregation="last_value"),
                    MetricField(
                        field_name="signup",
                        aggregation="count_distinct",
                        distinct_field="session_id",
                    ),
                ],
            )
        ],
    }

    requirements = collect_required_source_columns(assets)

    assert requirements["customers"].columns == frozenset(
        {"customer_id", "plan", "email", "country", "session_id"}
    )
    assert requirements["events"].columns == frozenset(
        {"user_id", "occurred_at", "kind", "campaign", "anonymous_id", "session_id", "plan"}
    )


def test_join_targets_are_conservatively_retained() -> None:
    lookup = StateSource(
        name="lookup",
        project="p",
        dataset="d",
        table="lookup",
        id_field="id",
    )
    facts = EventSource(
        name="facts",
        project="p",
        dataset="d",
        table="facts",
        entity_id_field="user_id",
        timestamp_field="ts",
        joins=[Join(source_name="lookup", join_key="lookup_id")],
    )

    requirements = collect_required_source_columns({"sources": [lookup, facts]})

    assert requirements["lookup"].columns is None
    assert requirements["facts"].columns == frozenset({"user_id", "ts", "lookup_id"})


def test_required_raw_columns_preserve_transform_json_filter_and_computed_inputs() -> None:
    source = StateSource(
        name="customers",
        project="p",
        dataset="d",
        table="customers",
        id_field="id",
        transforms=SourceTransforms(
            renames=[Rename(from_name="raw_email", to_name="email")],
        ),
        fields=[
            Field(
                name="country",
                type="STRING",
                json_path="$.country",
                source_column="profile_json",
            )
        ],
        computed_columns=[
            ComputedColumn(name="domain", expression="t.email.split('@')[1]"),
        ],
        filter="t.active == True",
    )

    raw = required_raw_columns_for_source(source, {"email", "country", "domain"})

    assert raw == frozenset({"id", "raw_email", "profile_json", "active"})
