"""Tests for pipeline DAG visualization."""

from fyrnheim.core.activity import ActivityDefinition, EventOccurred, RowAppeared
from fyrnheim.core.analytics_model import StreamAnalyticsModel, StreamMetric
from fyrnheim.core.entity_model import EntityModel, StateField
from fyrnheim.core.identity import IdentityGraph, IdentitySource
from fyrnheim.core.metrics_model import MetricField, MetricsModel
from fyrnheim.core.source import EventSource, StateSource
from fyrnheim.visualization.dag import generate_dag_html


def _make_state_source(name: str = "crm_contacts") -> StateSource:
    return StateSource(
        name=name,
        project="my_project",
        dataset="raw",
        table=name,
        id_field="contact_id",
        snapshot_grain="daily",
    )


def _make_event_source(name: str = "page_views") -> EventSource:
    return EventSource(
        name=name,
        project="my_project",
        dataset="raw",
        table=name,
        entity_id_field="user_id",
        timestamp_field="ts",
    )


def _make_activity(name: str = "signup", source: str = "crm_contacts") -> ActivityDefinition:
    return ActivityDefinition(
        name=name,
        source=source,
        trigger=RowAppeared(),
        entity_id_field="contact_id",
    )


def _make_identity_graph(
    name: str = "user_graph",
    source_names: tuple[str, ...] = ("crm_contacts", "page_views"),
) -> IdentityGraph:
    return IdentityGraph(
        name=name,
        canonical_id="canonical_user_id",
        sources=[
            IdentitySource(source=s, id_field=f"{s}_id", match_key_field="email")
            for s in source_names
        ],
    )


def _make_entity_model(
    name: str = "customer",
    identity_graph: str | None = "user_graph",
) -> EntityModel:
    return EntityModel(
        name=name,
        identity_graph=identity_graph,
        state_fields=[
            StateField(name="email", source="crm_contacts", field="email", strategy="latest"),
        ],
    )


def _make_analytics_model(
    name: str = "daily_metrics",
    identity_graph: str | None = "user_graph",
) -> StreamAnalyticsModel:
    return StreamAnalyticsModel(
        name=name,
        identity_graph=identity_graph,
        date_grain="daily",
        metrics=[StreamMetric(name="event_count", expression="count(*)", metric_type="count")],
    )


def _make_metrics_model(
    name: str = "revenue_metrics",
    source: str = "crm_contacts",
) -> MetricsModel:
    return MetricsModel(
        name=name,
        source=source,
        grain="daily",
        metric_fields=[MetricField(field_name="revenue", aggregation="sum_delta")],
    )


class TestEmptyInputs:
    def test_empty_inputs_returns_valid_html(self) -> None:
        result = generate_dag_html()
        assert "<!DOCTYPE html>" in result
        assert "fyrnheim pipeline" in result
        assert "</html>" in result


class TestSourcesAppear:
    def test_sources_appear_in_output(self) -> None:
        src = _make_state_source("crm_contacts")
        result = generate_dag_html(sources=[src])
        assert "crm_contacts" in result
        assert "STATE" in result

    def test_event_source_appears(self) -> None:
        src = _make_event_source("page_views")
        result = generate_dag_html(sources=[src])
        assert "page_views" in result
        assert "EVENT" in result


class TestActivitiesAppear:
    def test_activities_appear_in_output(self) -> None:
        act = _make_activity("signup")
        result = generate_dag_html(activities=[act])
        assert "signup" in result
        assert "row_appeared" in result


class TestIdentityGraphsAppear:
    def test_identity_graphs_appear_in_output(self) -> None:
        ig = _make_identity_graph("user_graph")
        result = generate_dag_html(identity_graphs=[ig])
        assert "user_graph" in result
        assert "2 sources" in result


class TestEntityModelsAppear:
    def test_entity_models_appear_in_output(self) -> None:
        em = _make_entity_model("customer")
        result = generate_dag_html(entity_models=[em])
        assert "customer" in result
        assert "1 fields" in result


class TestMetricsModelsAppear:
    def test_metrics_models_appear_in_output(self) -> None:
        mm = _make_metrics_model("revenue_metrics")
        result = generate_dag_html(metrics_models=[mm])
        assert "revenue_metrics" in result
        assert "1 metrics" in result


class TestEdges:
    def test_edges_connect_activity_to_source(self) -> None:
        src = _make_state_source("crm_contacts")
        act = _make_activity("signup", source="crm_contacts")
        result = generate_dag_html(sources=[src], activities=[act])
        assert '"from": "source-crm_contacts"' in result
        assert '"to": "activity-signup"' in result

    def test_edges_connect_identity_to_source(self) -> None:
        src1 = _make_state_source("crm_contacts")
        src2 = _make_event_source("page_views")
        ig = _make_identity_graph("user_graph", ("crm_contacts", "page_views"))
        result = generate_dag_html(sources=[src1, src2], identity_graphs=[ig])
        assert '"from": "source-crm_contacts"' in result
        assert '"to": "identity-user_graph"' in result

    def test_edges_connect_entity_to_identity(self) -> None:
        ig = _make_identity_graph("user_graph")
        em = _make_entity_model("customer", identity_graph="user_graph")
        result = generate_dag_html(identity_graphs=[ig], entity_models=[em])
        assert '"from": "identity-user_graph"' in result
        assert '"to": "entity-customer"' in result

    def test_edges_entity_without_identity_connects_to_source(self) -> None:
        src = _make_state_source("crm_contacts")
        em = _make_entity_model("customer", identity_graph=None)
        result = generate_dag_html(sources=[src], entity_models=[em])
        assert '"from": "source-crm_contacts"' in result
        assert '"to": "entity-customer"' in result

    def test_edges_connect_analytics_to_identity(self) -> None:
        ig = _make_identity_graph("user_graph")
        am = _make_analytics_model("daily_metrics", identity_graph="user_graph")
        result = generate_dag_html(identity_graphs=[ig], analytics_models=[am])
        assert '"from": "identity-user_graph"' in result
        assert '"to": "analytics-daily_metrics"' in result

    def test_edges_connect_metrics_to_source(self) -> None:
        src = _make_state_source("crm_contacts")
        mm = _make_metrics_model("revenue_metrics", source="crm_contacts")
        result = generate_dag_html(sources=[src], metrics_models=[mm])
        assert '"from": "source-crm_contacts"' in result
        assert '"to": "metrics-revenue_metrics"' in result


class TestFullPipeline:
    def test_full_pipeline(self) -> None:
        sources: list[StateSource | EventSource] = [
            _make_state_source("crm_contacts"),
            _make_event_source("page_views"),
        ]
        activities = [_make_activity("signup", source="crm_contacts")]
        identity_graphs = [_make_identity_graph("user_graph", ("crm_contacts", "page_views"))]
        entity_models = [_make_entity_model("customer", identity_graph="user_graph")]
        analytics_models = [_make_analytics_model("daily_metrics", identity_graph="user_graph")]
        metrics_models = [_make_metrics_model("revenue_metrics", source="crm_contacts")]

        result = generate_dag_html(
            sources=sources,
            activities=activities,
            identity_graphs=identity_graphs,
            entity_models=entity_models,
            analytics_models=analytics_models,
            metrics_models=metrics_models,
        )

        # All node names present
        assert "crm_contacts" in result
        assert "page_views" in result
        assert "signup" in result
        assert "user_graph" in result
        assert "customer" in result
        assert "daily_metrics" in result
        assert "revenue_metrics" in result

        # Structure present
        assert "<!DOCTYPE html>" in result
        assert "SOURCES" in result
        assert "ACTIVITIES" in result
        assert "IDENTITY" in result
        assert "MODELS" in result
        assert "ANALYTICS" in result

        # Edges present
        assert "source-crm_contacts" in result
        assert "activity-signup" in result
        assert "identity-user_graph" in result
        assert "entity-customer" in result
