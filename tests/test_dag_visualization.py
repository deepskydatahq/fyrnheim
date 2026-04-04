"""Tests for pipeline DAG visualization."""

from fyrnheim.core.activity import ActivityDefinition, RowAppeared
from fyrnheim.core.analytics_entity import AnalyticsEntity, Measure, StateField
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


def _make_analytics_entity(
    name: str = "customer",
    identity_graph: str | None = "user_graph",
) -> AnalyticsEntity:
    return AnalyticsEntity(
        name=name,
        identity_graph=identity_graph,
        state_fields=[
            StateField(name="email", source="crm_contacts", field="email", strategy="latest"),
        ],
        measures=[
            Measure(name="event_count", activity="signup", aggregation="count"),
        ],
    )


def _make_metrics_model(
    name: str = "revenue_metrics",
    sources: list[str] | None = None,
) -> MetricsModel:
    return MetricsModel(
        name=name,
        sources=sources or ["crm_contacts"],
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


class TestAnalyticsEntitiesAppear:
    def test_analytics_entities_appear_in_output(self) -> None:
        ae = _make_analytics_entity("customer")
        result = generate_dag_html(analytics_entities=[ae])
        assert "customer" in result
        assert "ANALYTICS ENTITY" in result
        assert "1 state fields" in result
        assert "1 measures" in result


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
        ae = _make_analytics_entity("customer", identity_graph="user_graph")
        result = generate_dag_html(identity_graphs=[ig], analytics_entities=[ae])
        assert '"from": "identity-user_graph"' in result
        assert '"to": "entity-customer"' in result

    def test_edges_entity_without_identity_connects_to_source(self) -> None:
        src = _make_state_source("crm_contacts")
        ae = _make_analytics_entity("customer", identity_graph=None)
        result = generate_dag_html(sources=[src], analytics_entities=[ae])
        assert '"from": "source-crm_contacts"' in result
        assert '"to": "entity-customer"' in result

    def test_edges_connect_metrics_to_source(self) -> None:
        src = _make_state_source("crm_contacts")
        mm = _make_metrics_model("revenue_metrics", sources=["crm_contacts"])
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
        analytics_entities = [_make_analytics_entity("customer", identity_graph="user_graph")]
        metrics_models = [_make_metrics_model("revenue_metrics", sources=["crm_contacts"])]

        result = generate_dag_html(
            sources=sources,
            activities=activities,
            identity_graphs=identity_graphs,
            analytics_entities=analytics_entities,
            metrics_models=metrics_models,
        )

        # All node names present
        assert "crm_contacts" in result
        assert "page_views" in result
        assert "signup" in result
        assert "user_graph" in result
        assert "customer" in result
        assert "revenue_metrics" in result

        # Structure present
        assert "<!DOCTYPE html>" in result
        assert "SOURCES" in result
        assert "ACTIVITIES" in result
        assert "IDENTITY" in result
        assert "ENTITIES" in result
        assert "METRICS" in result

        # Edges present
        assert "source-crm_contacts" in result
        assert "activity-signup" in result
        assert "identity-user_graph" in result
        assert "entity-customer" in result


class TestDetailPanel:
    def test_html_contains_node_details_json(self) -> None:
        src = _make_state_source("crm_contacts")
        result = generate_dag_html(sources=[src])
        assert "var nodeDetails" in result

    def test_panel_html_exists(self) -> None:
        result = generate_dag_html()
        assert 'id="detail-panel"' in result
        assert "detail-panel" in result

    def test_node_details_contain_table_for_source(self) -> None:
        import json

        src = _make_state_source("crm_contacts")
        result = generate_dag_html(sources=[src])
        # Extract the nodeDetails JSON from the HTML
        marker = "var nodeDetails = "
        start = result.index(marker) + len(marker)
        end = result.index(";", start)
        details = json.loads(result[start:end])
        assert "source-crm_contacts" in details
        assert details["source-crm_contacts"]["table"] == "my_project.raw.crm_contacts"
