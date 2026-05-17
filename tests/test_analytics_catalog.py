"""Tests for analytics catalog and MCP-ready tools."""

from __future__ import annotations

from pathlib import Path

from fyrnheim.analytics_catalog import (
    build_analytics_catalog,
    describe_dimension,
    describe_metric,
    list_analytics_models,
    list_dimensions,
    list_metrics,
)
from fyrnheim.inspect import build_manifest
from fyrnheim.mcp import analytics_tools
from fyrnheim.mcp.analytics_server import create_server


def _write_entities(tmp_path: Path) -> Path:
    entities_dir = tmp_path / "entities"
    entities_dir.mkdir()
    (entities_dir / "project.py").write_text(
        """
from fyrnheim import (
    ActivityDefinition,
    AnalyticsEntity,
    ComputedColumn,
    EventOccurred,
    EventSource,
    IdentityGraph,
    IdentitySource,
    Measure,
    MetricField,
    MetricsModel,
    StateField,
    StateSource,
)

accounts_source = StateSource(
    name="accounts_source",
    project="p",
    dataset="d",
    table="accounts",
    id_field="account_id",
)
web_events = EventSource(
    name="web_events",
    project="p",
    dataset="d",
    table="events",
    entity_id_field="account_id",
    timestamp_field="occurred_at",
    event_type_field="event_name",
)
signup = ActivityDefinition(
    name="signup",
    source="web_events",
    trigger=EventOccurred(event_type="signup"),
    entity_id_field="entity_id",
)
person = IdentityGraph(
    name="person",
    canonical_id="person_id",
    sources=[
        IdentitySource(source="accounts_source", id_field="account_id", match_key_field="email"),
        IdentitySource(source="web_events", id_field="account_id", match_key_field="email"),
    ],
)
accounts = AnalyticsEntity(
    name="accounts",
    identity_graph="person",
    state_fields=[
        StateField(name="email", source="accounts_source", field="email", strategy="latest"),
        StateField(name="plan", source="accounts_source", field="plan", strategy="latest"),
    ],
    measures=[
        Measure(name="signup_count", activity="signup", aggregation="count"),
        Measure(name="revenue_total", activity="purchase", aggregation="sum", field="amount"),
    ],
    computed_fields=[
        ComputedColumn(name="email_domain", expression="t.email.split('@')[1] if t.email else None"),
    ],
)
engagement = MetricsModel(
    name="engagement_daily",
    sources=["web_events"],
    grain="daily",
    dimensions=["source"],
    metric_fields=[
        MetricField(field_name="signup", aggregation="count"),
        MetricField(field_name="signup", aggregation="count_distinct", distinct_field="session_id"),
    ],
)
""".strip(),
        encoding="utf-8",
    )
    return entities_dir


def _catalog(tmp_path: Path) -> dict:
    entities_dir = _write_entities(tmp_path)
    manifest = build_manifest(entities_dir, project_path=tmp_path, include_git=False)
    return build_analytics_catalog(manifest)


def test_build_analytics_catalog_includes_models_metrics_and_dimensions(tmp_path: Path) -> None:
    catalog = _catalog(tmp_path)

    assert catalog["schema_version"] == "fyrnheim.analytics_catalog.v1"
    assert [model["name"] for model in catalog["models"]] == [
        "accounts",
        "engagement_daily",
    ]

    metric_ids = {metric["metric_id"] for metric in catalog["metrics"]}
    assert "analytics_entity:accounts:measure:signup_count" in metric_ids
    assert "analytics_entity:accounts:measure:revenue_total" in metric_ids
    assert "metrics_model:engagement_daily:count:signup:" in metric_ids
    assert "metrics_model:engagement_daily:count_distinct:signup:session_id" in metric_ids

    dimensions = {(dimension["model"], dimension["name"], dimension["kind"]) for dimension in catalog["dimensions"]}
    assert ("accounts", "canonical_id", "identity") in dimensions
    assert ("accounts", "email", "state_field") in dimensions
    assert ("accounts", "email_domain", "computed_field") in dimensions
    assert ("engagement_daily", "_date", "time_grain") in dimensions
    assert ("engagement_daily", "source", "dimension") in dimensions


def test_list_analytics_models_summarizes_catalog(tmp_path: Path) -> None:
    summary = list_analytics_models(_catalog(tmp_path))

    accounts = next(model for model in summary["models"] if model["name"] == "accounts")
    assert accounts["model_type"] == "analytics_entity"
    assert accounts["metrics"] == ["revenue_total", "signup_count"]
    assert "email_domain" in accounts["dimensions"]

    engagement = next(model for model in summary["models"] if model["name"] == "engagement_daily")
    assert engagement["grain"] == "daily"
    assert engagement["metrics"] == ["signup", "signup"]
    assert engagement["dimensions"] == ["source", "_date"]


def test_metric_and_dimension_listing_can_filter_by_model(tmp_path: Path) -> None:
    catalog = _catalog(tmp_path)

    account_metrics = list_metrics(catalog, model="accounts")["metrics"]
    assert {metric["name"] for metric in account_metrics} == {"signup_count", "revenue_total"}

    engagement_dimensions = list_dimensions(catalog, model="engagement_daily")["dimensions"]
    assert {dimension["name"] for dimension in engagement_dimensions} == {"_date", "source"}


def test_describe_metric_returns_dimensions_for_unambiguous_metric(tmp_path: Path) -> None:
    description = describe_metric(_catalog(tmp_path), "signup_count")

    assert description["ambiguous"] is False
    assert description["metric"]["aggregation"] == "count"
    assert description["metric"]["activity"] == "signup"
    assert {dimension["name"] for dimension in description["available_dimensions"]} >= {
        "canonical_id",
        "email",
        "plan",
        "email_domain",
    }


def test_describe_metric_reports_ambiguity(tmp_path: Path) -> None:
    description = describe_metric(_catalog(tmp_path), "signup")

    assert description["ambiguous"] is True
    assert description["count"] == 2
    assert {match["aggregation"] for match in description["matches"]} == {
        "count",
        "count_distinct",
    }


def test_describe_dimension_returns_usable_metrics(tmp_path: Path) -> None:
    description = describe_dimension(_catalog(tmp_path), "email_domain")

    assert description["ambiguous"] is False
    assert description["dimension"]["kind"] == "computed_field"
    assert {metric["name"] for metric in description["usable_with_metrics"]} == {
        "signup_count",
        "revenue_total",
    }


def test_mcp_ready_tools_load_catalog_from_entities_dir(tmp_path: Path) -> None:
    entities_dir = _write_entities(tmp_path)

    metrics = analytics_tools.list_metrics(entities_dir, project_path=tmp_path, model="accounts")
    dimensions = analytics_tools.list_dimensions(entities_dir, project_path=tmp_path, model="accounts")
    metric_description = analytics_tools.describe_metric(
        entities_dir,
        "signup_count",
        project_path=tmp_path,
    )

    assert {metric["name"] for metric in metrics["metrics"]} == {"signup_count", "revenue_total"}
    assert "email_domain" in {dimension["name"] for dimension in dimensions["dimensions"]}
    assert metric_description["metric"]["name"] == "signup_count"


def test_optional_mcp_server_entrypoint_is_importable() -> None:
    try:
        server = create_server()
    except RuntimeError as exc:
        assert "optional 'mcp' extra" in str(exc)
    else:
        assert server is not None
