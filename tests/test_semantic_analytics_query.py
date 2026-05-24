"""Tests for semantic analytics model context and generic query tools."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import ibis
import pandas as pd
import pytest

from fyrnheim.analytics_catalog import build_analytics_catalog, describe_analytics_model
from fyrnheim.analytics_query import (
    AnalyticsQueryError,
    describe_query_syntax,
    preview_analytics_query_sql,
    query_analytics_model,
)
from fyrnheim.inspect import build_manifest
from fyrnheim.mcp.analytics_server import create_streamable_http_app
from fyrnheim.mcp.analytics_tools import (
    describe_analytics_model as tool_describe_analytics_model,
    preview_analytics_query_sql as tool_preview_analytics_query_sql,
    query_analytics_model as tool_query_analytics_model,
)

pytest.importorskip("mcp")
pytest.importorskip("starlette")
from starlette.testclient import TestClient

MCP_HEADERS = {
    "accept": "application/json, text/event-stream",
    "content-type": "application/json",
}


def _write_project(tmp_path: Path) -> tuple[Path, Path, ibis.BaseBackend]:
    entities_dir = tmp_path / "entities"
    entities_dir.mkdir()
    (entities_dir / "content.py").write_text(
        """
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

content_metrics_daily = MetricsModel(
    name="content_metrics_daily",
    sources=["content_events"],
    grain="daily",
    metric_fields=[
        MetricField(field_name="impressions", aggregation="sum_delta"),
        MetricField(field_name="reactions", aggregation="sum_delta"),
        MetricField(field_name="comments", aggregation="sum_delta"),
        MetricField(field_name="shares", aggregation="sum_delta"),
    ],
    dimensions=["source"],
)
""".strip(),
        encoding="utf-8",
    )
    db_path = tmp_path / "analytics.duckdb"
    conn = ibis.duckdb.connect(str(db_path))
    conn.create_table(
        "content_metrics_daily",
        pd.DataFrame(
            [
                {
                    "_date": "2026-01-01",
                    "source": "linkedin",
                    "impressions_sum_delta": 1000,
                    "reactions_sum_delta": 50,
                    "comments_sum_delta": 12,
                    "shares_sum_delta": 8,
                },
                {
                    "_date": "2026-01-02",
                    "source": "linkedin",
                    "impressions_sum_delta": 1500,
                    "reactions_sum_delta": 70,
                    "comments_sum_delta": 20,
                    "shares_sum_delta": 10,
                },
                {
                    "_date": "2026-01-01",
                    "source": "website",
                    "impressions_sum_delta": 2200,
                    "reactions_sum_delta": 20,
                    "comments_sum_delta": 4,
                    "shares_sum_delta": 1,
                },
            ]
        ),
        overwrite=True,
    )
    config_path = tmp_path / "fyrnheim.yaml"
    config_path.write_text(
        f"""
entities_dir: entities
backend: duckdb
backend_config:
  db_path: "{db_path}"
""".strip(),
        encoding="utf-8",
    )
    return config_path, entities_dir, conn


def _catalog(entities_dir: Path, project_path: Path) -> dict[str, Any]:
    return build_analytics_catalog(
        build_manifest(entities_dir, project_path=project_path, include_git=False)
    )


def test_catalog_exposes_semantic_model_metric_and_dimension_context(tmp_path: Path) -> None:
    _, entities_dir, _ = _write_project(tmp_path)
    catalog = _catalog(entities_dir, tmp_path)

    description = describe_analytics_model(catalog, "content_metrics_daily")

    assert description["model_summary"] == {
        "name": "content_metrics_daily",
        "model_type": "metrics_model",
        "description": "Daily metrics model 'content_metrics_daily' aggregated from content_events.",
        "grain": "daily",
        "defining_entity": "daily aggregate over content_events",
        "metrics": ["comments", "impressions", "reactions", "shares"],
        "dimensions": ["source", "_date"],
        "limitations": [
            "daily aggregate grain; lower-grain records such as individual posts are not available unless declared as dimensions or exposed by another model."
        ],
        "recommended_questions": [
            "Which dimensions drive content_metrics_daily metrics?",
            "How do metrics trend by _date at daily grain?",
        ],
    }
    metric = description["model"]["metrics"][0]
    dimension = description["model"]["dimensions"][0]
    assert metric["description"].startswith("Metric '")
    assert metric["usage"]["safe_for_order_by"] is True
    assert dimension["usage"]["safe_for_group_by"] is True


def test_query_analytics_model_groups_declared_metrics_and_caps_limit(tmp_path: Path) -> None:
    _, entities_dir, conn = _write_project(tmp_path)
    catalog = _catalog(entities_dir, tmp_path)

    result = query_analytics_model(
        catalog,
        conn,
        model="content_metrics_daily",
        metrics=["impressions", "reactions"],
        dimensions=["source"],
        order_by=[{"field": "impressions", "direction": "desc"}],
        limit=9999,
    )

    assert result["limit"] == 500
    assert result["model_context"]["grain"] == "daily"
    assert result["rows"] == [
        {"source": "website", "impressions": 2200, "reactions": 20},
        {"source": "linkedin", "impressions": 2500, "reactions": 120},
    ][::-1]


def test_query_analytics_model_filters_dimensions(tmp_path: Path) -> None:
    _, entities_dir, conn = _write_project(tmp_path)
    catalog = _catalog(entities_dir, tmp_path)

    result = query_analytics_model(
        catalog,
        conn,
        model="content_metrics_daily",
        metrics=["impressions"],
        dimensions=["_date", "source"],
        filters={"source": "linkedin", "_date": {"gte": "2026-01-02"}},
        limit=10,
    )

    assert result["rows"] == [
        {"_date": "2026-01-02", "source": "linkedin", "impressions": 1500}
    ]


def test_query_syntax_describes_order_by_contract() -> None:
    syntax = describe_query_syntax()

    assert syntax["schema_version"] == "fyrnheim.analytics_query_syntax.v1"
    assert {"field": "reactions", "direction": "desc"} in syntax["order_by_schema"]["example"]
    assert syntax["examples"][0]["arguments"] == {
        "model": "content_metrics_daily",
        "metrics": ["reactions"],
        "dimensions": ["source"],
        "order_by": [{"field": "reactions", "direction": "desc"}],
        "limit": 5,
    }


def test_query_analytics_model_rejects_undeclared_fields(tmp_path: Path) -> None:
    _, entities_dir, conn = _write_project(tmp_path)
    catalog = _catalog(entities_dir, tmp_path)

    with pytest.raises(AnalyticsQueryError, match="Unknown metric"):
        query_analytics_model(catalog, conn, model="content_metrics_daily", metrics=["views"])
    with pytest.raises(AnalyticsQueryError, match="Unknown dimension"):
        query_analytics_model(
            catalog,
            conn,
            model="content_metrics_daily",
            metrics=["impressions"],
            dimensions=["post_id"],
        )


def test_query_analytics_model_rejects_malformed_order_by(tmp_path: Path) -> None:
    _, entities_dir, conn = _write_project(tmp_path)
    catalog = _catalog(entities_dir, tmp_path)

    with pytest.raises(AnalyticsQueryError, match="order_by must be an array of objects"):
        query_analytics_model(
            catalog,
            conn,
            model="content_metrics_daily",
            metrics=["reactions"],
            dimensions=["source"],
            order_by={"reactions": "desc"},  # type: ignore[arg-type]
        )
    with pytest.raises(AnalyticsQueryError, match="unknown key"):
        query_analytics_model(
            catalog,
            conn,
            model="content_metrics_daily",
            metrics=["reactions"],
            dimensions=["source"],
            order_by=[{"reactions": "desc"}],  # type: ignore[list-item]
        )
    with pytest.raises(AnalyticsQueryError, match="not a backing column name"):
        query_analytics_model(
            catalog,
            conn,
            model="content_metrics_daily",
            metrics=["reactions"],
            dimensions=["source"],
            order_by=[{"field": "reactions_sum_delta", "direction": "desc"}],
        )


def test_preview_analytics_query_sql_compiles_generated_query(tmp_path: Path) -> None:
    _, entities_dir, conn = _write_project(tmp_path)
    catalog = _catalog(entities_dir, tmp_path)

    preview = preview_analytics_query_sql(
        catalog,
        conn,
        model="content_metrics_daily",
        metrics=["impressions"],
        dimensions=["source"],
        order_by=[{"field": "impressions", "direction": "desc"}],
        limit=5,
    )

    assert "content_metrics_daily" in preview["sql"]
    assert "impressions_sum_delta" in preview["sql"]
    assert preview["limit"] == 5


def test_project_query_reads_parquet_materialized_model_when_table_is_not_registered(tmp_path: Path) -> None:
    config_path, _, conn = _write_project(tmp_path)
    table = conn.table("content_metrics_daily").execute()
    output_dir = tmp_path / "generated"
    output_dir.mkdir()
    table.to_parquet(output_dir / "content_metrics_daily.parquet")

    parquet_config = tmp_path / "fyrnheim-parquet.yaml"
    parquet_config.write_text(
        """
entities_dir: entities
output_dir: generated
backend: duckdb
backend_config:
  db_path: ":memory:"
""".strip(),
        encoding="utf-8",
    )

    result = tool_query_analytics_model(
        parquet_config,
        "content_metrics_daily",
        ["impressions"],
        dimensions=["source"],
        order_by=[{"field": "impressions", "direction": "desc"}],
    )
    preview = tool_preview_analytics_query_sql(
        parquet_config,
        "content_metrics_daily",
        ["impressions"],
        dimensions=["source"],
    )

    assert result["rows"][0] == {"source": "linkedin", "impressions": 2500}
    assert "ibis_read_parquet" in preview["sql"]
    assert config_path.exists()


def test_mcp_tools_query_project_config(tmp_path: Path) -> None:
    config_path, entities_dir, _ = _write_project(tmp_path)

    model = tool_describe_analytics_model(entities_dir, "content_metrics_daily", project_path=tmp_path)
    result = tool_query_analytics_model(
        config_path,
        "content_metrics_daily",
        ["impressions"],
        dimensions=["source"],
        order_by=[{"field": "impressions", "direction": "desc"}],
    )
    preview = tool_preview_analytics_query_sql(
        config_path,
        "content_metrics_daily",
        ["impressions"],
        dimensions=["source"],
    )

    assert model["model_summary"]["limitations"]
    assert result["rows"][0] == {"source": "linkedin", "impressions": 2500}
    assert "SELECT" in preview["sql"].upper()


def _rpc(client: TestClient, method: str, params: dict[str, Any] | None = None, rpc_id: int = 1) -> dict[str, Any]:
    response = client.post(
        "/mcp",
        headers=MCP_HEADERS,
        json={"jsonrpc": "2.0", "id": rpc_id, "method": method, "params": params or {}},
    )
    assert response.status_code == 200
    return response.json()


def _initialize(client: TestClient) -> None:
    _rpc(
        client,
        "initialize",
        {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "fyrnheim-test", "version": "1.0"},
        },
    )
    response = client.post(
        "/mcp",
        headers=MCP_HEADERS,
        json={"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
    )
    assert response.status_code == 202


def _tool_payload(result: dict[str, Any]) -> dict[str, Any]:
    return json.loads(result["result"]["content"][0]["text"])


def test_http_mcp_exposes_semantic_query_tools(tmp_path: Path) -> None:
    config_path, entities_dir, _ = _write_project(tmp_path)
    app = create_streamable_http_app(
        entities_dir=entities_dir,
        project_path=tmp_path,
        config_path=config_path,
        host="testserver",
        json_response=True,
        stateless_http=True,
    )

    with TestClient(app) as client:
        _initialize(client)
        tools = _rpc(client, "tools/list", rpc_id=2)
        tool_names = {tool["name"] for tool in tools["result"]["tools"]}
        assert {
            "describe_analytics_model",
            "describe_query_syntax",
            "query_analytics_model",
            "preview_analytics_query_sql",
        } <= tool_names

        syntax = _tool_payload(
            _rpc(
                client,
                "tools/call",
                {"name": "describe_query_syntax", "arguments": {}},
                rpc_id=3,
            )
        )
        assert syntax["order_by_schema"]["example"] == [{"field": "reactions", "direction": "desc"}]

        payload = _tool_payload(
            _rpc(
                client,
                "tools/call",
                {
                    "name": "query_analytics_model",
                    "arguments": {
                        "model": "content_metrics_daily",
                        "metrics": ["impressions"],
                        "dimensions": ["source"],
                        "limit": 1,
                    },
                },
                rpc_id=4,
            )
        )
        assert payload["row_count"] == 1
