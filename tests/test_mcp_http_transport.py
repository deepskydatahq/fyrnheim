"""Tests for Fyrnheim MCP Streamable HTTP transport."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import ibis
import pandas as pd
import pytest

pytest.importorskip("mcp")
pytest.importorskip("starlette")

from starlette.testclient import TestClient

from fyrnheim.mcp.analytics_server import create_streamable_http_app

MCP_HEADERS = {
    "accept": "application/json, text/event-stream",
    "content-type": "application/json",
}


def _write_project(tmp_path: Path) -> Path:
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

content_metrics = MetricsModel(
    name="content_metrics_daily",
    sources=["content_events"],
    grain="daily",
    metric_fields=[MetricField(field_name="view", aggregation="count")],
    dimensions=["channel"],
)
""".strip(),
        encoding="utf-8",
    )

    db_path = tmp_path / "smoke.duckdb"
    conn = ibis.duckdb.connect(str(db_path))
    conn.create_table(
        "content_scores",
        pd.DataFrame(
            [
                {
                    "content_id": "post-001",
                    "title": "Launch notes",
                    "channel": "linkedin",
                    "impressions": 1200,
                    "total_engagement": 84,
                    "engagement_rate_pct": 7.0,
                    "performance_tier": "high",
                },
                {
                    "content_id": "post-002",
                    "title": "Workshop recap",
                    "channel": "linkedin",
                    "impressions": 800,
                    "total_engagement": 32,
                    "engagement_rate_pct": 4.0,
                    "performance_tier": "medium",
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
insights:
  recipes:
    top_content_items:
      type: table_top
      table: content_scores
      tags: [content, smoke]
      columns: [content_id, title, channel, impressions, total_engagement, engagement_rate_pct, performance_tier]
      order_by:
        default: total_engagement
        allowed: [impressions, total_engagement, engagement_rate_pct]
      filters:
        allowed: [channel, performance_tier]
      limit:
        default: 5
        max: 25
""".strip(),
        encoding="utf-8",
    )
    return config_path


def _rpc(client: TestClient, method: str, params: dict[str, Any] | None = None, rpc_id: int = 1) -> dict[str, Any]:
    response = client.post(
        "/mcp",
        headers=MCP_HEADERS,
        json={"jsonrpc": "2.0", "id": rpc_id, "method": method, "params": params or {}},
    )
    assert response.status_code == 200
    return response.json()


def _initialize(client: TestClient) -> None:
    initialized = _rpc(
        client,
        "initialize",
        {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "fyrnheim-test", "version": "1.0"},
        },
    )
    assert initialized["result"]["serverInfo"]["name"] == "fyrnheim-analytics"

    notification = client.post(
        "/mcp",
        headers=MCP_HEADERS,
        json={"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
    )
    assert notification.status_code == 202


def _tool_payload(result: dict[str, Any]) -> dict[str, Any]:
    text = result["result"]["content"][0]["text"]
    return json.loads(text)


def test_streamable_http_transport_lists_and_calls_catalog_and_insight_tools(tmp_path: Path) -> None:
    config_path = _write_project(tmp_path)
    app = create_streamable_http_app(
        entities_dir=tmp_path / "entities",
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
        assert "list_analytics_models" in tool_names
        assert "list_insight_recipes" in tool_names
        assert "top_content_items" in tool_names

        models = _tool_payload(
            _rpc(
                client,
                "tools/call",
                {"name": "list_analytics_models", "arguments": {}},
                rpc_id=3,
            )
        )
        assert [model["name"] for model in models["models"]] == ["content_metrics_daily"]

        recipes = _tool_payload(
            _rpc(
                client,
                "tools/call",
                {"name": "list_insight_recipes", "arguments": {}},
                rpc_id=4,
            )
        )
        assert [recipe["name"] for recipe in recipes["recipes"]] == ["top_content_items"]

        content = _tool_payload(
            _rpc(
                client,
                "tools/call",
                {
                    "name": "top_content_items",
                    "arguments": {"metric": "total_engagement", "limit": 1},
                },
                rpc_id=5,
            )
        )
        assert content["rows"][0]["content_id"] == "post-001"
        assert content["limit"] == 1
