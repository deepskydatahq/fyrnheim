"""Tests for read-only insight recipes and MCP-ready tools."""

from __future__ import annotations

from pathlib import Path

import ibis
import pandas as pd
import pytest

from fyrnheim.insights import (
    InsightConfigError,
    list_insight_recipes,
    parse_insight_recipes,
    run_insight_recipe,
)
from fyrnheim.mcp.insight_tools import (
    find_promising_records,
    list_insight_recipes as tool_list_insight_recipes,
    run_insight_recipe as tool_run_insight_recipe,
    top_content_items,
)


def _recipe_config() -> dict:
    return {
        "top_linkedin_posts": {
            "type": "table_top",
            "table": "linkedin_posts",
            "tags": ["content", "linkedin"],
            "columns": [
                "post_id",
                "published_date",
                "text",
                "impressions",
                "total_engagement",
                "engagement_rate_pct",
                "performance_tier",
            ],
            "order_by": {
                "default": "total_engagement",
                "allowed": ["impressions", "total_engagement", "engagement_rate_pct"],
            },
            "filters": {"allowed": ["performance_tier"]},
            "limit": {"default": 2, "max": 3},
        },
        "promising_leads": {
            "type": "joined_top",
            "table": "accounts",
            "right_table": "companies",
            "tags": ["leads"],
            "join": {"right": "companies", "left_on": "email_domain", "right_on": "domain"},
            "columns": [
                "email",
                "company",
                "lead_level",
                "workshop_count",
                "newsletter_subscriptions",
                "industry",
                "is_icp_fit",
            ],
            "order_by": {
                "default": "workshop_count",
                "allowed": ["lead_level", "workshop_count", "newsletter_subscriptions"],
            },
            "filters": {
                "allowed": ["lead_level", "is_icp_fit"],
                "defaults": {"is_icp_fit": True},
            },
            "limit": {"default": 10, "max": 50},
        },
    }


def _duckdb_with_tables(tmp_path: Path) -> tuple[ibis.BaseBackend, Path]:
    db_path = tmp_path / "insights.duckdb"
    conn = ibis.duckdb.connect(str(db_path))
    conn.create_table(
        "linkedin_posts",
        pd.DataFrame(
            [
                {
                    "post_id": "p1",
                    "published_date": "2026-01-01",
                    "text": "solid post",
                    "impressions": 1000,
                    "total_engagement": 30,
                    "engagement_rate_pct": 3.0,
                    "performance_tier": "medium",
                },
                {
                    "post_id": "p2",
                    "published_date": "2026-01-02",
                    "text": "breakout post",
                    "impressions": 5000,
                    "total_engagement": 240,
                    "engagement_rate_pct": 4.8,
                    "performance_tier": "viral",
                },
                {
                    "post_id": "p3",
                    "published_date": "2026-01-03",
                    "text": "efficient post",
                    "impressions": 800,
                    "total_engagement": 80,
                    "engagement_rate_pct": 10.0,
                    "performance_tier": "high",
                },
            ]
        ),
        overwrite=True,
    )
    conn.create_table(
        "accounts",
        pd.DataFrame(
            [
                {
                    "email": "a@acme.com",
                    "email_domain": "acme.com",
                    "company": "acme",
                    "lead_level": "L3",
                    "workshop_count": 2,
                    "newsletter_subscriptions": 1,
                },
                {
                    "email": "b@example.org",
                    "email_domain": "example.org",
                    "company": "example",
                    "lead_level": "L2",
                    "workshop_count": 0,
                    "newsletter_subscriptions": 1,
                },
                {
                    "email": "c@data.co",
                    "email_domain": "data.co",
                    "company": "data",
                    "lead_level": "L4",
                    "workshop_count": 1,
                    "newsletter_subscriptions": 3,
                },
            ]
        ),
        overwrite=True,
    )
    conn.create_table(
        "companies",
        pd.DataFrame(
            [
                {"domain": "acme.com", "industry": "marketing", "is_icp_fit": True},
                {"domain": "example.org", "industry": "education", "is_icp_fit": False},
                {"domain": "data.co", "industry": "analytics", "is_icp_fit": True},
            ]
        ),
        overwrite=True,
    )
    return conn, db_path


def _write_project_config(tmp_path: Path, db_path: Path) -> Path:
    entities_dir = tmp_path / "entities"
    entities_dir.mkdir()
    config_path = tmp_path / "fyrnheim.yaml"
    config_path.write_text(
        f"""
entities_dir: entities
backend: duckdb
backend_config:
  db_path: "{db_path}"
insights:
  recipes:
    top_linkedin_posts:
      type: table_top
      table: linkedin_posts
      tags: [content, linkedin]
      columns: [post_id, published_date, text, impressions, total_engagement, engagement_rate_pct, performance_tier]
      order_by:
        default: total_engagement
        allowed: [impressions, total_engagement, engagement_rate_pct]
      filters:
        allowed: [performance_tier]
      limit:
        default: 2
        max: 3
    promising_leads:
      type: joined_top
      table: accounts
      right_table: companies
      tags: [leads]
      join:
        right: companies
        left_on: email_domain
        right_on: domain
      columns: [email, company, lead_level, workshop_count, newsletter_subscriptions, industry, is_icp_fit]
      order_by:
        default: workshop_count
        allowed: [lead_level, workshop_count, newsletter_subscriptions]
      filters:
        allowed: [lead_level, is_icp_fit]
        defaults:
          is_icp_fit: true
      limit:
        default: 10
        max: 50
""".strip(),
        encoding="utf-8",
    )
    return config_path


def test_parse_insight_recipes_returns_summaries() -> None:
    recipes = parse_insight_recipes(_recipe_config())

    summary = list_insight_recipes(recipes)

    assert summary["schema_version"] == "fyrnheim.insights.v1"
    assert [recipe["name"] for recipe in summary["recipes"]] == [
        "promising_leads",
        "top_linkedin_posts",
    ]
    assert summary["recipes"][1]["order_by"]["default"] == "total_engagement"


def test_table_top_recipe_sorts_filters_and_caps_limit(tmp_path: Path) -> None:
    conn, _ = _duckdb_with_tables(tmp_path)
    recipe = parse_insight_recipes(_recipe_config())["top_linkedin_posts"]

    result = run_insight_recipe(
        conn,
        recipe,
        filters={"performance_tier": "viral"},
        limit=50,
    )

    assert result["limit"] == 3
    assert result["row_count"] == 1
    assert result["rows"][0]["post_id"] == "p2"


def test_table_top_recipe_can_sort_by_allowed_metric(tmp_path: Path) -> None:
    conn, _ = _duckdb_with_tables(tmp_path)
    recipe = parse_insight_recipes(_recipe_config())["top_linkedin_posts"]

    result = run_insight_recipe(conn, recipe, order_by="engagement_rate_pct", limit=2)

    assert [row["post_id"] for row in result["rows"]] == ["p3", "p2"]


def test_joined_top_recipe_applies_default_filter(tmp_path: Path) -> None:
    conn, _ = _duckdb_with_tables(tmp_path)
    recipe = parse_insight_recipes(_recipe_config())["promising_leads"]

    result = run_insight_recipe(conn, recipe)

    assert [row["email"] for row in result["rows"]] == ["a@acme.com", "c@data.co"]
    assert {row["is_icp_fit"] for row in result["rows"]} == {True}


def test_recipe_rejects_undeclared_order_and_filters(tmp_path: Path) -> None:
    conn, _ = _duckdb_with_tables(tmp_path)
    recipe = parse_insight_recipes(_recipe_config())["top_linkedin_posts"]

    with pytest.raises(InsightConfigError, match="order_by"):
        run_insight_recipe(conn, recipe, order_by="text")
    with pytest.raises(InsightConfigError, match="Filter"):
        run_insight_recipe(conn, recipe, filters={"text": "x"})


def test_recipe_parse_rejects_unknown_order_column() -> None:
    raw = _recipe_config()
    raw["top_linkedin_posts"]["order_by"]["default"] = "missing"
    raw["top_linkedin_posts"]["order_by"]["allowed"] = ["missing"]

    with pytest.raises(InsightConfigError, match="non-selected"):
        parse_insight_recipes(raw)


def test_mcp_ready_tools_run_against_project_config(tmp_path: Path) -> None:
    _, db_path = _duckdb_with_tables(tmp_path)
    config_path = _write_project_config(tmp_path, db_path)

    recipes = tool_list_insight_recipes(config_path)
    content = top_content_items(config_path=config_path, metric="impressions", limit=1)
    leads = find_promising_records(config_path=config_path)
    direct = tool_run_insight_recipe(
        "top_linkedin_posts",
        config_path=config_path,
        filters={"performance_tier": "high"},
    )

    assert len(recipes["recipes"]) == 2
    assert content["rows"][0]["post_id"] == "p2"
    assert [row["email"] for row in leads["rows"]] == ["a@acme.com", "c@data.co"]
    assert direct["rows"][0]["post_id"] == "p3"
