"""Validation for MCP container deployment examples."""

from __future__ import annotations

import json
from pathlib import Path

import yaml

from fyrnheim.inspect import build_manifest
from fyrnheim.mcp.insight_tools import list_insight_recipes

EXAMPLE_DIR = Path("examples/mcp-container")
SMOKE_PROJECT = EXAMPLE_DIR / "smoke-project"


def test_container_reference_files_exist_and_use_stdio_invocation() -> None:
    dockerfile = (EXAMPLE_DIR / "Dockerfile").read_text(encoding="utf-8")
    compose = (EXAMPLE_DIR / "compose.yaml").read_text(encoding="utf-8")
    readme = (EXAMPLE_DIR / "README.md").read_text(encoding="utf-8")

    assert "ENTRYPOINT [\"fyr-mcp-analytics\"]" in dockerfile
    assert "--entities-dir" in dockerfile
    assert "--config" in dockerfile
    assert "stdin_open: true" in compose
    assert "./smoke-project:/project:ro" in compose
    assert "docker run --rm -i" in readme
    assert "docker compose run --rm" in readme


def test_mcp_client_config_launches_container_with_project_mount() -> None:
    config = json.loads((EXAMPLE_DIR / "mcp-client-config.json").read_text(encoding="utf-8"))

    server = config["mcpServers"]["fyrnheim-analytics-container"]
    args = server["args"]

    assert server["command"] == "docker"
    assert args[:3] == ["run", "--rm", "-i"]
    assert "type=bind,src=/absolute/path/to/your/fyrnheim-project,dst=/project,readonly" in args
    assert "fyrnheim-mcp-analytics:local" in args
    assert "/project/fyrnheim.yaml" in args


def test_smoke_project_manifest_and_insight_recipe_load() -> None:
    manifest = build_manifest(SMOKE_PROJECT / "entities", project_path=SMOKE_PROJECT, include_git=False)
    recipes = list_insight_recipes(SMOKE_PROJECT / "fyrnheim.yaml")

    source_names = {source["name"] for source in manifest["sources"]}
    metrics_model_names = {model["name"] for model in manifest["metrics_models"]}
    recipe_names = {recipe["name"] for recipe in recipes["recipes"]}

    assert "content_events" in source_names
    assert "content_metrics_daily" in metrics_model_names
    assert recipe_names == {"top_content_items"}
    assert recipes["recipes"][0]["limit"] == {"default": 5, "max": 25}


def test_compose_passes_warehouse_configuration_via_environment() -> None:
    compose = yaml.safe_load((EXAMPLE_DIR / "compose.yaml").read_text(encoding="utf-8"))

    environment = compose["services"]["fyrnheim-mcp-analytics"]["environment"]

    assert environment["FYRNHEIM_CONFIG"] == "/project/fyrnheim.yaml"
    assert "CLICKHOUSE_HOST" in environment
    assert "CLICKHOUSE_PASSWORD" in environment
    assert compose["services"]["fyrnheim-mcp-analytics"]["command"] == [
        "--entities-dir",
        "/project/entities",
        "--project-path",
        "/project",
        "--config",
        "/project/fyrnheim.yaml",
    ]


def test_deployment_doc_covers_secrets_smoke_and_propel_handoff() -> None:
    doc = Path("docs/mcp-container-deployment.md").read_text(encoding="utf-8")

    assert "stdio" in doc
    assert "docker run --rm -i" in doc
    assert "Pass warehouse credentials at runtime through environment variables" in doc
    assert "Smoke verification" in doc
    assert "Propel Data rollout" in doc
    assert "top_linkedin_posts" in doc
