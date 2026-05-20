"""Optional MCP server for Fyrnheim analytics catalog tools."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from fyrnheim.analytics_catalog import (
    build_analytics_catalog,
    describe_analytics_model as catalog_describe_analytics_model,
    describe_dimension as catalog_describe_dimension,
    describe_metric as catalog_describe_metric,
    list_analytics_models as catalog_list_analytics_models,
    list_dimensions as catalog_list_dimensions,
    list_metrics as catalog_list_metrics,
)
from fyrnheim.inspect import build_manifest
from fyrnheim.mcp.analytics_tools import (
    preview_analytics_query_sql as tool_preview_analytics_query_sql,
    query_analytics_model as tool_query_analytics_model,
)
from fyrnheim.mcp.insight_tools import (
    find_promising_records as tool_find_promising_records,
    list_insight_recipes as tool_list_insight_recipes,
    run_insight_recipe as tool_run_insight_recipe,
    top_content_items as tool_top_content_items,
)


def create_server(
    *,
    entities_dir: Path | str = "entities",
    project_path: Path | str | None = None,
    config_path: Path | str = "fyrnheim.yaml",
    host: str = "127.0.0.1",
    port: int = 8000,
    streamable_http_path: str = "/mcp",
    json_response: bool = False,
    stateless_http: bool = False,
) -> Any:
    """Create a FastMCP server exposing Fyrnheim analytics catalog tools."""
    try:
        from mcp.server.fastmcp import FastMCP
    except ImportError as exc:  # pragma: no cover - exercised only without optional extra
        raise RuntimeError(
            "The Fyrnheim MCP server requires the optional 'mcp' extra. "
            "Install with: pip install 'fyrnheim[mcp]'"
        ) from exc

    server = FastMCP(
        "fyrnheim-analytics",
        host=host,
        port=port,
        streamable_http_path=streamable_http_path,
        json_response=json_response,
        stateless_http=stateless_http,
    )

    def catalog() -> dict[str, Any]:
        manifest = build_manifest(
            entities_dir,
            project_path=project_path,
            include_git=True,
            strict=True,
        )
        return build_analytics_catalog(manifest)

    @server.tool()
    def list_analytics_models() -> dict[str, Any]:
        """List Fyrnheim analytics entities and metrics models."""
        return catalog_list_analytics_models(catalog())

    @server.tool()
    def describe_analytics_model(model: str) -> dict[str, Any]:
        """Describe a Fyrnheim analytics model, including grain and limitations."""
        return catalog_describe_analytics_model(catalog(), model)

    @server.tool()
    def list_metrics(model: str | None = None) -> dict[str, Any]:
        """List Fyrnheim metrics/measures, optionally filtered to one model."""
        return catalog_list_metrics(catalog(), model=model)

    @server.tool()
    def list_dimensions(model: str | None = None) -> dict[str, Any]:
        """List Fyrnheim dimensions, optionally filtered to one model."""
        return catalog_list_dimensions(catalog(), model=model)

    @server.tool()
    def describe_metric(metric: str, model: str | None = None) -> dict[str, Any]:
        """Describe a Fyrnheim metric by name or metric id."""
        return catalog_describe_metric(catalog(), metric, model=model)

    @server.tool()
    def describe_dimension(dimension: str, model: str | None = None) -> dict[str, Any]:
        """Describe a Fyrnheim dimension by name or dimension id."""
        return catalog_describe_dimension(catalog(), dimension, model=model)

    @server.tool()
    def query_analytics_model(
        model: str,
        metrics: list[str],
        dimensions: list[str] | None = None,
        filters: dict[str, Any] | None = None,
        order_by: list[dict[str, str]] | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """Run a bounded read-only query over declared model metrics/dimensions."""
        return tool_query_analytics_model(
            config_path,
            model,
            metrics,
            dimensions=dimensions,
            filters=filters,
            order_by=order_by,
            limit=limit,
            entities_dir=entities_dir,
            project_path=project_path,
        )

    @server.tool()
    def preview_analytics_query_sql(
        model: str,
        metrics: list[str],
        dimensions: list[str] | None = None,
        filters: dict[str, Any] | None = None,
        order_by: list[dict[str, str]] | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """Preview generated SQL for a bounded analytics model query."""
        return tool_preview_analytics_query_sql(
            config_path,
            model,
            metrics,
            dimensions=dimensions,
            filters=filters,
            order_by=order_by,
            limit=limit,
            entities_dir=entities_dir,
            project_path=project_path,
        )

    @server.tool()
    def list_insight_recipes() -> dict[str, Any]:
        """List configured read-only Fyrnheim insight recipes."""
        return tool_list_insight_recipes(config_path)

    @server.tool()
    def run_insight_recipe(
        recipe: str,
        order_by: str | None = None,
        filters: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """Run a configured read-only Fyrnheim insight recipe."""
        return tool_run_insight_recipe(
            recipe,
            config_path=config_path,
            order_by=order_by,
            filters=filters,
            limit=limit,
        )

    @server.tool()
    def top_content_items(
        recipe: str | None = None,
        metric: str | None = None,
        filters: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """Run a content-tagged read-only insight recipe."""
        return tool_top_content_items(
            config_path=config_path,
            recipe=recipe,
            metric=metric,
            filters=filters,
            limit=limit,
        )

    @server.tool()
    def find_promising_records(
        recipe: str | None = None,
        filters: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """Run a leads/prospects-tagged read-only insight recipe."""
        return tool_find_promising_records(
            config_path=config_path,
            recipe=recipe,
            filters=filters,
            limit=limit,
        )

    return server


def create_streamable_http_app(
    *,
    entities_dir: Path | str = "entities",
    project_path: Path | str | None = None,
    config_path: Path | str = "fyrnheim.yaml",
    host: str = "127.0.0.1",
    port: int = 8000,
    path: str = "/mcp",
    json_response: bool = False,
    stateless_http: bool = False,
) -> Any:
    """Create a Starlette app for Streamable HTTP MCP transport."""
    server = create_server(
        entities_dir=entities_dir,
        project_path=project_path,
        config_path=config_path,
        host=host,
        port=port,
        streamable_http_path=path,
        json_response=json_response,
        stateless_http=stateless_http,
    )
    return server.streamable_http_app()


def _add_project_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--entities-dir", default="entities")
    parser.add_argument("--project-path", default=None)
    parser.add_argument("--config", default="fyrnheim.yaml")


def main() -> None:
    """Run the Fyrnheim analytics MCP server over stdio."""
    parser = argparse.ArgumentParser(description="Run Fyrnheim analytics MCP server over stdio")
    _add_project_args(parser)
    args = parser.parse_args()

    server = create_server(
        entities_dir=args.entities_dir,
        project_path=args.project_path,
        config_path=args.config,
    )
    server.run()


def main_http() -> None:
    """Run the Fyrnheim analytics MCP server over Streamable HTTP."""
    parser = argparse.ArgumentParser(
        description="Run Fyrnheim analytics MCP server over Streamable HTTP"
    )
    _add_project_args(parser)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--path", default="/mcp")
    parser.add_argument(
        "--json-response",
        action="store_true",
        help="Return JSON responses instead of SSE where the MCP SDK supports it.",
    )
    parser.add_argument(
        "--stateless-http",
        action="store_true",
        help="Use stateless HTTP sessions; useful behind some proxies and for tests.",
    )
    args = parser.parse_args()

    server = create_server(
        entities_dir=args.entities_dir,
        project_path=args.project_path,
        config_path=args.config,
        host=args.host,
        port=args.port,
        streamable_http_path=args.path,
        json_response=args.json_response,
        stateless_http=args.stateless_http,
    )
    server.run("streamable-http")


if __name__ == "__main__":
    main()
