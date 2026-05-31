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
from fyrnheim.analytics_query import OrderByInput
from fyrnheim.inspect import build_manifest
from fyrnheim.mcp.analytics_tools import (
    describe_query_syntax as tool_describe_query_syntax,
    discover_property_keys as tool_discover_property_keys,
    list_property_bags as tool_list_property_bags,
    preview_analytics_query_sql as tool_preview_analytics_query_sql,
    query_analytics_model as tool_query_analytics_model,
    sample_property_values as tool_sample_property_values,
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
    def describe_query_syntax() -> dict[str, Any]:
        """Describe query_analytics_model syntax, order_by shape, and examples."""
        return tool_describe_query_syntax()

    @server.tool()
    def list_property_bags(model: str | None = None) -> dict[str, Any]:
        """List declared JSON/property bags, optionally filtered to one model."""
        return tool_list_property_bags(entities_dir, project_path=project_path, model=model)

    @server.tool()
    def discover_property_keys(
        model: str,
        property_bag: str,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """Discover bounded keys for a declared property bag."""
        return tool_discover_property_keys(
            config_path,
            model,
            property_bag,
            limit=limit,
            entities_dir=entities_dir,
            project_path=project_path,
        )

    @server.tool()
    def sample_property_values(
        model: str,
        property_bag: str,
        key: str,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """Sample bounded values for a declared property key."""
        return tool_sample_property_values(
            config_path,
            model,
            property_bag,
            key,
            limit=limit,
            entities_dir=entities_dir,
            project_path=project_path,
        )

    @server.tool()
    def query_analytics_model(
        model: str,
        metrics: list[str],
        dimensions: list[str] | None = None,
        filters: dict[str, Any] | None = None,
        order_by: list[OrderByInput] | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """Run a bounded read-only query over declared model metrics/dimensions.

        Use metric/dimension names exactly as returned by list_metrics/list_dimensions.
        order_by must be a list of objects like [{"field": "reactions", "direction": "desc"}],
        not an object like {"reactions": "desc"}. Use semantic metric names such as
        "reactions", not backing columns such as "reactions_sum_delta". For latest rows,
        use dimensions=["_date"], order_by=[{"field": "_date", "direction": "desc"}], limit=1.
        """
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
        order_by: list[OrderByInput] | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """Preview generated SQL for a bounded analytics model query.

        Uses the same syntax as query_analytics_model and never accepts arbitrary SQL.
        Use this tool to inspect generated SQL when a query_analytics_model call fails.
        """
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
