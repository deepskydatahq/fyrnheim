"""Optional MCP server for Fyrnheim analytics catalog tools."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from fyrnheim.analytics_catalog import (
    build_analytics_catalog,
    describe_dimension as catalog_describe_dimension,
    describe_metric as catalog_describe_metric,
    list_analytics_models as catalog_list_analytics_models,
    list_dimensions as catalog_list_dimensions,
    list_metrics as catalog_list_metrics,
)
from fyrnheim.inspect import build_manifest


def create_server(
    *,
    entities_dir: Path | str = "entities",
    project_path: Path | str | None = None,
) -> Any:
    """Create a FastMCP server exposing Fyrnheim analytics catalog tools."""
    try:
        from mcp.server.fastmcp import FastMCP
    except ImportError as exc:  # pragma: no cover - exercised only without optional extra
        raise RuntimeError(
            "The Fyrnheim MCP server requires the optional 'mcp' extra. "
            "Install with: pip install 'fyrnheim[mcp]'"
        ) from exc

    server = FastMCP("fyrnheim-analytics")

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

    return server


def main() -> None:
    """Run the Fyrnheim analytics MCP server over stdio."""
    parser = argparse.ArgumentParser(description="Run Fyrnheim analytics MCP server")
    parser.add_argument("--entities-dir", default="entities")
    parser.add_argument("--project-path", default=None)
    args = parser.parse_args()

    server = create_server(
        entities_dir=args.entities_dir,
        project_path=args.project_path,
    )
    server.run()


if __name__ == "__main__":
    main()
