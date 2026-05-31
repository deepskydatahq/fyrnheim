"""MCP-ready analytics catalog tool functions.

The functions return plain JSON-compatible dictionaries so they can be used by
an MCP server, tests, CLI wrappers, or other agent integrations.
"""

from __future__ import annotations

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
from fyrnheim.analytics_query import (
    OrderByInput,
    describe_query_syntax as query_syntax_contract,
    discover_property_keys as query_discover_property_keys,
    list_property_bags as query_list_property_bags,
    load_query_project,
    preview_project_analytics_query_sql,
    run_project_analytics_query,
    sample_property_values as query_sample_property_values,
)
from fyrnheim.inspect import build_manifest


def load_catalog(
    entities_dir: Path | str,
    *,
    project_path: Path | str | None = None,
    include_git: bool = True,
    strict: bool = True,
) -> dict[str, Any]:
    """Build the analytics catalog for a Fyrnheim project."""
    manifest = build_manifest(
        entities_dir,
        project_path=project_path,
        include_git=include_git,
        strict=strict,
    )
    return build_analytics_catalog(manifest)


def list_analytics_models(
    entities_dir: Path | str,
    *,
    project_path: Path | str | None = None,
) -> dict[str, Any]:
    """List analytics entities and metrics models with metric/dimension names."""
    return catalog_list_analytics_models(load_catalog(entities_dir, project_path=project_path))


def describe_analytics_model(
    entities_dir: Path | str,
    model: str,
    *,
    project_path: Path | str | None = None,
) -> dict[str, Any]:
    """Describe one analytics model with metrics, dimensions, grain, and limitations."""
    return catalog_describe_analytics_model(
        load_catalog(entities_dir, project_path=project_path),
        model,
    )


def list_metrics(
    entities_dir: Path | str,
    *,
    project_path: Path | str | None = None,
    model: str | None = None,
) -> dict[str, Any]:
    """List all metrics/measures, optionally filtered to a model."""
    return catalog_list_metrics(
        load_catalog(entities_dir, project_path=project_path),
        model=model,
    )


def list_dimensions(
    entities_dir: Path | str,
    *,
    project_path: Path | str | None = None,
    model: str | None = None,
) -> dict[str, Any]:
    """List all dimensions, optionally filtered to a model."""
    return catalog_list_dimensions(
        load_catalog(entities_dir, project_path=project_path),
        model=model,
    )


def describe_metric(
    entities_dir: Path | str,
    metric: str,
    *,
    project_path: Path | str | None = None,
    model: str | None = None,
) -> dict[str, Any]:
    """Describe one metric by name or metric id."""
    return catalog_describe_metric(
        load_catalog(entities_dir, project_path=project_path),
        metric,
        model=model,
    )


def describe_dimension(
    entities_dir: Path | str,
    dimension: str,
    *,
    project_path: Path | str | None = None,
    model: str | None = None,
) -> dict[str, Any]:
    """Describe one dimension by name or dimension id."""
    return catalog_describe_dimension(
        load_catalog(entities_dir, project_path=project_path),
        dimension,
        model=model,
    )


def describe_query_syntax() -> dict[str, Any]:
    """Describe the safe analytics query argument contract with examples."""
    return query_syntax_contract()


def list_property_bags(
    entities_dir: Path | str,
    *,
    project_path: Path | str | None = None,
    model: str | None = None,
) -> dict[str, Any]:
    """List declared property bags, optionally filtered to one model."""
    return query_list_property_bags(load_catalog(entities_dir, project_path=project_path), model=model)


def discover_property_keys(
    config_path: Path | str,
    model: str,
    property_bag: str,
    *,
    limit: int | None = None,
    entities_dir: Path | str | None = None,
    project_path: Path | str | None = None,
) -> dict[str, Any]:
    """Discover bounded keys for a declared property bag."""
    project = load_query_project(
        config_path,
        entities_dir=entities_dir,
        project_path=project_path,
    )
    with project.executor:
        return query_discover_property_keys(
            project.catalog,
            project.executor.connection,
            model=model,
            property_bag=property_bag,
            limit=limit,
        )


def sample_property_values(
    config_path: Path | str,
    model: str,
    property_bag: str,
    key: str,
    *,
    limit: int | None = None,
    entities_dir: Path | str | None = None,
    project_path: Path | str | None = None,
) -> dict[str, Any]:
    """Sample bounded values for a declared property key."""
    project = load_query_project(
        config_path,
        entities_dir=entities_dir,
        project_path=project_path,
    )
    with project.executor:
        return query_sample_property_values(
            project.catalog,
            project.executor.connection,
            model=model,
            property_bag=property_bag,
            key=key,
            limit=limit,
        )


def query_analytics_model(
    config_path: Path | str,
    model: str,
    metrics: list[str],
    *,
    dimensions: list[str] | None = None,
    filters: dict[str, Any] | None = None,
    order_by: list[OrderByInput] | None = None,
    limit: int | None = None,
    entities_dir: Path | str | None = None,
    project_path: Path | str | None = None,
) -> dict[str, Any]:
    """Run a safe bounded analytics model query over declared fields."""
    return run_project_analytics_query(
        config_path,
        model=model,
        metrics=metrics,
        dimensions=dimensions,
        filters=filters,
        order_by=order_by,
        limit=limit,
        entities_dir=entities_dir,
        project_path=project_path,
    )


def preview_analytics_query_sql(
    config_path: Path | str,
    model: str,
    metrics: list[str],
    *,
    dimensions: list[str] | None = None,
    filters: dict[str, Any] | None = None,
    order_by: list[OrderByInput] | None = None,
    limit: int | None = None,
    entities_dir: Path | str | None = None,
    project_path: Path | str | None = None,
) -> dict[str, Any]:
    """Preview SQL for a safe analytics model query over declared fields."""
    return preview_project_analytics_query_sql(
        config_path,
        model=model,
        metrics=metrics,
        dimensions=dimensions,
        filters=filters,
        order_by=order_by,
        limit=limit,
        entities_dir=entities_dir,
        project_path=project_path,
    )
