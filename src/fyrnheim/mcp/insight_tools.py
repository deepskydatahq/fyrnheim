"""MCP-ready read-only insight query tools."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fyrnheim.insights import (
    InsightConfigError,
    list_insight_recipes as list_recipe_summaries,
    load_insight_project_config,
    run_project_insight_recipe,
)


def list_insight_recipes(config_path: Path | str = "fyrnheim.yaml") -> dict[str, Any]:
    """List configured read-only insight recipes."""
    project = load_insight_project_config(config_path)
    return list_recipe_summaries(project.recipes)


def run_insight_recipe(
    recipe: str,
    *,
    config_path: Path | str = "fyrnheim.yaml",
    order_by: str | None = None,
    filters: dict[str, Any] | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """Run a configured read-only insight recipe."""
    return run_project_insight_recipe(
        recipe,
        config_path=config_path,
        order_by=order_by,
        filters=filters,
        limit=limit,
    )


def top_content_items(
    *,
    config_path: Path | str = "fyrnheim.yaml",
    recipe: str | None = None,
    metric: str | None = None,
    filters: dict[str, Any] | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """Run a content-tagged table_top insight recipe."""
    selected = _select_recipe(config_path, recipe=recipe, required_tag="content")
    return run_project_insight_recipe(
        selected,
        config_path=config_path,
        order_by=metric,
        filters=filters,
        limit=limit,
    )


def find_promising_records(
    *,
    config_path: Path | str = "fyrnheim.yaml",
    recipe: str | None = None,
    filters: dict[str, Any] | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """Run a leads/prospects-tagged insight recipe."""
    selected = _select_recipe(
        config_path,
        recipe=recipe,
        required_tag="leads",
        fallback_tag="promising_records",
    )
    return run_project_insight_recipe(
        selected,
        config_path=config_path,
        filters=filters,
        limit=limit,
    )


def _select_recipe(
    config_path: Path | str,
    *,
    recipe: str | None,
    required_tag: str,
    fallback_tag: str | None = None,
) -> str:
    project = load_insight_project_config(config_path)
    if recipe is not None:
        if recipe not in project.recipes:
            raise InsightConfigError(f"Unknown insight recipe: {recipe}")
        return recipe

    tags = {required_tag}
    if fallback_tag:
        tags.add(fallback_tag)
    matches = [
        name
        for name, candidate in project.recipes.items()
        if any(tag in candidate.tags for tag in tags)
    ]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise InsightConfigError(
            f"No insight recipe tagged with one of: {', '.join(sorted(tags))}"
        )
    raise InsightConfigError(
        "Multiple matching insight recipes; pass recipe explicitly: "
        + ", ".join(matches)
    )
