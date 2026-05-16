"""Project inspection and manifest export helpers."""

from __future__ import annotations

import importlib.util
import json
import logging
import subprocess
import sys
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from fyrnheim.core.activity import ActivityDefinition
from fyrnheim.core.analytics_entity import AnalyticsEntity
from fyrnheim.core.identity import IdentityGraph
from fyrnheim.core.metrics_model import MetricsModel
from fyrnheim.core.source import EventSource, StateSource
from fyrnheim.core.staging_view import StagingView

MANIFEST_SCHEMA_VERSION = "fyrnheim.manifest.v1"

AssetMap = dict[str, list[Any]]


def discover_assets(entities_dir: Path | str, *, strict: bool = True) -> AssetMap:
    """Discover Fyrnheim assets by importing Python files from ``entities_dir``.

    The discovery semantics mirror the CLI: each ``*.py`` file is imported and
    module-level instances, or non-empty lists of instances, of known Fyrnheim
    asset types are collected. Discovery imports project code, so callers that
    inspect untrusted repositories should run it in a sandbox.
    """
    entities_path = Path(entities_dir)
    assets: AssetMap = {
        "sources": [],
        "activities": [],
        "identity_graphs": [],
        "analytics_entities": [],
        "metrics_models": [],
        "staging_views": [],
    }

    if not entities_path.is_dir():
        return assets

    type_map: dict[type[Any], str] = {
        StateSource: "sources",
        EventSource: "sources",
        ActivityDefinition: "activities",
        IdentityGraph: "identity_graphs",
        AnalyticsEntity: "analytics_entities",
        MetricsModel: "metrics_models",
        StagingView: "staging_views",
    }

    project_path = entities_path.resolve().parent
    old_path = list(sys.path)
    if str(project_path) not in sys.path:
        sys.path.insert(0, str(project_path))

    try:
        for py_file in sorted(entities_path.glob("*.py")):
            module_name = f"_fyrnheim_entity_{py_file.stem}"
            spec = importlib.util.spec_from_file_location(module_name, py_file)
            if spec is None or spec.loader is None:
                continue
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            try:
                spec.loader.exec_module(module)  # type: ignore[union-attr]
            except Exception as exc:
                if strict:
                    raise RuntimeError(f"Failed to import {py_file}: {exc}") from exc
                logging.getLogger("fyrnheim").warning(
                    "Failed to import %s: %s", py_file.name, exc
                )
                continue

            for attr_name in dir(module):
                if attr_name.startswith("_"):
                    continue
                value = getattr(module, attr_name)
                _collect_asset(value, assets, type_map)
    finally:
        sys.path[:] = old_path

    for key, values in assets.items():
        assets[key] = _dedupe_and_sort(values)
    return assets


def build_manifest(
    entities_dir: Path | str,
    *,
    project_path: Path | str | None = None,
    include_git: bool = True,
    strict: bool = True,
) -> dict[str, Any]:
    """Build a deterministic, JSON-serializable Fyrnheim project manifest."""
    entities_path = Path(entities_dir).resolve()
    project = Path(project_path).resolve() if project_path else entities_path.parent
    assets = discover_assets(entities_path, strict=strict)

    manifest: dict[str, Any] = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "project_path": str(project),
        "entities_dir": str(entities_path),
        "git_commit": _git_commit(project) if include_git else None,
        "sources": [_source_record(source) for source in assets["sources"]],
        "activities": [_asset_record("activity", activity) for activity in assets["activities"]],
        "identity_graphs": [
            _asset_record("identity_graph", graph) for graph in assets["identity_graphs"]
        ],
        "analytics_entities": [
            _asset_record("analytics_entity", entity)
            for entity in assets["analytics_entities"]
        ],
        "metrics_models": [
            _asset_record("metrics_model", model) for model in assets["metrics_models"]
        ],
        "staging_views": [
            _asset_record("staging_view", view) for view in assets["staging_views"]
        ],
    }
    manifest["edges"] = build_manifest_edges(assets)
    return manifest


def manifest_json(manifest: dict[str, Any]) -> str:
    """Serialize a manifest as deterministic, pretty JSON."""
    return json.dumps(manifest, indent=2, sort_keys=True) + "\n"


def build_manifest_edges(assets: AssetMap) -> list[dict[str, str]]:
    """Derive semantic graph edges between discovered Fyrnheim assets."""
    sources = assets["sources"]
    activities = assets["activities"]
    identity_graphs = assets["identity_graphs"]
    analytics_entities = assets["analytics_entities"]
    metrics_models = assets["metrics_models"]
    staging_views = assets["staging_views"]

    source_names = {source.name for source in sources}
    activity_names = {activity.name for activity in activities}
    identity_names = {graph.name for graph in identity_graphs}
    staging_names = {view.name for view in staging_views}

    edges: list[dict[str, str]] = []

    for view in staging_views:
        for dependency in view.depends_on:
            if dependency in staging_names:
                edges.append(_edge("staging_view", dependency, "staging_view", view.name, "depends_on"))

    for source in sources:
        upstream = getattr(source, "upstream", None)
        if upstream is not None and upstream.name in staging_names:
            edges.append(_edge("staging_view", upstream.name, "source", source.name, "upstream"))
        for join in getattr(source, "joins", []):
            if join.source_name in source_names:
                edges.append(_edge("source", join.source_name, "source", source.name, "join"))

    for activity in activities:
        if activity.source in source_names:
            edges.append(_edge("source", activity.source, "activity", activity.name, "activity_source"))

    for graph in identity_graphs:
        for graph_source in graph.sources:
            if graph_source.source in source_names:
                edges.append(_edge("source", graph_source.source, "identity_graph", graph.name, "identity_source"))

    for entity in analytics_entities:
        if entity.identity_graph and entity.identity_graph in identity_names:
            edges.append(_edge("identity_graph", entity.identity_graph, "analytics_entity", entity.name, "identity_graph"))
        for state_field in entity.state_fields:
            if state_field.source in source_names:
                edges.append(_edge("source", state_field.source, "analytics_entity", entity.name, "state_field"))
        for measure in entity.measures:
            if measure.activity in activity_names:
                edges.append(_edge("activity", measure.activity, "analytics_entity", entity.name, "measure"))

    for model in metrics_models:
        for source_name in model.sources:
            if source_name in source_names:
                edges.append(_edge("source", source_name, "metrics_model", model.name, "metrics_source"))

    unique = {json.dumps(edge, sort_keys=True): edge for edge in edges}
    return [unique[key] for key in sorted(unique)]


def _collect_asset(value: Any, assets: AssetMap, type_map: dict[type[Any], str]) -> None:
    for asset_type, key in type_map.items():
        if isinstance(value, asset_type):
            assets[key].append(value)
            return

    if isinstance(value, list) and value:
        for asset_type, key in type_map.items():
            if isinstance(value[0], asset_type):
                assets[key].extend(value)
                return


def _dedupe_and_sort(values: list[Any]) -> list[Any]:
    seen_names: set[str] = set()
    deduped: list[Any] = []
    for item in values:
        item_name = getattr(item, "name", None) or str(id(item))
        if item_name not in seen_names:
            seen_names.add(item_name)
            deduped.append(item)
    return sorted(deduped, key=lambda item: getattr(item, "name", ""))


def _source_record(source: StateSource | EventSource) -> dict[str, Any]:
    source_type = "state_source" if isinstance(source, StateSource) else "event_source"
    return _asset_record(source_type, source)


def _asset_record(asset_type: str, asset: BaseModel) -> dict[str, Any]:
    data = asset.model_dump(mode="json")
    return {"type": asset_type, "name": data["name"], "config": data}


def _edge(
    from_type: str,
    from_name: str,
    to_type: str,
    to_name: str,
    relationship: str,
) -> dict[str, str]:
    return {
        "from": f"{from_type}:{from_name}",
        "to": f"{to_type}:{to_name}",
        "relationship": relationship,
    }


def _git_commit(project_path: Path) -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=project_path,
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return None
    commit = result.stdout.strip()
    return commit or None
