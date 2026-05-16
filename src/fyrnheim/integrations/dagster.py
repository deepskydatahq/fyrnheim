"""Dagster-compatible manifest metadata helpers.

These helpers intentionally do not import Dagster. They return plain
JSON-serializable values that Dagster jobs can attach to runs, observations, or
materializations using whichever Dagster APIs the project already uses.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from fyrnheim.inspect import build_manifest, manifest_json

_COUNT_KEYS = (
    "sources",
    "activities",
    "identity_graphs",
    "analytics_entities",
    "metrics_models",
    "staging_views",
    "edges",
)


def manifest_hash(manifest: dict[str, Any]) -> str:
    """Return a deterministic SHA-256 hash for a Fyrnheim manifest."""
    return hashlib.sha256(manifest_json(manifest).encode("utf-8")).hexdigest()


def manifest_metadata(manifest: dict[str, Any]) -> dict[str, str | int | dict[str, int] | None]:
    """Return Dagster-compatible metadata for an existing Fyrnheim manifest.

    The returned values are plain JSON-compatible scalars/dicts so callers can
    adapt them to Dagster's metadata APIs without Fyrnheim depending on Dagster.
    """
    counts = {
        key: len(manifest.get(key, []))
        for key in _COUNT_KEYS
    }
    return {
        "fyrnheim_schema_version": _optional_str(manifest.get("schema_version")),
        "fyrnheim_manifest_hash": manifest_hash(manifest),
        "fyrnheim_git_commit": _optional_str(manifest.get("git_commit")),
        "fyrnheim_project_path": _optional_str(manifest.get("project_path")),
        "fyrnheim_entities_dir": _optional_str(manifest.get("entities_dir")),
        "fyrnheim_asset_counts": counts,
        "fyrnheim_source_count": counts["sources"],
        "fyrnheim_activity_count": counts["activities"],
        "fyrnheim_identity_graph_count": counts["identity_graphs"],
        "fyrnheim_analytics_entity_count": counts["analytics_entities"],
        "fyrnheim_metrics_model_count": counts["metrics_models"],
        "fyrnheim_staging_view_count": counts["staging_views"],
        "fyrnheim_edge_count": counts["edges"],
    }


def build_manifest_metadata(
    entities_dir: Path | str,
    *,
    project_path: Path | str | None = None,
    include_git: bool = True,
    strict: bool = True,
) -> dict[str, str | int | dict[str, int] | None]:
    """Build Fyrnheim manifest metadata directly from an entities directory."""
    manifest = build_manifest(
        entities_dir,
        project_path=project_path,
        include_git=include_git,
        strict=strict,
    )
    return manifest_metadata(manifest)


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)
