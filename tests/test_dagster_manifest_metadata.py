"""Tests for Dagster-compatible manifest metadata helpers."""

from __future__ import annotations

from pathlib import Path

import fyrnheim.integrations.dagster as dagster_helpers
from fyrnheim.inspect import build_manifest
from fyrnheim.integrations.dagster import (
    build_manifest_metadata,
    manifest_hash,
    manifest_metadata,
)


def _write_entities(tmp_path: Path, *, extra_activity: bool = False) -> Path:
    entities_dir = tmp_path / "entities"
    entities_dir.mkdir(exist_ok=True)
    extra = """
extra_signup = ActivityDefinition(
    name="extra_signup",
    source="web_events",
    trigger=EventOccurred(event_type="extra_signup"),
    entity_id_field="entity_id",
)
""" if extra_activity else ""
    (entities_dir / "project.py").write_text(
        f"""
from fyrnheim import (
    ActivityDefinition,
    AnalyticsEntity,
    EventOccurred,
    EventSource,
    IdentityGraph,
    IdentitySource,
    Measure,
    MetricField,
    MetricsModel,
    StateField,
    StateSource,
)

accounts = StateSource(
    name="accounts",
    project="p",
    dataset="d",
    table="accounts",
    id_field="account_id",
)
web_events = EventSource(
    name="web_events",
    project="p",
    dataset="d",
    table="events",
    entity_id_field="account_id",
    timestamp_field="occurred_at",
    event_type_field="event_name",
)
signup = ActivityDefinition(
    name="signup",
    source="web_events",
    trigger=EventOccurred(event_type="signup"),
    entity_id_field="entity_id",
)
{extra}
person = IdentityGraph(
    name="person",
    canonical_id="person_id",
    sources=[
        IdentitySource(source="accounts", id_field="account_id", match_key_field="email"),
        IdentitySource(source="web_events", id_field="account_id", match_key_field="email"),
    ],
)
account_entity = AnalyticsEntity(
    name="account",
    identity_graph="person",
    state_fields=[StateField(name="plan", source="accounts", field="plan", strategy="latest")],
    measures=[Measure(name="signup_count", activity="signup", aggregation="count")],
)
engagement = MetricsModel(
    name="engagement",
    sources=["web_events"],
    grain="daily",
    metric_fields=[MetricField(field_name="signup", aggregation="count")],
)
""".strip(),
        encoding="utf-8",
    )
    return entities_dir


def test_manifest_hash_is_deterministic(tmp_path: Path) -> None:
    entities_dir = _write_entities(tmp_path)
    manifest = build_manifest(entities_dir, project_path=tmp_path, include_git=False)

    assert manifest_hash(manifest) == manifest_hash(manifest)


def test_manifest_hash_changes_when_manifest_changes(tmp_path: Path) -> None:
    entities_dir = _write_entities(tmp_path)
    first = build_manifest(entities_dir, project_path=tmp_path, include_git=False)

    _write_entities(tmp_path, extra_activity=True)
    second = build_manifest(entities_dir, project_path=tmp_path, include_git=False)

    assert manifest_hash(first) != manifest_hash(second)


def test_manifest_metadata_contains_correlation_fields(tmp_path: Path) -> None:
    entities_dir = _write_entities(tmp_path)
    manifest = build_manifest(entities_dir, project_path=tmp_path, include_git=False)

    metadata = manifest_metadata(manifest)

    assert metadata["fyrnheim_schema_version"] == "fyrnheim.manifest.v1"
    assert metadata["fyrnheim_manifest_hash"] == manifest_hash(manifest)
    assert metadata["fyrnheim_git_commit"] is None
    assert metadata["fyrnheim_project_path"] == str(tmp_path.resolve())
    assert metadata["fyrnheim_entities_dir"] == str(entities_dir.resolve())
    assert metadata["fyrnheim_source_count"] == 2
    assert metadata["fyrnheim_activity_count"] == 1
    assert metadata["fyrnheim_identity_graph_count"] == 1
    assert metadata["fyrnheim_analytics_entity_count"] == 1
    assert metadata["fyrnheim_metrics_model_count"] == 1
    assert metadata["fyrnheim_staging_view_count"] == 0
    assert metadata["fyrnheim_edge_count"] == 7
    assert metadata["fyrnheim_asset_counts"] == {
        "sources": 2,
        "activities": 1,
        "identity_graphs": 1,
        "analytics_entities": 1,
        "metrics_models": 1,
        "staging_views": 0,
        "edges": 7,
    }


def test_build_manifest_metadata_builds_from_entities_dir(tmp_path: Path) -> None:
    entities_dir = _write_entities(tmp_path)

    metadata = build_manifest_metadata(
        entities_dir,
        project_path=tmp_path,
        include_git=False,
    )

    assert metadata["fyrnheim_schema_version"] == "fyrnheim.manifest.v1"
    assert isinstance(metadata["fyrnheim_manifest_hash"], str)


def test_dagster_integration_does_not_import_dagster_dependency() -> None:
    assert "dagster" not in dagster_helpers.__dict__
