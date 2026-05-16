"""Tests for Fyrnheim project manifest export."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from fyrnheim.cli import main
from fyrnheim.inspect import build_manifest, manifest_json


def _write_entities(tmp_path: Path) -> Path:
    entities_dir = tmp_path / "entities"
    entities_dir.mkdir()
    (entities_dir / "project.py").write_text(
        """
from fyrnheim import (
    ActivityDefinition,
    AnalyticsEntity,
    EventOccurred,
    EventSource,
    IdentityGraph,
    IdentitySource,
    Join,
    Measure,
    MetricField,
    MetricsModel,
    RowAppeared,
    StateField,
    StateSource,
    StagingView,
)

base_view = StagingView(
    name="base_accounts",
    project="p",
    dataset="d",
    sql="select * from raw.accounts",
)
accounts_view = StagingView(
    name="accounts_view",
    project="p",
    dataset="d",
    sql="select * from base_accounts",
    depends_on=["base_accounts"],
)
accounts = StateSource(
    name="accounts",
    upstream=accounts_view,
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
    joins=[Join(source_name="accounts", join_key="account_id")],
)
signup = ActivityDefinition(
    name="signup",
    source="web_events",
    trigger=EventOccurred(event_type="signup"),
    entity_id_field="entity_id",
)
account_created = ActivityDefinition(
    name="account_created",
    source="accounts",
    trigger=RowAppeared(),
    entity_id_field="entity_id",
)
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


def test_build_manifest_is_json_serializable_and_contains_assets(tmp_path: Path) -> None:
    entities_dir = _write_entities(tmp_path)

    manifest = build_manifest(entities_dir, project_path=tmp_path, include_git=False)

    assert manifest["schema_version"] == "fyrnheim.manifest.v1"
    assert manifest["project_path"] == str(tmp_path.resolve())
    assert manifest["entities_dir"] == str(entities_dir.resolve())
    assert manifest["git_commit"] is None
    assert [source["name"] for source in manifest["sources"]] == [
        "accounts",
        "web_events",
    ]
    assert manifest["sources"][0]["type"] == "state_source"
    assert manifest["sources"][1]["type"] == "event_source"
    assert [activity["name"] for activity in manifest["activities"]] == [
        "account_created",
        "signup",
    ]
    assert manifest["identity_graphs"][0]["name"] == "person"
    assert manifest["analytics_entities"][0]["name"] == "account"
    assert manifest["metrics_models"][0]["name"] == "engagement"
    assert [view["name"] for view in manifest["staging_views"]] == [
        "accounts_view",
        "base_accounts",
    ]

    assert json.loads(manifest_json(manifest)) == manifest


def test_build_manifest_derives_semantic_edges(tmp_path: Path) -> None:
    entities_dir = _write_entities(tmp_path)

    manifest = build_manifest(entities_dir, project_path=tmp_path, include_git=False)
    edges = {
        (edge["from"], edge["to"], edge["relationship"])
        for edge in manifest["edges"]
    }

    assert (
        "staging_view:base_accounts",
        "staging_view:accounts_view",
        "depends_on",
    ) in edges
    assert ("staging_view:accounts_view", "source:accounts", "upstream") in edges
    assert ("source:accounts", "source:web_events", "join") in edges
    assert ("source:web_events", "activity:signup", "activity_source") in edges
    assert ("source:accounts", "activity:account_created", "activity_source") in edges
    assert ("source:accounts", "identity_graph:person", "identity_source") in edges
    assert ("source:web_events", "identity_graph:person", "identity_source") in edges
    assert (
        "identity_graph:person",
        "analytics_entity:account",
        "identity_graph",
    ) in edges
    assert ("source:accounts", "analytics_entity:account", "state_field") in edges
    assert ("activity:signup", "analytics_entity:account", "measure") in edges
    assert ("source:web_events", "metrics_model:engagement", "metrics_source") in edges


def test_manifest_cli_emits_deterministic_json(tmp_path: Path) -> None:
    entities_dir = _write_entities(tmp_path)
    runner = CliRunner()

    result = runner.invoke(
        main,
        [
            "manifest",
            "--entities-dir",
            str(entities_dir),
            "--project-path",
            str(tmp_path),
            "--no-git",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["schema_version"] == "fyrnheim.manifest.v1"
    assert payload["sources"][0]["name"] == "accounts"
    assert payload["edges"] == sorted(
        payload["edges"], key=lambda edge: json.dumps(edge, sort_keys=True)
    )


def test_manifest_cli_reports_import_errors(tmp_path: Path) -> None:
    entities_dir = tmp_path / "entities"
    entities_dir.mkdir()
    (entities_dir / "broken.py").write_text("raise RuntimeError('boom')\n")
    runner = CliRunner()

    result = runner.invoke(main, ["manifest", "--entities-dir", str(entities_dir)])

    assert result.exit_code != 0
    assert "Failed to import" in result.output
    assert "boom" in result.output
