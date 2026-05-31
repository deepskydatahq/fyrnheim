"""Tests for PropertyBag metadata, discovery, and dynamic queries."""

from __future__ import annotations

from pathlib import Path

import ibis
import pandas as pd
import pytest

from fyrnheim.analytics_catalog import build_analytics_catalog
from fyrnheim.analytics_query import (
    AnalyticsQueryError,
    discover_property_keys,
    infer_property_type,
    preview_analytics_query_sql,
    query_analytics_model,
    sample_property_values,
)
from fyrnheim.core import AnalyticsEntity, Measure, PropertyBag, StateField
from fyrnheim.inspect import build_manifest
from fyrnheim.primitives.json_ops import (
    clickhouse_json_extract_bool,
    clickhouse_json_extract_raw,
    clickhouse_json_extract_string,
    clickhouse_json_property_discovery_sql,
)


def _catalog() -> dict[str, object]:
    manifest = {
        "schema_version": "fyrnheim.manifest.v1",
        "git_commit": None,
        "project_path": None,
        "entities_dir": None,
        "analytics_entities": [
            {
                "type": "analytics_entity",
                "name": "workshop_people",
                "config": {
                    "name": "workshop_people",
                    "identity_graph": None,
                    "state_fields": [
                        {
                            "name": "email",
                            "source": "workshop_attendees",
                            "field": "email",
                            "strategy": "latest",
                            "priority": None,
                        }
                    ],
                    "property_bags": [
                        {
                            "name": "workshop_custom_properties",
                            "source": "workshop_attendees",
                            "field": "custom_properties",
                            "backend_type": "json",
                            "discoverable": True,
                        }
                    ],
                    "measures": [
                        {
                            "name": "attendee_count",
                            "activity": "workshop_attended",
                            "aggregation": "count",
                            "field": None,
                        }
                    ],
                    "computed_fields": [],
                    "quality_checks": [],
                    "materialization": "table",
                    "project": "p",
                    "dataset": "d",
                    "table": "workshop_people",
                },
            }
        ],
        "metrics_models": [],
    }
    return build_analytics_catalog(manifest)


def _duckdb_connection() -> ibis.BaseBackend:
    con = ibis.duckdb.connect()
    con.create_table(
        "workshop_people",
        pd.DataFrame(
            {
                "email": ["a@example.com", "b@example.com", "c@example.com"],
                "attendee_count": [1, 2, 3],
                "custom_properties": [
                    '{"company":"Acme","job_title":"Analyst","has_joined_event":true}',
                    '{"company":"Beta","job_title":"Engineer","has_joined_event":false}',
                    '{"company":"Acme","job_title":"Senior analyst","has_joined_event":true}',
                ],
            }
        ),
    )
    return con


def test_property_bag_primitive_validates_and_exports() -> None:
    bag = PropertyBag(
        name="workshop_custom_properties",
        source="workshop_attendees",
        field="custom_properties",
    )
    entity = AnalyticsEntity(name="workshop_people", property_bags=[bag])

    assert entity.property_bags[0].backend_type == "json"
    assert entity.property_bags[0].discoverable is True
    with pytest.raises(ValueError):
        PropertyBag(name="bad", source="workshop_attendees", field="", backend_type="json")
    with pytest.raises(ValueError):
        PropertyBag(  # type: ignore[arg-type]
            name="bad",
            source="workshop_attendees",
            field="custom_properties",
            backend_type="xml",
        )


def test_property_bag_coexists_with_fields_and_measures() -> None:
    entity = AnalyticsEntity(
        name="workshop_people",
        state_fields=[StateField(name="email", source="attendees", field="email", strategy="latest")],
        property_bags=[
            PropertyBag(name="workshop_custom_properties", source="attendees", field="custom_properties")
        ],
        measures=[Measure(name="attendee_count", activity="attended", aggregation="count")],
    )

    assert len(entity.state_fields) == 1
    assert len(entity.property_bags) == 1
    assert len(entity.measures) == 1


def test_manifest_and_catalog_include_property_bags(tmp_path: Path) -> None:
    entities_dir = tmp_path / "entities"
    entities_dir.mkdir()
    (entities_dir / "workshops.py").write_text(
        """
from fyrnheim import AnalyticsEntity, PropertyBag, StateSource

workshop_attendees = StateSource(
    name="workshop_attendees",
    project="p",
    dataset="d",
    table="workshop_attendees",
    id_field="email",
)
workshop_people = AnalyticsEntity(
    name="workshop_people",
    property_bags=[PropertyBag(
        name="workshop_custom_properties",
        source="workshop_attendees",
        field="custom_properties",
    )],
)
""".strip(),
        encoding="utf-8",
    )

    manifest = build_manifest(entities_dir, project_path=tmp_path, include_git=False)
    config = manifest["analytics_entities"][0]["config"]
    assert config["property_bags"][0]["field"] == "custom_properties"
    assert ("source:workshop_attendees", "analytics_entity:workshop_people", "property_bag") in {
        (edge["from"], edge["to"], edge["relationship"]) for edge in manifest["edges"]
    }

    catalog = build_analytics_catalog(manifest)
    assert catalog["models"][0]["property_bags"][0]["name"] == "workshop_custom_properties"
    assert catalog["property_bags"][0]["field"] == "custom_properties"
    assert "custom_properties" not in [dim["name"] for dim in catalog["dimensions"]]


def test_clickhouse_json_helpers_escape_and_generate_expected_sql() -> None:
    assert (
        clickhouse_json_extract_string("custom_properties", "What company do you work for?")
        == "JSONExtractString(toString(custom_properties), 'What company do you work for?')"
    )
    assert (
        clickhouse_json_extract_bool("custom_properties", "has_joined_event")
        == "JSONExtractBool(toString(custom_properties), 'has_joined_event')"
    )
    assert (
        clickhouse_json_extract_raw("custom_properties", "What is your job title?")
        == "JSONExtractRaw(toString(custom_properties), 'What is your job title?')"
    )
    assert "JSONExtractKeysAndValuesRaw(toString(custom_properties))" in (
        clickhouse_json_property_discovery_sql("workshop_people", "custom_properties")
    )
    assert clickhouse_json_extract_string("custom_properties", "Bob's company").endswith(
        "'Bob''s company')"
    )


def test_property_discovery_sampling_and_type_inference() -> None:
    catalog = _catalog()
    con = _duckdb_connection()

    discovered = discover_property_keys(
        catalog,
        con,
        model="workshop_people",
        property_bag="custom_properties",
        limit=10,
    )
    assert {row["key"] for row in discovered["keys"]} >= {"company", "job_title", "has_joined_event"}

    sample = sample_property_values(
        catalog,
        con,
        model="workshop_people",
        property_bag="workshop_custom_properties",
        key="company",
        limit=2,
    )
    assert sample["inferred_type"] == "string"
    assert set(sample["values"]) == {"Acme", "Beta"}
    assert infer_property_type(["true", "false"]) == "bool"
    assert infer_property_type(["2026-05-31", "2026-06-01"]) == "date-ish"


def test_query_analytics_model_supports_property_dimensions_and_filters() -> None:
    catalog = _catalog()
    con = _duckdb_connection()

    result = query_analytics_model(
        catalog,
        con,
        model="workshop_people",
        metrics=["attendee_count"],
        dimensions=["custom_properties.company"],
        filters={"custom_properties['job_title']": {"contains": "analyst"}},
        order_by=[{"field": "attendee_count", "direction": "desc"}],
        limit=5,
    )

    assert result["rows"] == [{"custom_properties__company": "Acme", "attendee_count": 4}]

    preview = preview_analytics_query_sql(
        catalog,
        con,
        model="workshop_people",
        metrics=["attendee_count"],
        dimensions=["custom_properties.company"],
        limit=5,
    )
    assert "json_extract_string" in preview["sql"]


def test_property_queries_reject_unknown_or_unsafe_keys() -> None:
    catalog = _catalog()
    con = _duckdb_connection()

    with pytest.raises(AnalyticsQueryError, match="Unknown dimension"):
        query_analytics_model(
            catalog,
            con,
            model="workshop_people",
            metrics=["attendee_count"],
            dimensions=["unknown_bag.company"],
        )
    with pytest.raises(AnalyticsQueryError, match="unsafe"):
        sample_property_values(
            catalog,
            con,
            model="workshop_people",
            property_bag="custom_properties",
            key="company; drop table",
        )
