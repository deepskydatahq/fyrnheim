from __future__ import annotations

import json
from pathlib import Path

import yaml
from click.testing import CliRunner

from fyrnheim.cli import main
from fyrnheim.dbt_classification import (
    DBT_CLASSIFICATION_SCHEMA_VERSION,
    DEFAULT_JOBS_TAXONOMY_VERSION,
    classification_summary,
    classify_inventory,
    load_default_classification_rules,
)


def _inventory() -> dict:
    return {
        "schema_version": "fyrnheim.dbt_inventory.v1",
        "generation_source": "manifest",
        "project": {"name": "jaffle_shop", "path": "/tmp/jaffle"},
        "resources": {
            "models": [
                {
                    "unique_id": "model.jaffle_shop.stg_customers",
                    "name": "stg_customers",
                    "path": "models/staging/stg_customers.sql",
                    "materialized": "view",
                    "tags": ["staging"],
                    "meta": {"domain": "core", "owner": "analytics"},
                    "depends_on": ["source.jaffle_shop.raw.customers"],
                    "downstream": ["model.jaffle_shop.fct_orders"],
                },
                {
                    "unique_id": "model.jaffle_shop.fct_orders",
                    "name": "fct_orders",
                    "path": "models/marts/fct_orders.sql",
                    "materialized": "table",
                    "tags": ["mart", "finance"],
                    "meta": {"domain": "finance"},
                    "depends_on": ["model.jaffle_shop.stg_customers"],
                    "downstream": ["exposure.jaffle_shop.orders_dashboard"],
                },
                {
                    "unique_id": "model.jaffle_shop.tmp_debug",
                    "name": "tmp_debug",
                    "path": "models/tmp_debug.sql",
                    "materialized": "view",
                    "tags": [],
                    "meta": {},
                    "depends_on": [],
                    "downstream": [],
                },
            ]
        },
        "warnings": [],
    }


def _rules(*, allow_multiple: bool = False) -> dict:
    return {
        "schema_version": "fyrnheim.dbt_classification_rules.v1",
        "allow_multiple_labels": allow_multiple,
        "rules": [
            {
                "id": "staging-path",
                "label": "source_mapping",
                "priority": 10,
                "any": [
                    {"field": "path", "operator": "contains", "value": "/staging/"},
                    {"field": "tags", "operator": "contains", "value": "staging"},
                ],
            },
            {
                "id": "marts-path",
                "label": "analytical_output",
                "priority": 20,
                "all": [
                    {"field": "path", "operator": "contains", "value": "/marts/"},
                    {"field": "downstream", "operator": "count_gte", "value": 1},
                ],
            },
            {
                "id": "finance-tag",
                "label": "finance_domain",
                "priority": 30,
                "all": [{"field": "meta.domain", "operator": "equals", "value": "finance"}],
            },
            {
                "id": "manifest-inventory",
                "label": "manifest_backed",
                "priority": 90,
                "all": [
                    {"field": "inventory.generation_source", "operator": "equals", "value": "manifest"}
                ],
            },
        ],
    }


def test_classify_inventory_emits_schema_labels_and_evidence() -> None:
    result = classify_inventory(_inventory(), _rules())

    assert result["schema_version"] == DBT_CLASSIFICATION_SCHEMA_VERSION
    assert result["summary"]["total"] == 3
    models = {model["name"]: model for model in result["models"]}

    stg = models["stg_customers"]
    assert stg["status"] == "ambiguous"
    assert stg["primary_label"] == "source_mapping"
    assert stg["matched_labels"] == ["source_mapping", "manifest_backed"]
    assert stg["labels"] == ["source_mapping"]
    assert stg["evidence"][0]["rule_id"] == "staging-path"
    assert stg["evidence"][0]["evidence"][0]["field"] in {"path", "tags"}


def test_classification_reports_unclassified_and_precedence() -> None:
    result = classify_inventory(_inventory(), {"rules": _rules()["rules"][:2]})
    models = {model["name"]: model for model in result["models"]}

    assert models["stg_customers"]["status"] == "classified"
    assert models["stg_customers"]["primary_label"] == "source_mapping"
    assert models["fct_orders"]["status"] == "classified"
    assert models["tmp_debug"]["status"] == "unclassified"
    assert result["summary"]["unclassified"] == 1


def test_classification_allows_multiple_labels_when_configured() -> None:
    result = classify_inventory(_inventory(), _rules(allow_multiple=True))
    models = {model["name"]: model for model in result["models"]}

    assert models["fct_orders"]["status"] == "classified"
    assert models["fct_orders"]["labels"] == [
        "analytical_output",
        "finance_domain",
        "manifest_backed",
    ]
    assert result["summary"]["ambiguous"] == 0


def test_classification_supports_materialization_lineage_name_and_regex() -> None:
    rules = {
        "rules": [
            {
                "label": "table_fact",
                "all": [
                    {"field": "materialized", "operator": "equals", "value": "table"},
                    {"field": "name", "operator": "regex", "value": "^fct_"},
                    {"field": "depends_on", "operator": "contains", "value": "model.jaffle_shop.stg_customers"},
                ],
            }
        ]
    }

    result = classify_inventory(_inventory(), rules)
    models = {model["name"]: model for model in result["models"]}

    assert models["fct_orders"]["primary_label"] == "table_fact"
    assert models["stg_customers"]["status"] == "unclassified"


def test_default_jobs_taxonomy_classifies_without_custom_rules() -> None:
    inventory = _inventory()
    inventory["resources"]["models"][0]["attached_tests"] = ["test.jaffle_shop.not_null_stg_customers_customer_id"]

    result = classify_inventory(inventory)
    models = {model["name"]: model for model in result["models"]}

    assert result["rules_source"] == DEFAULT_JOBS_TAXONOMY_VERSION
    assert result["allow_multiple_labels"] is True
    assert models["stg_customers"]["primary_label"] == "source_mapping"
    assert "data_contract_enforcement" in models["stg_customers"]["matched_labels"]
    assert models["stg_customers"]["evidence"][0]["rule_id"] == "default-source-mapping"
    assert models["fct_orders"]["primary_label"] == "analytical_output_shaping"
    assert "analytical_output_shaping" in models["fct_orders"]["matched_labels"]


def test_default_jobs_taxonomy_flags_layer_job_mismatch_evidence() -> None:
    inventory = _inventory()
    inventory["resources"]["models"][1]["path"] = "models/staging/fct_orders.sql"
    inventory["resources"]["models"][1]["tags"] = ["staging"]

    result = classify_inventory(inventory)
    fct_orders = {model["name"]: model for model in result["models"]}["fct_orders"]

    mismatch = fct_orders["layer_job_mismatch_evidence"]
    assert mismatch[0]["check_id"] == "staging-output-job-mismatch"
    assert "analytical_output_shaping" in mismatch[0]["unexpected_labels"]
    assert mismatch[0]["evidence"][0]["field"] in {"path", "tags"}
    assert result["summary"]["layer_job_mismatches"] == 1


def test_load_default_classification_rules_returns_isolated_copy() -> None:
    first = load_default_classification_rules()
    second = load_default_classification_rules()

    first["rules"].clear()

    assert second["rules"]
    assert second["taxonomy"]["source_mapping"]["description"]


def test_classification_summary_is_concise() -> None:
    result = classify_inventory(_inventory(), _rules(allow_multiple=True))

    summary = classification_summary(result)

    assert "dbt project: jaffle_shop" in summary
    assert "models: total=3" in summary
    assert "analytical_output=1" in summary


def test_dbt_classify_cli_uses_default_jobs_taxonomy_without_rules_file(tmp_path: Path) -> None:
    inventory_path = tmp_path / "inventory.json"
    inventory_path.write_text(json.dumps(_inventory()), encoding="utf-8")

    result = CliRunner().invoke(
        main,
        [
            "dbt",
            "classify",
            "--inventory",
            str(inventory_path),
            "--format",
            "json",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["rules_source"] == DEFAULT_JOBS_TAXONOMY_VERSION
    assert payload["summary"]["labels"]["source_mapping"] == 1


def test_dbt_classify_cli_writes_output_and_prints_summary(tmp_path: Path) -> None:
    inventory_path = tmp_path / "inventory.json"
    rules_path = tmp_path / "rules.yml"
    output_path = tmp_path / "classification.json"
    inventory_path.write_text(json.dumps(_inventory()), encoding="utf-8")
    rules_path.write_text(yaml.safe_dump(_rules(allow_multiple=True)), encoding="utf-8")

    result = CliRunner().invoke(
        main,
        [
            "dbt",
            "classify",
            "--inventory",
            str(inventory_path),
            "--rules",
            str(rules_path),
            "--output",
            str(output_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "dbt project: jaffle_shop" in result.output
    assert output_path.exists()
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == DBT_CLASSIFICATION_SCHEMA_VERSION
    assert payload["summary"]["classified"] == 3


def test_dbt_classify_cli_can_print_json(tmp_path: Path) -> None:
    inventory_path = tmp_path / "inventory.json"
    rules_path = tmp_path / "rules.yml"
    inventory_path.write_text(json.dumps(_inventory()), encoding="utf-8")
    rules_path.write_text(yaml.safe_dump(_rules()), encoding="utf-8")

    result = CliRunner().invoke(
        main,
        [
            "dbt",
            "classify",
            "--inventory",
            str(inventory_path),
            "--rules",
            str(rules_path),
            "--format",
            "json",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["schema_version"] == DBT_CLASSIFICATION_SCHEMA_VERSION
    assert payload["summary"]["ambiguous"] == 2
