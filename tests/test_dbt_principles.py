from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from fyrnheim.cli import main
from fyrnheim.dbt_classification import classify_inventory
from fyrnheim.dbt_principles import (
    DBT_PRINCIPLES_SCHEMA_VERSION,
    DEFAULT_PRINCIPLES_VERSION,
    evaluate_principles,
    load_default_principles,
    load_principle_config,
    principles_summary,
)


def _inventory() -> dict:
    return {
        "schema_version": "fyrnheim.dbt_inventory.v1",
        "project": {"name": "jaffle_shop"},
        "resources": {
            "models": [
                {
                    "unique_id": "model.jaffle_shop.stg_customers",
                    "resource_type": "model",
                    "name": "stg_customers",
                    "path": "models/staging/stg_customers.sql",
                    "materialized": "view",
                    "tags": ["staging"],
                    "description": "Maps raw customers into source-shaped columns.",
                    "owner": "analytics",
                    "meta": {},
                    "depends_on": ["source.jaffle.raw.customers"],
                    "downstream": ["model.jaffle_shop.fct_orders"],
                    "attached_tests": ["test.not_null_stg_customers_customer_id"],
                    "columns": [{"name": "customer_id"}],
                },
                {
                    "unique_id": "model.jaffle_shop.fct_orders",
                    "resource_type": "model",
                    "name": "fct_orders",
                    "path": "models/marts/fct_orders.sql",
                    "materialized": "table",
                    "tags": ["mart"],
                    "description": "One row per order for finance analytics.",
                    "owner": "finance",
                    "meta": {"grain": "order_id"},
                    "depends_on": ["model.jaffle_shop.stg_customers"],
                    "downstream": ["exposure.jaffle_shop.orders_dashboard", "metric.jaffle_shop.revenue"],
                    "attached_tests": ["test.not_null_fct_orders_order_id", "test.unique_fct_orders_order_id"],
                    "columns": [{"name": "order_id"}],
                },
                {
                    "unique_id": "model.jaffle_shop.tmp_debug",
                    "resource_type": "model",
                    "name": "tmp_debug",
                    "path": "models/tmp_debug.sql",
                    "materialized": "view",
                    "tags": [],
                    "description": "",
                    "owner": "",
                    "meta": {},
                    "depends_on": [],
                    "downstream": [],
                    "attached_tests": [],
                    "columns": [],
                },
                {
                    "unique_id": "model.jaffle_shop.complex_rollup",
                    "resource_type": "model",
                    "name": "complex_rollup",
                    "path": "models/intermediate/complex_rollup.sql",
                    "materialized": "view",
                    "tags": [],
                    "description": "Combines many sources for a rollup.",
                    "owner": "analytics",
                    "meta": {},
                    "depends_on": [f"model.jaffle_shop.upstream_{index}" for index in range(6)],
                    "downstream": [],
                    "attached_tests": [],
                    "columns": [],
                },
            ]
        },
    }


def _classification() -> dict:
    return classify_inventory(_inventory())


def _find(result: dict, model: str, principle: str) -> dict:
    return next(
        item
        for item in result["findings"]
        if item["model_name"] == model and item["principle_id"] == principle
    )


def test_default_principles_cover_pass_fail_warning_and_not_applicable() -> None:
    result = evaluate_principles(_inventory(), classification=_classification())

    assert result["schema_version"] == DBT_PRINCIPLES_SCHEMA_VERSION
    assert result["principles_source"] == DEFAULT_PRINCIPLES_VERSION
    assert _find(result, "fct_orders", "analytical-output-grain")["status"] == "pass"
    assert _find(result, "tmp_debug", "owner-presence")["status"] == "warning"
    assert _find(result, "tmp_debug", "description-presence")["status"] == "warning"
    assert _find(result, "tmp_debug", "source-contract-tests")["status"] == "not_applicable"
    assert _find(result, "complex_rollup", "complexity-warning")["status"] == "warning"


def test_default_principles_report_error_failures_for_high_risk_models() -> None:
    inventory = _inventory()
    inventory["resources"]["models"][1]["attached_tests"] = []

    result = evaluate_principles(inventory, classification=classify_inventory(inventory))
    high_risk = _find(result, "fct_orders", "high-risk-model-tests")

    assert high_risk["status"] == "fail"
    assert high_risk["severity"] == "error"
    assert high_risk["model_unique_id"] == "model.jaffle_shop.fct_orders"
    assert high_risk["remediation"]
    assert high_risk["evidence"][0]["field"] == "attached_tests"


def test_principle_config_conditions_cover_metadata_path_materialization_lineage_and_classification() -> None:
    config = {
        "principles": [
            {
                "id": "finance-mart-owner",
                "severity": "error",
                "applies_to": {
                    "all": [
                        {"field": "meta.grain", "operator": "exists"},
                        {"field": "path", "operator": "contains", "value": "marts"},
                        {"field": "materialized", "operator": "equals", "value": "table"},
                        {"field": "downstream", "operator": "count_gte", "value": 1},
                        {
                            "field": "classification.labels",
                            "operator": "contains",
                            "value": "analytical_output_shaping",
                        },
                    ]
                },
                "check": {"operator": "present", "field": "owner"},
                "remediation": "Add an owner.",
            }
        ]
    }

    result = evaluate_principles(_inventory(), classification=_classification(), config=config)

    assert _find(result, "fct_orders", "finance-mart-owner")["status"] == "pass"
    assert _find(result, "stg_customers", "finance-mart-owner")["status"] == "not_applicable"


def test_config_error_findings_are_reported() -> None:
    config = {
        "principles": [
            {
                "id": "bad-check",
                "severity": "warning",
                "check": {"operator": "not-supported", "field": "name"},
            }
        ]
    }

    result = evaluate_principles(_inventory(), config=config, model="stg_customers")
    finding = result["findings"][0]

    assert finding["status"] == "config_error"
    assert finding["severity"] == "error"
    assert "Unsupported principle check operator" in finding["message"]


def test_model_filter_and_summary() -> None:
    result = evaluate_principles(_inventory(), classification=_classification(), model="fct_orders")
    summary = principles_summary(result)

    assert result["summary"]["models"] == 1
    assert {item["model_name"] for item in result["findings"]} == {"fct_orders"}
    assert "dbt project: jaffle_shop" in summary
    assert "model: fct_orders" in summary


def test_load_principle_config_yaml_and_default_copy(tmp_path: Path) -> None:
    path = tmp_path / "principles.yml"
    path.write_text(yaml.safe_dump({"principles": [{"id": "owner", "check": {"field": "owner"}}]}), encoding="utf-8")

    loaded = load_principle_config(path)
    first = load_default_principles()
    second = load_default_principles()
    first["principles"].clear()

    assert loaded["principles_source"] == str(path)
    assert second["principles"]


def test_principle_config_rejects_invalid_top_level() -> None:
    with pytest.raises(ValueError, match="principles"):
        evaluate_principles(_inventory(), config={})


def test_dbt_principles_cli_reports_project_and_single_model(tmp_path: Path) -> None:
    inventory_path = tmp_path / "inventory.json"
    classification_path = tmp_path / "classification.json"
    output_path = tmp_path / "principles.json"
    inventory_path.write_text(json.dumps(_inventory()), encoding="utf-8")
    classification_path.write_text(json.dumps(_classification()), encoding="utf-8")

    runner = CliRunner()
    project_result = runner.invoke(
        main,
        [
            "dbt",
            "principles",
            "--inventory",
            str(inventory_path),
            "--classification",
            str(classification_path),
            "--output",
            str(output_path),
        ],
    )
    model_result = runner.invoke(
        main,
        [
            "dbt",
            "principles",
            "--inventory",
            str(inventory_path),
            "--classification",
            str(classification_path),
            "--model",
            "fct_orders",
            "--format",
            "json",
        ],
    )

    assert project_result.exit_code == 0, project_result.output
    assert "principle findings:" in project_result.output
    assert output_path.exists()
    assert model_result.exit_code == 0, model_result.output
    payload = json.loads(model_result.output)
    assert payload["model_filter"] == "fct_orders"
    assert payload["summary"]["models"] == 1
