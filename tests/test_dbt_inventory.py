from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from fyrnheim.cli import main
from fyrnheim.dbt_inventory import (
    DBT_INVENTORY_SCHEMA_VERSION,
    inventory_from_manifest,
    inventory_from_project_files,
    inventory_summary,
    load_dbt_inventory,
)


def _write_manifest(path: Path) -> Path:
    target = path / "target"
    target.mkdir(parents=True)
    manifest_path = target / "manifest.json"
    manifest = {
        "metadata": {
            "project_name": "jaffle_shop",
            "dbt_version": "1.8.0",
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
            "generated_at": "2026-06-20T00:00:00Z",
        },
        "nodes": {
            "model.jaffle_shop.stg_customers": {
                "resource_type": "model",
                "name": "stg_customers",
                "package_name": "jaffle_shop",
                "path": "staging/stg_customers.sql",
                "original_file_path": "models/staging/stg_customers.sql",
                "config": {"materialized": "view", "meta": {"owner": "analytics"}},
                "tags": ["staging"],
                "description": "Cleaned customers",
                "meta": {"domain": "core"},
                "columns": {
                    "customer_id": {
                        "name": "customer_id",
                        "description": "Customer id",
                        "data_tests": ["not_null"],
                    }
                },
                "depends_on": {"nodes": ["source.jaffle_shop.raw.customers"]},
            },
            "model.jaffle_shop.fct_orders": {
                "resource_type": "model",
                "name": "fct_orders",
                "package_name": "jaffle_shop",
                "path": "marts/fct_orders.sql",
                "original_file_path": "models/marts/fct_orders.sql",
                "config": {"materialized": "table"},
                "tags": ["mart"],
                "description": "Orders fact",
                "meta": {},
                "columns": {},
                "depends_on": {"nodes": ["model.jaffle_shop.stg_customers"]},
            },
            "test.jaffle_shop.not_null_stg_customers_customer_id": {
                "resource_type": "test",
                "name": "not_null_stg_customers_customer_id",
                "package_name": "jaffle_shop",
                "original_file_path": "models/staging/schema.yml",
                "depends_on": {"nodes": ["model.jaffle_shop.stg_customers"]},
            },
        },
        "sources": {
            "source.jaffle_shop.raw.customers": {
                "resource_type": "source",
                "name": "customers",
                "source_name": "raw",
                "package_name": "jaffle_shop",
                "original_file_path": "models/sources.yml",
                "description": "Raw customers",
                "tags": ["raw"],
                "meta": {"owner": "data-eng"},
                "columns": {"id": {"name": "id", "description": "id"}},
                "depends_on": {"nodes": []},
            }
        },
        "exposures": {
            "exposure.jaffle_shop.orders_dashboard": {
                "resource_type": "exposure",
                "name": "orders_dashboard",
                "package_name": "jaffle_shop",
                "description": "Dashboard",
                "depends_on": {"nodes": ["model.jaffle_shop.fct_orders"]},
            }
        },
        "metrics": {
            "metric.jaffle_shop.orders": {
                "resource_type": "metric",
                "name": "orders",
                "package_name": "jaffle_shop",
                "depends_on": {"nodes": ["model.jaffle_shop.fct_orders"]},
            }
        },
        "macros": {
            "macro.jaffle_shop.cents_to_dollars": {
                "resource_type": "macro",
                "name": "cents_to_dollars",
                "package_name": "jaffle_shop",
                "original_file_path": "macros/cents_to_dollars.sql",
            }
        },
    }
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return manifest_path


def _write_project_files(path: Path) -> None:
    (path / "models" / "staging").mkdir(parents=True)
    (path / "models" / "marts").mkdir(parents=True)
    (path / "dbt_project.yml").write_text("name: jaffle_shop\n", encoding="utf-8")
    (path / "models" / "staging" / "stg_customers.sql").write_text(
        "{{ config(materialized='view') }}\nselect * from {{ source('raw', 'customers') }}",
        encoding="utf-8",
    )
    (path / "models" / "marts" / "fct_orders.sql").write_text(
        "select * from {{ ref('stg_customers') }}", encoding="utf-8"
    )
    (path / "models" / "schema.yml").write_text(
        """
version: 2
models:
  - name: stg_customers
    description: Cleaned customers
    tags: [staging]
    meta:
      owner: analytics
    columns:
      - name: customer_id
        description: Customer id
        data_tests: [not_null]
sources:
  - name: raw
    description: Raw source
    tables:
      - name: customers
        description: Raw customers
        tests: [freshness]
""".strip(),
        encoding="utf-8",
    )


def test_inventory_from_manifest_extracts_resources_metadata_and_lineage(tmp_path: Path) -> None:
    manifest_path = _write_manifest(tmp_path)

    inventory = inventory_from_manifest(manifest_path, project_path=tmp_path)

    assert inventory["schema_version"] == DBT_INVENTORY_SCHEMA_VERSION
    assert inventory["generation_source"] == "manifest"
    assert inventory["project"]["name"] == "jaffle_shop"
    assert inventory["metadata"]["dbt_version"] == "1.8.0"

    models = {model["name"]: model for model in inventory["resources"]["models"]}
    assert models["stg_customers"]["materialized"] == "view"
    assert models["stg_customers"]["owner"] == "analytics"
    assert models["stg_customers"]["meta"] == {"owner": "analytics", "domain": "core"}
    assert models["stg_customers"]["depends_on"] == ["source.jaffle_shop.raw.customers"]
    assert models["stg_customers"]["downstream"] == [
        "model.jaffle_shop.fct_orders",
        "test.jaffle_shop.not_null_stg_customers_customer_id",
    ]
    assert models["stg_customers"]["attached_tests"] == [
        "test.jaffle_shop.not_null_stg_customers_customer_id"
    ]
    assert models["stg_customers"]["columns"] == [
        {"name": "customer_id", "description": "Customer id", "tests": ["not_null"]}
    ]

    assert inventory["resources"]["sources"][0]["downstream"] == [
        "model.jaffle_shop.stg_customers"
    ]
    assert len(inventory["resources"]["tests"]) == 1
    assert len(inventory["resources"]["exposures"]) == 1
    assert len(inventory["resources"]["metrics"]) == 1
    assert len(inventory["resources"]["macros"]) == 1


def test_load_dbt_inventory_prefers_manifest(tmp_path: Path) -> None:
    _write_manifest(tmp_path)
    _write_project_files(tmp_path)

    inventory = load_dbt_inventory(tmp_path)

    assert inventory["generation_source"] == "manifest"
    paths = {model["path"] for model in inventory["resources"]["models"]}
    assert "staging/stg_customers.sql" in paths


def test_inventory_from_project_files_discovers_models_yaml_sources_and_lineage(tmp_path: Path) -> None:
    _write_project_files(tmp_path)

    inventory = inventory_from_project_files(tmp_path)

    assert inventory["schema_version"] == DBT_INVENTORY_SCHEMA_VERSION
    assert inventory["generation_source"] == "project_files"
    assert inventory["project"]["name"] == "jaffle_shop"
    assert inventory["warnings"]

    models = {model["name"]: model for model in inventory["resources"]["models"]}
    assert models["stg_customers"]["materialized"] == "view"
    assert models["stg_customers"]["description"] == "Cleaned customers"
    assert models["stg_customers"]["owner"] == "analytics"
    assert models["stg_customers"]["depends_on"] == ["source.jaffle_shop.raw.customers"]
    assert models["stg_customers"]["downstream"] == ["model.jaffle_shop.fct_orders"]
    assert models["fct_orders"]["depends_on"] == ["model.jaffle_shop.stg_customers"]

    sources = inventory["resources"]["sources"]
    assert sources[0]["unique_id"] == "source.jaffle_shop.raw.customers"
    assert sources[0]["description"] == "Raw customers"
    assert sources[0]["downstream"] == ["model.jaffle_shop.stg_customers"]


def test_inventory_summary_is_concise(tmp_path: Path) -> None:
    manifest_path = _write_manifest(tmp_path)
    inventory = inventory_from_manifest(manifest_path, project_path=tmp_path)

    summary = inventory_summary(inventory)

    assert "dbt project: jaffle_shop" in summary
    assert "source: manifest" in summary
    assert "models=2" in summary
    assert "sources=1" in summary


def test_dbt_scan_cli_writes_output_and_prints_summary(tmp_path: Path) -> None:
    _write_project_files(tmp_path)
    output = tmp_path / "inventory.json"

    result = CliRunner().invoke(
        main,
        ["dbt", "scan", "--project-path", str(tmp_path), "--output", str(output)],
    )

    assert result.exit_code == 0, result.output
    assert "dbt project: jaffle_shop" in result.output
    assert output.exists()
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["schema_version"] == DBT_INVENTORY_SCHEMA_VERSION
    assert payload["generation_source"] == "project_files"


def test_dbt_scan_cli_can_print_json(tmp_path: Path) -> None:
    _write_manifest(tmp_path)

    result = CliRunner().invoke(
        main,
        ["dbt", "scan", "--project-path", str(tmp_path), "--format", "json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["schema_version"] == DBT_INVENTORY_SCHEMA_VERSION
    assert payload["generation_source"] == "manifest"
