"""Build dbt project inventory artifacts for Fyrnheim's context layer."""

from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

DBT_INVENTORY_SCHEMA_VERSION = "fyrnheim.dbt_inventory.v1"

_REF_RE = re.compile(r"\bref\(\s*['\"]([^'\"]+)['\"]\s*\)")
_SOURCE_RE = re.compile(r"\bsource\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)")
_CONFIG_MATERIALIZED_RE = re.compile(
    r"config\([^)]*materialized\s*=\s*['\"]([^'\"]+)['\"]", re.DOTALL
)


def load_dbt_inventory(
    project_path: Path | str = ".",
    *,
    manifest_path: Path | str | None = None,
) -> dict[str, Any]:
    """Load a dbt inventory from manifest.json or fallback project-file scanning.

    The function does not execute dbt. If ``manifest_path`` is not provided, it looks for
    common dbt artifact locations under ``project_path``. When no manifest exists, it scans
    ``dbt_project.yml`` plus ``models/**/*.sql`` and ``models/**/*.yml``/``.yaml``.
    """

    root = Path(project_path).resolve()
    manifest = Path(manifest_path).resolve() if manifest_path else _find_manifest(root)
    if manifest is not None:
        return inventory_from_manifest(manifest, project_path=root)
    return inventory_from_project_files(root)


def inventory_from_manifest(manifest_path: Path | str, *, project_path: Path | str | None = None) -> dict[str, Any]:
    """Build an inventory artifact from a dbt manifest.json file."""

    manifest_file = Path(manifest_path)
    root = Path(project_path).resolve() if project_path else manifest_file.parent.parent.resolve()
    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    metadata = manifest.get("metadata", {}) if isinstance(manifest.get("metadata"), dict) else {}

    models: list[dict[str, Any]] = []
    sources: list[dict[str, Any]] = []
    tests: list[dict[str, Any]] = []
    exposures: list[dict[str, Any]] = []
    metrics: list[dict[str, Any]] = []
    macros: list[dict[str, Any]] = []

    all_dependencies: dict[str, list[str]] = {}
    test_targets: dict[str, list[str]] = defaultdict(list)

    nodes = _dict_value(manifest, "nodes")
    for unique_id, node in nodes.items():
        if not isinstance(node, dict):
            continue
        resource_type = str(node.get("resource_type", ""))
        depends_on = _depends_on_nodes(node)
        all_dependencies[unique_id] = depends_on
        if resource_type == "test":
            tests.append(_resource_record(unique_id, node, resource_type, depends_on=depends_on))
            for target in depends_on:
                test_targets[target].append(unique_id)

    source_records = _dict_value(manifest, "sources")
    for unique_id, source in source_records.items():
        if not isinstance(source, dict):
            continue
        depends_on = _depends_on_nodes(source)
        all_dependencies[unique_id] = depends_on
        sources.append(_source_record(unique_id, source, depends_on=depends_on))

    for unique_id, node in nodes.items():
        if not isinstance(node, dict):
            continue
        resource_type = str(node.get("resource_type", ""))
        if resource_type == "model":
            models.append(
                _model_record(
                    unique_id,
                    node,
                    depends_on=_depends_on_nodes(node),
                    attached_tests=sorted(test_targets.get(unique_id, [])),
                )
            )

    for unique_id, exposure in _dict_value(manifest, "exposures").items():
        if isinstance(exposure, dict):
            exposures.append(
                _resource_record(
                    unique_id,
                    exposure,
                    "exposure",
                    depends_on=_depends_on_nodes(exposure),
                )
            )
            all_dependencies[unique_id] = _depends_on_nodes(exposure)

    for unique_id, metric in _dict_value(manifest, "metrics").items():
        if isinstance(metric, dict):
            metrics.append(
                _resource_record(unique_id, metric, "metric", depends_on=_depends_on_nodes(metric))
            )
            all_dependencies[unique_id] = _depends_on_nodes(metric)

    for unique_id, macro in _dict_value(manifest, "macros").items():
        if isinstance(macro, dict):
            macros.append(_macro_record(unique_id, macro))

    downstream = _reverse_dependencies(all_dependencies)
    _attach_downstream(models, downstream)
    _attach_downstream(sources, downstream)

    return _artifact(
        project_path=root,
        generation_source="manifest",
        dbt_project_name=str(metadata.get("project_name") or manifest.get("project_name") or ""),
        dbt_schema_version=str(metadata.get("dbt_schema_version") or ""),
        metadata={
            "dbt_version": metadata.get("dbt_version"),
            "generated_at": metadata.get("generated_at"),
            "manifest_path": str(manifest_file),
        },
        models=sorted(models, key=lambda item: item["unique_id"]),
        sources=sorted(sources, key=lambda item: item["unique_id"]),
        tests=sorted(tests, key=lambda item: item["unique_id"]),
        exposures=sorted(exposures, key=lambda item: item["unique_id"]),
        metrics=sorted(metrics, key=lambda item: item["unique_id"]),
        macros=sorted(macros, key=lambda item: item["unique_id"]),
        warnings=[],
    )


def inventory_from_project_files(project_path: Path | str) -> dict[str, Any]:
    """Build a lightweight inventory by scanning dbt project files without dbt."""

    root = Path(project_path).resolve()
    project_yml = root / "dbt_project.yml"
    project_config = _read_yaml(project_yml)
    project_name = str(project_config.get("name") or root.name)
    yaml_metadata = _read_model_yaml(root / "models")

    models: list[dict[str, Any]] = []
    all_dependencies: dict[str, list[str]] = {}

    for sql_file in sorted((root / "models").rglob("*.sql")):
        model_name = sql_file.stem
        unique_id = f"model.{project_name}.{model_name}"
        sql = sql_file.read_text(encoding="utf-8")
        refs = [f"model.{project_name}.{name}" for name in _REF_RE.findall(sql)]
        source_refs = [f"source.{project_name}.{source}.{table}" for source, table in _SOURCE_RE.findall(sql)]
        depends_on = sorted(set(refs + source_refs))
        metadata = yaml_metadata["models"].get(model_name, {})
        materialized_match = _CONFIG_MATERIALIZED_RE.search(sql)
        materialized = materialized_match.group(1) if materialized_match else None
        all_dependencies[unique_id] = depends_on
        models.append(
            {
                "unique_id": unique_id,
                "resource_type": "model",
                "name": model_name,
                "package_name": project_name,
                "path": _relative(sql_file, root),
                "original_file_path": _relative(sql_file, root),
                "materialized": materialized,
                "tags": sorted(set(_string_list(metadata.get("tags")))),
                "description": str(metadata.get("description") or ""),
                "owner": _owner(metadata),
                "meta": _dict_or_empty(metadata.get("meta")),
                "depends_on": depends_on,
                "downstream": [],
                "attached_tests": sorted(set(_string_list(metadata.get("tests")))),
                "columns": _columns(metadata),
            }
        )

    sources: list[dict[str, Any]] = []
    for source in yaml_metadata["sources"]:
        source_name = str(source.get("name") or "")
        for table in source.get("tables", []) if isinstance(source.get("tables"), list) else []:
            if not isinstance(table, dict):
                continue
            table_name = str(table.get("name") or "")
            unique_id = f"source.{project_name}.{source_name}.{table_name}"
            sources.append(
                {
                    "unique_id": unique_id,
                    "resource_type": "source",
                    "name": table_name,
                    "source_name": source_name,
                    "package_name": project_name,
                    "path": _relative(Path(str(table.get("_file", ""))), root),
                    "description": str(table.get("description") or source.get("description") or ""),
                    "tags": sorted(set(_string_list(table.get("tags")) + _string_list(source.get("tags")))),
                    "owner": _owner(table) or _owner(source),
                    "meta": {**_dict_or_empty(source.get("meta")), **_dict_or_empty(table.get("meta"))},
                    "depends_on": [],
                    "downstream": [],
                    "attached_tests": sorted(set(_string_list(table.get("tests")))),
                    "columns": _columns(table),
                }
            )
            all_dependencies[unique_id] = []

    downstream = _reverse_dependencies(all_dependencies)
    _attach_downstream(models, downstream)
    _attach_downstream(sources, downstream)

    return _artifact(
        project_path=root,
        generation_source="project_files",
        dbt_project_name=project_name,
        dbt_schema_version="",
        metadata={"dbt_project_path": str(project_yml) if project_yml.exists() else None},
        models=models,
        sources=sorted(sources, key=lambda item: item["unique_id"]),
        tests=[],
        exposures=[],
        metrics=[],
        macros=[],
        warnings=[
            "manifest.json was not found; inventory was generated from project files without executing dbt.",
            "Fallback scanning uses simple SQL ref/source pattern matching and does not compile Jinja.",
        ],
    )


def inventory_json(inventory: dict[str, Any]) -> str:
    """Serialize an inventory artifact as stable JSON."""

    return json.dumps(inventory, indent=2, sort_keys=True) + "\n"


def inventory_summary(inventory: dict[str, Any]) -> str:
    """Return a concise human-readable summary for an inventory artifact."""

    resources = inventory.get("resources", {})
    lines = [
        f"dbt project: {inventory.get('project', {}).get('name') or '(unknown)'}",
        f"source: {inventory.get('generation_source')}",
        "resources: "
        + ", ".join(
            f"{key}={len(resources.get(key, []))}"
            for key in ["models", "sources", "tests", "exposures", "metrics", "macros"]
        ),
    ]
    warnings = inventory.get("warnings", [])
    if warnings:
        lines.append(f"warnings: {len(warnings)}")
    return "\n".join(lines) + "\n"


def _artifact(
    *,
    project_path: Path,
    generation_source: str,
    dbt_project_name: str,
    dbt_schema_version: str,
    metadata: dict[str, Any],
    models: list[dict[str, Any]],
    sources: list[dict[str, Any]],
    tests: list[dict[str, Any]],
    exposures: list[dict[str, Any]],
    metrics: list[dict[str, Any]],
    macros: list[dict[str, Any]],
    warnings: list[str],
) -> dict[str, Any]:
    return {
        "schema_version": DBT_INVENTORY_SCHEMA_VERSION,
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "generation_source": generation_source,
        "project": {
            "name": dbt_project_name,
            "path": str(project_path),
            "dbt_schema_version": dbt_schema_version,
        },
        "metadata": metadata,
        "resources": {
            "models": models,
            "sources": sources,
            "tests": tests,
            "exposures": exposures,
            "metrics": metrics,
            "macros": macros,
        },
        "warnings": warnings,
    }


def _find_manifest(root: Path) -> Path | None:
    candidates = [root / "target" / "manifest.json", root / "manifest.json"]
    return next((candidate for candidate in candidates if candidate.exists()), None)


def _dict_value(data: dict[str, Any], key: str) -> dict[str, Any]:
    value = data.get(key)
    return value if isinstance(value, dict) else {}


def _depends_on_nodes(resource: dict[str, Any]) -> list[str]:
    depends_on = resource.get("depends_on")
    if not isinstance(depends_on, dict):
        return []
    nodes = depends_on.get("nodes")
    return sorted(str(node) for node in nodes) if isinstance(nodes, list) else []


def _model_record(
    unique_id: str,
    node: dict[str, Any],
    *,
    depends_on: list[str],
    attached_tests: list[str],
) -> dict[str, Any]:
    return {
        "unique_id": unique_id,
        "resource_type": "model",
        "name": str(node.get("name") or ""),
        "package_name": str(node.get("package_name") or ""),
        "path": str(node.get("path") or node.get("original_file_path") or ""),
        "original_file_path": str(node.get("original_file_path") or ""),
        "materialized": _materialized(node),
        "tags": _string_list(node.get("tags")),
        "description": str(node.get("description") or ""),
        "owner": _owner(node),
        "meta": _merged_meta(node),
        "depends_on": depends_on,
        "downstream": [],
        "attached_tests": attached_tests,
        "columns": _columns(node),
    }


def _source_record(unique_id: str, source: dict[str, Any], *, depends_on: list[str]) -> dict[str, Any]:
    return {
        "unique_id": unique_id,
        "resource_type": "source",
        "name": str(source.get("name") or ""),
        "source_name": str(source.get("source_name") or ""),
        "package_name": str(source.get("package_name") or ""),
        "path": str(source.get("original_file_path") or source.get("path") or ""),
        "description": str(source.get("description") or ""),
        "tags": _string_list(source.get("tags")),
        "owner": _owner(source),
        "meta": _merged_meta(source),
        "depends_on": depends_on,
        "downstream": [],
        "attached_tests": [],
        "columns": _columns(source),
    }


def _resource_record(
    unique_id: str,
    resource: dict[str, Any],
    resource_type: str,
    *,
    depends_on: list[str],
) -> dict[str, Any]:
    return {
        "unique_id": unique_id,
        "resource_type": resource_type,
        "name": str(resource.get("name") or ""),
        "package_name": str(resource.get("package_name") or ""),
        "path": str(resource.get("original_file_path") or resource.get("path") or ""),
        "description": str(resource.get("description") or ""),
        "tags": _string_list(resource.get("tags")),
        "owner": _owner(resource),
        "meta": _merged_meta(resource),
        "depends_on": depends_on,
    }


def _macro_record(unique_id: str, macro: dict[str, Any]) -> dict[str, Any]:
    return {
        "unique_id": unique_id,
        "resource_type": "macro",
        "name": str(macro.get("name") or ""),
        "package_name": str(macro.get("package_name") or ""),
        "path": str(macro.get("original_file_path") or macro.get("path") or ""),
        "description": str(macro.get("description") or ""),
        "meta": _merged_meta(macro),
    }


def _materialized(node: dict[str, Any]) -> str | None:
    config = node.get("config")
    if isinstance(config, dict) and config.get("materialized") is not None:
        return str(config.get("materialized"))
    return None


def _owner(resource: dict[str, Any]) -> str | None:
    meta = _merged_meta(resource)
    owner = meta.get("owner") or resource.get("owner")
    if isinstance(owner, dict):
        owner = owner.get("name") or owner.get("email")
    return str(owner) if owner else None


def _merged_meta(resource: dict[str, Any]) -> dict[str, Any]:
    config = resource.get("config")
    config_meta = config.get("meta") if isinstance(config, dict) else None
    return {**_dict_or_empty(config_meta), **_dict_or_empty(resource.get("meta"))}


def _dict_or_empty(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


def _columns(resource: dict[str, Any]) -> list[dict[str, Any]]:
    columns = resource.get("columns")
    if isinstance(columns, dict):
        items: list[Any] = list(columns.values())
    elif isinstance(columns, list):
        items = columns
    else:
        items = []
    result: list[dict[str, Any]] = []
    for column in items:
        if not isinstance(column, dict):
            continue
        result.append(
            {
                "name": str(column.get("name") or ""),
                "description": str(column.get("description") or ""),
                "tests": _string_list(column.get("tests")) + _string_list(column.get("data_tests")),
            }
        )
    return sorted(result, key=lambda item: item["name"])


def _reverse_dependencies(dependencies: dict[str, list[str]]) -> dict[str, list[str]]:
    downstream: dict[str, list[str]] = defaultdict(list)
    for node, upstream_nodes in dependencies.items():
        for upstream in upstream_nodes:
            downstream[upstream].append(node)
    return {key: sorted(set(value)) for key, value in downstream.items()}


def _attach_downstream(records: list[dict[str, Any]], downstream: dict[str, list[str]]) -> None:
    for record in records:
        record["downstream"] = downstream.get(str(record["unique_id"]), [])


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _read_model_yaml(models_dir: Path) -> dict[str, Any]:
    result: dict[str, Any] = {"models": {}, "sources": []}
    if not models_dir.exists():
        return result
    for yaml_file in sorted([*models_dir.rglob("*.yml"), *models_dir.rglob("*.yaml")]):
        payload = _read_yaml(yaml_file)
        for model in payload.get("models", []) if isinstance(payload.get("models"), list) else []:
            if isinstance(model, dict) and model.get("name"):
                model = dict(model)
                model["_file"] = str(yaml_file)
                result["models"][str(model["name"])] = model
        for source in payload.get("sources", []) if isinstance(payload.get("sources"), list) else []:
            if isinstance(source, dict):
                source = dict(source)
                source["_file"] = str(yaml_file)
                tables = source.get("tables")
                if isinstance(tables, list):
                    for table in tables:
                        if isinstance(table, dict):
                            table["_file"] = str(yaml_file)
                result["sources"].append(source)
    return result


def _relative(path: Path, root: Path) -> str:
    if not str(path):
        return ""
    try:
        return str(path.resolve().relative_to(root))
    except (OSError, ValueError):
        return str(path)
