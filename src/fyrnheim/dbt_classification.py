"""Rule-based dbt model classification for Fyrnheim context artifacts."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

DBT_CLASSIFICATION_SCHEMA_VERSION = "fyrnheim.dbt_classification.v1"


def load_classification_rules(path: Path | str) -> dict[str, Any]:
    """Load classification rules from YAML or JSON."""

    rule_path = Path(path)
    text = rule_path.read_text(encoding="utf-8")
    if rule_path.suffix.lower() == ".json":
        payload = json.loads(text)
    else:
        payload = yaml.safe_load(text)
    if not isinstance(payload, dict):
        raise ValueError("Classification rules must be a mapping")
    payload.setdefault("rules_source", str(rule_path))
    return payload


def classify_inventory(inventory: dict[str, Any], rules_config: dict[str, Any]) -> dict[str, Any]:
    """Classify dbt models in an M115 inventory artifact with inspectable rules."""

    rules = _rules(rules_config)
    allow_multiple = bool(rules_config.get("allow_multiple_labels", False))
    resources_raw = inventory.get("resources")
    resources: dict[str, Any] = resources_raw if isinstance(resources_raw, dict) else {}
    models_raw = resources.get("models")
    models: list[Any] = models_raw if isinstance(models_raw, list) else []
    classified_models = [
        _classify_model(model, inventory, rules, allow_multiple=allow_multiple)
        for model in models
        if isinstance(model, dict)
    ]
    summary = _summary(classified_models)
    return {
        "schema_version": DBT_CLASSIFICATION_SCHEMA_VERSION,
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "inventory_schema_version": inventory.get("schema_version"),
        "rules_schema_version": rules_config.get("schema_version", "fyrnheim.dbt_classification_rules.v1"),
        "rules_source": rules_config.get("rules_source", ""),
        "project": inventory.get("project", {}),
        "allow_multiple_labels": allow_multiple,
        "summary": summary,
        "models": classified_models,
    }


def classification_json(classification: dict[str, Any]) -> str:
    """Serialize classification output as stable JSON."""

    return json.dumps(classification, indent=2, sort_keys=True) + "\n"


def classification_summary(classification: dict[str, Any]) -> str:
    """Return a concise human-readable classification summary."""

    project_raw = classification.get("project")
    project: dict[str, Any] = project_raw if isinstance(project_raw, dict) else {}
    summary_raw = classification.get("summary")
    summary: dict[str, Any] = summary_raw if isinstance(summary_raw, dict) else {}
    lines = [
        f"dbt project: {project.get('name') or '(unknown)'}",
        "models: "
        + ", ".join(
            f"{key}={summary.get(key, 0)}"
            for key in ["total", "classified", "unclassified", "ambiguous"]
        ),
    ]
    labels_raw = summary.get("labels")
    labels: dict[str, Any] = labels_raw if isinstance(labels_raw, dict) else {}
    if labels:
        label_text = ", ".join(f"{label}={count}" for label, count in sorted(labels.items()))
        lines.append(f"labels: {label_text}")
    return "\n".join(lines) + "\n"


def _rules(rules_config: dict[str, Any]) -> list[dict[str, Any]]:
    raw_rules = rules_config.get("rules")
    if not isinstance(raw_rules, list):
        raise ValueError("Classification rules must include a `rules` list")
    rules: list[dict[str, Any]] = []
    for index, rule in enumerate(raw_rules):
        if not isinstance(rule, dict):
            raise ValueError(f"Rule at index {index} must be a mapping")
        label = rule.get("label")
        if not label:
            raise ValueError(f"Rule at index {index} is missing `label`")
        rules.append({**rule, "_index": index, "priority": int(rule.get("priority", 100))})
    return sorted(rules, key=lambda item: (item["priority"], item["_index"]))


def _classify_model(
    model: dict[str, Any],
    inventory: dict[str, Any],
    rules: list[dict[str, Any]],
    *,
    allow_multiple: bool,
) -> dict[str, Any]:
    matches = []
    for rule in rules:
        evidence = _match_rule(rule, model, inventory)
        if evidence is not None:
            matches.append(
                {
                    "label": str(rule["label"]),
                    "rule_id": str(rule.get("id") or rule["label"]),
                    "priority": rule["priority"],
                    "evidence": evidence,
                }
            )

    labels = [match["label"] for match in matches]
    primary_label = labels[0] if labels else None
    if not labels:
        status = "unclassified"
    elif len(labels) > 1 and not allow_multiple:
        status = "ambiguous"
    else:
        status = "classified"

    return {
        "unique_id": model.get("unique_id"),
        "name": model.get("name"),
        "path": model.get("path") or model.get("original_file_path"),
        "status": status,
        "primary_label": primary_label,
        "labels": labels if allow_multiple else ([primary_label] if primary_label else []),
        "matched_labels": labels,
        "evidence": matches,
    }


def _match_rule(
    rule: dict[str, Any], model: dict[str, Any], inventory: dict[str, Any]
) -> list[dict[str, Any]] | None:
    all_conditions = _condition_list(rule.get("all"))
    any_conditions = _condition_list(rule.get("any"))
    if not all_conditions and not any_conditions:
        conditions = _condition_list(rule.get("conditions"))
        all_conditions = conditions
    evidence: list[dict[str, Any]] = []
    for condition in all_conditions:
        condition_evidence = _match_condition(condition, model, inventory)
        if condition_evidence is None:
            return None
        evidence.append(condition_evidence)
    if any_conditions:
        any_evidence = [
            condition_evidence
            for condition in any_conditions
            if (condition_evidence := _match_condition(condition, model, inventory)) is not None
        ]
        if not any_evidence:
            return None
        evidence.extend(any_evidence)
    return evidence


def _condition_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _match_condition(
    condition: dict[str, Any], model: dict[str, Any], inventory: dict[str, Any]
) -> dict[str, Any] | None:
    field = str(condition.get("field") or "")
    if not field:
        raise ValueError("Classification condition missing `field`")
    operator = str(condition.get("operator") or condition.get("op") or "equals")
    expected = condition.get("value")
    actual = _field_value(field, model, inventory)
    matched = _compare(actual, operator, expected)
    if not matched:
        return None
    return {
        "field": field,
        "operator": operator,
        "expected": expected,
        "actual": actual,
        "message": f"{field} {operator} {expected!r}",
    }


def _field_value(field: str, model: dict[str, Any], inventory: dict[str, Any]) -> Any:
    if field.startswith("inventory."):
        return _path_value(inventory, field.removeprefix("inventory."))
    return _path_value(model, field)


def _path_value(data: Any, path: str) -> Any:
    current = data
    for part in path.split("."):
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


def _compare(actual: Any, operator: str, expected: Any) -> bool:
    if operator in {"exists", "not_empty"}:
        return actual not in (None, "", [], {})
    if operator == "missing":
        return actual in (None, "", [], {})
    if operator == "equals":
        return bool(actual == expected)
    if operator == "not_equals":
        return bool(actual != expected)
    if operator == "in":
        return actual in expected if isinstance(expected, list) else False
    if operator == "contains":
        if isinstance(actual, list):
            return expected in actual
        return str(expected) in str(actual or "")
    if operator == "contains_any":
        if not isinstance(expected, list):
            return False
        if isinstance(actual, list):
            return any(item in actual for item in expected)
        actual_text = str(actual or "")
        return any(str(item) in actual_text for item in expected)
    if operator == "startswith":
        return str(actual or "").startswith(str(expected))
    if operator == "endswith":
        return str(actual or "").endswith(str(expected))
    if operator == "regex":
        return re.search(str(expected), str(actual or "")) is not None
    if operator == "count_gte":
        return _count(actual) >= int(expected)
    if operator == "count_lte":
        return _count(actual) <= int(expected)
    raise ValueError(f"Unsupported classification operator: {operator}")


def _count(value: Any) -> int:
    if isinstance(value, (list, dict, str)):
        return len(value)
    return 0 if value is None else 1


def _summary(models: list[dict[str, Any]]) -> dict[str, Any]:
    statuses = {"classified": 0, "unclassified": 0, "ambiguous": 0}
    labels: dict[str, int] = {}
    for model in models:
        status = str(model.get("status"))
        if status in statuses:
            statuses[status] += 1
        for label in model.get("matched_labels", []):
            labels[str(label)] = labels.get(str(label), 0) + 1
    return {"total": len(models), **statuses, "labels": labels}
