"""Evaluate dbt model inventories against configurable modeling principles."""

from __future__ import annotations

import copy
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

DBT_PRINCIPLES_SCHEMA_VERSION = "fyrnheim.dbt_principles.v1"
DEFAULT_PRINCIPLES_VERSION = "fyrnheim.default_dbt_principles.v1"

DEFAULT_PRINCIPLES: dict[str, Any] = {
    "schema_version": "fyrnheim.dbt_principle_config.v1",
    "principles_source": DEFAULT_PRINCIPLES_VERSION,
    "principles": [
        {
            "id": "owner-presence",
            "title": "Every model has an owner",
            "severity": "warning",
            "description": "Models should identify an accountable owner in owner or meta.owner.",
            "applies_to": {"any": [{"field": "resource_type", "operator": "equals", "value": "model"}]},
            "check": {
                "operator": "any_present",
                "fields": ["owner", "meta.owner"],
            },
            "remediation": "Set owner or meta.owner in the model YAML metadata.",
        },
        {
            "id": "description-presence",
            "title": "Every model explains its purpose",
            "severity": "warning",
            "description": "A model should have a non-empty description for agent and reviewer context.",
            "applies_to": {"any": [{"field": "resource_type", "operator": "equals", "value": "model"}]},
            "check": {"operator": "present", "field": "description"},
            "remediation": "Add a model description that explains the model's purpose and grain.",
        },
        {
            "id": "analytical-output-grain",
            "title": "Analytical outputs state their grain",
            "severity": "error",
            "description": "Dashboard, metric, mart, and fact outputs should make row grain explicit.",
            "applies_to": {
                "any": [
                    {"field": "classification.labels", "operator": "contains", "value": "analytical_output_shaping"},
                    {"field": "path", "operator": "regex", "value": r"(^|/)(marts|mart|reports|reporting|metrics)(/|$)"},
                ]
            },
            "check": {
                "operator": "any_present_or_regex",
                "fields": ["meta.grain", "meta.primary_key", "grain"],
                "regex_fields": ["description"],
                "value": r"(?i)\b(grain|one row|per |each row|row represents)\b",
            },
            "remediation": "Document the model grain in meta.grain, meta.primary_key, or the description.",
        },
        {
            "id": "source-contract-tests",
            "title": "Source mapping models have contract tests",
            "severity": "error",
            "description": "Source mapping models should protect raw-source assumptions with at least one attached test.",
            "applies_to": {
                "any": [
                    {"field": "classification.labels", "operator": "contains", "value": "source_mapping"},
                    {"field": "depends_on", "operator": "regex", "value": r"source\."},
                ]
            },
            "check": {"operator": "count_gte", "field": "attached_tests", "value": 1},
            "remediation": "Add dbt tests for key source mapping assumptions such as not_null, unique, or accepted_values.",
        },
        {
            "id": "high-risk-model-tests",
            "title": "High-risk models have multiple tests",
            "severity": "error",
            "description": "Materialized or widely consumed models need enough tests to make changes safe.",
            "applies_to": {
                "any": [
                    {"field": "materialized", "operator": "contains_any", "value": ["table", "incremental"]},
                    {"field": "downstream", "operator": "count_gte", "value": 2},
                    {"field": "classification.labels", "operator": "contains", "value": "business_rule_application"},
                ]
            },
            "check": {"operator": "count_gte", "field": "attached_tests", "value": 2},
            "remediation": "Add tests covering keys, freshness, accepted values, or business-rule invariants.",
        },
        {
            "id": "complexity-warning",
            "title": "Complex models deserve decomposition review",
            "severity": "warning",
            "description": "Models with many upstream dependencies are often hard to reason about.",
            "applies_to": {"any": [{"field": "depends_on", "operator": "count_gte", "value": 6}]},
            "check": {"operator": "max_count", "field": "depends_on", "value": 5},
            "remediation": "Consider splitting the model or documenting why the fan-in is appropriate.",
        },
    ],
}


def load_default_principles() -> dict[str, Any]:
    """Return Fyrnheim's built-in starter data modeling principles."""

    return copy.deepcopy(DEFAULT_PRINCIPLES)


def load_principle_config(path: Path | str) -> dict[str, Any]:
    """Load principle configuration from YAML or JSON."""

    config_path = Path(path)
    text = config_path.read_text(encoding="utf-8")
    payload = json.loads(text) if config_path.suffix.lower() == ".json" else yaml.safe_load(text)
    if not isinstance(payload, dict):
        raise ValueError("Principle config must be a mapping")
    payload.setdefault("principles_source", str(config_path))
    return payload


def evaluate_principles(
    inventory: dict[str, Any],
    *,
    classification: dict[str, Any] | None = None,
    config: dict[str, Any] | None = None,
    model: str | None = None,
) -> dict[str, Any]:
    """Evaluate dbt models against configurable modeling principles."""

    effective_config = config if config is not None else load_default_principles()
    principles = _principles(effective_config)
    models = _models(inventory)
    classifications = _classification_index(classification)
    if model:
        models = [item for item in models if model in {str(item.get("name")), str(item.get("unique_id"))}]
    findings: list[dict[str, Any]] = []
    for dbt_model in models:
        model_classification = classifications.get(str(dbt_model.get("unique_id"))) or classifications.get(
            str(dbt_model.get("name"))
        )
        context = _model_context(dbt_model, model_classification)
        for principle in principles:
            findings.append(_evaluate_principle(principle, context))
    return {
        "schema_version": DBT_PRINCIPLES_SCHEMA_VERSION,
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "inventory_schema_version": inventory.get("schema_version"),
        "classification_schema_version": (classification or {}).get("schema_version"),
        "principles_schema_version": effective_config.get("schema_version", "fyrnheim.dbt_principle_config.v1"),
        "principles_source": effective_config.get("principles_source", ""),
        "project": inventory.get("project", {}),
        "model_filter": model or "",
        "summary": _summary(findings),
        "findings": findings,
    }


def principles_json(result: dict[str, Any]) -> str:
    """Serialize principle results as stable JSON."""

    return json.dumps(result, indent=2, sort_keys=True) + "\n"


def principles_summary(result: dict[str, Any]) -> str:
    """Return a concise human-readable principles summary."""

    project_raw = result.get("project")
    project: dict[str, Any] = project_raw if isinstance(project_raw, dict) else {}
    summary_raw = result.get("summary")
    summary: dict[str, Any] = summary_raw if isinstance(summary_raw, dict) else {}
    lines = [
        f"dbt project: {project.get('name') or '(unknown)'}",
        "principle findings: "
        + ", ".join(
            f"{key}={summary.get(key, 0)}"
            for key in ["total", "passed", "failed", "warnings", "not_applicable", "config_errors"]
        ),
    ]
    if result.get("model_filter"):
        lines.append(f"model: {result['model_filter']}")
    findings_raw = result.get("findings")
    findings = findings_raw if isinstance(findings_raw, list) else []
    failing = [
        item
        for item in findings
        if isinstance(item, dict) and item.get("status") in {"fail", "warning", "config_error"}
    ]
    for item in failing[:10]:
        lines.append(
            f"- {item.get('status')}: {item.get('model_name')} :: {item.get('principle_id')} "
            f"({item.get('severity')}) — {item.get('message')}"
        )
    if len(failing) > 10:
        lines.append(f"... {len(failing) - 10} more non-passing findings")
    return "\n".join(lines) + "\n"


def _models(inventory: dict[str, Any]) -> list[dict[str, Any]]:
    resources_raw = inventory.get("resources")
    resources: dict[str, Any] = resources_raw if isinstance(resources_raw, dict) else {}
    models_raw = resources.get("models")
    models = models_raw if isinstance(models_raw, list) else []
    return [item for item in models if isinstance(item, dict)]


def _principles(config: dict[str, Any]) -> list[dict[str, Any]]:
    raw = config.get("principles")
    if not isinstance(raw, list):
        raise ValueError("Principle config must include a `principles` list")
    principles: list[dict[str, Any]] = []
    for index, principle in enumerate(raw):
        if not isinstance(principle, dict):
            raise ValueError(f"Principle at index {index} must be a mapping")
        if not principle.get("id"):
            raise ValueError(f"Principle at index {index} is missing `id`")
        if not isinstance(principle.get("check"), dict):
            raise ValueError(f"Principle {principle.get('id')} is missing `check`")
        principles.append(principle)
    return principles


def _classification_index(classification: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not classification:
        return {}
    output: dict[str, dict[str, Any]] = {}
    raw_models_value = classification.get("models")
    raw_models = raw_models_value if isinstance(raw_models_value, list) else []
    for item in raw_models:
        if not isinstance(item, dict):
            continue
        for key in [item.get("unique_id"), item.get("name")]:
            if key:
                output[str(key)] = item
    return output


def _model_context(model: dict[str, Any], classification: dict[str, Any] | None) -> dict[str, Any]:
    context = copy.deepcopy(model)
    context.setdefault("resource_type", "model")
    context["classification"] = classification or {"labels": [], "primary_label": None, "status": "unclassified"}
    return context


def _evaluate_principle(principle: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    base = {
        "model_unique_id": context.get("unique_id"),
        "model_name": context.get("name"),
        "principle_id": str(principle.get("id")),
        "title": str(principle.get("title") or principle.get("id")),
        "severity": str(principle.get("severity") or "warning"),
        "remediation": str(principle.get("remediation") or ""),
    }
    try:
        applies, applies_evidence = _matches_scope(principle.get("applies_to"), context)
        if not applies:
            return {
                **base,
                "status": "not_applicable",
                "message": "Principle does not apply to this model.",
                "evidence": applies_evidence,
            }
        passed, check_evidence = _run_check(principle["check"], context)
        if passed:
            return {**base, "status": "pass", "message": "Principle satisfied.", "evidence": check_evidence}
        status = "warning" if base["severity"] == "warning" else "fail"
        return {
            **base,
            "status": status,
            "message": str(principle.get("description") or "Principle check did not pass."),
            "evidence": check_evidence,
        }
    except Exception as exc:  # noqa: BLE001 - config errors are reported as data
        return {
            **base,
            "status": "config_error",
            "severity": "error",
            "message": str(exc),
            "evidence": [],
        }


def _matches_scope(scope: Any, context: dict[str, Any]) -> tuple[bool, list[dict[str, Any]]]:
    if scope in (None, {}):
        return True, [{"message": "No applies_to conditions configured; principle applies."}]
    if not isinstance(scope, dict):
        raise ValueError("applies_to must be a mapping")
    evidence: list[dict[str, Any]] = []
    for condition in _condition_list(scope.get("all")):
        match = _match_condition(condition, context)
        if match is None:
            return False, evidence
        evidence.append(match)
    any_conditions = _condition_list(scope.get("any"))
    if any_conditions:
        any_evidence = [item for condition in any_conditions if (item := _match_condition(condition, context))]
        if not any_evidence:
            return False, evidence
        evidence.extend(any_evidence)
    return True, evidence


def _run_check(check: dict[str, Any], context: dict[str, Any]) -> tuple[bool, list[dict[str, Any]]]:
    operator = str(check.get("operator") or "present")
    if operator == "present":
        field = _required_field(check)
        actual = _field_value(field, context)
        return _present(actual), [_evidence(field, operator, True, actual)]
    if operator == "any_present":
        fields = _fields(check)
        evidence = [_evidence(field, operator, True, _field_value(field, context)) for field in fields]
        return any(_present(item["actual"]) for item in evidence), evidence
    if operator == "any_present_or_regex":
        fields = _fields(check)
        pattern = re.compile(str(check.get("value") or ""))
        evidence = [_evidence(field, "present", True, _field_value(field, context)) for field in fields]
        regex_fields = [str(item) for item in check.get("regex_fields", []) if item]
        for field in regex_fields:
            actual = _field_value(field, context)
            matched = bool(pattern.search(str(actual or "")))
            evidence.append(_evidence(field, "regex", check.get("value"), actual, matched=matched))
        return any(_present(item["actual"]) or item.get("matched") for item in evidence), evidence
    if operator in {"count_gte", "max_count"}:
        field = _required_field(check)
        expected = int(check.get("value", 0))
        actual = _field_value(field, context)
        count = len(actual) if isinstance(actual, (list, tuple, set, dict)) else (1 if _present(actual) else 0)
        passed = count >= expected if operator == "count_gte" else count <= expected
        return passed, [_evidence(field, operator, expected, actual, matched=passed)]
    raise ValueError(f"Unsupported principle check operator: {operator}")


def _condition_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _match_condition(condition: dict[str, Any], context: dict[str, Any]) -> dict[str, Any] | None:
    field = str(condition.get("field") or "")
    if not field:
        raise ValueError("Principle condition missing `field`")
    operator = str(condition.get("operator") or condition.get("op") or "equals")
    expected = condition.get("value")
    actual = _field_value(field, context)
    matched = _compare(actual, operator, expected)
    if not matched:
        return None
    return _evidence(field, operator, expected, actual, matched=True)


def _field_value(field: str, context: dict[str, Any]) -> Any:
    current: Any = context
    for part in field.split("."):
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


def _compare(actual: Any, operator: str, expected: Any) -> bool:
    if operator == "equals":
        return bool(actual == expected)
    if operator == "contains":
        if isinstance(actual, (list, tuple, set)):
            return expected in actual
        return str(expected) in str(actual or "")
    if operator == "contains_any":
        expected_values = expected if isinstance(expected, list) else [expected]
        if isinstance(actual, (list, tuple, set)):
            return any(item in actual for item in expected_values)
        return any(str(item) in str(actual or "") for item in expected_values)
    if operator == "regex":
        if isinstance(actual, (list, tuple, set)):
            return any(re.search(str(expected), str(item)) for item in actual)
        return bool(re.search(str(expected), str(actual or "")))
    if operator == "exists":
        return actual is not None
    if operator == "count_gte":
        count = len(actual) if isinstance(actual, (list, tuple, set, dict)) else (1 if _present(actual) else 0)
        return count >= int(expected)
    raise ValueError(f"Unsupported principle condition operator: {operator}")


def _required_field(check: dict[str, Any]) -> str:
    field = check.get("field")
    if not field:
        raise ValueError("Principle check missing `field`")
    return str(field)


def _fields(check: dict[str, Any]) -> list[str]:
    fields = [str(item) for item in check.get("fields", []) if item]
    if not fields:
        raise ValueError("Principle check missing `fields`")
    return fields


def _present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return True


def _evidence(
    field: str,
    operator: str,
    expected: Any,
    actual: Any,
    *,
    matched: bool | None = None,
) -> dict[str, Any]:
    return {
        "field": field,
        "operator": operator,
        "expected": expected,
        "actual": actual,
        "matched": bool(_present(actual)) if matched is None else matched,
    }


def _summary(findings: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "total": len(findings),
        "models": len({item.get("model_unique_id") for item in findings}),
        "passed": sum(1 for item in findings if item.get("status") == "pass"),
        "failed": sum(1 for item in findings if item.get("status") == "fail"),
        "warnings": sum(1 for item in findings if item.get("status") == "warning"),
        "not_applicable": sum(1 for item in findings if item.get("status") == "not_applicable"),
        "config_errors": sum(1 for item in findings if item.get("status") == "config_error"),
    }
