"""Ordered multi-model rule evaluation for channel/classification flows.

This module adapts Propel Explorer's YAML-driven multi-model attribution flow to
Fyrnheim. A model is an ordered list of rules; the first matching rule assigns a
primary output value and optional secondary output value. Multiple models can be
loaded from YAML and projected onto the same Ibis table for side-by-side model
comparison.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any, Literal

import ibis
import yaml
from ibis.expr.types import BooleanValue, Table, Value
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

RuleOperator = Literal[
    "equals",
    "not_equals",
    "contains",
    "not_contains",
    "starts_with",
    "ends_with",
    "in",
    "not_in",
    "is_empty",
    "is_not_empty",
]

DEFAULT_CHANNEL_FIELD = "channel"
DEFAULT_SUB_CHANNEL_FIELD = "sub_channel"


class ModelCondition(BaseModel):
    """A single field predicate in an ordered model rule."""

    field: str = Field(min_length=1)
    operator: RuleOperator
    value: str | int | float | bool | Sequence[str] | None = None

    @model_validator(mode="after")
    def _validate_value(self) -> ModelCondition:
        if self.operator not in {"is_empty", "is_not_empty"} and self.value is None:
            raise ValueError(f"operator {self.operator!r} requires a value")
        if self.operator in {"is_empty", "is_not_empty"} and self.value is not None:
            raise ValueError(f"operator {self.operator!r} does not accept a value")
        return self

    @property
    def values(self) -> list[Any]:
        """Return a normalized list of comparison values for set operators."""

        if isinstance(self.value, str):
            return [item.strip() for item in self.value.split(",") if item.strip()]
        if isinstance(self.value, Sequence):
            return list(self.value)
        if self.value is None:
            return []
        return [self.value]


class ModelRule(BaseModel):
    """One ordered classification rule.

    The first rule whose conditions all match wins for a model.
    """

    name: str = Field(min_length=1)
    target_channel: str = Field(min_length=1)
    conditions: list[ModelCondition] = Field(min_length=1)
    sub_channel: str | None = Field(default=None, min_length=1)


class MultiModelDefinition(BaseModel):
    """A YAML-loadable ordered model definition."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    rules: list[ModelRule] = Field(min_length=1)
    description: str = ""
    filename: str | None = None

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        if not value.replace("_", "-").replace("-", "").isalnum():
            raise ValueError("model name may only contain letters, numbers, '_' and '-'")
        return value

    def evaluate_record(
        self,
        record: Mapping[str, Any],
        *,
        default_channel: str | None = None,
        default_sub_channel: str | None = None,
    ) -> dict[str, Any]:
        """Evaluate this model against an in-memory record.

        Returns model metadata plus the winning channel/sub-channel and rule name.
        """

        for rule in self.rules:
            if all(_record_condition_matches(record, condition) for condition in rule.conditions):
                return {
                    "model": self.name,
                    "channel": rule.target_channel,
                    "sub_channel": rule.sub_channel or default_sub_channel,
                    "rule": rule.name,
                }
        return {
            "model": self.name,
            "channel": default_channel,
            "sub_channel": default_sub_channel,
            "rule": None,
        }


class MultiModelFlow(BaseModel):
    """A collection of ordered models that can be evaluated side-by-side."""

    models: list[MultiModelDefinition] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_unique_names(self) -> MultiModelFlow:
        names = [model.name for model in self.models]
        if len(names) != len(set(names)):
            raise ValueError("model names must be unique")
        return self

    def evaluate_record(
        self,
        record: Mapping[str, Any],
        *,
        default_channel: str | None = None,
        default_sub_channel: str | None = None,
    ) -> list[dict[str, Any]]:
        """Evaluate every model against the same in-memory record."""

        return [
            model.evaluate_record(
                record,
                default_channel=default_channel,
                default_sub_channel=default_sub_channel,
            )
            for model in self.models
        ]

    def project(
        self,
        table: Table,
        *,
        default_channel: str | None = None,
        default_sub_channel: str | None = None,
        prefix: str = "model",
        include_rule: bool = True,
    ) -> Table:
        """Return an Ibis table with one channel column per model.

        Output columns follow ``{prefix}_{safe_model_name}_channel`` and, when
        present, ``{prefix}_{safe_model_name}_sub_channel``. If ``include_rule``
        is true, a ``..._rule`` column records the winning rule name.
        """

        assignments: dict[str, Value] = {}
        for model in self.models:
            safe_name = _safe_column_token(model.name)
            assignments[f"{prefix}_{safe_name}_{DEFAULT_CHANNEL_FIELD}"] = _model_cases(
                table,
                model,
                attr="target_channel",
                default=default_channel,
            )
            assignments[f"{prefix}_{safe_name}_{DEFAULT_SUB_CHANNEL_FIELD}"] = _model_cases(
                table,
                model,
                attr="sub_channel",
                default=default_sub_channel,
            )
            if include_rule:
                assignments[f"{prefix}_{safe_name}_rule"] = _model_rule_cases(table, model)
        return table.mutate(**assignments)


def load_multi_model(path: str | Path, *, allowed_fields: Iterable[str] | None = None) -> MultiModelDefinition:
    """Load one model YAML file."""

    source = Path(path)
    payload = yaml.safe_load(source.read_text())
    if not isinstance(payload, dict):
        raise ValueError(f"{source}: expected a YAML mapping")
    payload = {**payload, "filename": source.name}
    model = MultiModelDefinition.model_validate(payload)
    _validate_allowed_fields([model], allowed_fields)
    return model


def load_multi_model_flow(
    paths: Iterable[str | Path] | str | Path,
    *,
    allowed_fields: Iterable[str] | None = None,
) -> MultiModelFlow:
    """Load multiple model YAML files or every ``*.yaml`` in a directory."""

    resolved = _resolve_model_paths(paths)
    models = [load_multi_model(path, allowed_fields=allowed_fields) for path in resolved]
    return MultiModelFlow(models=models)


def _resolve_model_paths(paths: Iterable[str | Path] | str | Path) -> list[Path]:
    if isinstance(paths, str | Path):
        path = Path(paths)
        if path.is_dir():
            return sorted(path.glob("*.yaml")) + sorted(path.glob("*.yml"))
        return [path]
    return [Path(path) for path in paths]


def _validate_allowed_fields(
    models: Iterable[MultiModelDefinition], allowed_fields: Iterable[str] | None
) -> None:
    if allowed_fields is None:
        return
    allowed = set(allowed_fields)
    for model in models:
        for rule in model.rules:
            for condition in rule.conditions:
                if condition.field not in allowed:
                    location = f"{model.filename or model.name}: rule {rule.name!r}"
                    raise ValueError(f"{location} uses unknown field {condition.field!r}")


def _record_condition_matches(record: Mapping[str, Any], condition: ModelCondition) -> bool:
    actual = record.get(condition.field)
    operator = condition.operator
    if operator == "is_empty":
        return actual is None or actual == ""
    if operator == "is_not_empty":
        return actual is not None and actual != ""

    if operator in {"in", "not_in"}:
        matched = actual in condition.values
        return matched if operator == "in" else not matched

    expected = condition.value
    if operator == "equals":
        return actual == expected
    if operator == "not_equals":
        return actual != expected

    actual_text = "" if actual is None else str(actual)
    expected_text = "" if expected is None else str(expected)
    if operator == "contains":
        return expected_text in actual_text
    if operator == "not_contains":
        return expected_text not in actual_text
    if operator == "starts_with":
        return actual_text.startswith(expected_text)
    if operator == "ends_with":
        return actual_text.endswith(expected_text)
    raise AssertionError(f"Unsupported operator: {operator}")


def _model_cases(
    table: Table,
    model: MultiModelDefinition,
    *,
    attr: Literal["target_channel", "sub_channel"],
    default: str | None,
) -> Value:
    cases: list[tuple[BooleanValue, Value | str]] = []
    for rule in model.rules:
        value = getattr(rule, attr)
        if value is not None:
            cases.append((_rule_condition(table, rule), value))
    if not cases:
        return _string_or_null(default)
    return ibis.cases(*cases, else_=_string_or_null(default))


def _model_rule_cases(table: Table, model: MultiModelDefinition) -> Value:
    return ibis.cases(
        *((_rule_condition(table, rule), rule.name) for rule in model.rules),
        else_=ibis.null().cast("string"),
    )


def _rule_condition(table: Table, rule: ModelRule) -> BooleanValue:
    conditions = [_condition_expression(table, condition) for condition in rule.conditions]
    combined = conditions[0]
    for condition in conditions[1:]:
        combined = combined & condition
    return combined


def _condition_expression(table: Table, condition: ModelCondition) -> BooleanValue:
    column = table[condition.field]
    operator = condition.operator
    if operator == "is_empty":
        return column.isnull() | (column == "")
    if operator == "is_not_empty":
        return column.notnull() & (column != "")
    if operator == "equals":
        return column == condition.value
    if operator == "not_equals":
        return column != condition.value
    if operator == "in":
        return column.isin(condition.values)
    if operator == "not_in":
        return ~column.isin(condition.values)

    text_column = column.cast("string").fill_null("")
    text_value = "" if condition.value is None else str(condition.value)
    if operator == "contains":
        return text_column.contains(text_value)
    if operator == "not_contains":
        return ~text_column.contains(text_value)
    if operator == "starts_with":
        return text_column.startswith(text_value)
    if operator == "ends_with":
        return text_column.endswith(text_value)
    raise AssertionError(f"Unsupported operator: {operator}")


def _string_or_null(value: str | None) -> Value | str:
    if value is None:
        return ibis.null().cast("string")
    return value


def _safe_column_token(value: str) -> str:
    return "".join(char if char.isalnum() else "_" for char in value).strip("_")
