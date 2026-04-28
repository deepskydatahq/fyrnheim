"""Conservative source column-pushdown analysis.

The collector prefers false negatives over false positives: if a source shape is
uncertain, callers may keep extra columns or skip projection entirely. Dropping a
column that may be required is never acceptable.
"""

from __future__ import annotations

import ast
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from fyrnheim.core.activity import ActivityDefinition, EventOccurred, FieldChanged
from fyrnheim.core.analytics_entity import AnalyticsEntity
from fyrnheim.core.identity import IdentityGraph
from fyrnheim.core.metrics_model import MetricsModel
from fyrnheim.core.source import BaseTableSource, EventSource, Field, SourceTransforms, StateSource


@dataclass(frozen=True)
class SourceColumnRequirements:
    """Required post-stage columns for one source.

    ``columns=None`` means the source should not be pruned because analysis is
    intentionally conservative for this source.
    """

    columns: frozenset[str] | None
    reason: str = ""

    @classmethod
    def all(cls, reason: str) -> SourceColumnRequirements:
        return cls(columns=None, reason=reason)

    @classmethod
    def only(cls, columns: Iterable[str]) -> SourceColumnRequirements:
        return cls(columns=frozenset(c for c in columns if c))


RequirementMap = dict[str, SourceColumnRequirements]


def collect_required_source_columns(assets: Mapping[str, list]) -> RequirementMap:
    """Derive conservative post-stage source columns from pipeline assets."""
    sources = assets.get("sources", [])
    by_name = {source.name: source for source in sources if hasattr(source, "name")}
    required: dict[str, set[str]] = {name: set() for name in by_name}
    retain_all: dict[str, str] = {}

    for source in sources:
        if isinstance(source, StateSource):
            required[source.name].add(source.id_field)
        elif isinstance(source, EventSource):
            required[source.name].update([source.entity_id_field, source.timestamp_field])
            if source.event_type_field:
                required[source.name].add(source.event_type_field)

    # Join targets can contribute arbitrary joined columns to a dependent
    # source's post-stage payload. Until we model joined-column lineage exactly,
    # keep target sources unpruned. Left-side join keys are still explicit.
    for source in sources:
        for join in getattr(source, "joins", None) or []:
            required[source.name].add(join.join_key)
            if join.source_name in by_name:
                retain_all[join.source_name] = "source is a declarative join target"

    activity_by_name: dict[str, ActivityDefinition] = {}
    for activity in assets.get("activities", []):
        if not isinstance(activity, ActivityDefinition):
            continue
        activity_by_name[activity.name] = activity
        if activity.source not in required:
            continue
        required[activity.source].add(activity.entity_id_field)
        if activity.person_id_field:
            required[activity.source].add(activity.person_id_field)
        required[activity.source].update(activity.include_fields)
        if isinstance(activity.trigger, FieldChanged):
            required[activity.source].add(activity.trigger.field)
        elif isinstance(activity.trigger, EventOccurred):
            # Event type itself is sourced from EventSource.event_type_field when dynamic.
            source = by_name.get(activity.source)
            if isinstance(source, EventSource) and source.event_type_field:
                required[activity.source].add(source.event_type_field)

    for graph in assets.get("identity_graphs", []):
        if not isinstance(graph, IdentityGraph):
            continue
        for identity_source in graph.sources:
            if identity_source.source in required:
                required[identity_source.source].update(
                    [identity_source.id_field, identity_source.match_key_field]
                )

    for entity in assets.get("analytics_entities", []):
        if not isinstance(entity, AnalyticsEntity):
            continue
        for state_field in entity.state_fields:
            if state_field.source in required:
                required[state_field.source].add(state_field.field)
        for measure in entity.measures:
            activity = activity_by_name.get(measure.activity)
            if activity is not None and measure.field and activity.source in required:
                required[activity.source].add(measure.field)

    for model in assets.get("metrics_models", []):
        if not isinstance(model, MetricsModel):
            continue
        for source_name in model.sources:
            if source_name not in required:
                continue
            for metric_field in model.metric_fields:
                if metric_field.aggregation in {"sum_delta", "last_value", "max_value"}:
                    required[source_name].add(metric_field.field_name)
                elif metric_field.aggregation == "count_distinct" and metric_field.distinct_field:
                    required[source_name].add(metric_field.distinct_field)

    result: RequirementMap = {}
    for name, columns in required.items():
        if name in retain_all:
            result[name] = SourceColumnRequirements.all(retain_all[name])
        else:
            result[name] = SourceColumnRequirements.only(columns)
    return result


def required_raw_columns_for_source(
    source: BaseTableSource,
    post_stage_columns: Iterable[str] | None,
) -> frozenset[str] | None:
    """Map post-stage required columns back to safe raw read columns.

    ``None`` means no projection should be applied.
    """
    if post_stage_columns is None:
        return None

    post = set(post_stage_columns)
    post.update(_source_intrinsic_columns(source))

    # Filters and computed columns are arbitrary Ibis expressions. We collect
    # simple ``t.column`` / ``t['column']`` references and keep those columns.
    if getattr(source, "filter", None):
        post.update(expression_column_references(source.filter or ""))
    for computed in getattr(source, "computed_columns", None) or []:
        if computed.name in post:
            post.update(expression_column_references(computed.expression))
            post.discard(computed.name)

    # JSON fields produce f.name from f.source_column (or f.name). If the
    # produced column is needed, keep the source JSON column.
    for field in getattr(source, "fields", None) or []:
        if isinstance(field, Field) and field.json_path and field.name in post:
            post.add(field.source_column or field.name)
            if field.source_column is not None:
                post.discard(field.name)

    post.update(join.join_key for join in getattr(source, "joins", None) or [])

    return frozenset(_map_transformed_columns_to_raw(post, getattr(source, "transforms", None)))


def expression_column_references(expression: str) -> set[str]:
    """Best-effort column reference extraction for Fyrnheim expression strings."""
    try:
        tree = ast.parse(expression, mode="eval")
    except SyntaxError:
        return set()

    refs: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "t":
            refs.add(node.attr)
        elif isinstance(node, ast.Subscript) and isinstance(node.value, ast.Name) and node.value.id == "t":
            value = node.slice
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                refs.add(value.value)
    return refs


def _source_intrinsic_columns(source: BaseTableSource) -> set[str]:
    if isinstance(source, StateSource):
        return {source.id_field}
    if isinstance(source, EventSource):
        columns = {source.entity_id_field, source.timestamp_field}
        if source.event_type_field:
            columns.add(source.event_type_field)
        return columns
    return set()


def _map_transformed_columns_to_raw(
    columns: set[str],
    transforms: SourceTransforms | None,
) -> set[str]:
    if transforms is None:
        return columns

    mapped = set(columns)

    # Renames are applied last, so invert them first.
    for rename in transforms.renames:
        if rename.to_name in mapped:
            mapped.add(rename.from_name)
            mapped.discard(rename.to_name)

    for divide in transforms.divides:
        if f"{divide.field}{divide.suffix}" in mapped:
            mapped.add(divide.field)
    for multiply in transforms.multiplies:
        if f"{multiply.field}{multiply.suffix}" in mapped:
            mapped.add(multiply.field)
    for type_cast in transforms.type_casts:
        if type_cast.field in mapped:
            mapped.add(type_cast.field)

    return mapped
