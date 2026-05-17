"""Read-only insight recipe parsing and execution.

Insight recipes are deliberately bounded. They let agents run predefined table
queries without accepting arbitrary SQL, mutating warehouses, or executing the
Fyrnheim pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import ibis
import yaml

from fyrnheim.engine.executor import IbisExecutor
from fyrnheim.inspect import build_manifest

INSIGHT_SCHEMA_VERSION = "fyrnheim.insights.v1"
RecipeType = Literal["table_top", "joined_top"]


class InsightConfigError(ValueError):
    """Raised when insight recipe configuration is invalid."""


@dataclass(frozen=True)
class LimitSpec:
    default: int = 20
    max: int = 100


@dataclass(frozen=True)
class OrderBySpec:
    default: str
    allowed: tuple[str, ...]


@dataclass(frozen=True)
class FilterSpec:
    allowed: tuple[str, ...]
    defaults: dict[str, Any]


@dataclass(frozen=True)
class JoinSpec:
    right: str
    left_on: str
    right_on: str


@dataclass(frozen=True)
class TableRef:
    name: str
    database: str | tuple[str, str] | None = None


@dataclass(frozen=True)
class InsightRecipe:
    """Validated read-only insight recipe."""

    name: str
    recipe_type: RecipeType
    columns: tuple[str, ...]
    order_by: OrderBySpec
    filters: FilterSpec
    limit: LimitSpec
    tags: tuple[str, ...] = ()
    source: str | None = None
    primary: str | None = None
    join: JoinSpec | None = None
    table: TableRef | None = None
    right_table: TableRef | None = None

    def summary(self) -> dict[str, Any]:
        """Return a JSON-compatible recipe summary."""
        return {
            "name": self.name,
            "type": self.recipe_type,
            "source": self.source,
            "primary": self.primary,
            "join": None if self.join is None else {
                "right": self.join.right,
                "left_on": self.join.left_on,
                "right_on": self.join.right_on,
            },
            "columns": list(self.columns),
            "order_by": {
                "default": self.order_by.default,
                "allowed": list(self.order_by.allowed),
            },
            "filters": {
                "allowed": list(self.filters.allowed),
                "defaults": dict(self.filters.defaults),
            },
            "limit": {"default": self.limit.default, "max": self.limit.max},
            "tags": list(self.tags),
        }


@dataclass(frozen=True)
class InsightProjectConfig:
    """Insight config loaded from a Fyrnheim project config file."""

    project_path: Path
    entities_dir: Path
    backend: str
    backend_config: dict[str, str]
    recipes: dict[str, InsightRecipe]


def load_insight_project_config(config_path: Path | str = "fyrnheim.yaml") -> InsightProjectConfig:
    """Load backend and insight recipe config from a Fyrnheim YAML file."""
    path = Path(config_path).resolve()
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise InsightConfigError(f"Expected mapping in {path}")

    project_path = path.parent
    raw_backend_config = raw.get("backend_config") or {}
    if not isinstance(raw_backend_config, dict):
        raise InsightConfigError("backend_config must be a mapping when provided")

    insights = raw.get("insights") or {}
    if not isinstance(insights, dict):
        raise InsightConfigError("insights must be a mapping when provided")

    return InsightProjectConfig(
        project_path=project_path,
        entities_dir=_resolve_path(project_path, raw.get("entities_dir", "entities")),
        backend=str(raw.get("backend", "duckdb")),
        backend_config={str(key): str(value) for key, value in raw_backend_config.items()},
        recipes=parse_insight_recipes(insights.get("recipes") or {}),
    )


def parse_insight_recipes(raw_recipes: dict[str, Any]) -> dict[str, InsightRecipe]:
    """Parse and validate insight recipes from YAML/JSON-compatible data."""
    if not isinstance(raw_recipes, dict):
        raise InsightConfigError("insights.recipes must be a mapping")
    recipes = {
        name: _parse_recipe(str(name), config)
        for name, config in raw_recipes.items()
    }
    return dict(sorted(recipes.items()))


def list_insight_recipes(recipes: dict[str, InsightRecipe]) -> dict[str, Any]:
    """Return available insight recipes."""
    return {
        "schema_version": INSIGHT_SCHEMA_VERSION,
        "recipes": [recipe.summary() for recipe in recipes.values()],
    }


def run_insight_recipe(
    conn: ibis.BaseBackend,
    recipe: InsightRecipe,
    *,
    manifest: dict[str, Any] | None = None,
    backend: str = "duckdb",
    order_by: str | None = None,
    filters: dict[str, Any] | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """Execute a bounded read-only insight recipe through Ibis."""
    applied_limit = _clamp_limit(limit, recipe.limit)
    applied_order_by = _validate_order_by(order_by, recipe.order_by)
    applied_filters = _validate_filters(filters, recipe.filters)

    if recipe.recipe_type == "table_top":
        table = _recipe_table(conn, recipe, manifest=manifest, backend=backend)
    elif recipe.recipe_type == "joined_top":
        table = _joined_recipe_table(conn, recipe, manifest=manifest, backend=backend)
    else:  # pragma: no cover - dataclass typing keeps this unreachable
        raise InsightConfigError(f"Unsupported recipe type: {recipe.recipe_type}")

    missing = [column for column in recipe.columns if column not in table.columns]
    if missing:
        raise InsightConfigError(
            f"Recipe {recipe.name!r} references missing column(s): {', '.join(missing)}"
        )

    projected = table.select(list(recipe.columns))
    for column, value in applied_filters.items():
        projected = projected.filter(projected[column] == value)

    projected = projected.order_by(ibis.desc(applied_order_by)).limit(applied_limit)
    rows = projected.execute().to_dict(orient="records")
    return {
        "recipe": recipe.name,
        "type": recipe.recipe_type,
        "order_by": applied_order_by,
        "filters": applied_filters,
        "limit": applied_limit,
        "row_count": len(rows),
        "rows": rows,
    }


def run_project_insight_recipe(
    recipe_name: str,
    *,
    config_path: Path | str = "fyrnheim.yaml",
    order_by: str | None = None,
    filters: dict[str, Any] | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """Load project config and execute one insight recipe read-only."""
    project = load_insight_project_config(config_path)
    if recipe_name not in project.recipes:
        raise InsightConfigError(f"Unknown insight recipe: {recipe_name}")

    manifest = build_manifest(
        project.entities_dir,
        project_path=project.project_path,
        include_git=True,
        strict=True,
    )
    executor = IbisExecutor.from_config(
        project.backend,
        project.backend_config,
        generated_dir=None,
    )
    try:
        return run_insight_recipe(
            executor.connection,
            project.recipes[recipe_name],
            manifest=manifest,
            backend=project.backend,
            order_by=order_by,
            filters=filters,
            limit=limit,
        )
    finally:
        executor.close()


def _parse_recipe(name: str, raw: Any) -> InsightRecipe:
    if not isinstance(raw, dict):
        raise InsightConfigError(f"Recipe {name!r} must be a mapping")

    recipe_type = raw.get("type")
    if recipe_type not in {"table_top", "joined_top"}:
        raise InsightConfigError(
            f"Recipe {name!r} has unsupported type {recipe_type!r}; "
            "supported: table_top, joined_top"
        )

    columns = _string_tuple(raw.get("columns"), f"Recipe {name!r} columns")
    if not columns:
        raise InsightConfigError(f"Recipe {name!r} must declare at least one column")

    order_by = _parse_order_by(name, raw.get("order_by"), columns)
    filters = _parse_filters(raw.get("filters"))
    limit = _parse_limit(raw.get("limit"))
    tags = _string_tuple(raw.get("tags", []), f"Recipe {name!r} tags")

    table = _parse_table_ref(raw.get("table"))
    source = _optional_str(raw.get("source"))
    primary = _optional_str(raw.get("primary"))
    join = None
    right_table = _parse_table_ref(raw.get("right_table"))

    if recipe_type == "table_top" and table is None and source is None:
        raise InsightConfigError(f"Recipe {name!r} must declare source or table")

    if recipe_type == "joined_top":
        if table is None and primary is None:
            raise InsightConfigError(f"Recipe {name!r} must declare primary or table")
        raw_join = raw.get("join")
        if not isinstance(raw_join, dict):
            raise InsightConfigError(f"Recipe {name!r} must declare join mapping")
        right = _required_str(raw_join, "right", name)
        left_on = _required_str(raw_join, "left_on", name)
        right_on = _required_str(raw_join, "right_on", name)
        join = JoinSpec(right=right, left_on=left_on, right_on=right_on)

    return InsightRecipe(
        name=name,
        recipe_type=recipe_type,
        columns=columns,
        order_by=order_by,
        filters=filters,
        limit=limit,
        tags=tags,
        source=source,
        primary=primary,
        join=join,
        table=table,
        right_table=right_table,
    )


def _parse_order_by(name: str, raw: Any, columns: tuple[str, ...]) -> OrderBySpec:
    if raw is None:
        return OrderBySpec(default=columns[0], allowed=columns)
    if not isinstance(raw, dict):
        raise InsightConfigError(f"Recipe {name!r} order_by must be a mapping")
    allowed = _string_tuple(raw.get("allowed", columns), f"Recipe {name!r} order_by.allowed")
    default = str(raw.get("default", allowed[0] if allowed else columns[0]))
    if default not in allowed:
        raise InsightConfigError(f"Recipe {name!r} order_by.default must be allowed")
    unknown = [column for column in allowed if column not in columns]
    if unknown:
        raise InsightConfigError(
            f"Recipe {name!r} order_by.allowed references non-selected column(s): "
            f"{', '.join(unknown)}"
        )
    return OrderBySpec(default=default, allowed=allowed)


def _parse_filters(raw: Any) -> FilterSpec:
    if raw is None:
        return FilterSpec(allowed=(), defaults={})
    if not isinstance(raw, dict):
        raise InsightConfigError("filters must be a mapping")
    return FilterSpec(
        allowed=_string_tuple(raw.get("allowed", []), "filters.allowed"),
        defaults=dict(raw.get("defaults") or {}),
    )


def _parse_limit(raw: Any) -> LimitSpec:
    if raw is None:
        return LimitSpec()
    if not isinstance(raw, dict):
        raise InsightConfigError("limit must be a mapping")
    default = int(raw.get("default", 20))
    max_limit = int(raw.get("max", 100))
    if default < 1 or max_limit < 1:
        raise InsightConfigError("limit.default and limit.max must be positive")
    if default > max_limit:
        raise InsightConfigError("limit.default must be <= limit.max")
    return LimitSpec(default=default, max=max_limit)


def _parse_table_ref(raw: Any) -> TableRef | None:
    if raw is None:
        return None
    if isinstance(raw, str):
        return TableRef(name=raw)
    if not isinstance(raw, dict):
        raise InsightConfigError("table references must be strings or mappings")
    name = raw.get("name")
    if not isinstance(name, str) or not name:
        raise InsightConfigError("table reference requires non-empty name")
    database = raw.get("database")
    if isinstance(database, list):
        if len(database) != 2:
            raise InsightConfigError("database list must have two entries")
        return TableRef(name=name, database=(str(database[0]), str(database[1])))
    return TableRef(name=name, database=str(database) if database else None)


def _recipe_table(
    conn: ibis.BaseBackend,
    recipe: InsightRecipe,
    *,
    manifest: dict[str, Any] | None,
    backend: str,
) -> ibis.expr.types.Table:
    table_ref = recipe.table or _manifest_table_ref(recipe.source, manifest, backend)
    return _conn_table(conn, table_ref)


def _joined_recipe_table(
    conn: ibis.BaseBackend,
    recipe: InsightRecipe,
    *,
    manifest: dict[str, Any] | None,
    backend: str,
) -> ibis.expr.types.Table:
    if recipe.join is None:
        raise InsightConfigError(f"Recipe {recipe.name!r} is missing join config")
    left_ref = recipe.table or _manifest_table_ref(recipe.primary, manifest, backend)
    right_ref = recipe.right_table or _manifest_table_ref(recipe.join.right, manifest, backend)
    left = _conn_table(conn, left_ref)
    right = _conn_table(conn, right_ref)
    if recipe.join.left_on not in left.columns:
        raise InsightConfigError(f"Missing left join column: {recipe.join.left_on}")
    if recipe.join.right_on not in right.columns:
        raise InsightConfigError(f"Missing right join column: {recipe.join.right_on}")
    joined = left.left_join(right, left[recipe.join.left_on] == right[recipe.join.right_on])
    return joined


def _conn_table(conn: ibis.BaseBackend, table_ref: TableRef) -> ibis.expr.types.Table:
    if table_ref.database is None:
        return conn.table(table_ref.name)
    return conn.table(table_ref.name, database=table_ref.database)


def _manifest_table_ref(
    asset_name: str | None,
    manifest: dict[str, Any] | None,
    backend: str,
) -> TableRef:
    if asset_name is None:
        raise InsightConfigError("Recipe needs a table/source/primary asset reference")
    if manifest is None:
        raise InsightConfigError("Manifest is required when recipe references an asset by name")

    for collection in ("sources", "analytics_entities", "metrics_models", "staging_views"):
        for record in manifest.get(collection, []):
            if record.get("name") == asset_name:
                return _record_table_ref(record, backend)
    raise InsightConfigError(f"Unknown manifest asset referenced by recipe: {asset_name}")


def _record_table_ref(record: dict[str, Any], backend: str) -> TableRef:
    config = record["config"]
    name = config.get("table") or record["name"]
    project = config.get("project")
    dataset = config.get("dataset")

    if record.get("type") in {"state_source", "event_source"} and backend == "clickhouse":
        if dataset and name:
            return TableRef(name=f"{dataset}___{name}", database=project)
    if backend == "bigquery" and project and dataset:
        return TableRef(name=name, database=(project, dataset))
    if dataset:
        return TableRef(name=name, database=dataset)
    return TableRef(name=name)


def _validate_order_by(order_by: str | None, spec: OrderBySpec) -> str:
    selected = order_by or spec.default
    if selected not in spec.allowed:
        raise InsightConfigError(
            f"order_by {selected!r} is not allowed; allowed: {', '.join(spec.allowed)}"
        )
    return selected


def _validate_filters(filters: dict[str, Any] | None, spec: FilterSpec) -> dict[str, Any]:
    merged = dict(spec.defaults)
    if filters:
        merged.update(filters)
    unknown = [column for column in merged if column not in spec.allowed]
    if unknown:
        raise InsightConfigError(
            f"Filter(s) not allowed: {', '.join(unknown)}; allowed: {', '.join(spec.allowed)}"
        )
    return merged


def _clamp_limit(limit: int | None, spec: LimitSpec) -> int:
    selected = spec.default if limit is None else int(limit)
    if selected < 1:
        raise InsightConfigError("limit must be positive")
    return min(selected, spec.max)


def _string_tuple(raw: Any, label: str) -> tuple[str, ...]:
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise InsightConfigError(f"{label} must be a list")
    values = tuple(str(value) for value in raw)
    if any(not value for value in values):
        raise InsightConfigError(f"{label} cannot contain empty values")
    return values


def _required_str(raw: dict[str, Any], field: str, recipe_name: str) -> str:
    value = raw.get(field)
    if not isinstance(value, str) or not value:
        raise InsightConfigError(f"Recipe {recipe_name!r} join.{field} is required")
    return value


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise InsightConfigError("Optional string fields must be non-empty strings")
    return value


def _resolve_path(project_path: Path, raw_path: str) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return project_path / path
