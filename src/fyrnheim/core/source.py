"""Source configuration classes for fyrnheim entities."""

import os
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field as PydanticField, field_validator, model_validator

from fyrnheim.components.computed_column import ComputedColumn


class Field(BaseModel):
    """Defines a source field with its type and metadata."""
    name: str
    type: str  # STRING, INT64, FLOAT64, TIMESTAMP, BOOLEAN, DATE, BYTES, etc.
    description: str | None = None
    nullable: bool = True
    json_path: str | None = None  # JSON path for extraction (e.g., "$.utm_source")


class TypeCast(BaseModel):
    """Type cast configuration."""
    field: str
    target_type: str


class Rename(BaseModel):
    """Column rename configuration."""
    from_name: str
    to_name: str


class Divide(BaseModel):
    """Divide column by constant (e.g., cents to dollars)."""
    field: str
    divisor: float
    target_type: str = "decimal"
    suffix: str = "_amount"


class Multiply(BaseModel):
    """Multiply column by constant."""
    field: str
    multiplier: float
    target_type: str = "decimal"
    suffix: str = "_value"


class SourceTransforms(BaseModel):
    """Read-time transformations applied to source data."""
    type_casts: list[TypeCast] = PydanticField(default_factory=list)
    renames: list[Rename] = PydanticField(default_factory=list)
    divides: list[Divide] = PydanticField(default_factory=list)
    multiplies: list[Multiply] = PydanticField(default_factory=list)


class BaseTableSource(BaseModel):
    """Base configuration for table sources.

    Provides common fields for sources that read from a warehouse table
    or local parquet files (via duckdb_path).
    """
    project: str = PydanticField(min_length=1)
    dataset: str = PydanticField(min_length=1)
    table: str = PydanticField(min_length=1)
    duckdb_path: str | None = None

    @field_validator("project", "dataset", "table")
    @classmethod
    def validate_not_empty(cls, v: str) -> str:
        """Reject empty strings for required location fields."""
        if not v:
            raise ValueError("project, dataset, and table are required")
        return v

    def read_table(self, conn: Any, backend: str) -> Any:
        """Read table from warehouse or DuckDB based on backend.

        Args:
            conn: Ibis connection
            backend: "bigquery", "duckdb", etc.

        Returns:
            Ibis table expression
        """
        if backend == "duckdb":
            if not self.duckdb_path:
                raise ValueError("duckdb_path is required for duckdb backend")
            parquet_path = os.path.expanduser(self.duckdb_path)
            return conn.read_parquet(parquet_path)
        else:
            return conn.table(self.table, database=(self.project, self.dataset))


class TableSource(BaseTableSource):
    """Standard table source with optional field definitions and transforms.

    When used inside a UnionSource, the optional field_mappings and
    literal_columns enable per-source normalization before union:

    - field_mappings: Rename source columns before union.
      Keys are source column names, values are unified column names.
      Example: {'contact_email': 'email'} renames the source's
      'contact_email' column to 'email'.

    - literal_columns: Inject constant values as new columns.
      Example: {'product_type': 'video'} adds a column 'product_type'
      with value 'video' for every row from this source.
    """
    transforms: SourceTransforms | None = None
    fields: list[Field] | None = None
    field_mappings: dict[str, str] = PydanticField(default_factory=dict)
    literal_columns: dict[str, Any] = PydanticField(default_factory=dict)


class DerivedEntitySource(BaseModel):
    """Source from identity graph or derived logic."""
    type: Literal["identity_graph"]
    identity_graph: Any = None
    fields: list[Field] | None = None


class IdentityGraphSource(BaseModel):
    """Configuration for one source in an identity graph.

    Exactly one of ``entity`` or ``source`` must be provided:
    - ``entity``: reference to a named entity registered elsewhere.
    - ``source``: an inline TableSource definition (avoids boilerplate entity files).

    When using an inline ``source``, optional ``prep_columns`` allow lightweight
    computed-column transforms before the identity-graph join.
    """
    model_config = ConfigDict(frozen=True)

    name: str = PydanticField(min_length=1)
    entity: str | None = PydanticField(default=None, min_length=1)
    source: TableSource | None = None
    match_key_field: str = PydanticField(min_length=1)
    fields: dict[str, str] = PydanticField(default_factory=dict)
    id_field: str | None = None
    date_field: str | None = None
    prep_columns: list[ComputedColumn] = PydanticField(default_factory=list)

    @model_validator(mode="after")
    def _validate_entity_or_source(self) -> "IdentityGraphSource":
        """Ensure exactly one of entity/source is set."""
        has_entity = self.entity is not None
        has_source = self.source is not None
        if has_entity and has_source:
            raise ValueError(
                "IdentityGraphSource must have either 'entity' or 'source', not both"
            )
        if not has_entity and not has_source:
            raise ValueError(
                "IdentityGraphSource requires either 'entity' or 'source'"
            )
        return self


class IdentityGraphConfig(BaseModel):
    """Configuration for an identity graph that merges multiple sources."""
    model_config = ConfigDict(frozen=True)

    match_key: str = PydanticField(min_length=1)
    sources: list[IdentityGraphSource] = PydanticField(min_length=2)
    priority: list[str]

    @field_validator("priority")
    @classmethod
    def validate_priority_not_empty(cls, v: list[str]) -> list[str]:
        """Ensure priority list is not empty."""
        if not v:
            raise ValueError("priority must not be empty")
        return v

    @field_validator("sources")
    @classmethod
    def validate_unique_source_names(cls, v: list[IdentityGraphSource]) -> list[IdentityGraphSource]:
        """Reject duplicate source names within an identity graph."""
        names = [s.name for s in v]
        if len(names) != len(set(names)):
            dupes = [n for n in names if names.count(n) > 1]
            raise ValueError(f"Duplicate source names: {set(dupes)}")
        return v

    @model_validator(mode="after")
    def validate_priority_matches_sources(self) -> "IdentityGraphConfig":
        """Ensure priority list contains exactly the same names as sources."""
        source_names = {s.name for s in self.sources}
        priority_names = set(self.priority)
        if source_names != priority_names:
            missing_from_priority = source_names - priority_names
            extra_in_priority = priority_names - source_names
            parts = []
            if missing_from_priority:
                parts.append(f"sources not in priority: {missing_from_priority}")
            if extra_in_priority:
                parts.append(f"priority names not in sources: {extra_in_priority}")
            raise ValueError(
                f"priority must contain exactly the source names. {'; '.join(parts)}"
            )
        return self


class DerivedSource(BaseModel):
    """Source for derived entities created via identity graph resolution."""
    model_config = ConfigDict(frozen=True)

    identity_graph: str = PydanticField(min_length=1)
    depends_on: list[str] = PydanticField(default_factory=list)
    identity_graph_config: IdentityGraphConfig | None = None

    @field_validator("identity_graph")
    @classmethod
    def validate_identity_graph(cls, v: str) -> str:
        """Ensure identity_graph is a non-empty string."""
        if not isinstance(v, str) or not v:
            raise ValueError("identity_graph must be a non-empty string")
        return v

    @model_validator(mode="after")
    def _derive_depends_on(self) -> "DerivedSource":
        """Auto-populate depends_on from identity_graph_config sources."""
        if self.identity_graph_config is not None:
            config_entities = [
                s.entity for s in self.identity_graph_config.sources
                if s.entity is not None
            ]
            merged = list(dict.fromkeys(list(self.depends_on) + config_entities))
            object.__setattr__(self, "depends_on", merged)
        return self


class AggregationSource(BaseModel):
    """Source for entities aggregated from other entities.

    Example: Account entity aggregating from Person entity.
    """
    source_entity: str
    group_by_column: str
    filter_expression: str | None = None
    fields: list[Field] | None = None
    depends_on: list[str] = PydanticField(default_factory=list)
    aggregations: list[ComputedColumn] = PydanticField(default_factory=list)


class EventAggregationSource(BaseTableSource):
    """Source for entities aggregated from raw event streams.

    Handles the pattern: raw events -> pre-processing -> GROUP BY -> entity.
    Used for transforming event-level data into entity-level records.
    """
    group_by_column: str = PydanticField(min_length=1)
    group_by_expression: str | None = None
    filter_expression: str | None = None
    fields: list[Field] | None = None

    @field_validator("group_by_column")
    @classmethod
    def validate_group_by_column(cls, v: str) -> str:
        """Ensure group_by_column is not empty."""
        if not v:
            raise ValueError("group_by_column is required")
        return v


class UnionSource(BaseModel):
    """Source that unions multiple table sources into a common schema.

    Used for entities that combine data from multiple upstream sources
    into a single unified table.
    """
    sources: list[TableSource]

    @field_validator("sources")
    @classmethod
    def validate_sources(cls, v: list) -> list:
        """Ensure at least one source is provided for union."""
        if not v:
            raise ValueError("UnionSource requires at least one source")
        return v
