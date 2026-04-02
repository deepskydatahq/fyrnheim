"""Source configuration classes for fyrnheim entities."""

import os
from typing import Any

from pydantic import BaseModel, Field as PydanticField, field_validator, model_validator


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


class StateSource(BaseTableSource):
    """Source for entity-shaped (state) data like CRM exports or user profiles.

    Represents a snapshot of current state for an entity type.
    Requires a name to identify the source and an id_field that uniquely
    identifies each record.
    """
    name: str = PydanticField(min_length=1)
    id_field: str = PydanticField(min_length=1)
    transforms: SourceTransforms | None = None
    fields: list[Field] | None = None


class EventSource(BaseTableSource):
    """Source for event-shaped data like page views and transactions.

    Represents a stream of events associated with an entity.
    Requires entity_id_field (foreign key to entity) and timestamp_field.
    Optionally, set event_type (static string) or event_type_field (column name)
    to classify events, but not both.
    """
    name: str = PydanticField(min_length=1)
    entity_id_field: str = PydanticField(min_length=1)
    timestamp_field: str = PydanticField(min_length=1)
    event_type: str | None = None
    event_type_field: str | None = None
    transforms: SourceTransforms | None = None
    fields: list[Field] | None = None

    @model_validator(mode="after")
    def _validate_event_type_exclusivity(self) -> "EventSource":
        """Ensure event_type and event_type_field are mutually exclusive."""
        if self.event_type is not None and self.event_type_field is not None:
            raise ValueError(
                "EventSource cannot have both 'event_type' and 'event_type_field'; "
                "use one or the other"
            )
        return self
