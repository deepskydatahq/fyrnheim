"""Layer configuration classes for entity transformation pipelines."""

from typing import Any

from pydantic import BaseModel, Field as PydanticField

from .types import MaterializationType

# Forward reference: resolved to fyrnheim.components.ComputedColumn
# via model_rebuild() in fyrnheim/__init__.py
ComputedColumn = Any


class PrepLayer(BaseModel):
    """Prep/staging layer configuration."""

    model_name: str
    computed_columns: list[ComputedColumn] = PydanticField(default_factory=list)
    materialization: MaterializationType = MaterializationType.TABLE
    target_schema: str = "prep"
    tags: list[str] = PydanticField(default_factory=list)
    quality_checks: list[Any] = PydanticField(default_factory=list)
    depends_on: list[str] = PydanticField(default_factory=list)

    def model_post_init(self, __context: Any) -> None:
        """Add default tags."""
        if "prep" not in self.tags:
            self.tags.append("prep")


class DimensionLayer(BaseModel):
    """Dimension layer configuration (Type 1 or Type 2 SCD)."""

    model_name: str
    computed_columns: list[ComputedColumn] = PydanticField(default_factory=list)
    materialization: MaterializationType = MaterializationType.TABLE
    target_schema: str = "business"
    tags: list[str] = PydanticField(default_factory=list)
    quality_checks: list[Any] = PydanticField(default_factory=list)


class SnapshotLayer(BaseModel):
    """Snapshot layer configuration (daily snapshots)."""

    enabled: bool = True
    date_column: str = "ds"
    natural_key: str | list[str] = "id"
    deduplication_order_by: str = "updated_at DESC"
    include_validity_range: bool = False
    partitioning_field: str = "ds"
    partitioning_type: str = "DAY"
    clustering_fields: list[str] = PydanticField(default_factory=list)
    materialization: MaterializationType = MaterializationType.INCREMENTAL
