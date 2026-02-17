"""Core Entity and LayersConfig classes."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, Field as PydanticField, model_validator

from .activity import ActivityConfig
from .analytics import AnalyticsLayer
from .layer import DimensionLayer, PrepLayer, SnapshotLayer
from .source import (
    AggregationSource,
    DerivedSource,
    EventAggregationSource,
    Field,
    TableSource,
    UnionSource,
)

if TYPE_CHECKING:
    from fyrnheim.components import ComputedColumn, Measure
    from fyrnheim.quality import QualityConfig

Source = (
    TableSource
    | DerivedSource
    | AggregationSource
    | EventAggregationSource
    | UnionSource
)


class LayersConfig(BaseModel):
    """Configuration for all transformation layers."""

    prep: PrepLayer | None = None
    dimension: DimensionLayer | None = None
    activity: ActivityConfig | None = None
    snapshot: SnapshotLayer | None = None
    analytics: AnalyticsLayer | None = None

    @model_validator(mode="after")
    def validate_at_least_one_layer(self) -> LayersConfig:
        """Validate at least one layer is configured."""
        if not any(
            [
                self.prep,
                self.dimension,
                self.activity,
                self.snapshot,
                self.analytics,
            ]
        ):
            raise ValueError("At least one layer must be configured")
        return self


class Entity(BaseModel):
    """Complete entity definition.

    An entity represents a business object (customer, transaction, product)
    with its schema, source, transformation layers, and quality rules.

    Two patterns for defining entity fields:
    - Contract pattern: Set required_fields (+ optional_fields) and connect
      data sources via SourceMapping
    - Direct source pattern: Set source to point at data directly

    At least one of required_fields or source must be provided.
    """

    model_config = {"arbitrary_types_allowed": True}

    # Identity
    name: str = PydanticField(min_length=1, pattern=r"^[a-z_][a-z0-9_]*$")
    description: str = PydanticField(min_length=1)

    # Transformation layers (at least one required)
    layers: LayersConfig

    # Contract fields (the "what this entity expects" pattern)
    required_fields: list[Field] | None = None
    optional_fields: list[Field] | None = None

    # Direct source (the "where data comes from" pattern)
    source: Source | None = None

    # Core computed columns - always generated for this entity
    core_computed: list[ComputedColumn] | None = None

    # Core measures - aggregation expressions for this entity
    core_measures: list[Measure] | None = None

    # Visibility
    is_internal: bool = False

    # Quality checks
    quality: QualityConfig | None = None

    @model_validator(mode="after")
    def validate_fields_or_source(self) -> Entity:
        """Validate entity has required_fields or source."""
        if self.required_fields is None and self.source is None:
            raise ValueError("Entity must have required_fields or source")
        return self

    @property
    def all_fields(self) -> list[Field]:
        """All entity fields (required + optional, or from source)."""
        if self.required_fields is not None:
            return self.required_fields + (self.optional_fields or [])
        elif self.source and hasattr(self.source, "fields") and self.source.fields:
            return self.source.fields
        raise ValueError("Entity must have required_fields or source with fields")

    @property
    def all_computed_columns(self) -> list[ComputedColumn]:
        """All computed columns (core + dimension layer)."""
        columns = list(self.core_computed or [])
        if self.layers.dimension and self.layers.dimension.computed_columns:
            columns.extend(self.layers.dimension.computed_columns)
        return columns

    @property
    def all_measures(self) -> list[Measure]:
        """All measures defined for this entity."""
        return list(self.core_measures or [])

    def get_layer(self, layer_name: str) -> BaseModel | None:
        """Get layer by name."""
        return getattr(self.layers, layer_name, None)

    def has_layer(self, layer_name: str) -> bool:
        """Check if a layer is configured."""
        return self.get_layer(layer_name) is not None
