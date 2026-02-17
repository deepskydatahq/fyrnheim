"""Source mapping for connecting entities to data sources."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, Field as PydanticField, model_validator

if TYPE_CHECKING:
    from .entity import Entity, Source


class SourceMapping(BaseModel):
    """Maps an entity to a specific data source with field mappings.

    This allows the same entity definition to be used with different
    data sources by specifying how source columns map to entity fields.

    Attributes:
        entity: The entity this mapping is for
        source: The data source backing this entity
        field_mappings: Dict mapping entity field names to source column names
                       e.g., {"transaction_id": "id"} means source column "id"
                       maps to entity field "transaction_id"
    """

    model_config = {"arbitrary_types_allowed": True}

    entity: Entity
    source: Source
    field_mappings: dict[str, str] = PydanticField(default_factory=dict)

    @model_validator(mode="after")
    def validate_required_fields(self) -> SourceMapping:
        """Validate that all required fields have mappings."""
        if self.entity.required_fields is None:
            return self

        required_field_names = {f.name for f in self.entity.required_fields}
        mapped_field_names = set(self.field_mappings.keys())

        missing = required_field_names - mapped_field_names
        if missing:
            raise ValueError(
                f"SourceMapping missing required field mappings: {missing}. "
                f"All required fields must be mapped."
            )
        return self
