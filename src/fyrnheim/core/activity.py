"""Activity layer configuration for entities."""

from typing import Literal

from pydantic import BaseModel, field_validator


class ActivityType(BaseModel):
    """Defines one activity type derived from an entity."""

    name: str
    trigger: Literal["row_appears", "status_becomes", "field_changes"]
    timestamp_field: str
    values: list[str] | None = None
    field: str | None = None


class ActivityConfig(BaseModel):
    """Activity layer configuration for an entity."""

    model_name: str
    types: list[ActivityType]
    entity_id_field: str
    person_id_field: str | None = None
    anon_id_field: str | None = None

    @field_validator("types")
    @classmethod
    def validate_types(cls, v: list) -> list:
        if not v:
            raise ValueError("ActivityConfig requires at least one activity type")
        return v

    def model_post_init(self, __context) -> None:
        if self.person_id_field is None and self.anon_id_field is None:
            raise ValueError("ActivityConfig requires person_id_field or anon_id_field")

    @property
    def identity_field(self) -> str:
        result = self.person_id_field or self.anon_id_field
        assert result is not None
        return result
