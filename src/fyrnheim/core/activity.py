"""Activity layer configuration for entities."""

from typing import Annotated, Any, Literal

from pydantic import BaseModel, Discriminator, Field, Tag, field_validator


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

    def model_post_init(self, __context: Any) -> None:
        if self.person_id_field is None and self.anon_id_field is None:
            raise ValueError("ActivityConfig requires person_id_field or anon_id_field")

    @property
    def identity_field(self) -> str:
        result = self.person_id_field or self.anon_id_field
        assert result is not None
        return result


# ---------------------------------------------------------------------------
# M028 — Activity definitions and named events (trigger models)
# ---------------------------------------------------------------------------


class RowAppeared(BaseModel):
    """Trigger that fires when a new row appears in the source."""

    trigger_type: Literal["row_appeared"] = "row_appeared"


class FieldChanged(BaseModel):
    """Trigger that fires when a field changes value."""

    trigger_type: Literal["field_changed"] = "field_changed"
    field: str
    from_values: list[str] | None = None
    to_values: list[str] | None = None


class RowDisappeared(BaseModel):
    """Trigger that fires when a row disappears from the source."""

    trigger_type: Literal["row_disappeared"] = "row_disappeared"


class EventOccurred(BaseModel):
    """Trigger that fires when an event occurs in an event source."""

    trigger_type: Literal["event_occurred"] = "event_occurred"
    event_type: str | None = None


TriggerType = Annotated[
    Annotated[RowAppeared, Tag("row_appeared")]
    | Annotated[FieldChanged, Tag("field_changed")]
    | Annotated[RowDisappeared, Tag("row_disappeared")]
    | Annotated[EventOccurred, Tag("event_occurred")],
    Discriminator("trigger_type"),
]


class ActivityDefinition(BaseModel):
    """A top-level asset that interprets raw events into named business events."""

    name: str = Field(min_length=1)
    source: str = Field(min_length=1)
    trigger: TriggerType
    include_fields: list[str] = Field(default_factory=list)
