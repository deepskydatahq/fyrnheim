"""Activity definitions and trigger types."""

from typing import Annotated, Literal

from pydantic import BaseModel, Discriminator, Field, Tag


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
