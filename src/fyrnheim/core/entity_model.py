"""EntityModel and StateField configuration for state projections."""

from typing import Literal

from pydantic import BaseModel, Field, model_validator

from fyrnheim.components.computed_column import ComputedColumn


class StateField(BaseModel):
    """Defines how a single field is projected from the activity stream."""

    name: str = Field(min_length=1)
    source: str = Field(min_length=1)
    field: str = Field(min_length=1)
    strategy: Literal["latest", "first", "coalesce"]
    priority: list[str] | None = None

    @model_validator(mode="after")
    def _validate_coalesce_priority(self) -> "StateField":
        if self.strategy == "coalesce" and not self.priority:
            raise ValueError("coalesce strategy requires a priority list")
        return self


class EntityModel(BaseModel):
    """Projects current state from the enriched activity stream."""

    name: str = Field(min_length=1)
    identity_graph: str | None = None
    state_fields: list[StateField] = Field(min_length=1)
    computed_fields: list[ComputedColumn] = Field(default_factory=list)
