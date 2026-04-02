"""Top-level identity graph asset models.

These are the NEW top-level asset types for identity resolution.
They are distinct from the entity-centric IdentityGraphConfig and
IdentityGraphSource in source.py, which remain unchanged.
"""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class IdentitySource(BaseModel):
    """A source that contributes identifiers to an identity graph.

    Each source provides an id_field (the source-local identifier) and
    a match_key_field used to link records across sources.
    """

    model_config = ConfigDict(frozen=True)

    source: str = Field(min_length=1)
    id_field: str = Field(min_length=1)
    match_key_field: str = Field(min_length=1)


class IdentityGraph(BaseModel):
    """Top-level identity graph asset.

    Observes match keys across event streams from multiple sources and
    produces a canonical_id mapping.

    Requires at least 2 sources (identity resolution needs multiple
    streams to be meaningful).
    """

    model_config = ConfigDict(frozen=True)

    name: str = Field(min_length=1)
    canonical_id: str = Field(min_length=1)
    sources: list[IdentitySource] = Field(min_length=2)
    resolution_strategy: Literal["match_key"] = "match_key"

    @field_validator("sources")
    @classmethod
    def validate_min_sources(cls, v: list[IdentitySource]) -> list[IdentitySource]:
        """Ensure at least 2 sources are provided."""
        if len(v) < 2:
            raise ValueError("IdentityGraph requires at least 2 sources")
        return v
