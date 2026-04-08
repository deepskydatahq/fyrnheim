"""StagingView primitive for declaring in-warehouse derived sources."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any, Literal

import jinja2
from pydantic import BaseModel, Field as PydanticField, field_validator, model_validator

_WHITESPACE_RE = re.compile(r"\s+")


class StagingView(BaseModel):
    """Declares a derived warehouse object (view) as a first-class model asset.

    In v1, only materialization='view' is supported. The StagingView SQL is
    rendered through jinja2 using sql_params with StrictUndefined so missing
    parameters fail fast. content_hash() returns a deterministic hash that is
    stable across whitespace-only changes but sensitive to semantic changes.
    """

    name: str = PydanticField(min_length=1)
    project: str = PydanticField(min_length=1)
    dataset: str = PydanticField(min_length=1)
    sql: str = PydanticField(min_length=1)
    materialization: Literal["view"] = "view"
    sql_params: dict[str, Any] = PydanticField(default_factory=dict)
    depends_on: list[str] = PydanticField(default_factory=list)
    description: str | None = None
    tags: list[str] = PydanticField(default_factory=list)

    @field_validator("sql", mode="before")
    @classmethod
    def _load_sql_from_path(cls, v: Any) -> Any:
        """If sql is a pathlib.Path, read it at validation time."""
        if isinstance(v, Path):
            return v.read_text(encoding="utf-8")
        return v

    @model_validator(mode="after")
    def _validate_sql_not_blank(self) -> StagingView:
        if not self.sql or not self.sql.strip():
            raise ValueError("sql must not be empty")
        return self

    @property
    def rendered_sql(self) -> str:
        """Return SQL with sql_params rendered via jinja2 (StrictUndefined)."""
        return self.render_sql()

    def render_sql(self) -> str:
        """Render self.sql as a jinja2 template using self.sql_params.

        Raises a ValueError with the missing param name when a template
        references an undefined variable.
        """
        env = jinja2.Environment(
            undefined=jinja2.StrictUndefined,
            autoescape=False,
            keep_trailing_newline=True,
        )
        try:
            template = env.from_string(self.sql)
            return template.render(**self.sql_params)
        except jinja2.UndefinedError as exc:
            # jinja2 messages look like: "'foo' is undefined"
            msg = str(exc)
            match = re.search(r"'([^']+)' is undefined", msg)
            param = match.group(1) if match else msg
            raise ValueError(
                f"StagingView '{self.name}' is missing sql_params entry: {param}"
            ) from exc

    def content_hash(self) -> str:
        """Return a deterministic sha256 hex digest of the normalized content.

        Normalization collapses whitespace runs to single spaces and strips
        trailing whitespace, so purely cosmetic SQL edits do not change the
        hash. Changes to materialization or dataset always change the hash.
        """
        normalized_sql = _WHITESPACE_RE.sub(" ", self.sql).strip()
        payload = f"{normalized_sql}|{self.materialization}|{self.dataset}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()
