"""Source configuration classes for fyrnheim entities."""

import os
from typing import Any

from pydantic import BaseModel, Field as PydanticField, field_validator, model_validator

from fyrnheim.components.computed_column import ComputedColumn
from fyrnheim.core.staging_view import StagingView


class Field(BaseModel):
    """Defines a source field with its type and metadata.

    ``type`` is a BigQuery-style type label. For ``json_path`` extraction,
    the engine supports: ``STRING``, ``INT64``, ``FLOAT64``, ``BOOLEAN``,
    ``DATE``, ``TIMESTAMP``, ``BYTES``. Unsupported types raise
    ``ValueError`` at pipeline setup.
    """
    name: str
    type: str  # STRING, INT64, FLOAT64, TIMESTAMP, BOOLEAN, DATE, BYTES, etc.
    description: str | None = None
    nullable: bool = True
    # JSON dot-notation path for extraction. Supported forms:
    # ``$.<key>`` (e.g. "$.utm_source") for top-level extraction or
    # ``$.<key1>.<key2>...`` (e.g. "$.user.email") for nested extraction.
    # Bracket notation (``$['key']``, ``$.array[0]``) is NOT supported —
    # raises ValueError at pipeline setup as a future-enhancement signal.
    json_path: str | None = None
    source_column: str | None = PydanticField(
        default=None,
        description=(
            "When json_path is set, extract from this column. If None, "
            "defaults to Field.name — extract from a column matching the "
            "field name."
        ),
    )


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


class Join(BaseModel):
    """Declarative left-join of another declared source for row enrichment.

    Minimum-viable shape (M070, v0.12.0):

    * **Left-join only.** ``how`` is implicit — the engine emits a
      ``left_join`` and there is no toggle to switch to inner / right /
      full-outer / cross. If you need a different join type, use a
      ``StagingView`` (which can express any SQL join shape) and point
      ``upstream`` at it instead.
    * **Single-column equality.** Joins are always
      ``table[left_fk] == other[right_pk]``. Multi-key joins, range
      predicates, NULL-aware predicates, and ON-clause filters are
      deliberately out of scope — file a follow-up if you hit them.
    * **Source-name based lookup.** ``source_name`` must reference
      another ``StateSource`` declared in the same pipeline. The
      pipeline runner topologically sorts sources by their inferred
      join dependencies (cycles raise a clear error at pipeline load).
    * **``join_key`` is the LEFT-side foreign-key column** — the column
      on THIS source's read shape (post-transforms) that points at the
      joined source's primary key. The runner resolves the RIGHT-side
      primary-key column from the joined source's declared ``id_field``.

      Example: ``lifecycle_history`` has ``previous_stage_id`` /
      ``next_stage_id`` columns; ``lifecycle_stage`` has ``id`` as its
      ``id_field``. Declaring two joins —
      ``Join(source_name="lifecycle_stage", join_key="previous_stage_id")``
      and ``Join(source_name="lifecycle_stage", join_key="next_stage_id")``
      — left-joins lifecycle_stage twice using the appropriate FK each
      time. The runner generates predicates
      ``table.previous_stage_id == lifecycle_stage.id`` and
      ``table.next_stage_id == lifecycle_stage.id``.

    Multiple ``Join`` entries on the same source are allowed and apply
    in declaration order (so e.g. duplicate-self-join shapes like
    lifecycle_history's prev/next pair are supported).

    The ``upstream`` (StagingView) and ``joins`` declarations are
    mutually exclusive on a source — ``upstream`` already provides a
    pre-joined warehouse view, so layering declarative joins on top is
    ambiguous (which applies first?). The validator on
    :class:`BaseTableSource` raises ``ValueError`` if both are set.
    """
    source_name: str = PydanticField(
        min_length=1,
        description=(
            "Name of another declared source (StateSource) to left-join. "
            "Must match the joined source's `name` field exactly."
        ),
    )
    join_key: str = PydanticField(
        min_length=1,
        description=(
            "LEFT-side foreign-key column name on THIS source's read "
            "shape (post-transforms). The RIGHT-side primary-key column "
            "is resolved from the joined source's declared `id_field`."
        ),
    )


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
    project: str | None = None
    dataset: str | None = None
    table: str | None = None
    duckdb_path: str | None = None
    duckdb_fixture_is_transformed: bool = PydanticField(
        default=False,
        description=(
            "When True AND the source reads from `duckdb_path` AND the "
            "backend is DuckDB, the engine SKIPS `transforms`, `fields` "
            "(json_path extraction), and `filter` at source read time. "
            "Use this when your parquet fixture is already in the "
            "post-transform shape of the BigQuery output — the fixture "
            "IS the final shape. Re-applying transforms would either "
            "fail (pre-rename columns missing) or double-transform. "
            "WARNING: if the fixture does NOT match the post-transform "
            "BQ output, DuckDB-only runs will produce wrong results "
            "with no backend-parity error (cross-backend tests catch "
            "this). `computed_columns` STILL APPLY on the skip path — "
            "they are backend-independent expressions, not transforms. "
            "Default False preserves v0.9.1 behavior (transforms apply "
            "uniformly on both backends)."
        ),
    )
    upstream: StagingView | None = None
    filter: str | None = PydanticField(
        default=None,
        description=(
            "Ibis-expression string applied after transforms + "
            "computed_columns to filter rows. Evaluated with "
            "{'ibis': ibis, 't': table} context (same pattern as "
            "ComputedColumn.expression). "
            "NOTE: SQL three-valued logic — expressions like "
            "`t.col != True` drop NULL rows silently; use "
            "`t.col.fillna(False) != True` if you want NULLs "
            "treated as False."
        ),
    )

    @field_validator("project", "dataset", "table")
    @classmethod
    def validate_not_empty(cls, v: str | None) -> str | None:
        """Reject empty strings for location fields when explicitly set."""
        if v is not None and not v:
            raise ValueError("project, dataset, and table cannot be empty strings")
        return v

    @model_validator(mode="after")
    def _validate_joins_vs_upstream(self) -> "BaseTableSource":
        """``upstream`` (StagingView) and ``joins`` are mutually exclusive.

        ``upstream`` already provides a pre-joined warehouse view;
        declarative ``joins`` apply at the engine layer. Mixing both
        leaves the ordering ambiguous. We surface this at construction
        time (rather than silently picking one) so the user picks
        explicitly. Sources without a ``joins`` field (e.g. the union
        helpers) are unaffected — ``getattr`` returns ``None`` and the
        check no-ops.
        """
        joins = getattr(self, "joins", None)
        if self.upstream is not None and joins:
            raise ValueError(
                "`upstream` (StagingView) and `joins` are mutually exclusive — "
                "`upstream` already provides a pre-joined warehouse view; "
                "declarative `joins` apply at the engine layer. Pick one."
            )
        return self

    @model_validator(mode="after")
    def _resolve_upstream_coords(self) -> "BaseTableSource":
        """Fill missing project/dataset/table from upstream StagingView if set.

        Explicit values always win. If upstream is None and any coord is
        missing, raise a validation error.
        """
        if self.upstream is not None:
            if self.project is None:
                self.project = self.upstream.project
            if self.dataset is None:
                self.dataset = self.upstream.dataset
            if self.table is None:
                self.table = self.upstream.name
        missing = [
            n for n, v in (
                ("project", self.project),
                ("dataset", self.dataset),
                ("table", self.table),
            ) if v is None
        ]
        if missing:
            raise ValueError(
                f"project, dataset, and table are required "
                f"(missing: {', '.join(missing)}); set them explicitly or pass upstream="
            )
        return self

    def read_table(self, conn: Any, backend: str, data_dir: str | os.PathLike[str] | None = None) -> Any:
        """Read table from warehouse or DuckDB based on backend.

        Args:
            conn: Ibis connection
            backend: "bigquery", "duckdb", etc.
            data_dir: Base directory for resolving relative duckdb_path values.
                If provided, relative paths are resolved against this directory.
                Absolute paths and ~ paths are left as-is.

        Returns:
            Ibis table expression
        """
        if backend == "duckdb":
            if not self.duckdb_path:
                raise ValueError("duckdb_path is required for duckdb backend")
            parquet_path = os.path.expanduser(self.duckdb_path)
            if data_dir and not os.path.isabs(parquet_path) and not parquet_path.startswith("~"):
                parquet_path = os.path.join(str(data_dir), parquet_path)
            return conn.read_parquet(parquet_path)
        elif backend == "clickhouse":
            # ClickHouse uses database.table, not (project, dataset) catalogs.
            # dlt-loaded sources use dataset-prefixed names: dataset___table
            table_name = f"{self.dataset}___{self.table}"
            return conn.table(table_name, database=self.project)
        elif backend == "bigquery":
            return conn.table(self.table, database=(self.project, self.dataset))
        else:
            # Generic fallback — try BigQuery-style catalog namespace
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
    full_refresh: bool = PydanticField(
        default=False,
        description=(
            "When True, skip the snapshot-diff machinery entirely and "
            "emit row_appeared for every current row on every run. "
            "Useful for state-only sources where CDC-style field_changed "
            "events are not needed, or to force deterministic "
            "state-reflects-current behavior independent of snapshot-"
            "store state."
        ),
    )
    transforms: SourceTransforms | None = None
    fields: list[Field] | None = None
    computed_columns: list[ComputedColumn] = PydanticField(default_factory=list)
    joins: list[Join] = PydanticField(
        default_factory=list,
        description=(
            "Declarative left-joins to other StateSources declared in "
            "the same pipeline. Applied AFTER `transforms` (rename) and "
            "BEFORE `json_path` extraction in the source pipeline stage "
            "chain. The pipeline runner topologically sorts sources by "
            "their inferred join dependencies — declaration order is "
            "preserved when no joins are declared. See :class:`Join` for "
            "the join_key column-name semantics. Mutually exclusive with "
            "`upstream` (StagingView). M070 / v0.12.0; EventSource is a "
            "future enhancement (file a follow-up if needed)."
        ),
    )


class EventSource(BaseTableSource):
    """Source for event-shaped data like page views and transactions.

    Represents a stream of events associated with an entity.
    Requires entity_id_field (foreign key to entity) and timestamp_field.
    Optionally, set event_type (static string) or event_type_field (column name)
    to classify events, but not both.

    payload_exclude lists column names to drop from the packed JSON payload —
    useful for skipping noisy nested columns (e.g. GA4 event_params,
    user_properties) without building a flattening view in the warehouse.
    """
    name: str = PydanticField(min_length=1)
    entity_id_field: str = PydanticField(min_length=1)
    timestamp_field: str = PydanticField(min_length=1)
    event_type: str | None = None
    event_type_field: str | None = None
    transforms: SourceTransforms | None = None
    fields: list[Field] | None = None
    computed_columns: list[ComputedColumn] = PydanticField(default_factory=list)
    payload_exclude: list[str] = PydanticField(default_factory=list)

    @model_validator(mode="after")
    def _validate_event_type_exclusivity(self) -> "EventSource":
        """Ensure event_type and event_type_field are mutually exclusive."""
        if self.event_type is not None and self.event_type_field is not None:
            raise ValueError(
                "EventSource cannot have both 'event_type' and 'event_type_field'; "
                "use one or the other"
            )
        return self
