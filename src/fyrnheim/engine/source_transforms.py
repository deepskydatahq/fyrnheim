"""Source-level transforms applied at read time.

Shared by ``load_event_source`` and ``_load_state_source``. Applies
:class:`SourceTransforms` in a stable order: ``type_casts`` →
``divides`` → ``multiplies`` → ``renames``.

* ``type_casts`` first so subsequent divide/multiply operations see the
  cast column type.
* ``divides`` / ``multiplies`` create suffixed columns; the originals
  are preserved.
* ``renames`` last so users can rename any column, including ones just
  produced by divides/multiplies.

``computed_columns`` are applied by the callers AFTER this helper, so
user expressions can reference transformed (renamed / cast) columns.

The M069 ``_apply_json_path_extractions`` helper runs between the
transforms and the computed_columns step. See that function's docstring
for the pipeline-stage order rationale.
"""

from __future__ import annotations

import re

import ibis

from fyrnheim.core.source import BaseTableSource, Field, Join, SourceTransforms

# Regex for allowed json_path: dot-notation paths with one or more
# segments where each segment is a python-identifier-style token
# (letters/digits/underscore, leading letter or underscore). Accepts
# ``$.foo``, ``$.foo.bar``, ``$.foo.bar.baz`` etc. Bracket notation
# (``$['key']``, ``$.array[0]``, ``$.users[*]``) is deliberately
# rejected — M074 supports nested dot-notation only; bracket notation
# is a separate future enhancement.
#
# M069 originally rejected nested paths (``$.a.b``); M074 lifts that
# scope limit for the dot-notation case after the client-flowable
# reporter (2026-04-25) hit silent NULLs on BigQuery from
# ``Field(json_path="$.user.email", ...)``.
_JSON_PATH_DOT_NOTATION = re.compile(r"^\$(\.[a-zA-Z_][a-zA-Z0-9_]*)+$")

# Mapping from Field.type (BigQuery-style labels) to ibis type strings
# for use with ``JSONValue.unwrap_as``. Keep this list narrow —
# unsupported types raise a clear error rather than producing a silent
# wrong-type coercion.
_FIELD_TYPE_TO_IBIS: dict[str, str] = {
    "STRING": "string",
    "INT64": "int64",
    "FLOAT64": "float64",
    "BOOLEAN": "bool",
    "DATE": "date",
    "TIMESTAMP": "timestamp",
    "BYTES": "bytes",
}


def _reads_duckdb_fixture(
    source: BaseTableSource,
    backend: str,
) -> bool:
    """True iff the engine is reading the DuckDB fixture parquet path and
    the source has opted into fixture-shadow mode (M072 / FR-8).

    Mirrors the branch in :meth:`BaseTableSource.read_table`
    (``src/fyrnheim/core/source.py``, ~lines 132-161): when
    ``backend == "duckdb"`` the ``read_table`` implementation calls
    ``conn.read_parquet(duckdb_path)`` and requires ``duckdb_path`` to be
    set — there is no live-DuckDB-table branch today. A future
    ``read_table`` that added such a branch would need the gate here to
    mirror the same selector.

    The gate fires only when ALL of:

    * ``source.duckdb_fixture_is_transformed`` is True — explicit opt-in.
    * ``backend == "duckdb"`` — BigQuery / ClickHouse / other backends
      read the raw upstream table and must still apply transforms.
    * ``source.duckdb_path`` is truthy — DuckDB-as-production users
      (live DuckDB tables, no fixture) must still apply transforms.
      Truthiness (not ``is not None``) mirrors ``read_table``'s
      ``if not self.duckdb_path`` check, which treats the empty
      string as missing.

    Returns False otherwise — preserves v0.9.1 uniform-transform behavior
    for the non-opt-in and non-fixture-read cases.
    """
    return (
        bool(source.duckdb_fixture_is_transformed)
        and backend == "duckdb"
        and bool(source.duckdb_path)
    )


def _apply_source_transforms(
    table: ibis.Table,
    transforms: SourceTransforms | None,
) -> ibis.Table:
    """Apply read-time transforms to a source table.

    Args:
        table: The source Ibis table returned from ``source.read_table``.
        transforms: A ``SourceTransforms`` instance or ``None``. When
            ``None`` the table is returned unchanged.

    Returns:
        The transformed Ibis table. Only Ibis ops are used
        (``.cast()``, ``.mutate()``, ``.rename()``) so the result stays
        push-down-safe across backends.
    """
    if transforms is None:
        return table

    # 1. type_casts — applied first so later ops see the cast type.
    for tc in transforms.type_casts:
        table = table.mutate(**{tc.field: table[tc.field].cast(tc.target_type)})

    # 2. divides — create suffixed column (original preserved).
    for d in transforms.divides:
        new_col = f"{d.field}{d.suffix}"
        table = table.mutate(
            **{new_col: (table[d.field] / d.divisor).cast(d.target_type)}
        )

    # 3. multiplies — create suffixed column (original preserved).
    for m in transforms.multiplies:
        new_col = f"{m.field}{m.suffix}"
        table = table.mutate(
            **{new_col: (table[m.field] * m.multiplier).cast(m.target_type)}
        )

    # 4. renames — last so users can rename any column, including ones
    #    just produced by divides/multiplies. ibis ``.rename`` takes
    #    ``{new_name: old_name}``.
    for r in transforms.renames:
        table = table.rename({r.to_name: r.from_name})

    return table


def _parse_json_path_segments(json_path: str) -> list[str]:
    """Validate a ``json_path`` and return its dot-notation segment keys.

    Accepts dot-notation paths with one or more segments where each
    segment is a python-identifier-style token. Bracket notation
    (``$['key']``, ``$.array[0]``) is NOT yet supported — it raises
    ``ValueError`` as a future-enhancement signal.

    Examples
    --------
    >>> _parse_json_path_segments("$.utm_source")
    ['utm_source']
    >>> _parse_json_path_segments("$.user.email")
    ['user', 'email']
    >>> _parse_json_path_segments("$.a.b.c")
    ['a', 'b', 'c']

    Args:
        json_path: The path string from ``Field.json_path``.

    Returns:
        Ordered list of segment keys (leading ``$`` stripped, dots
        consumed). Always non-empty when validation passes.

    Raises:
        ValueError: when ``json_path`` does not match the dot-notation
            grammar — bracket notation, empty segments (``$..foo``),
            trailing dots (``$.foo.``), bare ``$`` etc.
    """
    if not _JSON_PATH_DOT_NOTATION.match(json_path):
        raise ValueError(
            f"json_path {json_path!r} is not a supported dot-notation path. "
            "Supported forms: $.<key> or $.<key1>.<key2>... where each key "
            "is a Python-identifier-style token. Bracket notation "
            "($['key'], $.array[0]) is not yet supported — file a "
            "future-enhancement request if needed."
        )
    # Strip leading '$' and split on '.'. The regex guarantees no empty
    # segments, but the filter is defense-in-depth in case the grammar
    # ever loosens.
    return [seg for seg in json_path[1:].split(".") if seg]


def _apply_json_path_extractions(
    table: ibis.Table,
    fields: list[Field] | None,
) -> ibis.Table:
    """Extract JSON values into typed columns for each Field with json_path.

    Applied between ``_apply_source_transforms`` and the caller's
    ``computed_columns`` step — so users can rename a JSON source column
    and then extract from the renamed column, and computed_columns can
    reference the extracted column.

    For each field with ``json_path`` set:

    * ``source_column`` defaults to ``field.name`` when unset (the
      common "extract into a column with the same name" case).
    * Dot-notation paths are supported: ``$.<key>`` for top-level
      extraction or ``$.<key1>.<key2>...`` for nested extraction. The
      engine chains ibis JSON subscripts (``cast('json')[k1][k2]...``)
      across all segments. Bracket notation (``$['key']``,
      ``$.array[0]``) is not yet supported; raises ``ValueError`` as a
      future-enhancement signal.
    * The extracted JSON scalar is unwrapped to the ibis type
      corresponding to ``field.type`` via ``unwrap_as(<ibis_type>)`` —
      which returns NULL when the JSON scalar's type does not match
      (best-effort extraction).

    Args:
        table: The source Ibis table (post-transforms).
        fields: Source fields list (from ``StateSource.fields`` /
            ``EventSource.fields``). When ``None`` or empty, the table
            is returned unchanged.

    Returns:
        The Ibis table with one extra column per Field-with-json_path,
        named ``field.name`` and typed per ``field.type``.

    Raises:
        ValueError: if a field declares an unsupported ``json_path``
            (bracket notation, malformed dot-notation) or an
            unsupported ``type``.
    """
    if not fields:
        return table

    for f in fields:
        if f.json_path is None:
            continue

        try:
            segments = _parse_json_path_segments(f.json_path)
        except ValueError as exc:
            # Re-raise with the field name prefixed so users can locate
            # the offending Field in their source declaration.
            raise ValueError(f"Field {f.name!r}: {exc}") from exc

        ibis_type = _FIELD_TYPE_TO_IBIS.get(f.type)
        if ibis_type is None:
            raise ValueError(
                f"Field {f.name!r}: type {f.type!r} is not supported for "
                f"json_path extraction. Supported types: "
                f"{sorted(_FIELD_TYPE_TO_IBIS)}."
            )

        source_col = f.source_column or f.name
        # Chain subscript access across all dot-notation segments. The
        # single-segment case (``$.foo``) iterates once and matches the
        # M069 behavior bit-for-bit.
        expr = table[source_col].cast("json")
        for key in segments:
            expr = expr[key]
        table = table.mutate(**{f.name: expr.unwrap_as(ibis_type)})

    return table


def _apply_joins(
    table: ibis.Table,
    joins: list[Join],
    source_registry: dict[str, ibis.Table],
    right_pk_registry: dict[str, str],
) -> ibis.Table:
    """Left-join sibling source tables onto ``table`` (M070 / v0.12.0).

    The minimum-viable Join API ships with a single column-name
    semantic (Option A, locked here for v0.12.0):

    * ``Join.join_key`` is the LEFT-side foreign-key column on
      ``table``.
    * The RIGHT-side primary-key column is resolved from the joined
      source's declared ``id_field``, supplied via ``right_pk_registry``.
    * Predicate emitted: ``table[j.join_key] == other[right_pk]``.

    Multiple joins on the same source apply in declaration order — the
    pardot_lifecycle_history shape (two left joins to lifecycle_stage
    via ``previous_stage_id`` and ``next_stage_id``) is the canonical
    test case.

    Args:
        table: The source Ibis table (post-transforms).
        joins: List of :class:`Join` declarations in declaration order.
            When empty / falsy the table is returned unchanged.
        source_registry: Mapping ``source_name → ibis.Table`` for the
            sibling sources already loaded in this pipeline run. The
            pipeline's topological sort guarantees that every
            ``j.source_name`` referenced here is present.
        right_pk_registry: Mapping ``source_name → id_field`` (the
            right-side primary-key column) for the same sibling
            sources. Looked up alongside ``source_registry`` so the
            engine can build the equality predicate without re-reading
            the StateSource declaration here.

    Returns:
        The (potentially multi-)joined Ibis table. ibis resolves
        duplicate column names using its standard ``_right`` /
        positional-index suffix rules — callers that care about the
        joined-column naming should reference the ibis docs.

    Raises:
        ValueError: when a ``Join.source_name`` is not present in
            ``source_registry``. This is normally prevented by the
            topological sort; if it surfaces it usually means a typo
            in ``source_name`` or a source that lives outside this
            pipeline.
    """
    if not joins:
        return table
    for j in joins:
        if j.source_name not in source_registry:
            raise ValueError(
                f"Join references source {j.source_name!r} which has not "
                "been loaded. The pipeline runner topologically sorts "
                "sources by their inferred join dependencies; if you "
                "see this error, the named source is either misspelled "
                "or not declared in this pipeline."
            )
        other = source_registry[j.source_name]
        right_pk = right_pk_registry.get(j.source_name)
        if right_pk is None:
            raise ValueError(
                f"Join references source {j.source_name!r} but no "
                "right-side primary key was registered for it. Only "
                "StateSource targets are supported for join targets in "
                "v0.12.0."
            )
        # ibis 11+ requires distinct identities when the same right
        # table is joined more than once (e.g. lifecycle_history's
        # double-join to lifecycle_stage). ``.view()`` produces a
        # fresh op-tree node that ibis treats as a separate join
        # target. The per-join ``rname`` suffix ensures the resulting
        # column names are unambiguous when right-side columns
        # collide with the LEFT shape (or with prior joined columns).
        # Left column names are preserved via ``lname='{name}'``.
        other_view = other.view()
        suffix = f"_{j.source_name}_{j.join_key}"
        table = table.left_join(
            other_view,
            predicates=[table[j.join_key] == other_view[right_pk]],
            lname="{name}",
            rname="{name}" + suffix,
        )
    return table
