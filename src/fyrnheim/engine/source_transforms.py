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

from fyrnheim.core.source import Field, SourceTransforms

# Regex for allowed json_path: top-level ``$.<key>`` where key is a
# python-identifier-style token (letters/digits/underscore, leading
# letter or underscore). Nested paths like ``$.a.b`` are deliberately
# rejected — M069 scope limit, documented in the CHANGELOG.
_JSON_PATH_TOPLEVEL = re.compile(r"^\$\.([a-zA-Z_][a-zA-Z0-9_]*)$")

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
    * Only top-level ``$.<key>`` paths are supported; nested paths
      (``$.a.b``) raise ``ValueError`` with a pointer at the
      future-enhancement request.
    * The extracted JSON scalar is unwrapped to the ibis type
      corresponding to ``field.type`` via
      ``.cast("json")[<key>].unwrap_as(<ibis_type>)`` — which returns
      NULL when the JSON scalar's type does not match (best-effort
      extraction).

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
            (nested path) or an unsupported ``type``.
    """
    if not fields:
        return table

    for f in fields:
        if f.json_path is None:
            continue

        m = _JSON_PATH_TOPLEVEL.match(f.json_path)
        if not m:
            raise ValueError(
                f"Field {f.name!r}: json_path {f.json_path!r} is not a supported "
                "top-level path ($.<key>). Nested paths (e.g. '$.a.b') are not "
                "yet supported — file a future-enhancement request to extend "
                "fyrnheim's json_path wiring."
            )
        key = m.group(1)

        ibis_type = _FIELD_TYPE_TO_IBIS.get(f.type)
        if ibis_type is None:
            raise ValueError(
                f"Field {f.name!r}: type {f.type!r} is not supported for "
                f"json_path extraction. Supported types: "
                f"{sorted(_FIELD_TYPE_TO_IBIS)}."
            )

        source_col = f.source_column or f.name
        table = table.mutate(
            **{
                f.name: table[source_col].cast("json")[key].unwrap_as(ibis_type),
            }
        )

    return table
