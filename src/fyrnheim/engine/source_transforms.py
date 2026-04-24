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
"""

from __future__ import annotations

import ibis

from fyrnheim.core.source import SourceTransforms


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
