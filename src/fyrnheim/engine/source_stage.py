"""Shared source-stage processing for StateSource and EventSource.

The source-stage chain is the behavior-preserving part shared by
StateSource and EventSource before their source-specific conversions:

``read_table -> transforms -> joins -> json_path -> computed_columns -> filter``

StateSource then runs snapshot diff. EventSource then packs rows into
the universal event schema. Keeping the shared chain here prevents future
source-stage extensions from drifting between the two paths.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import ibis

from fyrnheim.core.source import BaseTableSource
from fyrnheim.engine.source_transforms import (
    _apply_joins,
    _apply_json_path_extractions,
    _apply_source_transforms,
    _reads_duckdb_fixture,
)


def build_source_stage_table(
    source: BaseTableSource,
    conn: ibis.BaseBackend,
    backend: str,
    *,
    data_dir: str | os.PathLike[str] | None = None,
    source_registry: dict[str, ibis.Table] | None = None,
    right_pk_registry: dict[str, str] | None = None,
    log: logging.Logger | None = None,
    source_kind: str | None = None,
) -> ibis.Table:
    """Run the shared source-stage chain for StateSource/EventSource.

    Stage order is load-bearing and intentionally mirrors the M068-M076
    feature sequence:

    ``read_table -> transforms -> joins -> json_path -> computed_columns -> filter``

    M072 fixture-shadow semantics are preserved: when the source reads a
    transformed DuckDB fixture, transforms/joins/json_path/filter skip.
    Computed columns still apply unless M075's skip-if-output-exists rule
    preserves a precomputed fixture column.

    Args:
        source: StateSource or EventSource instance. The public models share
            the fields used here via ``BaseTableSource`` plus duck-typed
            ``transforms``, ``joins``, ``fields``, and ``computed_columns``.
        conn: Ibis backend connection.
        backend: Backend name passed through to ``source.read_table``.
        data_dir: Optional base directory for relative DuckDB fixture paths.
        source_registry: Mapping of loaded sibling source names to their
            post-stage Ibis tables for source-level joins.
        right_pk_registry: Mapping of join target source names to their
            right-side primary key columns.
        log: Logger for source-specific diagnostics.
        source_kind: Human-readable source kind for log messages. Defaults
            to the source class name.

    Returns:
        The post-stage Ibis table, before StateSource snapshot diff or
        EventSource event-shape conversion.
    """
    logger = log or logging.getLogger("fyrnheim.source_stage")
    kind = source_kind or type(source).__name__
    source_name = getattr(source, "name", "<unnamed>")

    table = source.read_table(conn, backend, data_dir=data_dir)
    reads_fixture = _reads_duckdb_fixture(source, backend)

    if reads_fixture:
        logger.info(
            "%s %s: duckdb_fixture_is_transformed=True, "
            "skipping transforms/joins/fields/filter (reading duckdb_path fixture)",
            kind,
            source_name,
        )
    else:
        table = _apply_source_transforms(table, getattr(source, "transforms", None))

        joins = getattr(source, "joins", None) or []
        if joins:
            logger.info(
                "%s %s: applying %d join(s) to %s",
                kind,
                source_name,
                len(joins),
                [j.source_name for j in joins],
            )
            table = _apply_joins(
                table,
                joins,
                source_registry or {},
                right_pk_registry or {},
            )

        table = _apply_json_path_extractions(table, getattr(source, "fields", None))

    computed_columns = getattr(source, "computed_columns", None) or []
    for cc in computed_columns:
        if reads_fixture and cc.name in table.columns:
            logger.info(
                "%s %s: computed_column %s skipped (output already in fixture)",
                kind,
                source_name,
                cc.name,
            )
            continue
        table = table.mutate(**{cc.name: eval(cc.expression, {"ibis": ibis, "t": table})})  # noqa: S307

    source_filter: Any = getattr(source, "filter", None)
    if not reads_fixture and source_filter:
        table = table.filter(eval(source_filter, {"ibis": ibis, "t": table}))  # noqa: S307

    return table
