"""Tests for the shared source-stage runner (M078)."""

from __future__ import annotations

from types import SimpleNamespace

import ibis
import pandas as pd

from fyrnheim.components.computed_column import ComputedColumn
from fyrnheim.core.source import EventSource, Join, SourceTransforms, StateSource
from fyrnheim.engine import event_source_loader, pipeline, source_stage
from fyrnheim.engine.source_stage import build_source_stage_table


def _table() -> ibis.Table:
    return ibis.memtable(
        pd.DataFrame(
            {
                "id": [1, 2],
                "entity_id": ["u1", "u2"],
                "ts": ["2026-04-27", "2026-04-28"],
                "value": [10, 20],
                "keep": [True, False],
            }
        )
    )


def test_build_source_stage_table_applies_non_fixture_stages_in_order(monkeypatch):
    """The shared helper owns the load-bearing stage order.

    Spies avoid retesting the transform/join/json helpers themselves;
    this test pins the order in which the shared runner invokes them
    around computed_columns and filter.
    """
    calls: list[str] = []
    table = _table()

    def read_table(self, conn, backend, data_dir=None):
        calls.append("read")
        return table

    def apply_transforms(table, transforms):
        calls.append("transforms")
        return table

    def apply_joins(table, joins, source_registry, right_pk_registry):
        calls.append("joins")
        return table

    def apply_json_path(table, fields):
        calls.append("json_path")
        return table

    monkeypatch.setattr(StateSource, "read_table", read_table)
    monkeypatch.setattr(source_stage, "_apply_source_transforms", apply_transforms)
    monkeypatch.setattr(source_stage, "_apply_joins", apply_joins)
    monkeypatch.setattr(source_stage, "_apply_json_path_extractions", apply_json_path)

    source = StateSource(
        name="state",
        project="p",
        dataset="d",
        table="t",
        id_field="id",
        transforms=SourceTransforms(),
        fields=[],
        joins=[Join(source_name="lookup", join_key="id")],
        computed_columns=[
            ComputedColumn(name="computed", expression="t.value + 1"),
        ],
        filter="t.keep",
    )

    result = build_source_stage_table(
        source,
        conn=object(),
        backend="duckdb",
        source_registry={"lookup": table},
        right_pk_registry={"lookup": "id"},
    )

    calls.append("execute")
    df = result.execute()

    assert calls == [
        "read",
        "transforms",
        "joins",
        "json_path",
        "execute",
    ]
    assert df["computed"].tolist() == [11]
    assert df["id"].tolist() == [1]


def test_build_source_stage_table_preserves_fixture_shadow_semantics(monkeypatch):
    """Fixture-shadow skips transforms/joins/json_path/filter but keeps M075.

    Computed columns still apply unless their output already exists in the
    fixture, in which case the fixture value is preserved.
    """
    table = ibis.memtable(
        pd.DataFrame(
            {
                "id": [1],
                "base": [41],
                "existing": ["fixture-value"],
            }
        )
    )

    def read_table(self, conn, backend, data_dir=None):
        return table

    def fail_stage(*args, **kwargs):  # pragma: no cover - assertion helper
        raise AssertionError("fixture-shadow should skip this stage")

    monkeypatch.setattr(StateSource, "read_table", read_table)
    monkeypatch.setattr(source_stage, "_apply_source_transforms", fail_stage)
    monkeypatch.setattr(source_stage, "_apply_joins", fail_stage)
    monkeypatch.setattr(source_stage, "_apply_json_path_extractions", fail_stage)

    source = StateSource(
        name="state",
        project="p",
        dataset="d",
        table="t",
        id_field="id",
        duckdb_path="state.parquet",
        duckdb_fixture_is_transformed=True,
        transforms=SourceTransforms(),
        fields=[],
        joins=[Join(source_name="lookup", join_key="id")],
        computed_columns=[
            ComputedColumn(name="existing", expression="t.missing + 1"),
            ComputedColumn(name="created", expression="t.base + 1"),
        ],
        filter="t.missing == 1",
    )

    result = build_source_stage_table(source, conn=object(), backend="duckdb")
    df = result.execute()

    assert df["existing"].tolist() == ["fixture-value"]
    assert df["created"].tolist() == [42]


def test_state_source_builder_delegates_to_shared_source_stage(monkeypatch):
    table = _table()
    seen: dict[str, object] = {}

    def shared_helper(source, conn, backend, **kwargs):
        seen["source"] = source
        seen["backend"] = backend
        seen["source_kind"] = kwargs["source_kind"]
        return table

    monkeypatch.setattr(pipeline, "build_source_stage_table", shared_helper)

    source = StateSource(
        name="state",
        project="p",
        dataset="d",
        table="t",
        id_field="id",
    )
    config = SimpleNamespace(backend="duckdb", data_dir=None)

    result = pipeline._build_state_source_table(source, config, conn=object())

    assert result is table
    assert seen == {
        "source": source,
        "backend": "duckdb",
        "source_kind": "StateSource",
    }


def test_event_source_builder_delegates_to_shared_source_stage(monkeypatch):
    table = _table()
    seen: dict[str, object] = {}

    def shared_helper(source, conn, backend, **kwargs):
        seen["source"] = source
        seen["backend"] = backend
        seen["source_kind"] = kwargs["source_kind"]
        return table

    monkeypatch.setattr(event_source_loader, "build_source_stage_table", shared_helper)

    source = EventSource(
        name="event",
        project="p",
        dataset="d",
        table="t",
        entity_id_field="entity_id",
        timestamp_field="ts",
    )

    result = event_source_loader._build_event_source_table(
        conn=object(),
        event_source=source,
        backend="duckdb",
    )

    assert result is table
    assert seen == {
        "source": source,
        "backend": "duckdb",
        "source_kind": "EventSource",
    }
