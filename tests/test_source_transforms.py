"""Unit tests for the ``_apply_source_transforms`` helper (M068) and the
``_apply_json_path_extractions`` helper (M069).

Exercises the helpers directly (not through the loaders) to pin:
  * transforms=None returns the table unchanged
  * each transform category (type_cast, rename, divide, multiply) works
  * transform order: type_casts → divides → multiplies → renames
  * M069: json_path extraction via Field.json_path / Field.source_column
"""

from __future__ import annotations

import ibis
import pandas as pd
import pytest

from fyrnheim.core.source import (
    Divide,
    EventSource,
    Field,
    Multiply,
    Rename,
    SourceTransforms,
    StateSource,
    TypeCast,
)
from fyrnheim.engine.source_transforms import (
    _apply_json_path_extractions,
    _apply_source_transforms,
    _reads_duckdb_fixture,
)


def _make_table() -> ibis.Table:
    return ibis.memtable(
        pd.DataFrame(
            {
                "id": [1, 2, 3],
                "amount_cents": [1000, 2500, 4200],
                "name": ["a", "b", "c"],
            }
        )
    )


def test_apply_transforms_none_returns_unchanged() -> None:
    """A None transforms argument returns the table unchanged (identity)."""
    t = _make_table()
    result = _apply_source_transforms(t, None)
    assert result is t


def test_type_cast_applied() -> None:
    """type_cast mutates the column to the target_type."""
    t = _make_table()
    transforms = SourceTransforms(
        type_casts=[TypeCast(field="amount_cents", target_type="float64")]
    )
    result = _apply_source_transforms(t, transforms)
    assert result.schema()["amount_cents"] == ibis.dtype("float64")


def test_rename_applied() -> None:
    """rename swaps the column name from from_name to to_name."""
    t = _make_table()
    transforms = SourceTransforms(
        renames=[Rename(from_name="id", to_name="account_id")]
    )
    result = _apply_source_transforms(t, transforms)
    assert "account_id" in result.columns
    assert "id" not in result.columns


def test_divide_creates_suffixed_column_with_default_suffix() -> None:
    """divide creates ``{field}{suffix}`` column (default suffix ``_amount``)."""
    t = _make_table()
    transforms = SourceTransforms(
        divides=[Divide(field="amount_cents", divisor=100.0)]
    )
    result = _apply_source_transforms(t, transforms)
    # Original column preserved; new suffixed column added.
    assert "amount_cents" in result.columns
    assert "amount_cents_amount" in result.columns
    df = result.execute()
    assert [float(v) for v in df["amount_cents_amount"].tolist()] == [
        10.0,
        25.0,
        42.0,
    ]


def test_multiply_creates_suffixed_column_with_default_suffix() -> None:
    """multiply creates ``{field}{suffix}`` column (default suffix ``_value``)."""
    t = _make_table()
    transforms = SourceTransforms(
        multiplies=[Multiply(field="amount_cents", multiplier=2.0)]
    )
    result = _apply_source_transforms(t, transforms)
    assert "amount_cents" in result.columns
    assert "amount_cents_value" in result.columns
    df = result.execute()
    assert [float(v) for v in df["amount_cents_value"].tolist()] == [
        2000.0,
        5000.0,
        8400.0,
    ]


def test_transform_order_cast_then_divide_then_rename() -> None:
    """Pins the order: cast → divide → rename.

    cast ``amount_cents`` to float64, divide (creating
    ``amount_cents_amount``), then rename ``amount_cents`` →
    ``amount_raw``. The divide sees the cast column; the rename happens
    last so it applies to the (now cast + divided) schema — ``amount_cents``
    disappears and is replaced by ``amount_raw``, while the divided
    suffix column ``amount_cents_amount`` remains.
    """
    t = _make_table()
    transforms = SourceTransforms(
        type_casts=[TypeCast(field="amount_cents", target_type="float64")],
        divides=[Divide(field="amount_cents", divisor=100.0)],
        renames=[Rename(from_name="amount_cents", to_name="amount_raw")],
    )
    result = _apply_source_transforms(t, transforms)
    assert "amount_raw" in result.columns
    assert "amount_cents_amount" in result.columns
    assert "amount_cents" not in result.columns


# ---------------------------------------------------------------------------
# M069: _apply_json_path_extractions helper
# ---------------------------------------------------------------------------


def _json_table() -> ibis.Table:
    """Table with JSON-shaped string columns for extraction tests."""
    return ibis.memtable(
        pd.DataFrame(
            {
                "id": [1, 2, 3],
                "utm_source": [
                    '{"utm_source": "google"}',
                    '{"utm_source": "facebook"}',
                    '{"utm_source": "twitter"}',
                ],
                "custom_type": [
                    '{"value": "premium"}',
                    '{"value": "free"}',
                    '{"value": "trial"}',
                ],
                "numeric_blob": [
                    '{"count": 10}',
                    '{"count": 25}',
                    '{"count": 42}',
                ],
            }
        )
    )


def test_json_path_fields_none_returns_unchanged() -> None:
    """fields=None is a no-op — the table is returned unchanged."""
    t = _json_table()
    assert _apply_json_path_extractions(t, None) is t


def test_json_path_fields_empty_list_returns_unchanged() -> None:
    """fields=[] is a no-op (distinct from None but same behavior)."""
    t = _json_table()
    assert _apply_json_path_extractions(t, []) is t


def test_json_path_extraction_default_source_column() -> None:
    """Field.source_column defaults to Field.name — extract from the
    column with the same name as the field being produced (overwrites
    the JSON column with the extracted scalar)."""
    t = _json_table()
    fields = [Field(name="utm_source", type="STRING", json_path="$.utm_source")]
    result = _apply_json_path_extractions(t, fields)
    df = result.execute()
    # The utm_source column now holds the extracted string, not the JSON blob.
    assert list(df["utm_source"]) == ["google", "facebook", "twitter"]


def test_json_path_extraction_explicit_source_column() -> None:
    """Field.source_column lets you extract from a differently-named
    column (account_type field derived from custom_type JSON column)."""
    t = _json_table()
    fields = [
        Field(
            name="account_type",
            type="STRING",
            json_path="$.value",
            source_column="custom_type",
        )
    ]
    result = _apply_json_path_extractions(t, fields)
    df = result.execute()
    # New column appears with the extracted values; source column untouched.
    assert "account_type" in df.columns
    assert list(df["account_type"]) == ["premium", "free", "trial"]
    # Source column preserved (distinct-column extraction)
    assert list(df["custom_type"]) == list(t.execute()["custom_type"])


def test_json_path_with_int64_type() -> None:
    """INT64 field extracts as int64 — unwrap_as returns the numeric."""
    t = _json_table()
    fields = [
        Field(
            name="event_count",
            type="INT64",
            json_path="$.count",
            source_column="numeric_blob",
        )
    ]
    result = _apply_json_path_extractions(t, fields)
    assert result.schema()["event_count"] == ibis.dtype("int64")
    df = result.execute()
    assert [int(v) for v in df["event_count"].tolist()] == [10, 25, 42]


def test_fields_without_json_path_are_ignored() -> None:
    """Field entries without json_path are descriptive-only metadata and
    do not mutate the table — reassert the pre-M069 model contract."""
    t = _json_table()
    fields = [Field(name="id", type="INT64")]  # no json_path
    result = _apply_json_path_extractions(t, fields)
    # Table unchanged schema-wise.
    assert set(result.columns) == set(t.columns)


def test_nested_json_path_raises_valueerror() -> None:
    """Nested paths (``$.a.b``) are deliberately unsupported in M069 —
    raise a ValueError at pipeline setup pointing at the future
    enhancement, rather than silently doing the wrong thing."""
    t = _json_table()
    fields = [
        Field(
            name="nested",
            type="STRING",
            json_path="$.a.b",
            source_column="custom_type",
        )
    ]
    with pytest.raises(ValueError, match="Nested paths"):
        _apply_json_path_extractions(t, fields)


def test_unknown_field_type_raises_valueerror() -> None:
    """Unsupported Field.type raises a clear error at pipeline setup
    rather than coercing to a silently-wrong ibis type."""
    t = _json_table()
    fields = [
        Field(
            name="blob",
            type="GEOGRAPHY",
            json_path="$.value",
            source_column="custom_type",
        )
    ]
    with pytest.raises(ValueError, match="is not supported"):
        _apply_json_path_extractions(t, fields)


def test_json_path_non_toplevel_key_with_bracket_raises() -> None:
    """Paths that aren't the ``$.<key>`` form (e.g. array indexes) raise."""
    t = _json_table()
    fields = [
        Field(
            name="first",
            type="STRING",
            json_path="$[0]",
            source_column="custom_type",
        )
    ]
    with pytest.raises(ValueError, match="top-level path"):
        _apply_json_path_extractions(t, fields)


# ---------------------------------------------------------------------------
# M072 / FR-8: backend-aware fixture shadowing
# ---------------------------------------------------------------------------


def test_fixture_is_transformed_defaults_false() -> None:
    """New `duckdb_fixture_is_transformed` field defaults to False on all
    BaseTableSource subclasses — v0.9.1-behavior-preservation invariant.
    """
    state = StateSource(
        name="s",
        project="p",
        dataset="d",
        table="t",
        id_field="id",
    )
    event = EventSource(
        name="e",
        project="p",
        dataset="d",
        table="t",
        entity_id_field="id",
        timestamp_field="ts",
    )
    assert state.duckdb_fixture_is_transformed is False
    assert event.duckdb_fixture_is_transformed is False


def test_reads_duckdb_fixture_flag_true_and_duckdb_and_duckdb_path() -> None:
    """Gate fires on (flag=True, backend='duckdb', duckdb_path set)."""
    src = StateSource(
        name="s",
        project="p",
        dataset="d",
        table="t",
        id_field="id",
        duckdb_path="/tmp/fixture.parquet",
        duckdb_fixture_is_transformed=True,
    )
    assert _reads_duckdb_fixture(src, "duckdb") is True


def test_reads_duckdb_fixture_false_when_flag_not_set() -> None:
    """Default flag=False — gate does not fire even with duckdb_path."""
    src = StateSource(
        name="s",
        project="p",
        dataset="d",
        table="t",
        id_field="id",
        duckdb_path="/tmp/fixture.parquet",
    )
    assert _reads_duckdb_fixture(src, "duckdb") is False


def test_reads_duckdb_fixture_false_when_backend_is_bigquery() -> None:
    """Gate never fires on non-DuckDB backends — the flag is
    DuckDB-fixture-specific, BQ reads the raw upstream table."""
    src = StateSource(
        name="s",
        project="p",
        dataset="d",
        table="t",
        id_field="id",
        duckdb_path="/tmp/fixture.parquet",
        duckdb_fixture_is_transformed=True,
    )
    assert _reads_duckdb_fixture(src, "bigquery") is False
    assert _reads_duckdb_fixture(src, "clickhouse") is False


def test_reads_duckdb_fixture_false_when_no_duckdb_path() -> None:
    """Gate does not fire when duckdb_path is None — DuckDB-as-production
    backend users with live tables still see transforms applied."""
    src = StateSource(
        name="s",
        project="p",
        dataset="d",
        table="live_table",
        id_field="id",
        duckdb_fixture_is_transformed=True,
    )
    assert _reads_duckdb_fixture(src, "duckdb") is False


def test_reads_duckdb_fixture_false_when_duckdb_path_is_empty_string() -> None:
    """Gate does not fire when duckdb_path is the empty string.

    Mirrors ``BaseTableSource.read_table``'s ``if not self.duckdb_path``
    check (``src/fyrnheim/core/source.py`` ~line 165), which raises
    ``ValueError`` for the empty string — the helper must not claim the
    engine reads a fixture when read_table itself would fail.
    """
    src = StateSource(
        name="s",
        project="p",
        dataset="d",
        table="t",
        id_field="id",
        duckdb_path="",
        duckdb_fixture_is_transformed=True,
    )
    assert _reads_duckdb_fixture(src, "duckdb") is False
