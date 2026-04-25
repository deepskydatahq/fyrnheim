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
    Join,
    Multiply,
    Rename,
    SourceTransforms,
    StateSource,
    TypeCast,
)
from fyrnheim.engine.source_transforms import (
    _apply_joins,
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
    """Paths that aren't the dot-notation form (e.g. ``$[0]`` array
    indexes) raise — bracket notation stays out of scope in M074."""
    t = _json_table()
    fields = [
        Field(
            name="first",
            type="STRING",
            json_path="$[0]",
            source_column="custom_type",
        )
    ]
    with pytest.raises(ValueError, match="dot-notation"):
        _apply_json_path_extractions(t, fields)


# ---------------------------------------------------------------------------
# M074: nested json_path dot-notation support
# ---------------------------------------------------------------------------


def _nested_json_table() -> ibis.Table:
    """Table with nested-shape JSON columns for M074 extraction tests."""
    return ibis.memtable(
        pd.DataFrame(
            {
                "id": [1, 2, 3],
                "payload": [
                    '{"user":{"email":"a@b.com","id":1}}',
                    '{"user":{"email":"c@d.com","id":2}}',
                    '{"user":{"email":"e@f.com","id":3}}',
                ],
                "deep": [
                    '{"a":{"b":{"c":42}}}',
                    '{"a":{"b":{"c":99}}}',
                    '{"a":{"b":{"c":7}}}',
                ],
            }
        )
    )


def test_json_path_two_segment_extraction() -> None:
    """M074: two-segment dot-notation path ($.user.email) chains
    subscripts and extracts the nested string value."""
    t = _nested_json_table()
    fields = [
        Field(
            name="email",
            type="STRING",
            json_path="$.user.email",
            source_column="payload",
        )
    ]
    result = _apply_json_path_extractions(t, fields)
    df = result.execute()
    assert "email" in df.columns
    assert list(df["email"]) == ["a@b.com", "c@d.com", "e@f.com"]


def test_json_path_three_segment_extraction() -> None:
    """M074: three-segment dot-notation path ($.a.b.c) chains three
    subscripts and unwraps the int64 value at the leaf."""
    t = _nested_json_table()
    fields = [
        Field(
            name="deep_value",
            type="INT64",
            json_path="$.a.b.c",
            source_column="deep",
        )
    ]
    result = _apply_json_path_extractions(t, fields)
    assert result.schema()["deep_value"] == ibis.dtype("int64")
    df = result.execute()
    assert [int(v) for v in df["deep_value"].tolist()] == [42, 99, 7]


def test_json_path_single_segment_still_works() -> None:
    """M074 regression: the M069 single-segment shape ($.utm_source)
    still extracts identically through the new chained-subscript code
    path. Single-segment is now just a one-iteration loop."""
    t = _json_table()
    fields = [Field(name="utm_source", type="STRING", json_path="$.utm_source")]
    result = _apply_json_path_extractions(t, fields)
    df = result.execute()
    assert list(df["utm_source"]) == ["google", "facebook", "twitter"]


def test_json_path_bracket_notation_raises_valueerror() -> None:
    """M074: bracket notation ($.foo[0]) stays unsupported — separate
    future enhancement. The error message must mention bracket
    notation so users know how to file the feature request."""
    t = _nested_json_table()
    fields = [
        Field(
            name="first_user",
            type="STRING",
            json_path="$.foo[0]",
            source_column="payload",
        )
    ]
    with pytest.raises(ValueError, match="[Bb]racket notation"):
        _apply_json_path_extractions(t, fields)


def test_json_path_empty_segment_raises_valueerror() -> None:
    """M074: empty segment ($..foo) is malformed dot-notation —
    rejected by the regex with the dot-notation grammar message."""
    t = _nested_json_table()
    fields = [
        Field(
            name="bad",
            type="STRING",
            json_path="$..foo",
            source_column="payload",
        )
    ]
    with pytest.raises(ValueError, match="dot-notation"):
        _apply_json_path_extractions(t, fields)


def test_json_path_trailing_dot_raises_valueerror() -> None:
    """M074: trailing dot ($.foo.) is malformed dot-notation —
    rejected by the regex (last segment fails the identifier rule)."""
    t = _nested_json_table()
    fields = [
        Field(
            name="bad",
            type="STRING",
            json_path="$.foo.",
            source_column="payload",
        )
    ]
    with pytest.raises(ValueError, match="dot-notation"):
        _apply_json_path_extractions(t, fields)


def test_json_path_two_segment_reporter_regression() -> None:
    """Reporter regression: $.user.email shape on a `created_by` column.

    Client-flowable reporter (2026-04-25) flagged that
    ``Field(json_path="$.user.email", source_column="created_by")``
    was silently producing NULL for ``assigned_to_email`` on their
    BigQuery pipeline because M069's regex rejected nested paths and
    raised ``ValueError`` at pipeline setup — masked downstream as a
    NULL column. M074 supports the dot-notation form so this exact
    use case extracts correctly. Pinned here for traceability so any
    future regression that breaks nested extraction surfaces against
    the reporter's exact shape.
    """
    df = pd.DataFrame(
        {
            "id": [1, 2],
            "created_by": [
                '{"user":{"email":"alice@flowable.com"}}',
                '{"user":{"email":"bob@flowable.com"}}',
            ],
        }
    )
    table = ibis.memtable(df)

    fields = [
        Field(
            name="assigned_to_email",
            type="STRING",
            json_path="$.user.email",
            source_column="created_by",
        )
    ]

    result = _apply_json_path_extractions(table, fields)
    assert "assigned_to_email" in result.columns
    out = result.execute()
    assert list(out["assigned_to_email"]) == [
        "alice@flowable.com",
        "bob@flowable.com",
    ]


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


# ---------------------------------------------------------------------------
# M070 (v0.12.0) — _apply_joins helper unit tests
# ---------------------------------------------------------------------------
#
# The Join API ships with Option A column-name semantics: ``Join.join_key``
# is the LEFT-side foreign-key column on the source declaring the join;
# the RIGHT-side primary-key column comes from the joined source's
# declared ``id_field`` and is supplied via the ``right_pk_registry``
# argument. These tests exercise that contract directly.


def _lifecycle_history_left() -> ibis.Table:
    """Test fixture mirroring pardot_lifecycle_history's left-side shape:
    a row per lifecycle change with two FK columns (previous / next)."""
    return ibis.memtable(
        pd.DataFrame(
            {
                "id": ["lh-1", "lh-2", "lh-3"],
                "prospect_id": ["P-1", "P-1", "P-2"],
                "previous_stage_id": [10, 20, 10],
                "next_stage_id": [20, 30, 20],
            }
        )
    )


def _lifecycle_stage_right() -> ibis.Table:
    """Test fixture mirroring pardot_lifecycle_stage: id + name only."""
    return ibis.memtable(
        pd.DataFrame(
            {
                "id": [10, 20, 30],
                "name": ["Lead", "Qualified", "Customer"],
            }
        )
    )


def test_apply_joins_no_joins_returns_unchanged() -> None:
    """An empty ``joins`` list returns the table unchanged (identity)."""
    t = _lifecycle_history_left()
    result = _apply_joins(t, [], {}, {})
    assert result is t


def test_apply_joins_single_left_join() -> None:
    """A single Join produces a left-join on the FK → PK predicate.

    join_key='previous_stage_id' targets lifecycle_stage.id (looked up
    from the right_pk_registry). The resulting table contains the
    LEFT row count and the joined ``name`` column.
    """
    left = _lifecycle_history_left()
    right = _lifecycle_stage_right()
    joins = [Join(source_name="lifecycle_stage", join_key="previous_stage_id")]
    result = _apply_joins(
        left,
        joins,
        {"lifecycle_stage": right},
        {"lifecycle_stage": "id"},
    )
    df = result.execute()
    # Left-join preserves all left rows.
    assert len(df) == 3
    # The joined ``name`` column is present and resolves correctly:
    #   lh-1 prev_stage 10 → Lead
    #   lh-2 prev_stage 20 → Qualified
    #   lh-3 prev_stage 10 → Lead
    by_lh = {row["id"]: row["name"] for _, row in df.iterrows()}
    assert by_lh["lh-1"] == "Lead"
    assert by_lh["lh-2"] == "Qualified"
    assert by_lh["lh-3"] == "Lead"


def test_apply_joins_multiple_joins_to_same_source() -> None:
    """Mirrors pardot_lifecycle_history: TWO LEFT JOINs on lifecycle_stage,
    one for each FK (previous_stage_id, next_stage_id). Both joined
    rows must surface — the prev_name and next_name are different
    columns of the same right-side table joined twice.
    """
    left = _lifecycle_history_left()
    right = _lifecycle_stage_right()
    joins = [
        Join(source_name="lifecycle_stage", join_key="previous_stage_id"),
        Join(source_name="lifecycle_stage", join_key="next_stage_id"),
    ]
    result = _apply_joins(
        left,
        joins,
        {"lifecycle_stage": right},
        {"lifecycle_stage": "id"},
    )
    df = result.execute()
    # Three left rows preserved.
    assert len(df) == 3
    # ibis resolves duplicate ``name`` columns by suffixing one of them.
    # We don't pin the exact suffix (ibis-version-dependent) — instead
    # assert that we now have AT LEAST TWO ``name``-derived columns
    # bringing the prev + next stage names visible to downstream
    # computed_columns / projections.
    name_like = [c for c in df.columns if c.startswith("name")]
    assert len(name_like) >= 2, (
        f"expected 2+ name-like columns from double-join; got {df.columns.tolist()}"
    )


def test_apply_joins_missing_source_raises_clear_error() -> None:
    """Joining to a source that's not in the registry raises a clear
    ValueError mentioning the missing source name. Normally the
    pipeline runner's topological sort prevents this, but the helper's
    runtime guard is what users see if the typo escapes the validator.
    """
    left = _lifecycle_history_left()
    joins = [Join(source_name="lifecycle_stage", join_key="previous_stage_id")]
    with pytest.raises(ValueError, match="lifecycle_stage"):
        _apply_joins(left, joins, {}, {})


def test_apply_joins_missing_right_pk_registry_raises_clear_error() -> None:
    """When ``source_registry`` has the joined source but
    ``right_pk_registry`` does NOT, ``_apply_joins`` raises a clear
    ValueError pinning the StateSource-only contract. This is the
    branch users hit when they declare a join to an EventSource (or to
    any non-StateSource that never made it into the right-PK map).
    """
    left = _lifecycle_history_left()
    right = _lifecycle_stage_right()
    joins = [Join(source_name="lifecycle_stage", join_key="previous_stage_id")]
    with pytest.raises(ValueError, match="right-side primary key"):
        _apply_joins(
            left,
            joins,
            {"lifecycle_stage": right},
            {},  # missing lifecycle_stage -> right PK mapping
        )


def test_apply_joins_propagates_join_key_resolution() -> None:
    """Option A semantics: ``Join.join_key`` is the LEFT-side FK; the
    RIGHT-side PK comes from ``right_pk_registry``. Swap the registry
    PK to a non-default name and confirm the predicate uses it.
    """
    left = _lifecycle_history_left()
    # Build a "right" side whose PK is named ``stage_pk`` instead of
    # ``id`` — declaring lifecycle_stage with a non-default id_field.
    right = ibis.memtable(
        pd.DataFrame(
            {
                "stage_pk": [10, 20, 30],
                "name": ["Lead", "Qualified", "Customer"],
            }
        )
    )
    joins = [Join(source_name="lifecycle_stage", join_key="previous_stage_id")]
    result = _apply_joins(
        left,
        joins,
        {"lifecycle_stage": right},
        {"lifecycle_stage": "stage_pk"},
    )
    df = result.execute()
    by_lh = {row["id"]: row["name"] for _, row in df.iterrows()}
    assert by_lh["lh-1"] == "Lead"
    assert by_lh["lh-2"] == "Qualified"
