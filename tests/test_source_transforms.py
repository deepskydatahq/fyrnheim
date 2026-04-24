"""Unit tests for the ``_apply_source_transforms`` helper (M068).

Exercises the helper directly (not through the loaders) to pin:
  * transforms=None returns the table unchanged
  * each transform category (type_cast, rename, divide, multiply) works
  * transform order: type_casts → divides → multiplies → renames
"""

from __future__ import annotations

import ibis
import pandas as pd

from fyrnheim.core.source import (
    Divide,
    Multiply,
    Rename,
    SourceTransforms,
    TypeCast,
)
from fyrnheim.engine.source_transforms import _apply_source_transforms


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
