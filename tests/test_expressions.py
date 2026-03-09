"""Tests for expression helper functions."""

import pytest

from fyrnheim.components import ComputedColumn
from fyrnheim.components.expressions import contains_any, isin_literal


class TestContainsAny:
    """Tests for contains_any() expression helper."""

    def test_multiple_values(self):
        result = contains_any("t.tags", ["masterdoc", "cohort1", "beta"])
        assert result == (
            "(t.tags.contains('masterdoc').fill_null(False)"
            " | t.tags.contains('cohort1').fill_null(False)"
            " | t.tags.contains('beta').fill_null(False))"
        )

    def test_single_value(self):
        result = contains_any("t.tags", ["masterdoc"])
        assert result == "t.tags.contains('masterdoc').fill_null(False)"

    def test_empty_values_raises(self):
        with pytest.raises(ValueError, match="at least one value"):
            contains_any("t.tags", [])

    def test_usable_in_computed_column(self):
        expr = contains_any("t.tags", ["masterdoc", "cohort1"])
        col = ComputedColumn(name="has_tags", expression=expr)
        assert col.expression == expr
        assert col.name == "has_tags"

    def test_result_is_valid_python_syntax(self):
        expr = contains_any("t.tags", ["masterdoc", "cohort1", "beta"])
        compile(expr, "<test>", "eval")


class TestIsinLiteral:
    """Tests for isin_literal() expression helper."""

    def test_multiple_values(self):
        result = isin_literal("t.domain", ["gmail.com", "yahoo.com", "hotmail.com"])
        assert result == "t.domain.isin(['gmail.com', 'yahoo.com', 'hotmail.com'])"

    def test_empty_list(self):
        result = isin_literal("t.domain", [])
        assert result == "t.domain.isin([])"

    def test_single_value(self):
        result = isin_literal("t.status", ["active"])
        assert result == "t.status.isin(['active'])"

    def test_special_characters(self):
        result = isin_literal("t.name", ["O'Brien", "with spaces", "a@b.com"])
        # Single quotes in values should be escaped
        assert "O\\'Brien" in result
        assert result == "t.name.isin(['O\\'Brien', 'with spaces', 'a@b.com'])"

    def test_usable_in_computed_column(self):
        expr = isin_literal("t.domain", ["gmail.com", "yahoo.com"])
        col = ComputedColumn(name="is_freemail", expression=expr)
        assert col.expression == expr
        assert col.name == "is_freemail"

    def test_result_is_valid_python_syntax(self):
        expr = isin_literal("t.domain", ["gmail.com", "yahoo.com"])
        compile(expr, "<test>", "eval")
