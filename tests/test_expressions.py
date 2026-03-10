"""Tests for expression helper functions."""

from fyrnheim.components.computed_column import ComputedColumn
from fyrnheim.components.expressions import CaseColumn, contains_any, isin_literal


class TestContainsAny:
    """Tests for contains_any() expression helper."""

    def test_multiple_values_generates_chained_contains(self):
        result = contains_any("t.tags", ["masterdoc", "cohort1", "beta"])
        assert result == (
            "(t.tags.contains('masterdoc').fill_null(False)"
            " | t.tags.contains('cohort1').fill_null(False)"
            " | t.tags.contains('beta').fill_null(False))"
        )

    def test_single_value_generates_simple_contains(self):
        result = contains_any("t.tags", ["masterdoc"])
        assert result == "t.tags.contains('masterdoc').fill_null(False)"

    def test_result_usable_in_computed_column(self):
        expr = contains_any("t.tags", ["a", "b"])
        col = ComputedColumn(name="has_tag", expression=expr)
        assert col.expression == expr


class TestCaseColumn:
    """Tests for CaseColumn computed column type."""

    def test_generates_ibis_cases_expression(self):
        col = CaseColumn(
            name="tier",
            cases=[("t.score >= 90", "high"), ("t.score >= 50", "medium")],
            default="low",
        )
        assert col.expression == (
            "ibis.cases((t.score >= 90, 'high'), (t.score >= 50, 'medium')).else_('low')"
        )

    def test_no_default_omits_else(self):
        col = CaseColumn(
            name="tier",
            cases=[("t.score >= 90", "high")],
        )
        assert col.expression == "ibis.cases((t.score >= 90, 'high'))"

    def test_is_computed_column_subclass(self):
        col = CaseColumn(
            name="tier",
            cases=[("t.score >= 90", "high")],
        )
        assert isinstance(col, ComputedColumn)

    def test_works_as_drop_in_for_computed_column(self):
        col = CaseColumn(
            name="tier",
            cases=[("t.score >= 90", "high"), ("t.score >= 50", "medium")],
            default="low",
        )
        # Should have all ComputedColumn attributes
        assert col.name == "tier"
        assert col.expression  # non-empty
        assert col.description is None

    def test_with_description(self):
        col = CaseColumn(
            name="tier",
            cases=[("t.score >= 90", "high")],
            description="Score-based tier",
        )
        assert col.description == "Score-based tier"


class TestIsinLiteral:
    """Tests for isin_literal() expression helper."""

    def test_multiple_values(self):
        result = isin_literal("t.domain", ["gmail.com", "yahoo.com", "hotmail.com"])
        assert result == "t.domain.isin(['gmail.com', 'yahoo.com', 'hotmail.com'])"

    def test_empty_list(self):
        result = isin_literal("t.domain", [])
        assert result == "t.domain.isin([])"

    def test_result_usable_in_computed_column(self):
        expr = isin_literal("t.domain", ["gmail.com"])
        col = ComputedColumn(name="is_freemail", expression=expr)
        assert col.expression == expr

    def test_special_characters_in_values(self):
        result = isin_literal("t.name", ["O'Brien", 'say "hi"'])
        # Values should be properly quoted
        assert "O'Brien" in result or "O\\'Brien" in result
