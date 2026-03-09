"""Tests for CaseColumn computed column type."""

from fyrnheim.components import CaseColumn, ComputedColumn


class TestCaseColumnExpression:
    """Test that CaseColumn generates correct ibis.cases() expression strings."""

    def test_basic_case_with_default(self):
        col = CaseColumn(
            name="tier",
            cases=[("t.score >= 90", "high"), ("t.score >= 50", "medium")],
            default="low",
        )
        assert col.expression == (
            "ibis.cases((t.score >= 90, 'high'), (t.score >= 50, 'medium')).else_('low')"
        )

    def test_case_without_default(self):
        col = CaseColumn(
            name="tier",
            cases=[("t.score >= 90", "high"), ("t.score >= 50", "medium")],
        )
        assert col.expression == (
            "ibis.cases((t.score >= 90, 'high'), (t.score >= 50, 'medium'))"
        )

    def test_single_case(self):
        col = CaseColumn(
            name="is_premium",
            cases=[("t.plan == 'enterprise'", "yes")],
            default="no",
        )
        assert col.expression == (
            "ibis.cases((t.plan == 'enterprise', 'yes')).else_('no')"
        )


class TestCaseColumnIsComputedColumn:
    """Test that CaseColumn works as a drop-in replacement for ComputedColumn."""

    def test_is_subclass(self):
        assert issubclass(CaseColumn, ComputedColumn)

    def test_isinstance_check(self):
        col = CaseColumn(
            name="tier",
            cases=[("t.score >= 90", "high")],
            default="low",
        )
        assert isinstance(col, ComputedColumn)

    def test_has_name_and_expression(self):
        col = CaseColumn(
            name="tier",
            cases=[("t.score >= 90", "high")],
            default="low",
        )
        assert col.name == "tier"
        assert col.expression  # non-empty

    def test_to_sql_works(self):
        col = CaseColumn(
            name="tier",
            cases=[("t.score >= 90", "high")],
            default="low",
        )
        sql = col.to_sql()
        assert "AS tier" in sql

    def test_description_field(self):
        col = CaseColumn(
            name="tier",
            cases=[("t.score >= 90", "high")],
            description="Score-based tier",
        )
        assert col.description == "Score-based tier"


class TestCaseColumnCodegen:
    """Test that CaseColumn expressions work correctly with codegen rendering."""

    def test_expression_starts_with_ibis(self):
        """Codegen uses _bind_expression which passes through ibis. prefixed expressions."""
        col = CaseColumn(
            name="tier",
            cases=[("t.score >= 90", "high"), ("t.score >= 50", "medium")],
            default="low",
        )
        # The expression starts with ibis. so _bind_expression will pass it through
        assert col.expression.startswith("ibis.")

    def test_expression_is_valid_python(self):
        """The generated expression should be syntactically valid Python."""
        col = CaseColumn(
            name="tier",
            cases=[("t.score >= 90", "high"), ("t.score >= 50", "medium")],
            default="low",
        )
        # Should parse as valid Python (though it won't execute without ibis)
        compile(col.expression, "<test>", "eval")

    def test_repr(self):
        col = CaseColumn(
            name="tier",
            cases=[("t.score >= 90", "high"), ("t.score >= 50", "medium")],
        )
        assert "CaseColumn" in repr(col)
        assert "tier" in repr(col)
