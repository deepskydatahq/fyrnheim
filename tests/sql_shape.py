"""SQLGlot-backed SQL shape assertions for Ibis expressions.

These helpers intentionally live in tests: Fyrnheim's runtime IR remains Ibis.
SQLGlot is used here to parse generated warehouse SQL and assert structural
properties without live warehouse credentials or brittle formatting checks.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TypeVar

import ibis
import sqlglot
from sqlglot import exp

ExpressionT = TypeVar("ExpressionT", bound=exp.Expression)


@dataclass(frozen=True)
class SqlShape:
    """Parsed/normalized BigQuery SQL generated from an Ibis expression."""

    sql: str
    tree: exp.Expression
    normalized: str

    def find_all(self, expression_type: type[ExpressionT]) -> list[ExpressionT]:
        """Return every node of ``expression_type`` from the parsed AST."""
        return list(self.tree.find_all(expression_type))

    def count(self, expression_type: type[exp.Expression]) -> int:
        """Count nodes of ``expression_type`` in the parsed AST."""
        return len(self.find_all(expression_type))

    def has(self, expression_type: type[exp.Expression]) -> bool:
        """Return True when the AST contains ``expression_type``."""
        return self.tree.find(expression_type) is not None

    def assert_has(self, expression_type: type[exp.Expression]) -> None:
        """Assert that the parsed AST contains ``expression_type``."""
        assert self.has(expression_type), (
            f"Expected {expression_type.__name__} in generated SQL:\n{self.sql}"
        )

    def assert_function(self, name: str) -> None:
        """Assert that generated SQL contains a function name.

        SQLGlot normalizes many functions into specialized nodes, but backend
        UDFs and some dialect-specific functions remain easiest to verify by
        normalized SQL text. This still avoids asserting exact formatting.
        """
        assert f"{name.upper()}(" in self.normalized.upper(), self.normalized


def compile_bigquery_shape(expr: ibis.expr.types.Table) -> SqlShape:
    """Compile an Ibis table expression to BigQuery SQL and parse with SQLGlot."""
    sql = ibis.to_sql(expr, dialect="bigquery")
    tree = sqlglot.parse_one(sql, read="bigquery")
    normalized = tree.sql(dialect="bigquery", pretty=False)
    return SqlShape(sql=sql, tree=tree, normalized=normalized)
