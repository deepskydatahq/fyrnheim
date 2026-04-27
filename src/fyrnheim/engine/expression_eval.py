"""Validated expression evaluation for trusted Fyrnheim expressions.

Fyrnheim expressions are Python snippets authored in project code/config, such
as ``ComputedColumn(expression="t.email.lower()")`` or source filters like
``t.is_deleted != True``. They intentionally support a narrow Python-expression
subset over explicit context names (for example ``t`` and ``ibis``), but should
not expose arbitrary builtins or dunder escape hatches.
"""

from __future__ import annotations

import ast
from collections.abc import Mapping
from typing import Any

_ALLOWED_NODES = (
    ast.Expression,
    ast.BoolOp,
    ast.BinOp,
    ast.UnaryOp,
    ast.IfExp,
    ast.Compare,
    ast.Call,
    ast.Name,
    ast.Load,
    ast.Attribute,
    ast.Subscript,
    ast.Constant,
    ast.List,
    ast.Tuple,
    ast.Dict,
    ast.Slice,
    ast.keyword,
    ast.And,
    ast.Or,
    ast.Not,
    ast.UAdd,
    ast.USub,
    ast.Invert,
    ast.Add,
    ast.Sub,
    ast.Mult,
    ast.Div,
    ast.FloorDiv,
    ast.Mod,
    ast.Pow,
    ast.BitAnd,
    ast.BitOr,
    ast.BitXor,
    ast.Eq,
    ast.NotEq,
    ast.Lt,
    ast.LtE,
    ast.Gt,
    ast.GtE,
    ast.Is,
    ast.IsNot,
    ast.In,
    ast.NotIn,
)


class UnsafeExpressionError(ValueError):
    """Raised when an expression uses syntax outside Fyrnheim's subset."""


class _ExpressionValidator(ast.NodeVisitor):
    def __init__(self, allowed_names: set[str]) -> None:
        self._allowed_names = allowed_names

    def generic_visit(self, node: ast.AST) -> None:
        if not isinstance(node, _ALLOWED_NODES):
            raise UnsafeExpressionError(
                f"Unsupported expression syntax: {type(node).__name__}"
            )
        super().generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:  # noqa: N802
        if node.id.startswith("_") or node.id not in self._allowed_names:
            raise UnsafeExpressionError(f"Unsupported expression name: {node.id!r}")
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:  # noqa: N802
        if node.attr.startswith("_"):
            raise UnsafeExpressionError(
                f"Unsupported private/dunder attribute access: {node.attr!r}"
            )
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802
        if isinstance(node.func, ast.Name) and node.func.id not in self._allowed_names:
            raise UnsafeExpressionError(
                f"Unsupported callable name: {node.func.id!r}"
            )
        self.generic_visit(node)


def evaluate_expression(expression: str, context: Mapping[str, Any]) -> Any:
    """Evaluate a Fyrnheim expression after AST validation.

    The validator allows ordinary expression syntax needed by existing
    Fyrnheim configs: arithmetic, boolean operators, comparisons, ternary
    expressions, attribute/method access such as ``t.email.lower()``, and
    indexing such as ``t.email.split('@')[1]``. It rejects statements,
    comprehensions, lambdas, imports, unknown names, and private/dunder
    attribute access before compiling.

    Args:
        expression: Python expression snippet from a Fyrnheim definition.
        context: Explicit names available to the expression.

    Returns:
        The evaluated expression result.

    Raises:
        UnsafeExpressionError: if the expression uses unsupported syntax.
    """
    tree = ast.parse(expression, mode="eval")
    _ExpressionValidator(set(context)).visit(tree)
    code = compile(tree, "<fyrnheim-expression>", "eval")
    return eval(code, {"__builtins__": {}}, dict(context))  # noqa: S307
