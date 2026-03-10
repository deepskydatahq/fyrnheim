# Expression Helpers Design (M018-E002)

## Goal

Provide `CaseColumn`, `contains_any()`, and `isin_literal()` helpers so users don't write fragile string-encoded ibis expressions for common patterns.

## Approach

Helper functions + one subclass in `src/fyrnheim/components/expressions.py`. All return expression strings compatible with `ComputedColumn.expression`. No codegen or executor changes needed.

### CaseColumn (ComputedColumn subclass)

```python
CaseColumn(
    name="tier",
    cases=[("t.score >= 90", "high"), ("t.score >= 50", "medium")],
    default="low",
)
# expression: "ibis.cases((t.score >= 90, 'high'), (t.score >= 50, 'medium')).else_('low')"
```

- `cases: list[tuple[str, str]]` — (condition, value) pairs
- `default: str | None` — optional else clause
- `model_validator` builds `expression` from structured data

### contains_any(column, values) -> str

```python
contains_any("t.tags", ["masterdoc", "cohort1"])
# "(t.tags.contains('masterdoc').fill_null(False) | t.tags.contains('cohort1').fill_null(False))"
```

### isin_literal(column, values) -> str

```python
isin_literal("t.domain", ["gmail.com", "yahoo.com"])
# "t.domain.isin(['gmail.com', 'yahoo.com'])"
```

## What doesn't change

- Codegen: `_bind_expression()` already handles `ibis.`, `(`, and `t.` prefixed expressions
- Executor: no changes
- All existing tests pass unchanged

## Testing

Unit tests for each helper's string output + edge cases (single value, empty list, special chars, no default).
