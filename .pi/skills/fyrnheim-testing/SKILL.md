---
name: fyrnheim-testing
description: Use when writing or reviewing Fyrnheim tests. Applies Python pytest patterns, fixture guidance, and project quality gates.
---

# Fyrnheim Testing

Fyrnheim is a Python project using pytest, ruff, mypy, Pydantic, Ibis, and Click.

## Commands

```bash
uv run pytest
uv run pytest tests/test_cli.py
uv run pytest -x
uv run ruff check src/ tests/
uv run mypy src/
```

## Test principles

- Every feature should have tests.
- Prefer behavior-focused tests over implementation details.
- Keep tests small enough to diagnose failures quickly.
- Use fixtures for reusable setup, but avoid hidden shared mutable state.
- Test edge cases and error cases from acceptance criteria.
- When adding validation, test both valid and invalid inputs.

## Common test targets

- Pydantic model validation and defaults
- Ibis expression construction and backend behavior
- Click CLI command behavior and error messages
- generated artifacts and file outputs
- transformation execution paths

## Patterns

For pure functions:

```python
def test_calculates_expected_value():
    assert calculate_value(input_data) == expected
```

For validation:

```python
import pytest
from pydantic import ValidationError


def test_rejects_missing_required_field():
    with pytest.raises(ValidationError):
        ModelUnderTest()
```

For Click CLI behavior, prefer the project's existing CLI testing pattern. Search `tests/` for `CliRunner` or command invocation examples before adding a new style.

## Before marking work complete

- Run focused tests for changed behavior.
- Run `uv run pytest` unless scope or environment makes that impractical.
- Run `uv run ruff check src/ tests/`.
- Run `uv run mypy src/` for typed code changes.
- Document any skipped gate and why.
