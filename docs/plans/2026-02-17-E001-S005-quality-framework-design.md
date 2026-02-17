# Design: M001-E001-S005 -- Extract quality check framework

**Date:** 2026-02-17
**Source:** `timo-data-stack/metadata/quality/`
**Target:** `fyrnheim/quality/`

---

## 1. Source Code Inventory

| File | Lines | Purpose |
|------|-------|---------|
| `checks.py` | 259 | Abstract `QualityCheck` base + 9 concrete types + `QualityConfig` |
| `runner.py` | 243 | `QualityRunner` (BigQuery execution), `CheckResult`, `EntityResult` |
| `__init__.py` | 34 | Re-exports all public names |

Check types: `NotNull`, `NotEmpty`, `InRange`, `InSet`, `MatchesPattern`, `ForeignKey`, `Unique`, `MaxAge`, `CustomSQL`.

---

## 2. Design Decision: Raw SQL vs Ibis Expressions

### Current state

`QualityRunner` constructs raw SQL strings throughout:

- `build_failure_query()` -- assembles `SELECT ... FROM ... WHERE ...` with backtick-quoted BigQuery identifiers
- `build_unique_check_query()` -- raw `GROUP BY ... HAVING COUNT(*) > 1`
- `build_max_age_check_query()` -- uses `DATE_DIFF(CURRENT_DATE(), DATE(MAX(...)), DAY)` (BigQuery SQL)
- `build_foreign_key_query()` -- raw `LEFT JOIN` with backtick-quoted tables
- `_build_count_query()` -- raw `SELECT COUNT(*)` variants

The runner also hard-codes the BigQuery client (`google.cloud.bigquery`).

### Decision: Separate check definitions (portable) from runner (backend-specific)

**Phase 1 (this story):** Extract checks and runner as-is, but restructure into two layers:

1. **`fyrnheim.quality.checks`** -- Pure Pydantic models. Keep `get_where_clause()` returning SQL fragments for now, but mark it as the interface that will evolve.
2. **`fyrnheim.quality.results`** -- `CheckResult` and `EntityResult` (pure data, no backend dependency).
3. **`fyrnheim.quality.runner`** -- `QualityRunner` stays SQL-based but is explicitly labeled as the BigQuery-backed runner. Constructor takes an Ibis connection instead of project/dataset strings.

**Phase 2 (future story):** Replace `get_where_clause() -> str` with `get_filter(table: ibis.Table) -> ibis.BooleanColumn` on each check. This makes checks portable across DuckDB, Postgres, BigQuery, etc. The runner then calls `table.filter(check.get_filter(table))` instead of building SQL strings.

**Rationale:** Doing the full Ibis conversion in this story would expand scope significantly (every check type, the runner, and all query builders). The story's acceptance criteria explicitly reference `get_where_clause()` returning SQL fragments, so we preserve that interface for now and plan the Ibis migration as a follow-up.

---

## 3. Design Decision: BigQuery-Specific SQL Functions

### Audit of BigQuery-specific constructs found

| Location | Construct | BigQuery-specific? | Ibis equivalent |
|----------|-----------|-------------------|-----------------|
| `MatchesPattern.get_where_clause()` | `REGEXP_CONTAINS(col, r'pattern')` | **Yes.** BigQuery-only function with `r''` raw string syntax. Postgres uses `~`, DuckDB uses `regexp_matches()`. | `col.re_search(pattern)` |
| `MaxAge` runner query | `DATE_DIFF(CURRENT_DATE(), DATE(MAX(col)), DAY)` | **Yes.** BigQuery's `DATE_DIFF` syntax. Postgres uses `CURRENT_DATE - MAX(col)`, DuckDB uses `date_diff()`. | `ibis.now() - col.max()` then `.cast('interval')` |
| All runner queries | Backtick-quoted identifiers: `` `dataset.table` `` | **Yes.** BigQuery uses backticks. Others use double-quotes or no quoting. | Ibis handles quoting per backend. |
| `InRange._format_value()` | Passes through strings like `CURRENT_DATE()` | **Loosely.** `CURRENT_DATE()` is standard SQL but with parens (BigQuery style). Standard SQL is `CURRENT_DATE`. | `ibis.now().date()` |
| `ForeignKey._ref_table` | Hard-codes `dim_` prefix | Not BigQuery-specific but convention-specific. | Should be configurable. |

### Decision: Tag BigQuery-specific checks, keep SQL for now

1. **`MatchesPattern`**: Keep `REGEXP_CONTAINS` in Phase 1. Add a `# TODO(ibis): replace with col.re_search()` comment. In Phase 2, this becomes `table[col].re_search(pattern)`.
2. **`MaxAge` runner query**: Keep `DATE_DIFF` in Phase 1. This is runner-level, not check-level, so it naturally moves to Ibis when the runner is ported.
3. **Backtick quoting**: Remove from the runner. Have the runner accept a fully-qualified table reference or Ibis table expression rather than constructing `` `dataset.table` `` strings.
4. **`InRange` string pass-through**: Document that string values in `min`/`max` are raw SQL expressions and will need Ibis equivalents in Phase 2.

---

## 4. Design Decision: Should checks generate Ibis expressions instead of SQL strings?

### Decision: Not yet -- dual interface planned

Generating Ibis expressions is the right long-term direction, but doing it now would:

- Break the acceptance criteria (which test `get_where_clause()` returning SQL)
- Require Ibis as a hard dependency of the checks module (currently zero dependencies beyond Pydantic)
- Require reworking all 9 check types simultaneously

**Plan:**

```
Phase 1 (this story):
  QualityCheck.get_where_clause() -> str          # SQL fragment, as-is

Phase 2 (future story):
  QualityCheck.get_filter(t: ibis.Table) -> ibis.BooleanColumn  # Ibis expression
  QualityCheck.get_where_clause() -> str                        # Deprecated, kept for compat
```

Phase 2 example for `NotNull`:
```python
class NotNull(QualityCheck, BaseModel):
    columns: tuple[str, ...]

    def get_filter(self, t: ibis.Table) -> ibis.BooleanColumn:
        predicates = [t[col].isnull() for col in self.columns]
        return functools.reduce(operator.or_, predicates)
```

This keeps checks importable without Ibis in Phase 1, and enables backend-portable filtering in Phase 2.

---

## 5. Design Decision: API Cleanup

### Issues identified and resolutions

**5a. `QualityCheck` type annotation in `QualityConfig`**

Current: `checks: list[Any]` with comment `# list[QualityCheck]`

Fix: Use a Pydantic discriminated union or `list[QualityCheck]`. Since `QualityCheck` is an ABC mixed with `BaseModel` subclasses, we can use:
```python
checks: list[QualityCheck] = Field(default_factory=list)
```
Pydantic v2 handles ABC base classes in type annotations. This gives proper validation.

**5b. `ForeignKey` hard-codes `dim_` prefix**

Current: `_ref_table` returns `f"dim_{entity}"`. This bakes in a naming convention.

Fix: Add an optional `ref_table` parameter. If not provided, fall back to `references.split('.')[0]` without the `dim_` prefix (let the runner or config handle table naming conventions):
```python
class ForeignKey(QualityCheck, BaseModel):
    column: str
    references: str          # "entity.column"
    ref_table: str | None = None  # Override inferred table name

    @property
    def _ref_table(self) -> str:
        return self.ref_table or self.references.split(".")[0]
```

**5c. `QualityRunner` constructor is BigQuery-specific**

Current: `__init__(self, dataset: str, project: str = "deepskydata")` with lazy BigQuery client.

Fix: Accept an Ibis backend connection instead. Even in Phase 1, this makes the runner testable with DuckDB:
```python
class QualityRunner:
    def __init__(self, connection: ibis.BaseBackend, dataset: str | None = None):
        self.connection = connection
        self.dataset = dataset
```
The runner still builds raw SQL and calls `connection.raw_sql()`, but the connection object is injected rather than hard-coded.

**5d. `Unique.get_where_clause()` returns a comment, not SQL**

Current: Returns `"-- UNIQUE check requires GROUP BY, handled specially"`

Fix: This is a code smell -- the base class contract says "return SQL WHERE clause" but `Unique` cannot satisfy it. Options:
- **(Chosen)** Keep `get_where_clause()` raising `NotImplementedError` for check types that need special handling. Add a `requires_special_query: bool` property (default `False`, overridden to `True` by `Unique` and `MaxAge`). The runner checks this before calling `get_where_clause()`.

**5e. `CustomSQL` should be preserved but documented as non-portable**

`CustomSQL` accepts arbitrary SQL predicates. This is inherently non-portable. Keep it, but add a docstring noting it bypasses backend portability. In Phase 2, add a companion `CustomIbis` check that accepts a callable `(ibis.Table) -> ibis.BooleanColumn`.

---

## 6. Proposed Module Structure

```
fyrnheim/quality/
    __init__.py          # Re-exports public API
    checks.py            # QualityCheck ABC + 9 concrete check types + QualityConfig
    results.py           # CheckResult, EntityResult (pure data models)
    runner.py            # QualityRunner (takes ibis connection, builds SQL)
```

Splitting `results.py` out of `runner.py` allows importing result types without pulling in the runner's dependencies.

---

## 7. External Dependencies

| Dependency | Required by | Notes |
|------------|-------------|-------|
| `pydantic` (>=2.0) | `checks.py`, `results.py` | Already a core fyrnheim dependency |
| `ibis-framework` | `runner.py` | Connection injection only in Phase 1; full expression API in Phase 2 |

No new dependencies beyond what fyrnheim already requires.

---

## 8. Summary of Decisions

| # | Question | Decision |
|---|----------|----------|
| 1 | Raw SQL or Ibis in runner? | Keep raw SQL in Phase 1, but inject Ibis connection instead of BigQuery client. Full Ibis expressions in Phase 2. |
| 2 | BigQuery-specific SQL? | Yes: `REGEXP_CONTAINS`, `DATE_DIFF`, backtick quoting, `CURRENT_DATE()`. Tagged for Phase 2 replacement. |
| 3 | Checks as Ibis expressions? | Phase 2. Add `get_filter(t) -> ibis.BooleanColumn` alongside deprecated `get_where_clause()`. |
| 4 | API cleanup? | Fix `QualityConfig.checks` typing, make `ForeignKey.ref_table` configurable, inject connection into runner, fix `Unique`/`MaxAge` where-clause contract, split out `results.py`. |

---

## 9. Implementation Plan

### Prerequisites

This story depends on M001-E001-S001 (package structure), which creates the empty `src/fyrnheim/quality/` sub-package. The implementation below assumes that skeleton exists.

### Step 1: Create `src/fyrnheim/quality/checks.py`

Copy from `timo-data-stack/metadata/quality/checks.py` with the following modifications:

**1a. Fix `QualityConfig.checks` type annotation (design 5a)**

```python
# Before:
checks: list[Any] = PydanticField(default_factory=list)  # list[QualityCheck]

# After:
checks: list[QualityCheck] = PydanticField(default_factory=list)
```

Remove the `Any` import if no longer used.

**1b. Add `requires_special_query` property to `QualityCheck` (design 5d)**

```python
class QualityCheck(ABC):
    @abstractmethod
    def get_where_clause(self) -> str:
        """Return SQL WHERE clause that matches failing rows."""
        pass

    @property
    @abstractmethod
    def display_name(self) -> str:
        """Human-readable name for display."""
        pass

    @property
    def columns_to_show(self) -> list[str]:
        """Columns to include in failure output. Override in subclasses."""
        return []

    @property
    def requires_special_query(self) -> bool:
        """Whether this check requires special query handling in the runner.

        When True, the runner must handle this check type directly rather
        than calling get_where_clause().
        """
        return False
```

**1c. Fix `Unique.get_where_clause()` and `MaxAge.get_where_clause()` (design 5d)**

Both currently return comment strings instead of raising. Change them to raise `NotImplementedError` and override `requires_special_query`:

```python
class Unique(QualityCheck, BaseModel):
    columns: tuple[str, ...]

    def __init__(self, *columns: str):
        if not columns:
            raise ValueError("At least one column must be specified")
        super().__init__(columns=columns)

    def get_where_clause(self) -> str:
        raise NotImplementedError(
            "Unique check requires GROUP BY; handled by the runner"
        )

    @property
    def requires_special_query(self) -> bool:
        return True

    # ... display_name, columns_to_show unchanged


class MaxAge(QualityCheck, BaseModel):
    column: str
    days: int

    def __init__(self, column: str, days: int):
        super().__init__(column=column, days=days)

    def get_where_clause(self) -> str:
        raise NotImplementedError(
            "MaxAge check requires MAX() aggregation; handled by the runner"
        )

    @property
    def requires_special_query(self) -> bool:
        return True

    # ... display_name, columns_to_show unchanged
```

**1d. Make `ForeignKey.ref_table` configurable (design 5b)**

```python
class ForeignKey(QualityCheck, BaseModel):
    column: str
    references: str  # Format: "entity.column"
    ref_table: str | None = None  # Override inferred table name

    def __init__(self, column: str, references: str, ref_table: str | None = None):
        if "." not in references:
            raise ValueError("references must be in format 'entity.column'")
        super().__init__(column=column, references=references, ref_table=ref_table)

    @property
    def _ref_table(self) -> str:
        return self.ref_table or self.references.split(".")[0]

    # ... rest unchanged
```

Note: The `dim_` prefix is removed from `_ref_table`. The runner or calling code can pass `ref_table="dim_entity"` explicitly if that convention is needed.

**1e. Add BigQuery-specific TODO comments (design 3)**

```python
class MatchesPattern(QualityCheck, BaseModel):
    # ...
    def get_where_clause(self) -> str:
        # TODO(ibis): Replace REGEXP_CONTAINS with col.re_search(pattern)
        escaped_pattern = self.pattern.replace("'", "\\'")
        return f"NOT REGEXP_CONTAINS({self.column}, r'{escaped_pattern}')"
```

```python
class InRange(QualityCheck, BaseModel):
    # ...
    def _format_value(self, value: int | float | str) -> str:
        if isinstance(value, str):
            # TODO(ibis): String values are raw SQL expressions (e.g. CURRENT_DATE()).
            # Replace with ibis equivalents in Phase 2.
            return value
        return str(value)
```

**1f. Add non-portable docstring to `CustomSQL` (design 5e)**

```python
class CustomSQL(QualityCheck, BaseModel):
    """Custom SQL check with user-defined predicate.

    Note: This check accepts raw SQL and is inherently non-portable across
    backends. In a future phase, a companion CustomIbis check will provide
    a portable alternative via callable (ibis.Table) -> ibis.BooleanColumn.
    """
```

### Step 2: Create `src/fyrnheim/quality/results.py` (new file)

Extract `CheckResult` and `EntityResult` from the source `runner.py` into a standalone module with zero runner dependencies.

```python
"""Quality check result models."""

from typing import Any

from pydantic import BaseModel


class CheckResult(BaseModel):
    """Result of running a single quality check."""

    check_name: str
    passed: bool
    failure_count: int
    sample_failures: list[dict[str, Any]]
    error: str | None = None


class EntityResult(BaseModel):
    """Result of running all checks for an entity."""

    entity_name: str
    table_name: str
    results: list[CheckResult]

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results)

    @property
    def passed_count(self) -> int:
        return sum(1 for r in self.results if r.passed)

    @property
    def failed_count(self) -> int:
        return sum(1 for r in self.results if not r.passed)
```

This is a direct lift with no modifications. Splitting it out means `results.py` can be imported without pulling in `ibis-framework`.

### Step 3: Create `src/fyrnheim/quality/runner.py`

Copy from `timo-data-stack/metadata/quality/runner.py` with the following modifications:

**3a. Remove `CheckResult` and `EntityResult` (moved to `results.py`)**

Import them instead:

```python
from .results import CheckResult, EntityResult
```

**3b. Refactor constructor to accept `ibis.BaseBackend` (design 5c)**

```python
from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import ibis

from .checks import ForeignKey, MaxAge, QualityCheck, QualityConfig, Unique
from .results import CheckResult, EntityResult


class QualityRunner:
    """Runs quality checks against a database backend.

    Constructs raw SQL and executes via an Ibis connection. The SQL is
    currently BigQuery-flavored (backtick quoting, DATE_DIFF, etc.)
    and will be migrated to native Ibis expressions in a future phase.
    """

    def __init__(self, connection: ibis.BaseBackend, dataset: str | None = None):
        self.connection = connection
        self.dataset = dataset
```

**3c. Replace `self.client.query(query).result()` with `self.connection.raw_sql()`**

The BigQuery client API (`client.query(sql).result()` returning Row objects) is replaced with Ibis's `connection.raw_sql()`. This returns a cursor-like object; we convert rows to dicts.

```python
def _execute_query(self, query: str) -> list[dict[str, Any]]:
    """Execute a SQL query and return results as list of dicts."""
    cursor = self.connection.raw_sql(query)
    try:
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        cursor.close()
```

Then replace all `self.client.query(query).result()` / `[dict(row) for row in result]` calls with `self._execute_query(query)`.

**3d. Use `requires_special_query` in `run_check()` instead of `isinstance` for dispatch**

The current code uses `isinstance(check, Unique)` / `isinstance(check, MaxAge)` to decide query strategy. This still works and is actually clearer than a boolean flag for dispatching to different query builders. Keep `isinstance` for the dispatch logic but add a guard:

```python
def run_check(self, table: str, check: QualityCheck, primary_key: str, limit: int = 10) -> CheckResult:
    try:
        select_columns = [primary_key] + [c for c in check.columns_to_show if c != primary_key]

        if isinstance(check, Unique):
            query = self.build_unique_check_query(table, check, select_columns, limit)
        elif isinstance(check, MaxAge):
            query = self.build_max_age_check_query(table, check)
        elif isinstance(check, ForeignKey):
            query = self.build_foreign_key_query(table, check, select_columns, limit)
        else:
            query = self.build_failure_query(table, check, select_columns, limit)

        rows = self._execute_query(query)
        # ... rest of logic unchanged
```

**3e. Update table reference formatting**

Replace backtick-quoted `` `{self.dataset}.{table}` `` patterns. When `self.dataset` is set, use `{self.dataset}.{table}` (no backticks -- Ibis `raw_sql` handles identifier quoting per backend). When `self.dataset` is None, use just `{table}`.

Add a helper:

```python
def _table_ref(self, table: str) -> str:
    """Build a fully-qualified table reference."""
    if self.dataset:
        return f"{self.dataset}.{table}"
    return table
```

Replace all `` `{self.dataset}.{table}` `` with `{self._table_ref(table)}` throughout the query builders.

**3f. Update `ForeignKey` query to use `check._ref_table` (from 1d)**

The runner's `build_foreign_key_query` currently duplicates the `dim_` prefix logic. Change it to use the check's `_ref_table` property:

```python
def build_foreign_key_query(self, table: str, check: ForeignKey, select_columns: list[str], limit: int = 10) -> str:
    cols = ", ".join(f"t.{c}" for c in select_columns)
    ref_table = check._ref_table
    ref_column = check._ref_column
    tbl = self._table_ref(table)
    ref = self._table_ref(ref_table)

    return f"""
SELECT {cols}
FROM {tbl} t
LEFT JOIN {ref} r ON t.{check.column} = r.{ref_column}
WHERE r.{ref_column} IS NULL AND t.{check.column} IS NOT NULL
LIMIT {limit}
"""
```

Same for `_build_count_query` ForeignKey branch.

### Step 4: Update `src/fyrnheim/quality/__init__.py`

Update re-exports to reflect the new `results.py` module:

```python
"""Quality check framework for entity data validation."""

from .checks import (
    CustomSQL,
    ForeignKey,
    InRange,
    InSet,
    MatchesPattern,
    MaxAge,
    NotEmpty,
    NotNull,
    QualityCheck,
    QualityConfig,
    Unique,
)
from .results import CheckResult, EntityResult
from .runner import QualityRunner

__all__ = [
    "QualityCheck",
    "QualityConfig",
    "NotNull",
    "NotEmpty",
    "InRange",
    "InSet",
    "MatchesPattern",
    "ForeignKey",
    "Unique",
    "MaxAge",
    "CustomSQL",
    "CheckResult",
    "EntityResult",
    "QualityRunner",
]
```

The only change from the source is importing `CheckResult` and `EntityResult` from `.results` instead of `.runner`.

### Step 5: Write tests (`tests/quality/`)

**5a. `tests/quality/__init__.py`** -- empty.

**5b. `tests/quality/test_checks.py`** -- Unit tests for acceptance criteria:

| Test | Acceptance Criterion |
|------|---------------------|
| `test_imports` | QualityConfig, NotNull, Unique, InRange importable from `fyrnheim.quality` |
| `test_not_null_where_clause` | `NotNull('email').get_where_clause()` returns `"email IS NULL"` |
| `test_in_range_where_clause` | `InRange('amount', min=0, max=10000).get_where_clause()` returns `"NOT (amount >= 0 AND amount <= 10000)"` |
| `test_quality_config_validates` | `QualityConfig(checks=[NotNull('id')], primary_key='id')` validates |
| `test_runner_result_imports` | QualityRunner, CheckResult, EntityResult importable from `fyrnheim.quality` |
| `test_not_empty_where_clause` | `NotEmpty('name').get_where_clause()` returns correct SQL |
| `test_in_set_where_clause` | `InSet('status', ['A', 'B']).get_where_clause()` returns correct SQL |
| `test_matches_pattern_where_clause` | `MatchesPattern('email', r'^.+@.+$').get_where_clause()` returns `REGEXP_CONTAINS` SQL |
| `test_foreign_key_where_clause` | `ForeignKey('user_id', 'user.id').get_where_clause()` returns correct SQL |
| `test_foreign_key_custom_ref_table` | `ForeignKey('user_id', 'user.id', ref_table='dim_user')._ref_table` returns `'dim_user'` |
| `test_unique_requires_special_query` | `Unique('email').requires_special_query` is `True` |
| `test_unique_get_where_clause_raises` | `Unique('email').get_where_clause()` raises `NotImplementedError` |
| `test_max_age_requires_special_query` | `MaxAge('updated_at', days=7).requires_special_query` is `True` |
| `test_max_age_get_where_clause_raises` | `MaxAge('updated_at', days=7).get_where_clause()` raises `NotImplementedError` |
| `test_custom_sql_where_clause` | `CustomSQL(name='test', sql='amount > 0').get_where_clause()` returns `"NOT (amount > 0)"` |
| `test_quality_config_checks_typed` | `QualityConfig.model_fields['checks'].annotation` is `list[QualityCheck]` |

**5c. `tests/quality/test_results.py`** -- Unit tests for result models:

| Test | Description |
|------|-------------|
| `test_check_result_passed` | `CheckResult` with `passed=True` |
| `test_check_result_with_error` | `CheckResult` with error string |
| `test_entity_result_all_passed` | `EntityResult.passed` is `True` when all checks pass |
| `test_entity_result_some_failed` | `EntityResult.passed` is `False` when any check fails |
| `test_entity_result_counts` | `passed_count` and `failed_count` are correct |

**5d. `tests/quality/test_runner.py`** -- Unit tests for runner (using DuckDB):

| Test | Description |
|------|-------------|
| `test_runner_init` | `QualityRunner(connection, dataset='test')` stores attributes |
| `test_table_ref_with_dataset` | `_table_ref('users')` returns `'test.users'` |
| `test_table_ref_without_dataset` | `_table_ref('users')` returns `'users'` when dataset is None |
| `test_build_failure_query` | Produces valid SQL structure |
| `test_build_unique_check_query` | Produces `GROUP BY ... HAVING` SQL |
| `test_build_max_age_check_query` | Produces `DATE_DIFF` SQL |
| `test_run_check_not_null_passes` | End-to-end with DuckDB: insert clean data, verify passes |
| `test_run_check_not_null_fails` | End-to-end with DuckDB: insert NULL, verify failure detected |

Note: DuckDB end-to-end tests for the runner may need to skip `MaxAge` and `MatchesPattern` tests since those use BigQuery-specific SQL. Mark them with `@pytest.mark.skip(reason="BigQuery-specific SQL, Phase 2 migration")` or adapt the SQL for DuckDB in the test fixture.

### Step 6: Verification

Run `pytest tests/quality/` and confirm all acceptance criteria pass:

1. QualityConfig, NotNull, Unique, InRange importable from `fyrnheim.quality`
2. `NotNull('email').get_where_clause()` returns valid SQL fragment
3. `InRange('amount', min=0, max=10000).get_where_clause()` returns correct SQL
4. `QualityConfig` validates with checks list and primary_key
5. QualityRunner, CheckResult, EntityResult importable from `fyrnheim.quality`

### Summary of Changes from Source

| Change | Source | Target | Rationale |
|--------|--------|--------|-----------|
| `QualityConfig.checks` type | `list[Any]` | `list[QualityCheck]` | Proper Pydantic v2 validation (design 5a) |
| `Unique.get_where_clause()` | Returns comment string | Raises `NotImplementedError` | Honest contract (design 5d) |
| `MaxAge.get_where_clause()` | Returns comment string | Raises `NotImplementedError` | Honest contract (design 5d) |
| `requires_special_query` property | (new) | Added to `QualityCheck` base | Runner can check before calling `get_where_clause()` (design 5d) |
| `ForeignKey._ref_table` | Hard-coded `dim_` prefix | Configurable via `ref_table` param | Remove convention coupling (design 5b) |
| `QualityRunner.__init__` | `dataset: str, project: str` | `connection: ibis.BaseBackend, dataset: str \| None` | Backend injection, testable with DuckDB (design 5c) |
| `CheckResult`, `EntityResult` | In `runner.py` | Moved to `results.py` | Import without runner dependencies (design sec 6) |
| Query execution | `self.client.query().result()` | `self.connection.raw_sql()` | Ibis-based execution (design 5c) |
| Table references | Backtick-quoted | `_table_ref()` helper, no backticks | Backend-agnostic quoting (design 3.3) |
| BigQuery-specific SQL | Unmarked | Tagged with `# TODO(ibis)` comments | Phase 2 migration breadcrumbs (design 3) |
| `CustomSQL` docstring | Minimal | Documents non-portability | Explicit about limitations (design 5e) |

### Files Created/Modified

```
src/fyrnheim/quality/
    __init__.py          # Modified: import CheckResult/EntityResult from results
    checks.py            # New: from source with 6 modifications (1a-1f)
    results.py           # New: extracted from source runner.py
    runner.py            # New: from source with 6 modifications (3a-3f)

tests/quality/
    __init__.py          # New: empty
    test_checks.py       # New: 16 tests
    test_results.py      # New: 5 tests
    test_runner.py       # New: 8 tests
```
