# Plan: Portable regex checks for non-BigQuery backends

**Task:** typedata-147
**Date:** 2026-02-24

---

## Problem

`MatchesPattern.get_where_clause()` generates `NOT REGEXP_CONTAINS(col, r'pattern')` which is BigQuery-specific SQL. This fails on DuckDB and other backends.

## Current Architecture

The runner dispatches checks by type — `Unique`, `MaxAge`, and `ForeignKey` already have specialized execution paths in the runner (bypassing `get_where_clause()`). `MatchesPattern` is the only check with backend-specific SQL in its `get_where_clause()` that still goes through the generic `build_failure_query()` path.

```
run_check()
  ├─ isinstance(Unique)       → build_unique_check_query()     # custom SQL
  ├─ isinstance(MaxAge)       → build_max_age_check_query()    # custom SQL
  ├─ isinstance(ForeignKey)   → build_foreign_key_query()      # custom SQL
  └─ else                     → build_failure_query()          # uses get_where_clause()
                                  └─ MatchesPattern → REGEXP_CONTAINS (broken on DuckDB!)
```

## Solution

Use Ibis expressions (`column.re_search(pattern)`) in the runner for `MatchesPattern`. This follows the established pattern of runner-level dispatch and uses Ibis's cross-backend regex compilation.

```
run_check()
  ├─ isinstance(Unique)          → build_unique_check_query()
  ├─ isinstance(MaxAge)          → build_max_age_check_query()
  ├─ isinstance(ForeignKey)      → build_foreign_key_query()
  ├─ isinstance(MatchesPattern)  → _run_ibis_filter()  ← NEW (Ibis expressions)
  └─ else                        → build_failure_query()
```

## Implementation Steps

### Step 1: Update `src/fyrnheim/quality/checks.py`

Mark `MatchesPattern` as requiring special query handling (consistent with `Unique` and `MaxAge`):

```python
class MatchesPattern(QualityCheck, BaseModel):
    """Check that values match a regex pattern.

    Uses Ibis's re_search() for portable regex matching across backends.
    The runner handles execution via Ibis expressions rather than raw SQL.
    """

    column: str
    pattern: str

    def __init__(self, column: str, pattern: str):
        super().__init__(column=column, pattern=pattern)

    def get_where_clause(self) -> str:
        raise NotImplementedError(
            "MatchesPattern uses Ibis expressions for portable regex; handled by the runner."
        )

    @property
    def requires_special_query(self) -> bool:
        return True

    # display_name, columns_to_show unchanged
```

### Step 2: Update `src/fyrnheim/quality/runner.py`

**2a. Import MatchesPattern:**

```python
from .checks import ForeignKey, MatchesPattern, MaxAge, QualityCheck, QualityConfig, Unique
```

**2b. Add Ibis table helper:**

```python
def _get_ibis_table(self, table: str) -> ibis.Table:
    """Get an Ibis table expression, handling dataset qualification."""
    ref = self._table_ref(table)
    return self.connection.sql(f"SELECT * FROM {ref}")
```

Using `connection.sql()` rather than `connection.table()` because it handles the dataset-qualified table references that `_table_ref()` already builds.

**2c. Add Ibis-based regex execution in `run_check()`:**

Add a new isinstance branch before the generic `else`:

```python
elif isinstance(check, MatchesPattern):
    t = self._get_ibis_table(table)
    failing = t.filter(~t[check.column].re_search(check.pattern))
    result_expr = failing.select(*select_columns).limit(limit)
    df = result_expr.execute()
    rows = df.to_dict("records")
```

**2d. Update `_build_count_query()` for MatchesPattern:**

Add an isinstance branch that uses Ibis expressions for counting too:

```python
elif isinstance(check, MatchesPattern):
    t = self._get_ibis_table(table)
    count = t.filter(~t[check.column].re_search(check.pattern)).count().execute()
    return None  # Signal to caller that count was already computed
```

Actually, simpler approach: since `MatchesPattern` now has `requires_special_query = True`, the count query path in `run_check()` (lines 148-152) calls `_build_count_query()` only when `failure_count == limit`. For Ibis-based execution, we handle the count inline:

```python
elif isinstance(check, MatchesPattern):
    t = self._get_ibis_table(table)
    failing = t.filter(~t[check.column].re_search(check.pattern))
    sample = failing.select(*select_columns).limit(limit).execute()
    rows = sample.to_dict("records")
    failure_count = len(rows)
    if failure_count == limit:
        failure_count = failing.count().execute()
    return CheckResult(
        check_name=check.display_name,
        passed=(failure_count == 0),
        failure_count=failure_count,
        sample_failures=rows,
    )
```

This returns early from `run_check()`, bypassing the shared count-query logic.

### Step 3: Update tests

**3a. `tests/test_quality_checks.py` — Update MatchesPattern tests:**

```python
class TestMatchesPattern:
    def test_requires_special_query(self) -> None:
        check = MatchesPattern("email", r"^.+@.+$")
        assert check.requires_special_query is True

    def test_get_where_clause_raises(self) -> None:
        check = MatchesPattern("email", r"^.+@.+$")
        with pytest.raises(NotImplementedError):
            check.get_where_clause()

    def test_display_name(self) -> None:
        check = MatchesPattern("email", r"^.+@.+$")
        assert check.display_name == "MatchesPattern: email"

    def test_columns_to_show(self) -> None:
        check = MatchesPattern("email", r"^.+@.+$")
        assert check.columns_to_show == ["email"]
```

**3b. `tests/test_quality_runner.py` — Add MatchesPattern integration test:**

```python
class TestMatchesPatternExecution:
    def test_matches_pattern_passes(self, tmp_path) -> None:
        """MatchesPattern should pass when all values match the regex."""
        db_path = tmp_path / "test.duckdb"
        conn = ibis.duckdb.connect(str(db_path))
        conn.raw_sql("CREATE TABLE users AS SELECT 'alice@example.com' as email, 1 as id")
        runner = QualityRunner(connection=conn)
        check = MatchesPattern("email", r"^.+@.+\..+$")
        result = runner.run_check("users", check, primary_key="id")
        assert result.passed is True
        assert result.failure_count == 0

    def test_matches_pattern_fails(self, tmp_path) -> None:
        """MatchesPattern should detect rows that don't match the regex."""
        db_path = tmp_path / "test.duckdb"
        conn = ibis.duckdb.connect(str(db_path))
        conn.raw_sql("""
            CREATE TABLE users AS
            SELECT 1 as id, 'alice@example.com' as email
            UNION ALL SELECT 2, 'not-an-email'
        """)
        runner = QualityRunner(connection=conn)
        check = MatchesPattern("email", r"^.+@.+\..+$")
        result = runner.run_check("users", check, primary_key="id")
        assert result.passed is False
        assert result.failure_count == 1
```

**3c. `tests/test_cli_check.py` — Add MatchesPattern to E2E test:**

Add `MatchesPattern("email", r"^.+@.+$")` to the test entity's quality config and verify the CLI check command works end-to-end with DuckDB.

### Step 4: Run tests and verify

```bash
uv run pytest tests/test_quality_checks.py tests/test_quality_runner.py tests/test_cli_check.py -v
uv run pytest  # full suite
uv run ruff check src/ tests/
uv run mypy src/
```

## Files Changed

| File | Change |
|------|--------|
| `src/fyrnheim/quality/checks.py` | `MatchesPattern`: set `requires_special_query = True`, raise `NotImplementedError` in `get_where_clause()`, update docstring |
| `src/fyrnheim/quality/runner.py` | Import `MatchesPattern`, add `_get_ibis_table()` helper, add isinstance branch with Ibis expression execution |
| `tests/test_quality_checks.py` | Update `TestMatchesPattern` tests for new behavior |
| `tests/test_quality_runner.py` | Add DuckDB integration tests for MatchesPattern |
| `tests/test_cli_check.py` | Add MatchesPattern to E2E test entity |

## Out of Scope

- **MaxAge runner query** (`DATE_DIFF` is also BigQuery-specific) — separate task
- **Full Phase 2 migration** (adding `get_filter()` to all check types) — future work
- **Other backend-specific SQL** (`InRange` string pass-through, `CustomSQL`) — documented, not addressed here

## Risk

- Low risk. Only `MatchesPattern` execution path changes. All other checks untouched.
- Ibis `re_search()` is well-supported across DuckDB, BigQuery, PostgreSQL, MySQL.
- The `connection.sql()` approach for getting Ibis table expressions is standard Ibis API.
