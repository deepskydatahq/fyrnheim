# Design: M001-E001-S004 -- Extract primitives and components packages

**Date:** 2026-02-17
**Story:** M001-E001-S004 -- Extract primitives and components packages
**Status:** Plan

---

## 1. Source inventory

### Primitives (8 files, 37 public functions)

| Module | Functions | Style | Notes |
|--------|-----------|-------|-------|
| `hashing.py` | `hash_email`, `hash_id`, `hash_sha256`, `hash_md5`, `concat_hash` | Ibis expression strings | All produce Ibis method-chain strings (`.hash()`, `.cast()`, `.lower()`, `.strip()`) |
| `dates.py` | `date_diff_days`, `date_trunc_month/quarter/year`, `days_since`, `extract_year/month/day`, `earliest_date`, `latest_date` | Ibis expression strings | All produce Ibis method-chain strings (`.truncate()`, `.delta()`, `.year()`, `ibis.least()`, `ibis.greatest()`) |
| `categorization.py` | `categorize`, `categorize_contains`, `lifecycle_flag`, `boolean_to_int` | Ibis expression strings | All produce Ibis API strings (`ibis.cases()`, `.isin()`, `.cast()`) |
| `aggregations.py` | `sum_`, `count_`, `count_distinct`, `avg_`, `min_`, `max_`, `any_value`, `row_number_by`, `cumulative_sum`, `lag_value`, `lead_value`, `first_value`, `last_value` | Raw SQL strings | All produce raw SQL fragments (`SUM(col)`, `ROW_NUMBER() OVER (...)`) |
| `json_ops.py` | `to_json_struct`, `json_extract_scalar`, `json_value` | Raw SQL strings | BigQuery-specific SQL (`TO_JSON_STRING`, `JSON_EXTRACT_SCALAR`, `JSON_VALUE`) |
| `strings.py` | `extract_email_domain`, `is_personal_email_domain`, `account_id_from_domain` | Raw SQL strings | DuckDB/PostgreSQL SQL (`split_part`, `IN`, `CASE WHEN`, `MD5()`) |
| `time.py` | `parse_iso8601_duration` | Ibis expression strings | Produces Ibis method-chain strings (`.re_extract()`, `ibis.ifelse()`) |

### Components (5 files, 5 classes)

| Module | Class | Dependencies | Notes |
|--------|-------|--------------|-------|
| `computed_column.py` | `ComputedColumn` | `pydantic` only | Base building block. Has `to_sql()` method. |
| `measure.py` | `Measure` | `pydantic` only | Aggregation measure definition. |
| `lifecycle_flags.py` | `LifecycleFlags` | `pydantic`, `primitives.lifecycle_flag`, `ComputedColumn` | Imports from sibling `primitives` package. |
| `time_metrics.py` | `TimeBasedMetrics` | `pydantic`, `primitives.{date_diff_days, date_trunc_month, extract_year}`, `ComputedColumn` | Imports from sibling `primitives` package. |
| `quality_checks.py` | `DataQualityChecks` | `pydantic`, `ComputedColumn` | No primitives dependency. |

---

## 2. Design decisions

### Decision 1: Are primitives pure SQL string generators or do they use Ibis expressions?

**Finding:** They are a mix. There are two distinct paradigms in the existing code:

- **Ibis expression string generators** (hashing, dates, categorization, time): These produce strings that represent Ibis Python API calls (e.g., `t.email.lower().strip().hash().cast("string")`, `ibis.cases(...)`, `t.created_at.truncate("M")`). They auto-prefix bare column names with `t.` and produce code meant to be `eval()`'d against an Ibis table expression.

- **Raw SQL string generators** (aggregations, json_ops, strings): These produce literal SQL fragments (e.g., `SUM(column)`, `ROW_NUMBER() OVER (...)`, `TO_JSON_STRING(STRUCT(...))`, `split_part(col, '@', 2)`).

**Decision for typedata:** Extract as-is for now. Both paradigms are "expression string generators" -- they take column names and return expression strings. The difference is the target language (Ibis API vs raw SQL). This is an internal detail that does not affect the public API shape, since both return `str`. Document the paradigm difference clearly and track the unification question for a future story (see Decision 5).

**Ibis dependency impact:** The primitives package itself does NOT import ibis at runtime. All functions are pure Python string-formatting operations. They generate strings that _reference_ Ibis API calls (like `ibis.cases(...)` and `t.column.method()`), but the actual ibis import happens only at evaluation time in the transformation engine. Therefore: **primitives has ZERO runtime dependencies** -- not even ibis.

### Decision 2: BigQuery-specific SQL in primitives that needs to be made backend-agnostic

**Finding:** There are three files with backend-specific SQL:

| File | Function | SQL used | Backend |
|------|----------|----------|---------|
| `json_ops.py` | `to_json_struct` | `TO_JSON_STRING(STRUCT(...))` | BigQuery-specific |
| `json_ops.py` | `json_extract_scalar` | `JSON_EXTRACT_SCALAR(col, path)` | BigQuery-specific (ANSI has different syntax) |
| `json_ops.py` | `json_value` | `JSON_VALUE(col, path)` | BigQuery (also supported by some others) |
| `strings.py` | `extract_email_domain` | `split_part(col, '@', 2)` | DuckDB/PostgreSQL (not BigQuery) |
| `strings.py` | `is_personal_email_domain` | `col IN (...)` | Standard SQL (portable) |
| `strings.py` | `account_id_from_domain` | `CASE WHEN ... ELSE MD5(col) END` | `MD5()` function varies by backend |
| `aggregations.py` | `any_value` | `ANY_VALUE(col)` | BigQuery/DuckDB (not all backends) |

**Decision for typedata:** Extract `json_ops.py` and `strings.py` as-is but add a docstring-level note that these produce backend-specific SQL. Mark the module docstrings clearly:

```python
"""JSON operation primitives.

NOTE: These functions generate raw SQL strings that are BigQuery-specific.
For cross-backend compatibility, prefer Ibis-native JSON operations
when available, or use backend-specific dispatch in a future version.
"""
```

Rationale: The story scope is "extract and make importable," not "rewrite for portability." Backend-agnostic rewrites belong in a dedicated follow-up story. The `aggregations.py` raw SQL is standard SQL that works across most backends (ROW_NUMBER, SUM, COUNT, etc.) with the exception of `ANY_VALUE`.

### Decision 3: Which components are generic vs timo-specific?

**Finding:** All five components are fully generic. None contain timo-specific business logic:

| Component | Generic? | Reasoning |
|-----------|----------|-----------|
| `ComputedColumn` | Yes | A name + expression + optional description. Universal concept. |
| `Measure` | Yes | A named aggregation expression. Universal analytics concept. |
| `LifecycleFlags` | Yes | Generates is_active/is_churned/is_at_risk from a status column. Parameterized -- the user provides the active/churned/at-risk state lists. |
| `TimeBasedMetrics` | Yes | Generates days_since_created, created_month, created_year from a timestamp column. Universal time analysis. |
| `DataQualityChecks` | Yes | Generates boolean flag columns from arbitrary SQL conditions. Fully parameterized. |

The only timo-specific element is the hardcoded personal email domain list in `strings.py::is_personal_email_domain`, but that is in primitives not components, and it is still a generally useful list.

**Decision for typedata:** Extract all five components as-is. They are all suitable for a public library. No timo-specific code needs to be removed.

### Decision 4: API improvements for public library

**Identified improvements (defer to follow-up story unless trivial):**

1. **`ComputedColumn.to_sql()` naming**: The `to_sql()` method generates `expression AS name` format. This is fine, but for an Ibis-based library, consider adding a `to_ibis_expr(table)` method that evaluates the expression string against an actual Ibis table. **Defer** -- this couples to the expression evaluation engine (story S005+).

2. **`t.` prefix auto-injection in primitives**: Every Ibis-expression primitive has the pattern:
   ```python
   if not col.startswith(("t.", "ibis.")):
       col = f"t.{col}"
   ```
   This assumes the table variable is always named `t`. For a public library, this should be configurable or use a different mechanism. However, since these generate strings that are later evaluated in a controlled context where `t` is the conventional Ibis table variable name, this is acceptable for v0. **Defer** -- this is an evaluation-engine concern.

3. **Type annotations**: All primitives return `str`. This is correct. No changes needed.

4. **Validation in components**: `ComputedColumn` and `Measure` already use Pydantic validators (`field_validator`, `min_length`). Good for a public API.

5. **`__repr__` methods**: Components have useful `__repr__` implementations. Keep as-is.

6. **Missing `__all__` in component modules**: Individual component modules don't have `__all__`, but the package `__init__.py` has it. Acceptable.

**Decision for typedata:** Extract as-is with no API changes. The current API is clean and well-typed. Future improvements (configurable table variable name, `to_ibis_expr()` methods) belong in later stories when the expression evaluation engine is built.

### Decision 5: How primitives should work with Ibis (string expressions vs actual Ibis operations)

**Current state:** Primitives are string template generators. They produce strings like:
```python
hash_email("email")  # -> 't.email.lower().strip().hash().cast("string")'
categorize("revenue", [(1000, "small")], "large")  # -> 'ibis.cases(...)'
sum_("amount")  # -> 'SUM(amount)'
```

These strings are presumably `eval()`'d or interpolated later by the transformation engine.

**Trade-offs of each approach:**

| Approach | Pros | Cons |
|----------|------|------|
| **String templates (current)** | Zero dependencies; serializable to TOML/YAML; simple to understand; easy to compose in config files | No type safety; eval() security concerns; no IDE autocomplete; mix of Ibis-API and raw-SQL strings |
| **Actual Ibis operations** | Type-safe; composable; backend-portable via Ibis; IDE support | Requires ibis as dependency; cannot serialize to config; more complex API |
| **Hybrid: deferred Ibis expressions** | Type-safe when evaluated; can still be serialized; ibis only needed at execution time | More complex implementation; two-phase design |

**Decision for typedata:** Keep the current string-template approach for the `typedata.primitives` package. Rationale:

1. **Zero-dependency extraction** is the goal of this story. String templates achieve this perfectly.
2. **Serialization matters**: The timo-data-stack (and likely typedata users) store transformation definitions in TOML/YAML config files. String expressions can round-trip through these formats; Ibis expression objects cannot.
3. **The transformation engine** (a future story) is responsible for evaluating these strings in an Ibis context. That engine will handle the `eval()` safely and provide the `t` (table) and `ibis` references.
4. **Unification of the two paradigms** (Ibis-API strings vs raw SQL strings) should be a future story. The likely path is to convert the `aggregations.py` and `strings.py` raw SQL to Ibis-expression strings, since Ibis can express all of these operations natively (`t.col.sum()`, `t.email.split('@')[1]`, etc.).

---

## 3. Extraction plan

### Package structure

```
typedata/
  primitives/
    __init__.py          # Re-export all public functions (copy from source)
    aggregations.py      # Copy verbatim
    categorization.py    # Copy verbatim
    dates.py             # Copy verbatim
    hashing.py           # Copy verbatim
    json_ops.py          # Copy verbatim + add backend-specific note in docstring
    strings.py           # Copy verbatim + add backend-specific note in docstring
    time.py              # Copy verbatim
  components/
    __init__.py          # Re-export all public classes (copy from source)
    computed_column.py   # Copy verbatim
    lifecycle_flags.py   # Update import: from ..primitives import lifecycle_flag
    measure.py           # Copy verbatim
    quality_checks.py    # Copy verbatim
    time_metrics.py      # Update import: from ..primitives import ...
```

### Import path changes

The components package has two files that import from primitives via relative imports:

- `lifecycle_flags.py`: `from ..primitives import lifecycle_flag` -- **No change needed** if the package structure preserves the same relative layout (`typedata.primitives` and `typedata.components` as sibling packages under `typedata`).
- `time_metrics.py`: `from ..primitives import date_diff_days, date_trunc_month, extract_year` -- **Same, no change needed.**

### Dependencies

| Package | Runtime deps |
|---------|-------------|
| `typedata.primitives` | None (pure Python string formatting) |
| `typedata.components` | `pydantic` (already a project dependency), `typedata.primitives` (sibling) |

### What NOT to change

- No function signatures
- No return value formats
- No behavioral changes
- No ibis dependency added
- No backend-agnostic rewrites (deferred)

---

## 4. Risks and open questions

1. **`eval()` security**: The string-template primitives are designed to be eval'd. The typedata public library should document that these strings are meant for controlled evaluation contexts only, not user-provided input. This is an existing design characteristic, not something introduced by extraction.

2. **`any_value` portability**: `ANY_VALUE()` is not supported in all SQL backends (e.g., older PostgreSQL). Since this is a raw SQL primitive, it will fail on unsupported backends. Acceptable for v0 -- document the limitation.

3. **`t.` prefix convention**: All Ibis-expression primitives assume the table variable is named `t`. This is a strong convention in the timo-data-stack codebase. For a public library, it may need to be made configurable. **Defer to a later story** when the expression evaluation engine is built.

4. **Personal email domain list**: The `is_personal_email_domain` function has a hardcoded list of 8 personal email domains. For a public library, consider making this configurable with a default. **Low priority -- defer.**

---

## 5. Follow-up stories identified

- **Unify primitives to Ibis-expression style**: Convert `aggregations.py`, `strings.py`, and `json_ops.py` from raw SQL to Ibis-expression strings for cross-backend portability.
- **Configurable table variable name**: Replace hardcoded `t.` prefix with configurable table reference.
- **`to_ibis_expr()` methods on components**: Add methods that produce actual Ibis expressions from string templates, for use in the transformation engine.
- **Backend-specific dispatch for json_ops**: Implement backend-aware JSON primitive generation.

---

## 6. Implementation plan

### Summary

Extract 8 primitives modules (7 function modules + `__init__.py`) and 6 components modules (5 class modules + `__init__.py`) from `timo-data-stack/metadata/` into `src/typedata/primitives/` and `src/typedata/components/`. The primitives package contains 37 pure-Python string-formatting functions with zero runtime dependencies. The components package contains 5 Pydantic model classes that depend on `pydantic` (already a project dependency) and on the sibling `typedata.primitives` package. Two component files (`lifecycle_flags.py`, `time_metrics.py`) import from `..primitives` via relative imports -- these imports work as-is because the target package layout preserves the same sibling structure. No function signatures, return values, or behavior changes are needed.

### Acceptance criteria

1. `hash_email`, `concat_hash`, `hash_md5`, `hash_sha256` importable from `typedata.primitives`
2. `categorize`, `lifecycle_flag`, `boolean_to_int` importable from `typedata.primitives`
3. `date_diff_days`, `days_since`, `extract_year`, `date_trunc_month` importable from `typedata.primitives`
4. `sum_`, `count_`, `count_distinct`, `avg_`, `row_number_by` importable from `typedata.primitives`
5. `ComputedColumn`, `Measure`, `LifecycleFlags`, `TimeBasedMetrics`, `DataQualityChecks` importable from `typedata.components`
6. `ComputedColumn` validates with `name` and `expression` fields (Pydantic model with `min_length=1` validators)
7. Primitives return valid SQL/Ibis expression strings when called

### Implementation tasks

All paths below are relative to the typedata repo root (`/home/tmo/roadtothebeach/tmo/typedata/`).

#### Task 1: Copy primitives package (8 files)

| # | Source file | Target file | Action |
|---|------------|-------------|--------|
| 1.1 | `timo-data-stack/metadata/primitives/__init__.py` | `src/typedata/primitives/__init__.py` | **Overwrite** the empty `__init__.py` created by S001. Copy verbatim -- all relative imports (`.aggregations`, `.categorization`, etc.) resolve correctly because the module files will be siblings in the same package. |
| 1.2 | `timo-data-stack/metadata/primitives/hashing.py` | `src/typedata/primitives/hashing.py` | **Copy verbatim.** No imports, no changes. 5 functions: `hash_email`, `hash_id`, `hash_sha256`, `hash_md5`, `concat_hash`. |
| 1.3 | `timo-data-stack/metadata/primitives/dates.py` | `src/typedata/primitives/dates.py` | **Copy verbatim.** No imports, no changes. 10 functions: `date_diff_days`, `date_trunc_month`, `date_trunc_quarter`, `date_trunc_year`, `days_since`, `extract_year`, `extract_month`, `extract_day`, `earliest_date`, `latest_date`. |
| 1.4 | `timo-data-stack/metadata/primitives/categorization.py` | `src/typedata/primitives/categorization.py` | **Copy verbatim.** No imports, no changes. 4 functions: `categorize`, `categorize_contains`, `lifecycle_flag`, `boolean_to_int`. |
| 1.5 | `timo-data-stack/metadata/primitives/aggregations.py` | `src/typedata/primitives/aggregations.py` | **Copy verbatim.** No imports, no changes. 13 functions: `sum_`, `count_`, `count_distinct`, `avg_`, `min_`, `max_`, `any_value`, `row_number_by`, `cumulative_sum`, `lag_value`, `lead_value`, `first_value`, `last_value`. |
| 1.6 | `timo-data-stack/metadata/primitives/json_ops.py` | `src/typedata/primitives/json_ops.py` | **Copy and update module docstring.** Add backend-specific note: `"NOTE: These functions generate raw SQL strings that are BigQuery-specific."` 3 functions: `to_json_struct`, `json_extract_scalar`, `json_value`. |
| 1.7 | `timo-data-stack/metadata/primitives/strings.py` | `src/typedata/primitives/strings.py` | **Copy and update module docstring.** Add backend-specific note: `"NOTE: These functions generate raw SQL strings targeting DuckDB/PostgreSQL."` 3 functions: `extract_email_domain`, `is_personal_email_domain`, `account_id_from_domain`. |
| 1.8 | `timo-data-stack/metadata/primitives/time.py` | `src/typedata/primitives/time.py` | **Copy verbatim.** No imports, no changes. 1 function: `parse_iso8601_duration`. |

**Import path changes for primitives:** None. All primitives modules are pure Python with zero imports (no `import` statements at all). The `__init__.py` uses only relative imports (`.hashing`, `.dates`, etc.) which resolve correctly.

#### Task 2: Copy components package (6 files)

| # | Source file | Target file | Action |
|---|------------|-------------|--------|
| 2.1 | `timo-data-stack/metadata/components/__init__.py` | `src/typedata/components/__init__.py` | **Overwrite** the empty `__init__.py` created by S001. Copy verbatim -- all relative imports (`.computed_column`, `.lifecycle_flags`, etc.) resolve correctly. |
| 2.2 | `timo-data-stack/metadata/components/computed_column.py` | `src/typedata/components/computed_column.py` | **Copy verbatim.** Imports: `pydantic` only. No path changes. |
| 2.3 | `timo-data-stack/metadata/components/measure.py` | `src/typedata/components/measure.py` | **Copy verbatim.** Imports: `pydantic` only. No path changes. |
| 2.4 | `timo-data-stack/metadata/components/lifecycle_flags.py` | `src/typedata/components/lifecycle_flags.py` | **Copy verbatim.** Imports: `pydantic`, `from ..primitives import lifecycle_flag`, `from .computed_column import ComputedColumn`. The `..primitives` relative import resolves to `typedata.primitives` -- **no change needed** because components and primitives are sibling packages under `typedata`. |
| 2.5 | `timo-data-stack/metadata/components/time_metrics.py` | `src/typedata/components/time_metrics.py` | **Copy verbatim.** Imports: `pydantic`, `from ..primitives import date_diff_days, date_trunc_month, extract_year`, `from .computed_column import ComputedColumn`. Same as 2.4 -- **no change needed.** |
| 2.6 | `timo-data-stack/metadata/components/quality_checks.py` | `src/typedata/components/quality_checks.py` | **Copy verbatim.** Imports: `pydantic`, `from .computed_column import ComputedColumn`. No path changes. |

**Import path changes for components:** None. The source code already uses `from ..primitives import ...` relative imports, and the target layout (`src/typedata/primitives/` and `src/typedata/components/` as siblings under `src/typedata/`) preserves this relationship identically.

#### Task 3: Verify no additional dependencies needed

- `typedata.primitives`: Zero runtime dependencies. All 37 functions are pure `str` formatting with no imports.
- `typedata.components`: Requires `pydantic` (already declared in `pyproject.toml` by S001). No new dependencies to add.
- No `ibis` dependency needed at runtime. Primitives generate strings that *reference* Ibis API calls but do not *import* ibis.

### Test plan

All tests go in `tests/test_primitives.py` and `tests/test_components.py`.

#### `tests/test_primitives.py`

**AC1 -- Hashing imports and output:**
- `test_hash_email_importable`: `from typedata.primitives import hash_email`; call `hash_email("email")` and assert result is a `str` containing `".lower().strip().hash()"`
- `test_concat_hash_importable`: `from typedata.primitives import concat_hash`; call `concat_hash("a", "b")` and assert result is a `str` containing `"ibis.concat("`
- `test_hash_md5_importable`: `from typedata.primitives import hash_md5`; call `hash_md5("col")` and assert result contains `'.hashbytes("md5")'`
- `test_hash_sha256_importable`: `from typedata.primitives import hash_sha256`; call `hash_sha256("col")` and assert result contains `".hash()"`

**AC2 -- Categorization imports and output:**
- `test_categorize_importable`: `from typedata.primitives import categorize`; call `categorize("revenue", [(1000, "small")], "large")` and assert result contains `"ibis.cases("`
- `test_lifecycle_flag_importable`: `from typedata.primitives import lifecycle_flag`; call `lifecycle_flag("status", ["active"])` and assert result contains `".isin("`
- `test_boolean_to_int_importable`: `from typedata.primitives import boolean_to_int`; call `boolean_to_int("flag")` and assert result contains `'.cast("int64")'`

**AC3 -- Date imports and output:**
- `test_date_diff_days_importable`: `from typedata.primitives import date_diff_days`; call `date_diff_days("created_at")` and assert result is `str` containing `".delta("`
- `test_days_since_importable`: `from typedata.primitives import days_since`; call `days_since("created_at")` and assert result is `str`
- `test_extract_year_importable`: `from typedata.primitives import extract_year`; call `extract_year("date")` and assert result contains `".year()"`
- `test_date_trunc_month_importable`: `from typedata.primitives import date_trunc_month`; call `date_trunc_month("date")` and assert result contains `'.truncate("M")'`

**AC4 -- Aggregation imports and output:**
- `test_sum_importable`: `from typedata.primitives import sum_`; call `sum_("amount")` and assert result == `"SUM(amount)"`
- `test_count_importable`: `from typedata.primitives import count_`; call `count_()` and assert result == `"COUNT(*)"`
- `test_count_distinct_importable`: `from typedata.primitives import count_distinct`; call `count_distinct("id")` and assert result == `"COUNT(DISTINCT id)"`
- `test_avg_importable`: `from typedata.primitives import avg_`; call `avg_("price")` and assert result == `"AVG(price)"`
- `test_row_number_by_importable`: `from typedata.primitives import row_number_by`; call `row_number_by("id", "created_at")` and assert result contains `"ROW_NUMBER()"`

**AC7 -- Additional primitives return strings:**
- `test_json_ops_return_strings`: Import and call all 3 json_ops functions, assert all return `str`
- `test_strings_return_strings`: Import and call all 3 strings functions, assert all return `str`
- `test_time_returns_string`: Import and call `parse_iso8601_duration("duration")`, assert returns `str`
- `test_t_prefix_injection`: Call `hash_email("email")` (bare column), assert result starts with `"t.email"`; call `hash_email("t.email")` (already prefixed), assert no double prefix

#### `tests/test_components.py`

**AC5 -- Component imports:**
- `test_computed_column_importable`: `from typedata.components import ComputedColumn`; instantiate successfully
- `test_measure_importable`: `from typedata.components import Measure`; instantiate successfully
- `test_lifecycle_flags_importable`: `from typedata.components import LifecycleFlags`; instantiate with `status_column`, `active_states`, `churned_states`
- `test_time_based_metrics_importable`: `from typedata.components import TimeBasedMetrics`; instantiate with `created_at_col`
- `test_data_quality_checks_importable`: `from typedata.components import DataQualityChecks`; instantiate with `checks` dict

**AC6 -- ComputedColumn validation:**
- `test_computed_column_valid`: `ComputedColumn(name="total", expression="SUM(amount)")` succeeds
- `test_computed_column_empty_name_fails`: `ComputedColumn(name="", expression="SUM(amount)")` raises `ValidationError` (min_length=1)
- `test_computed_column_empty_expression_fails`: `ComputedColumn(name="total", expression="")` raises `ValidationError` (min_length=1)
- `test_computed_column_strips_expression`: `ComputedColumn(name="total", expression="  SUM(amount)  ")` has `expression == "SUM(amount)"`
- `test_computed_column_to_sql`: `ComputedColumn(name="total", expression="SUM(amount)").to_sql()` returns `"    SUM(amount) AS total"`
- `test_computed_column_repr`: `repr(ComputedColumn(name="total", expression="SUM(amount)"))` contains `"ComputedColumn(name='total')"`

**Integration -- Components use primitives correctly:**
- `test_lifecycle_flags_generates_columns`: Create `LifecycleFlags(status_column="status", active_states=["active"], churned_states=["cancelled"])`, call `to_computed_columns()`, assert 2 `ComputedColumn` objects returned with names `"is_active"` and `"is_churned"`, and expressions contain `".isin("`
- `test_lifecycle_flags_with_at_risk`: Same but include `at_risk_states=["trial_expired"]`, assert 3 columns returned
- `test_time_metrics_generates_columns`: Create `TimeBasedMetrics(created_at_col="created_at")`, call `to_computed_columns()`, assert 3 columns with names `"days_since_created"`, `"created_month"`, `"created_year"`
- `test_time_metrics_with_updated_at`: Create with `updated_at_col="updated_at"`, assert 5 columns returned (adds `"days_since_updated"` and `"days_between_created_and_updated"`)
- `test_quality_checks_generates_columns`: Create `DataQualityChecks(checks={"missing_email": "email IS NULL"})`, call `to_computed_columns()`, assert 1 column with name `"has_missing_email"`
- `test_measure_validation`: `Measure(name="revenue", expression="SUM(amount)")` succeeds; `Measure(name="", expression="SUM(amount)")` raises `ValidationError`
