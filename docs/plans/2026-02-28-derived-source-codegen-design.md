# Design: DerivedSource Codegen -- Join Chain, PriorityCoalesce, Auto Columns

**Task:** typedata-q0z (M005-E003-S002)
**Date:** 2026-02-28
**Status:** brainstorm -> plan

---

## Problem Statement

DerivedSource entities currently generate an empty string from `_generate_source_functions()` because the dispatch in `ibis_code_generator.py` falls through to the `else: return ""` branch. The identity graph pattern requires generating ~200-300 LOC of join/coalesce/auto-column logic that has a fundamentally different function signature from all other source types: `source_{name}(sources: dict)` instead of `source_{name}(conn, backend)`.

The generated code must:
1. Accept a dict of pre-materialized source tables
2. Rename per-source columns to unified names
3. Cascade FULL OUTER JOINs on a match key
4. Apply PriorityCoalesce via `.fillna()` chains
5. Generate auto columns (source flags, IDs, dates)

---

## Codebase Analysis

### Current Dispatch (ibis_code_generator.py:75-88)

```python
def _generate_source_functions(self) -> str:
    source = self.entity.source
    if source is None:
        return ""
    if isinstance(source, UnionSource):
        return self._generate_union_source_functions(source)
    elif isinstance(source, (TableSource, EventAggregationSource)):
        return self._generate_single_source_function(source)
    elif isinstance(source, AggregationSource):
        return self._generate_aggregation_source_function(source)
    else:
        return ""  # <-- DerivedSource lands here
```

DerivedSource is imported in `entity.py` but not in `ibis_code_generator.py`. Adding it to the dispatch is the entry point.

### DerivedSource Model (source.py:119-140)

Currently has `identity_graph: str` and `depends_on: list[str]`. S001 (in-progress) will add `identity_graph_config: IdentityGraphConfig | None = None` with:

- `IdentityGraphConfig.match_key: str` -- unified key name in output
- `IdentityGraphConfig.sources: list[IdentityGraphSource]` -- min 2
- `IdentityGraphConfig.priority: list[str]` -- source names in coalesce order
- `IdentityGraphSource.name: str` -- source label (e.g. "hubspot")
- `IdentityGraphSource.entity: str` -- entity name to read from
- `IdentityGraphSource.match_key_field: str` -- join key column in this source
- `IdentityGraphSource.fields: dict[str, str]` -- {unified_name: source_column}
- `IdentityGraphSource.id_field: str | None` -- for source ID column
- `IdentityGraphSource.date_field: str | None` -- for first-seen date column

### Existing Patterns

All other `_generate_*` methods follow the same pattern:
1. Build f-string with function def
2. Chain string concatenation for body lines
3. Return the complete function string

The generated code uses `ibis.Table` API (`.rename()`, `.mutate()`, `.select()`, `.filter()`, `.outer_join()`).

### Ibis outer_join API Constraints

**Critical finding:** Ibis uses `lname`/`rname` parameters (not `suffixes`) to handle column name conflicts after joins. Default: left columns keep names, right columns get `_right` suffix.

```python
t1.outer_join(t2, t1.match_key == t2.match_key, lname="", rname="{name}_right")
```

**Critical finding:** Chaining `.outer_join()` calls directly hits [ibis-project/ibis#10293](https://github.com/ibis-project/ibis/issues/10293) -- an open bug where the second join in a chain fails with `IntegrityError: Cannot add ... they belong to another relation`. This is still open as of July 2025.

**Workaround:** After each `outer_join()`, call `.select()` to project the result into a fresh table expression. This materializes the column references and breaks the relation chain, allowing the next join to succeed.

---

## Design

### Architecture Decision: Single Method with Inline Helpers

The generator will add one new method `_generate_derived_source_function(source: DerivedSource)` that generates a single `source_{name}(sources: dict) -> ibis.Table` function. The generated function itself will be procedural -- no sub-functions -- because:

1. It follows the pattern of all existing generator methods (one `_generate_*` -> one function def)
2. The generated code runs as a data pipeline (sequential steps, not reusable logic)
3. Breaking the generated code into sub-functions adds complexity without benefit (the generated code is never edited by humans)

### Generated Code Structure

For a person entity with 2 sources (hubspot priority 1, stripe priority 2) and unified fields `name` and `company`:

```python
def source_person(sources: dict) -> ibis.Table:
    """Merge person from identity graph sources."""
    # --- Extract source tables ---
    t_hubspot = sources["hubspot"]
    t_stripe = sources["stripe"]

    # --- Rename to unified columns ---
    t_hubspot = t_hubspot.rename({"email": "hs_email", "name": "full_name"})
    t_stripe = t_stripe.rename({"email": "contact_email", "name": "cust_name"})

    # --- Preserve source-specific columns before join ---
    t_hubspot = t_hubspot.mutate(
        _hubspot_match_key=t_hubspot.email,
        hubspot_id=t_hubspot.person_id,
        first_seen_hubspot=t_hubspot.signup_date,
    )
    t_stripe = t_stripe.mutate(
        _stripe_match_key=t_stripe.email,
        stripe_id=t_stripe.customer_id,
        first_seen_stripe=t_stripe.created_at,
    )

    # --- Cascading FULL OUTER JOIN ---
    result = t_hubspot.outer_join(
        t_stripe, t_hubspot.email == t_stripe.email, lname="", rname="{name}_right"
    ).select(
        email=ibis.coalesce(t_hubspot.email, t_stripe.email),
        name_hubspot=t_hubspot.name,
        name_stripe=t_stripe.name,
        company_hubspot=t_hubspot.company,
        company_stripe=t_stripe.company,
        _hubspot_match_key=t_hubspot._hubspot_match_key,
        _stripe_match_key=t_stripe._stripe_match_key,
        hubspot_id=t_hubspot.hubspot_id,
        stripe_id=t_stripe.stripe_id,
        first_seen_hubspot=t_hubspot.first_seen_hubspot,
        first_seen_stripe=t_stripe.first_seen_stripe,
    )

    # --- PriorityCoalesce (hubspot > stripe) ---
    result = result.mutate(
        name=result.name_hubspot.fillna(result.name_stripe),
        company=result.company_hubspot.fillna(result.company_stripe),
    )

    # --- Source flags ---
    result = result.mutate(
        is_hubspot=result._hubspot_match_key.notnull(),
        is_stripe=result._stripe_match_key.notnull(),
    )

    # --- Drop intermediate columns ---
    result = result.drop(
        "name_hubspot", "name_stripe",
        "company_hubspot", "company_stripe",
        "_hubspot_match_key", "_stripe_match_key",
    )

    return result
```

### Key Design Decisions

#### D1: Explicit `.select()` After Each Join (Not Suffixes)

**Why:** The Ibis chaining bug (#10293) means we cannot simply chain `.outer_join()` calls. After each join, we must project into a new table expression via `.select()`. This also gives us full control over column naming -- we prefix shared field columns with the source name (`name_hubspot`, `name_stripe`) so they don't collide.

**Pattern for N sources:**
```
result = t_0.outer_join(t_1, ...).select(...)
result = result.outer_join(t_2, ...).select(...)
result = result.outer_join(t_3, ...).select(...)
```

Each `.select()` explicitly enumerates:
- The coalesced match key (only on first join, then carried forward)
- Per-source versions of shared fields (`{field}_{source}`)
- Auto columns (`_*_match_key` for flags, `{source}_id`, `first_seen_{source}`)

#### D2: Pre-Join Column Naming Strategy

Before each join, rename source columns to avoid conflicts:
1. `match_key_field` -> unified `match_key` name (via `.rename()`)
2. Shared fields renamed to `{unified_name}` (via `.rename()` with `fields` mapping)
3. Auto columns added via `.mutate()`:
   - `_{source}_match_key` = copy of match key (for post-join `notnull()` flag)
   - `{source}_id` = copy of `id_field` (when set)
   - `first_seen_{source}` = copy of `date_field` (when set)

#### D3: `.select()` Projection After Join

The `.select()` after each `outer_join()` explicitly lists every output column. This:
- Breaks the Ibis relation chain (workaround for #10293)
- Coalesces the match key: `email=ibis.coalesce(left.email, right.email)`
- Carries forward per-source field variants and auto columns
- Drops the duplicate match key from the right side

For 3+ sources, each subsequent join only has the match_key in common between `result` and `t_next`. The per-source field variants from earlier joins are carried through in the select.

#### D4: PriorityCoalesce via `.fillna()` Chains

After all joins complete, apply coalesce for each shared field:

```python
result = result.mutate(
    name=result.name_hubspot.fillna(result.name_stripe),
)
```

For 3+ sources with priority `[a, b, c]`:
```python
name=result.name_a.fillna(result.name_b).fillna(result.name_c)
```

This is cleaner than `ibis.coalesce()` and makes the priority ordering visually obvious in the generated code.

#### D5: Source Flags From Preserved Match Key Copies

Before the join, each source table gets a `_{source}_match_key` column (a copy of the match key). After all joins, this column is NULL only for rows that did not come from that source. So:

```python
is_hubspot=result._hubspot_match_key.notnull()
```

This is more reliable than checking any particular field, because the match key is the join key and is guaranteed to be present for every row from that source.

#### D6: Backward Compatibility

```python
if isinstance(source, DerivedSource):
    if source.identity_graph_config is not None:
        return self._generate_derived_source_function(source)
    return ""
```

DerivedSource without `identity_graph_config` (existing usage) still returns empty string.

### Generator Method Structure

```python
def _generate_derived_source_function(self, source: DerivedSource) -> str:
    """Generate multi-input source function for identity graph join."""
    config = source.identity_graph_config
    name = self.entity_name
    sources = config.sources          # list[IdentityGraphSource]
    priority = config.priority        # list[str]
    match_key = config.match_key      # str

    # 1. Collect shared fields (fields that appear in 2+ sources)
    # 2. Build function header: def source_{name}(sources: dict) -> ibis.Table:
    # 3. Build table extraction lines: t_{src.name} = sources["{src.name}"]
    # 4. Build rename lines per source (match_key + field_mappings)
    # 5. Build pre-join mutate lines (auto columns)
    # 6. Build cascading join chain with explicit .select()
    # 7. Build PriorityCoalesce .fillna() chains
    # 8. Build source flag mutates
    # 9. Build drop list for intermediate columns
    # 10. Assemble and return
```

### Identifying Shared Fields

A "shared field" is a unified field name that appears in 2+ sources' `fields` dict. These need PriorityCoalesce.

```python
field_to_sources: dict[str, list[str]] = {}
for src in sources:
    for unified_name in src.fields:
        field_to_sources.setdefault(unified_name, []).append(src.name)

shared_fields = {f for f, srcs in field_to_sources.items() if len(srcs) > 1}
```

Fields that appear in only one source are unique -- they pass through the join without coalescing.

### Column Naming Convention in Generated Code

| Column Type | Name in Generated Code | When |
|---|---|---|
| Match key (unified) | `{match_key}` (e.g. `email`) | Always |
| Match key copy for flags | `_{source}_{match_key}` | Always, dropped at end |
| Shared field per source | `{field}_{source}` | During join, dropped after coalesce |
| Coalesced shared field | `{field}` | After PriorityCoalesce |
| Unique field | `{field}` | Passed through directly |
| Source flag | `is_{source}` | Always |
| Source ID | `{source}_id` | When `id_field` is set |
| First-seen date | `first_seen_{source}` | When `date_field` is set |

### Handling 3+ Sources (Join Chain)

For sources `[A, B, C]`:

**Join 1:** `t_A.outer_join(t_B, match_key)`
- `.select()` outputs: match_key, {field}_A, {field}_B, unique_A_fields, unique_B_fields, auto_columns_A, auto_columns_B

**Join 2:** `result.outer_join(t_C, match_key)`
- `.select()` outputs: match_key (re-coalesced with C), all from join 1 + {field}_C, unique_C_fields, auto_columns_C

**PriorityCoalesce:** Applied once after all joins, using priority order.

### What Gets Generated vs What the Generator Computes

The generator (Python code in `ibis_code_generator.py`) computes:
- Which fields are shared vs unique
- The priority-ordered `.fillna()` chains
- Which auto columns to generate
- The explicit `.select()` column lists

The generated code (the output Python file) is straightforward procedural Ibis calls with no loops or conditionals.

---

## File Changes

### `src/fyrnheim/generators/ibis_code_generator.py`

1. Add `DerivedSource` to imports (line 13-18)
2. Add `DerivedSource` branch in `_generate_source_functions()` dispatch
3. Add `_generate_derived_source_function(self, source: DerivedSource) -> str` method (~120 LOC)

### `tests/test_ibis_code_generator.py`

New test class `TestDerivedSourceCodeGeneration` with tests for:
- Non-empty output when `identity_graph_config` is set
- Function signature `source_{name}(sources: dict)`
- FULL OUTER JOIN via `.outer_join()` for 2 sources
- Cascading join chain for 3 sources
- `.fillna()` chains in priority order
- `is_{source}` boolean flags
- `{source}_id` columns when `id_field` is set
- `first_seen_{source}` columns when `date_field` is set
- `ast.parse()` validity
- Backward compat: empty string without config

---

## Simplification Review

**What would I remove?**

1. **first_seen_{source} columns:** These are optional (`date_field` can be None). The generator already handles this -- only emits when set. No simplification needed; it is conditional, not mandatory.

2. **{source}_id columns:** Same as above -- conditional on `id_field`. Keep.

3. **The intermediate drop step:** Could skip `.drop()` and let downstream layers handle extra columns. However, the `.drop()` keeps the output schema clean and predictable, which matters for dimension layer composition. Keep.

4. **Pre-join `.mutate()` for auto columns:** Could compute `is_{source}` flags after the join using the match key directly. But after multi-way joins, the original per-source match key is lost (coalesced). Preserving via `_{source}_match_key` copies before the join is the correct approach. Keep.

**Is every component essential?**

- Extract tables: essential (entry point)
- Rename columns: essential (sources have different schemas)
- Pre-join mutate: essential (preserve source identity through join)
- Cascading join: essential (core operation)
- `.select()` after join: essential (workaround for Ibis bug, column control)
- PriorityCoalesce: essential (field resolution)
- Source flags: essential (acceptance criteria)
- Drop intermediates: desirable but could be deferred
- Source IDs: conditional, already guarded by `id_field is not None`
- First-seen dates: conditional, already guarded by `date_field is not None`

**Verdict: APPROVED**

All components are either essential for correctness or already conditional. The design is the minimum needed to satisfy the acceptance criteria. The `.select()` after each join is the one non-obvious element, but it is required due to the Ibis chaining bug.

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Ibis outer_join chaining bug (#10293) gets fixed differently | Low | Our `.select()` pattern works regardless; it is also cleaner |
| S001 model shape changes | Medium | Design depends on `IdentityGraphConfig` fields; adjust if S001 changes |
| Large number of sources (5+) | Low | Generator loop handles N sources; `.select()` list grows linearly |
| Column name collisions with reserved names | Low | Convention uses `_{source}_match_key` (underscore prefix) for internals |

---

## Dependencies

- **Hard dependency:** M005-E003-S001 must be complete (IdentityGraphConfig model must exist)
- **Soft dependency:** Ibis >= 9.x (outer_join API exists in all recent versions)

---

## Estimated Scope

- Generator method: ~120-150 LOC (building the f-string)
- Tests: ~100-150 LOC (10-12 test methods)
- Total: ~250-300 LOC
