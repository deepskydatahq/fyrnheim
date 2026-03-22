# Zeffy Attribution PoC: Learnings and Gaps

End-to-end validation of the Zeffy marketing attribution pipeline using Fyrnheim on DuckDB with synthetic data. This document captures what worked, what broke, and follow-up missions needed.

## Pipeline Summary

| Entity | Rows | Method | Status |
|--------|------|--------|--------|
| touchpoints | 200 | workaround | OK |
| account | 80 | workaround | OK |
| attribution_first_touch | 28 | workaround | OK |
| attribution_paid_priority | 28 | workaround | OK |
| acquisition_signal | 28 | workaround | OK |
| account_attributed | 80 | workaround | OK |

All 6 entities executed successfully, but all required workarounds due to framework gaps. None ran purely through the framework's `fyr run` CLI path.

---

## What Worked Well

### Entity definition model is expressive
The Pydantic-based entity definitions (Entity, TableSource, DerivedSource, AggregationSource, CaseColumn, etc.) are highly expressive. The Zeffy attribution pipeline maps naturally to the entity model:
- TableSource for raw events
- DerivedSource with IdentityGraphConfig for account identity resolution
- AggregationSource for attribution and acquisition signals
- CaseColumn for channel classification rules
- ComputedColumn with helper functions (first_value_by, dedup_by, isin_literal)

### Dependency resolution works correctly
The topological sort in `resolution.py` correctly orders entities by their dependencies. When entities are properly registered, the execution order is deterministic and correct.

### Identity graph concept is powerful
The IdentityGraphConfig model (match_key, sources, priority, field mappings) is a clean abstraction for identity resolution. The generated outer-join + coalesce + priority logic is sound.

### Code generation architecture is solid
The IbisCodeGenerator produces clean, readable Python. The generated code is straightforward to debug when issues arise. The layer-by-layer approach (source -> prep -> dim) maps well to data transformation patterns.

---

## Gaps Found

### GAP-001: json_extract_scalar produces invalid Ibis expressions
**Severity:** High (blocks any entity using JSON extraction)
**Component:** `fyrnheim.primitives.json_ops.json_extract_scalar` + `IbisCodeGenerator._bind_expression`

`json_extract_scalar("event_properties", "gclid")` returns the string `JSON_EXTRACT_SCALAR(event_properties, 'gclid')`. The code generator's `_bind_expression()` then prefixes it with `t.`, producing:
```python
t.JSON_EXTRACT_SCALAR(event_properties, 'gclid')
```
This is not valid Ibis. On DuckDB, the correct approach is `t.event_properties.json_extract_string('gclid')` or using `ibis.literal` with SQL.

**Follow-up:** Create an Ibis-native JSON extraction primitive that generates proper Ibis expressions per backend.

### GAP-002: Entity registry only discovers `module.entity`
**Severity:** Medium (blocks multi-entity files)
**Component:** `fyrnheim.engine.registry.EntityRegistry.discover`

The registry scans Python files and only looks for `module.entity`. The attribution module defines two entities (`attribution_first_touch` and `attribution_paid_priority`) as separate module-level variables, which are silently ignored.

**Follow-up:** Support discovering multiple entities per file, e.g., by scanning for all module-level `Entity` instances, or by supporting an `entities = [...]` list convention.

### GAP-003: DerivedSource without identity_graph_config generates no source function
**Severity:** High (blocks join-based derived entities)
**Component:** `IbisCodeGenerator._generate_source_functions`

`account_attributed` uses `DerivedSource(depends_on=["account", "attribution_first_touch", "attribution_paid_priority"])` to express a wide join. But the codegen only handles DerivedSource when `identity_graph_config` is provided. Without it, no source function is generated, and the executor has no way to resolve the entity's inputs.

**Follow-up:** Add a "join mode" to DerivedSource that generates left-join logic from `depends_on` entities, with configurable join keys.

### GAP-004: CaseColumn generates chained `.else_()` but Ibis uses keyword arg
**Severity:** High (breaks all CaseColumn entities at runtime)
**Component:** `fyrnheim.components.expressions.CaseColumn.build_expression`

CaseColumn generates: `ibis.cases(...).else_('value')`. But `ibis.cases()` in Ibis >= 10.0.0 takes `else_` as a keyword argument, not a chained method. The generated code fails at runtime with `'StringColumn' object has no attribute 'else_'`.

**Fix:** Change `CaseColumn.build_expression` to generate `ibis.cases(..., else_='value')`.

### GAP-005: IdentityGraphSource rename collision
**Severity:** Medium (blocks specific identity graph configurations)
**Component:** `IbisCodeGenerator._generate_derived_source_function`

When `match_key_field` and a field in `fields` both reference the same source column, the generated `.rename()` dict has duplicate keys. Example: amplitude source has `match_key_field='amplitude_id'` and `fields={'primary_amplitude_id': 'amplitude_id'}`. Both try to rename FROM `amplitude_id`, producing `{'organization_id': 'amplitude_id', 'primary_amplitude_id': 'amplitude_id'}`.

**Follow-up:** Detect rename collisions and handle them by first renaming the match key, then aliasing the field separately (e.g., via `.mutate()`).

### GAP-006: Window functions in AggregationSource aggregate context
**Severity:** High (breaks first_value_by in aggregation entities)
**Component:** `IbisCodeGenerator._generate_aggregation_source_function` + `fyrnheim.components.expressions.first_value_by`

`first_value_by()` generates window expressions like `t.channel.first().over(window)`. These are placed inside `.group_by().aggregate()` by the AggregationSource codegen. Ibis requires aggregate metrics to be scalar reductions (like `.min()`, `.count()`), not window functions.

**Follow-up:** Either:
1. Detect window expressions in aggregation context and use a row_number + filter approach instead, or
2. Add a new expression helper that uses a subquery/CTE pattern for "first value per group."

---

## Comparison with dbt Approach

| Aspect | Fyrnheim | dbt |
|--------|----------|-----|
| **Language** | Python (Pydantic models) | SQL + Jinja |
| **Type safety** | Pydantic validation at definition time | No type safety until runtime |
| **JSON extraction** | Needs backend-aware primitives (GAP-001) | Native SQL per backend (ref/adapter macros) |
| **Identity graphs** | First-class concept with IdentityGraphConfig | Must be hand-coded in SQL |
| **Multi-entity files** | Not supported (GAP-002) | Each model is one .sql file |
| **Channel classification** | CaseColumn (elegant, but GAP-004) | SQL CASE WHEN (just works) |
| **Dependency resolution** | Automatic from source types | Explicit ref() calls |
| **Aggregation patterns** | AggregationSource (but GAP-006) | SQL GROUP BY (just works) |
| **Testing** | pytest (Python-native) | dbt test (schema + data tests) |

### Key Takeaways

1. **Fyrnheim's entity model is more expressive than dbt's SQL models** for identity graphs and typed transformations. The IdentityGraphConfig is a genuine improvement over hand-coded SQL joins.

2. **The codegen layer is the weakest link.** Most gaps are in code generation, not in the entity model or executor. The generated Ibis code needs to handle more edge cases.

3. **Backend-specific primitives need work.** JSON extraction, string operations, and other functions need to produce valid Ibis expressions per backend, not raw SQL strings.

4. **The "one entity per file" constraint is limiting** for related entities like attribution models that share the same source and logic pattern.

---

## Follow-Up Missions

### M023 (suggested): Fix Ibis codegen gaps
- Fix CaseColumn else_ generation (GAP-004) - quick fix
- Fix json_extract_scalar to produce Ibis-native expressions (GAP-001)
- Fix rename collision in identity graph codegen (GAP-005)
- Handle window-in-aggregate pattern (GAP-006)

### M024 (suggested): Multi-entity registry support
- Support discovering multiple entities per file (GAP-002)
- Convention: `entities = [entity1, entity2]` list, or scan all Entity instances

### M025 (suggested): DerivedSource join mode
- Add join semantics to DerivedSource without identity_graph_config (GAP-003)
- Support left_join, inner_join, cross_join with configurable keys
- Generate proper source functions that accept multiple dependency tables
