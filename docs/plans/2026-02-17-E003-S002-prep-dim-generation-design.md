# Design: M001-E003-S002 -- Generate Ibis code for PrepLayer and DimensionLayer

**Date:** 2026-02-17
**Story:** M001-E003-S002-prep-dimension-generation
**Depends on:** M001-E003-S001 (generator base + source generation)
**Source:** `/home/tmo/roadtothebeach/tmo/timo-data-stack/metadata/generators/ibis_code_generator.py` (lines 261-320)
**Target:** `src/fyrnheim/generators/ibis_code_generator.py` (extend existing class from S001)

---

## Source Analysis

### Existing generator behavior (timo-data-stack)

The current `_generate_prep_function()` and `_generate_dimension_function()` methods are
nearly identical. Each:

1. Creates a function signature: `def prep_{name}(source_{name}: ibis.Table) -> ibis.Table:`
   or `def dim_{name}(prep_{name}: ibis.Table) -> ibis.Table:`
2. Aliases the input to `t`
3. If `computed_columns` exists, generates a single `t.mutate(...)` call with all computed
   columns as keyword arguments
4. Uses `_bind_expression(expr)` to prefix bare column references with `t.`
5. If no computed columns, returns `t` unchanged

### ComputedColumn expression format

`ComputedColumn` is a Pydantic model with three fields:

```python
class ComputedColumn(BaseModel):
    name: str          # output column name
    expression: str    # Ibis Python expression as string
    description: str | None = None
```

Expressions are **Ibis Python code strings** -- not SQL. Examples from timo-data-stack entities:

| Expression (in entity definition) | After `_bind_expression()` | What it does |
| --- | --- | --- |
| `hash_email("customer_email")` | `t.customer_email.lower().strip().hash().cast("string")` | SHA256 hash of email |
| `t.status == 'active'` | `t.status == 'active'` | Boolean flag |
| `t.status.isin(["active", "on_trial"])` | `t.status.isin(["active", "on_trial"])` | Lifecycle flag |
| `email.lower().strip().hash().cast("string")` | `t.email.lower().strip().hash().cast("string")` | Hashing via primitive |
| `ibis.ifelse(t.x.notnull(), "yes", "no")` | `ibis.ifelse(t.x.notnull(), "yes", "no")` | Conditional |
| `ibis.cases((t.x < 100, "low"), else_="high")` | `ibis.cases((t.x < 100, "low"), else_="high")` | Multi-case |

The key insight: **primitive functions** (like `hash_email`, `lifecycle_flag`, `categorize`)
are called at *entity definition time* and return *Ibis expression strings*. By the time the
generator sees the `ComputedColumn.expression`, it is already a plain string of Ibis Python
code. The generator never calls primitives -- it just embeds the string.

### TypeCast, Rename, Divide, Multiply

These live on `SourceOverrides` in the source config, not on the layer config. In the current
timo-data-stack codebase:

```python
class SourceOverrides(BaseModel):
    type_casts: list[TypeCast] = []    # TypeCast(field="created_at", target_type="timestamp")
    renames: list[Rename] = []          # Rename(from_name="id", to_name="subscription_id")
    divides: list[Divide] = []          # Divide(field="amount", divisor=100.0, suffix="_dollars")
    multiplies: list[Multiply] = []     # Multiply(field="rate", multiplier=100.0, suffix="_pct")
```

These are **not currently handled by the IbisCodeGenerator at all**. The DuckDB SQL generator
handles them in its staging layer (SQL `CAST`, `AS` renames, division expressions). The Ibis
code generator skips them -- the `_generate_prep_function()` only processes `computed_columns`.

This is a gap that fyrnheim should fill.

---

## Design Decisions

### Decision 1: ComputedColumn expressions are Ibis Python code strings, embedded verbatim

**Decision:** Keep the current approach exactly. Expressions are Ibis Python code strings
that get embedded into generated `.mutate()` calls after `_bind_expression()` processing.

**Rationale:**

- This is what the current generator does and it works. The primitives library
  (`fyrnheim.primitives`) produces expression strings at definition time. The generator
  has no need to parse, compile, or interpret them -- it embeds them as Python source code.
- `_bind_expression()` handles one simple task: if the expression does not already contain
  `t.` or `ibis.`, prefix it with `t.` so that bare column references bind to the table.
- The generated code is valid Python that `ast.parse()` can verify.
- No SQL translation needed. No expression DSL needed. Just string embedding.

**What this means for primitives:**

- `hash_email("email")` returns `'t.email.lower().strip().hash().cast("string")'`
- `lifecycle_flag("status", ["active"])` returns `'t.status.isin(["active"])'`
- `categorize("revenue", [(1000, "small")], "large")` returns an `ibis.cases(...)` string
- These are called in entity definitions, resolved to strings, stored in `ComputedColumn.expression`
- The generator embeds the resolved string

**No changes needed** from the current approach.

### Decision 2: TypeCast, Rename, Divide, Multiply translate to prep layer Ibis operations

**Decision:** Generate these as explicit Ibis operations in the prep function, *before*
computed columns. The prep function becomes a pipeline of three stages:
(1) source overrides, (2) computed columns, (3) return.

**Translation table:**

| Operation | Pydantic Model | Generated Ibis Code |
| --- | --- | --- |
| TypeCast | `TypeCast(field="created_at", target_type="timestamp")` | `t = t.mutate(created_at=t.created_at.cast("timestamp"))` |
| Rename | `Rename(from_name="id", to_name="subscription_id")` | `t = t.rename(subscription_id="id")` |
| Divide | `Divide(field="amount", divisor=100.0, suffix="_dollars")` | `t = t.mutate(amount_dollars=(t.amount / 100.0).cast("decimal"))` |
| Multiply | `Multiply(field="rate", multiplier=100.0, suffix="_pct")` | `t = t.mutate(rate_pct=(t.rate * 100.0).cast("decimal"))` |

**Ibis type mapping** (same as the DuckDB generator's `_map_type`, but for Ibis):

| Source type string | Ibis cast type |
| --- | --- |
| `timestamp` | `"timestamp"` |
| `boolean` | `"boolean"` |
| `integer`, `bigint`, `int64` | `"int64"` |
| `float`, `double`, `float64` | `"float64"` |
| `decimal`, `numeric` | `"decimal"` |
| `string`, `text` | `"string"` |
| `date` | `"date"` |

**Order of operations in generated prep function:**

1. Renames first (so subsequent operations can reference new names)
2. TypeCasts second (so computed columns work on correctly typed columns)
3. Divides and Multiplies third (these create new columns)
4. Computed columns last (these can reference any of the above)

**Where do source overrides come from?**

Source overrides live on `entity.source.overrides` (a `SourceOverrides` instance on
`BigQuerySource`). The generator checks for this and generates the operations.

```python
# In generator
source = self.entity.source
overrides = getattr(source, 'overrides', None)
if overrides:
    # generate rename, cast, divide, multiply ops
```

**Why not convert SourceOverrides to ComputedColumns?**

- SourceOverrides and ComputedColumns serve different semantic purposes. Overrides are
  mechanical transformations (fix types, rename columns from source). ComputedColumns are
  business logic (derive new values).
- Keeping them separate makes the generated code clearer: the reader can see "these are
  schema fixes" vs "these are business computations."
- The DuckDB generator already treats them differently. The Ibis generator should too.

### Decision 3: Generated code uses Ibis expr API directly -- no abstractions

**Decision:** Generated code calls Ibis methods directly (`t.mutate()`, `t.cast()`,
`t.rename()`, etc.). No wrapper layer, no abstractions.

**Rationale:**

- Generated code should be readable by anyone who knows Ibis. Adding an abstraction
  layer between the generated code and Ibis would mean users need to learn two APIs.
- The fyrnheim vision says "Pydantic + Ibis" -- Ibis *is* the abstraction layer. It
  already handles backend portability. There is no benefit to wrapping it further.
- The current timo-data-stack generator does this correctly: `.mutate()`, `.filter()`,
  `.select()`, `.cast()` are all direct Ibis API calls.
- If we later want to add instrumentation or logging to transformation steps, the execution
  engine (E004) can wrap calls externally. The generated code itself stays clean.

**Implication for testing:**

- Tests can create a DuckDB in-memory table, pass it to the generated function, and
  verify the output schema/data. No mocking needed for the Ibis layer.

### Decision 4: Concrete prep layer function output

**Decision:** A generated prep function for an entity with source overrides and computed
columns looks like this.

**Example entity definition:**

```python
entity = Entity(
    name="transactions",
    source=BigQuerySource(
        project="warehouse",
        dataset="stripe",
        table="charges",
        overrides=SourceOverrides(
            renames=[Rename(from_name="id", to_name="transaction_id")],
            type_casts=[TypeCast(field="created_at", target_type="timestamp")],
            divides=[Divide(field="subtotal", divisor=100.0, suffix="_amount")],
        ),
    ),
    layers=LayersConfig(
        prep=PrepLayer(
            model_name="prep_transactions",
            computed_columns=[
                ComputedColumn(
                    name="customer_email_hash",
                    expression='t.customer_email.lower().strip().hash().cast("string")',
                    description="Hashed email for identity resolution",
                ),
            ],
        ),
        ...
    ),
)
```

**Generated output:**

```python
def prep_transactions(source_transactions: ibis.Table) -> ibis.Table:
    """Prep layer transformations for transactions."""
    t = source_transactions

    # Renames
    t = t.rename(transaction_id="id")

    # Type casts
    t = t.mutate(created_at=t.created_at.cast("timestamp"))

    # Derived columns (divide/multiply)
    t = t.mutate(subtotal_amount=(t.subtotal / 100.0).cast("decimal"))

    # Computed columns
    prep = t.mutate(
        # Hashed email for identity resolution
        customer_email_hash=t.customer_email.lower().strip().hash().cast("string"),
    )
    return prep
```

**When there are no overrides or computed columns:**

```python
def prep_transactions(source_transactions: ibis.Table) -> ibis.Table:
    """Prep layer transformations for transactions."""
    t = source_transactions
    return t
```

**Key details:**

- Each override category gets its own `t = t.mutate(...)` or `t = t.rename(...)` line.
  This is clearer than bundling everything into one giant mutate call.
- Computed columns get a single `.mutate()` call with all columns as kwargs (matching
  the current generator behavior).
- Comments from `description` fields are preserved inline.
- The function signature uses the entity name in both the function name and parameter
  name, preserving the chain: `source_{name} -> prep_{name} -> dim_{name}`.

### Decision 5: Concrete dimension layer function output

**Decision:** A generated dimension function looks the same as the current generator
produces, with one refinement: if the entity has `core_computed` columns on the Entity
itself (not just on the layer), those are included too.

**Example entity definition:**

```python
entity = Entity(
    name="subscriptions",
    core_computed=[
        ComputedColumn(
            name="customer_email_hash",
            expression='t.user_email.lower().strip().hash().cast("string")',
            description="Hashed email for identity resolution",
        ),
        ComputedColumn(
            name="is_active",
            expression='t.status.isin(["active", "on_trial"])',
            description="Active subscription flag",
        ),
        ComputedColumn(
            name="is_churned",
            expression='t.status.isin(["cancelled", "expired", "unpaid"])',
            description="Churned subscription flag",
        ),
    ],
    layers=LayersConfig(
        dimension=DimensionLayer(
            model_name="dim_subscriptions",
            computed_columns=[
                ComputedColumn(
                    name="days_active",
                    expression='ibis.now().date().delta(t.created_at.cast("date"), unit="day")',
                    description="Days since subscription created",
                ),
            ],
        ),
    ),
)
```

**Generated output:**

```python
def dim_subscriptions(prep_subscriptions: ibis.Table) -> ibis.Table:
    """Dimension layer for subscriptions."""
    t = prep_subscriptions

    # Apply dimension computed columns
    dim = t.mutate(
        # Hashed email for identity resolution
        customer_email_hash=t.user_email.lower().strip().hash().cast("string"),
        # Active subscription flag
        is_active=t.status.isin(["active", "on_trial"]),
        # Churned subscription flag
        is_churned=t.status.isin(["cancelled", "expired", "unpaid"]),
        # Days since subscription created
        days_active=ibis.now().date().delta(t.created_at.cast("date"), unit="day"),
    )
    return dim
```

**Key details:**

- The dimension function takes `prep_{name}` as input. If there is no prep layer, it
  takes `source_{name}` instead. The generator checks `self.entity.layers.prep` to
  determine the input parameter name.
- `core_computed` columns from the Entity are merged with `layer.computed_columns`.
  Entity-level `core_computed` come first (they are the entity's canonical computations),
  then layer-specific computed columns. No deduplication needed -- if both define the
  same column name, it is a user error (Pydantic validation can catch this in a future story).
- The current generator already does this merge in its `generate_module()` method
  (the entity's `all_computed_columns` property). Fyrnheim should replicate this.
- When no computed columns exist at all, the function body is just `return t`.

**When dimension layer has no prep predecessor:**

```python
def dim_leads(source_leads: ibis.Table) -> ibis.Table:
    """Dimension layer for leads."""
    t = source_leads

    # Apply dimension computed columns
    dim = t.mutate(
        # Hashed email for identity resolution
        email_hash=t.email.lower().strip().hash().cast("string"),
        # Active lead flag
        is_active=t.status == 'active',
    )
    return dim
```

---

## Implementation Details

### _bind_expression() -- keep exactly as-is

The existing `_bind_expression()` method handles expression binding correctly:

```python
def _bind_expression(self, expr: str) -> str:
    """Bind column references to table 't'."""
    # Already has t. references -> pass through
    if "t." in expr:
        return expr
    # Starts with ibis. or ( -> pass through
    if expr.startswith("ibis.") or expr.startswith("("):
        return expr
    # Otherwise prefix with t.
    return f"t.{expr}"
```

This handles all current expression patterns:
- `hash_email("email")` resolves to `t.email.lower()...` (already has `t.`)
- `ibis.cases(...)` starts with `ibis.` (pass through)
- `email.lower()` gets prefixed to `t.email.lower()`
- `(t.x + t.y)` starts with `(` (pass through)

### New method: _generate_source_overrides()

```python
def _generate_source_overrides(self) -> str:
    """Generate Ibis code for SourceOverrides (renames, casts, divides, multiplies)."""
    source = self.entity.source
    overrides = getattr(source, 'overrides', None)
    if not overrides:
        return ""

    lines = []

    # 1. Renames
    if overrides.renames:
        rename_args = ", ".join(
            f'{r.to_name}="{r.from_name}"' for r in overrides.renames
        )
        lines.append(f"    t = t.rename({rename_args})")

    # 2. Type casts
    if overrides.type_casts:
        for tc in overrides.type_casts:
            ibis_type = self._map_ibis_type(tc.target_type)
            lines.append(f'    t = t.mutate({tc.field}=t.{tc.field}.cast("{ibis_type}"))')

    # 3. Divides
    if overrides.divides:
        for d in overrides.divides:
            new_name = f"{d.field}{d.suffix}"
            ibis_type = self._map_ibis_type(d.target_type)
            lines.append(
                f'    t = t.mutate({new_name}=(t.{d.field} / {d.divisor}).cast("{ibis_type}"))'
            )

    # 4. Multiplies
    if overrides.multiplies:
        for m in overrides.multiplies:
            new_name = f"{m.field}{m.suffix}"
            ibis_type = self._map_ibis_type(m.target_type)
            lines.append(
                f'    t = t.mutate({new_name}=(t.{m.field} * {m.multiplier}).cast("{ibis_type}"))'
            )

    return "\n".join(lines) + "\n" if lines else ""
```

### New method: _map_ibis_type()

```python
IBIS_TYPE_MAP = {
    "timestamp": "timestamp",
    "timestamptz": "timestamp",
    "boolean": "boolean",
    "integer": "int64",
    "bigint": "int64",
    "int64": "int64",
    "float": "float64",
    "double": "float64",
    "float64": "float64",
    "decimal": "decimal",
    "numeric": "decimal",
    "string": "string",
    "text": "string",
    "date": "date",
}

def _map_ibis_type(self, type_str: str) -> str:
    """Map generic type names to Ibis type strings."""
    return IBIS_TYPE_MAP.get(type_str.lower(), type_str.lower())
```

### Updated _generate_prep_function()

```python
def _generate_prep_function(self) -> str:
    """Generate prep layer transformation function."""
    layer = self.entity.layers.prep
    computed_cols = layer.computed_columns

    func = f'''
def prep_{self.entity_name}(source_{self.entity_name}: ibis.Table) -> ibis.Table:
    """Prep layer transformations for {self.entity_name}."""
    t = source_{self.entity_name}
'''

    # Source overrides (renames, casts, divides, multiplies)
    overrides_code = self._generate_source_overrides()
    if overrides_code:
        func += "\n" + overrides_code

    # Computed columns
    if computed_cols:
        func += "\n    # Computed columns\n"
        func += "    prep = t.mutate(\n"
        for i, col in enumerate(computed_cols):
            comma = "," if i < len(computed_cols) - 1 else ""
            expr = self._bind_expression(col.expression)
            if col.description:
                func += f"        # {col.description}\n"
            func += f"        {col.name}={expr}{comma}\n"
        func += "    )\n"
        func += "    return prep\n"
    else:
        func += "    return t\n"

    return func
```

### Updated _generate_dimension_function()

```python
def _generate_dimension_function(self) -> str:
    """Generate dimension layer function."""
    layer = self.entity.layers.dimension

    # Merge core_computed + layer computed columns
    core_computed = getattr(self.entity, 'core_computed', []) or []
    layer_computed = layer.computed_columns or []
    computed_cols = core_computed + layer_computed

    # Determine input parameter name
    if self.entity.layers.prep:
        input_name = f"prep_{self.entity_name}"
    else:
        input_name = f"source_{self.entity_name}"

    func = f'''
def dim_{self.entity_name}({input_name}: ibis.Table) -> ibis.Table:
    """Dimension layer for {self.entity_name}."""
    t = {input_name}
'''

    if computed_cols:
        func += "\n    # Apply dimension computed columns\n"
        func += "    dim = t.mutate(\n"
        for i, col in enumerate(computed_cols):
            comma = "," if i < len(computed_cols) - 1 else ""
            expr = self._bind_expression(col.expression)
            if col.description:
                func += f"        # {col.description}\n"
            func += f"        {col.name}={expr}{comma}\n"
        func += "    )\n"
        func += "    return dim\n"
    else:
        func += "    return t\n"

    return func
```

---

## Acceptance Criteria Mapping

| Acceptance Criterion | How Satisfied |
| --- | --- |
| PrepLayer generation produces Ibis rename, type cast, and computed column operations | `_generate_prep_function()` generates `.rename()`, `.cast()`, `.mutate()` for overrides, then `.mutate()` for computed columns |
| DimensionLayer generation produces Ibis select with computed columns appended | `_generate_dimension_function()` generates `.mutate()` with merged core + layer computed columns |
| Generated code for entity with PrepLayer + DimensionLayer is syntactically valid (ast.parse) | All generated code is valid Python Ibis expressions; test with `ast.parse(generated_code)` |
| Generated code contains correct function signatures referencing entity name | Signatures follow pattern `def prep_{name}(source_{name}: ibis.Table) -> ibis.Table:` |

---

## Testing Approach

### Test 1: Prep with source overrides

```python
def test_prep_generates_rename_cast_divide():
    """PrepLayer generation produces Ibis rename, type cast, and divide operations."""
    entity = Entity(
        name="transactions",
        source=BigQuerySource(
            project="p", dataset="d", table="t",
            overrides=SourceOverrides(
                renames=[Rename(from_name="id", to_name="transaction_id")],
                type_casts=[TypeCast(field="created_at", target_type="timestamp")],
                divides=[Divide(field="subtotal", divisor=100.0)],
            ),
        ),
        layers=LayersConfig(
            prep=PrepLayer(model_name="prep_transactions"),
        ),
    )
    gen = IbisCodeGenerator(entity)
    code = gen._generate_prep_function()

    assert 't.rename(transaction_id="id")' in code
    assert 't.created_at.cast("timestamp")' in code
    assert '(t.subtotal / 100.0)' in code
```

### Test 2: Prep with computed columns

```python
def test_prep_generates_computed_columns():
    """PrepLayer with computed columns produces mutate call."""
    entity = Entity(
        name="leads",
        layers=LayersConfig(
            prep=PrepLayer(
                model_name="prep_leads",
                computed_columns=[
                    ComputedColumn(name="email_hash", expression="email.lower().hash()"),
                ],
            ),
        ),
    )
    gen = IbisCodeGenerator(entity)
    code = gen._generate_prep_function()

    assert "t.mutate(" in code
    assert "email_hash=t.email.lower().hash()" in code
```

### Test 3: Dimension with core + layer computed columns

```python
def test_dimension_merges_core_and_layer_computed():
    """DimensionLayer merges entity core_computed with layer computed_columns."""
    entity = Entity(
        name="subscriptions",
        core_computed=[
            ComputedColumn(name="is_active", expression='t.status.isin(["active"])'),
        ],
        layers=LayersConfig(
            prep=PrepLayer(model_name="prep_subscriptions"),
            dimension=DimensionLayer(
                model_name="dim_subscriptions",
                computed_columns=[
                    ComputedColumn(name="days_active", expression="ibis.now()"),
                ],
            ),
        ),
    )
    gen = IbisCodeGenerator(entity)
    code = gen._generate_dimension_function()

    assert "is_active=" in code
    assert "days_active=" in code
    assert "prep_subscriptions: ibis.Table" in code
```

### Test 4: Full module is syntactically valid

```python
def test_full_module_ast_parses():
    """Generated code for entity with PrepLayer + DimensionLayer parses."""
    import ast
    entity = make_sample_entity_with_both_layers()
    gen = IbisCodeGenerator(entity)
    code = gen.generate_module()
    ast.parse(code)  # raises SyntaxError if invalid
```

### Test 5: Function signatures reference entity name

```python
def test_function_signatures_use_entity_name():
    """Generated functions use entity name in signatures."""
    entity = Entity(name="leads", ...)
    gen = IbisCodeGenerator(entity)
    code = gen.generate_module()

    assert "def prep_leads(source_leads: ibis.Table)" in code
    assert "def dim_leads(prep_leads: ibis.Table)" in code
```

### Test 6: Dimension without prep predecessor

```python
def test_dimension_without_prep_takes_source():
    """Dimension function takes source_ input when no prep layer exists."""
    entity = Entity(
        name="leads",
        layers=LayersConfig(
            dimension=DimensionLayer(model_name="dim_leads"),
        ),
    )
    gen = IbisCodeGenerator(entity)
    code = gen._generate_dimension_function()

    assert "source_leads: ibis.Table" in code
```

---

## Risks and Open Questions

1. **SourceOverrides availability.** Not all source types have `overrides`. Only
   `BigQuerySource` currently has it. The generator should use `getattr(source, 'overrides', None)`
   defensively. Other source types (UnionSource, DerivedSource, etc.) do not have overrides
   and should produce prep functions without the override section.

2. **core_computed merging.** The current timo-data-stack Entity model has `core_computed`
   as a field. The E002 extraction stories need to confirm this field is preserved in
   fyrnheim's Entity model. If the field name changes, the dimension generator needs to
   track that.

3. **Expression validity.** The `_bind_expression()` heuristic is simple (check for `t.`
   or `ibis.` prefix). It works for all current expressions but could break for edge cases
   like `(amount + tax).cast("float64")` where a parenthesized expression should not get
   prefixed. The current behavior (pass through expressions starting with `(`) handles
   this correctly. No change needed, but worth noting.

4. **Ibis `.rename()` syntax.** Ibis uses `t.rename(new_name="old_name")` where the keyword
   is the new name and the string value is the old name. This is the reverse of what might
   be intuitive. The generator must produce this correctly. The `Rename(from_name, to_name)`
   model maps to `rename(to_name="from_name")`.

5. **Generated code not yet executable against real data.** The generated code from S002
   is validated via `ast.parse()` (syntactic) but not executed against actual Ibis tables.
   Execution testing comes in E004 (execution engine). S002 focuses on code generation
   correctness.

---

## Implementation Plan

### Prerequisites

S001 (generator base + source generation) must be complete. S001 delivers:
- `src/fyrnheim/generators/__init__.py` -- exports `IbisCodeGenerator`, `generate`
- `src/fyrnheim/generators/ibis_code_generator.py` -- class with `__init__`, `_generate_imports`,
  `_generate_source_functions`, `_generate_single_source_functions`, `_bind_expression`,
  `generate_module`, `write_module`

E001-S002 (core types) must be complete. S002 uses:
- `TypeCast`, `Rename`, `Divide`, `Multiply`, `SourceTransforms` from `fyrnheim.core.source`

E002-S001 (layer configs) must be complete. S002 uses:
- `PrepLayer`, `DimensionLayer` from `fyrnheim.core.layer`

E002-S002 (entity) must be complete. S002 uses:
- `Entity` with `core_computed`, `all_computed_columns`, `layers: LayersConfig`

### Naming Reconciliation

The design doc uses timo-data-stack naming in some places. The fyrnheim equivalents per
the upstream design decisions:

| Design doc name | fyrnheim name | Source |
| --- | --- | --- |
| `BigQuerySource` | `TableSource` | E001-S002 renames it |
| `SourceOverrides` | `SourceTransforms` | E001-S002 renames it |
| `source.overrides` | `source.transforms` | Field renamed to match class |

The generator must use `getattr(source, 'transforms', None)` (not `overrides`).

### Step 1: Add `IBIS_TYPE_MAP` constant and `_map_ibis_type` method

**File:** `src/fyrnheim/generators/ibis_code_generator.py`

Add a module-level constant and an instance method:

```python
IBIS_TYPE_MAP: dict[str, str] = {
    "timestamp": "timestamp",
    "timestamptz": "timestamp",
    "boolean": "boolean",
    "integer": "int64",
    "bigint": "int64",
    "int64": "int64",
    "float": "float64",
    "double": "float64",
    "float64": "float64",
    "decimal": "decimal",
    "numeric": "decimal",
    "string": "string",
    "text": "string",
    "date": "date",
}
```

The `_map_ibis_type` method looks up from this map with a lowercase fallback:

```python
def _map_ibis_type(self, type_str: str) -> str:
    """Map generic type names to Ibis type strings."""
    return IBIS_TYPE_MAP.get(type_str.lower(), type_str.lower())
```

**Why module-level constant:** The map is static data, shared across all instances. Keeping
it at module level avoids recreating it per instance and makes it testable independently.

### Step 2: Add `_generate_source_transforms` method

**File:** `src/fyrnheim/generators/ibis_code_generator.py`

This is a new private method. It reads `self.entity.source` and generates Ibis code for
each transform category in the correct order: renames, type casts, divides, multiplies.

Key implementation details:

1. **Access pattern:** Use `getattr(source, 'transforms', None)` to safely handle source
   types that do not have a `transforms` field (e.g., `UnionSource`, `DerivedSource`).

2. **Section comments:** Each category gets a heading comment (`# Renames`, `# Type casts`,
   `# Derived columns (divide/multiply)`) for readability.

3. **Rename syntax:** Ibis `.rename()` uses `new_name="old_name"` keyword syntax.
   `Rename(from_name="id", to_name="transaction_id")` becomes
   `t = t.rename(transaction_id="id")`. Multiple renames go in a single `.rename()` call.

4. **Type cast syntax:** Each cast is a separate `.mutate()` call:
   `t = t.mutate(field=t.field.cast("type"))`. Separate calls because each cast is
   independent and the generated code is more readable this way.

5. **Divide/Multiply syntax:** Create a new column named `{field}{suffix}`:
   `t = t.mutate(amount_dollars=(t.amount / 100.0).cast("decimal"))`.
   The `.cast()` uses `_map_ibis_type(d.target_type)`.

6. **Return:** Returns a string of indented code lines (each starting with 4 spaces), or
   empty string if no transforms exist. Lines are joined with `\n`.

### Step 3: Update `_generate_prep_function` method

**File:** `src/fyrnheim/generators/ibis_code_generator.py`

Replace the existing `_generate_prep_function` from S001 (which is either a stub or a
direct copy of the timo-data-stack version that only handles computed columns).

The updated method has three code blocks assembled in order:

1. **Function signature + alias:**
   ```python
   def prep_{name}(source_{name}: ibis.Table) -> ibis.Table:
       """Prep layer transformations for {name}."""
       t = source_{name}
   ```

2. **Source transforms** (from `_generate_source_transforms()`):
   - Only present if `entity.source` has `transforms` with at least one non-empty list.
   - Inserted as a block after the alias line, with a blank line separator.

3. **Computed columns** (from `layer.computed_columns`):
   - If computed columns exist: generate `prep = t.mutate(...)` with all columns as kwargs.
   - Each column passes through `_bind_expression()`.
   - If `col.description` is set, emit a `# description` comment above the kwarg.
   - Return `prep`.
   - If no computed columns and source transforms were present: return `t`.
   - If nothing at all: return `t` (passthrough prep function).

### Step 4: Update `_generate_dimension_function` method

**File:** `src/fyrnheim/generators/ibis_code_generator.py`

Replace the existing `_generate_dimension_function`. Two changes from the timo-data-stack
version:

1. **Input parameter name is conditional:**
   - If `self.entity.layers.prep` is not None: `input_name = f"prep_{self.entity_name}"`
   - Otherwise: `input_name = f"source_{self.entity_name}"`

2. **Merge `core_computed` + layer `computed_columns`:**
   - `core_computed = getattr(self.entity, 'core_computed', []) or []`
   - `layer_computed = layer.computed_columns or []`
   - `computed_cols = core_computed + layer_computed`
   - This matches the Entity model's `all_computed_columns` property from E002-S002.
   - Entity-level `core_computed` come first, then layer-specific columns.

The rest of the method is identical in structure to the existing timo-data-stack version:
single `.mutate()` call with all columns as kwargs, `_bind_expression()` applied, inline
comments from descriptions, `return dim` or `return t`.

### Step 5: Update `generate_module` to wire in prep and dimension

**File:** `src/fyrnheim/generators/ibis_code_generator.py`

The `generate_module()` method from S001 should already have conditional blocks for
`self.entity.layers.prep` and `self.entity.layers.dimension`. Verify that these blocks
call the updated methods. If S001 left stubs (e.g., `pass` or `raise NotImplementedError`),
replace them with actual calls.

Expected structure in `generate_module()`:

```python
if self.entity.layers.prep:
    parts.append(self._generate_prep_function())

if self.entity.layers.dimension:
    parts.append(self._generate_dimension_function())
```

No changes should be needed here if S001 already wired this up. Verify and confirm.

### Step 6: Write tests

**File:** `tests/generators/test_prep_dimension_generation.py`

Create a single test file with the following test functions. Each maps directly to an
acceptance criterion or covers a specific design decision.

**Fixtures needed:**

- `make_entity_with_transforms(name, transforms, computed_columns)` -- helper that builds
  an `Entity` with a `TableSource` that has `SourceTransforms` and a `PrepLayer`.
- `make_entity_with_dimension(name, core_computed, layer_computed, has_prep)` -- helper that
  builds an `Entity` with a `DimensionLayer` and optional `PrepLayer`.

**Test 1: `test_prep_generates_rename_cast_divide`** (AC: PrepLayer generation produces
Ibis rename, type cast, and computed column operations)

Build an entity with:
- `SourceTransforms(renames=[Rename(from_name="id", to_name="transaction_id")], type_casts=[TypeCast(field="created_at", target_type="timestamp")], divides=[Divide(field="subtotal", divisor=100.0)])`
- `PrepLayer(model_name="prep_transactions")`

Assert the generated code contains:
- `t.rename(transaction_id="id")`
- `t.created_at.cast("timestamp")`
- `(t.subtotal / 100.0)`

**Test 2: `test_prep_generates_multiply`**

Build entity with `SourceTransforms(multiplies=[Multiply(field="rate", multiplier=100.0, suffix="_pct")])`.

Assert: `(t.rate * 100.0)` and `rate_pct=` in generated code.

**Test 3: `test_prep_generates_computed_columns`** (AC: PrepLayer generation produces
computed column operations)

Build entity with `PrepLayer(computed_columns=[ComputedColumn(name="email_hash", expression="email.lower().hash()")])`.

Assert:
- `t.mutate(` in code
- `email_hash=t.email.lower().hash()` in code (note `_bind_expression` prefixed `t.`)

**Test 4: `test_prep_with_transforms_and_computed_columns`**

Build entity with both source transforms and computed columns.

Assert:
- Rename appears before cast (order: renames, casts, divides, computed)
- Cast appears before computed columns
- `ast.parse(code)` succeeds

**Test 5: `test_prep_passthrough_when_no_transforms_or_computed`**

Build entity with empty `PrepLayer(model_name="prep_things")`, no source transforms.

Assert:
- Code contains `return t`
- Does not contain `.mutate(` or `.rename(`

**Test 6: `test_dimension_merges_core_and_layer_computed`** (AC: DimensionLayer generation
produces Ibis select with computed columns appended)

Build entity with `core_computed=[ComputedColumn(name="is_active", expression='t.status.isin(["active"])')]` and `DimensionLayer(computed_columns=[ComputedColumn(name="days_active", expression="ibis.now()")])`.

Assert:
- Both `is_active=` and `days_active=` appear in the generated code
- `is_active=` appears before `days_active=` (core first, then layer)

**Test 7: `test_dimension_with_prep_takes_prep_input`** (AC: Generated code contains correct
function signatures referencing entity name)

Build entity with both `PrepLayer` and `DimensionLayer`.

Assert: `prep_subscriptions: ibis.Table` in function signature.

**Test 8: `test_dimension_without_prep_takes_source_input`**

Build entity with `DimensionLayer` only (no `PrepLayer`).

Assert: `source_leads: ibis.Table` in function signature.

**Test 9: `test_dimension_passthrough_when_no_computed`**

Build entity with `DimensionLayer` but no computed columns (neither core nor layer).

Assert: Code contains `return t`.

**Test 10: `test_full_module_ast_parses`** (AC: Generated code is syntactically valid)

Build entity with `TableSource` (with transforms), `PrepLayer` (with computed columns),
and `DimensionLayer` (with core + layer computed columns).

Call `gen.generate_module()` and run `ast.parse(code)`. Assert no `SyntaxError`.

**Test 11: `test_function_signatures_use_entity_name`** (AC: correct function signatures)

Build entity named `"leads"` with both layers.

Assert:
- `def prep_leads(source_leads: ibis.Table) -> ibis.Table:` in code
- `def dim_leads(prep_leads: ibis.Table) -> ibis.Table:` in code

**Test 12: `test_bind_expression_passthrough_patterns`**

Test `_bind_expression` directly (carried over from S001, but verify it works for S002
expression patterns):
- `t.status == 'active'` -> unchanged (has `t.`)
- `ibis.cases(...)` -> unchanged (starts with `ibis.`)
- `(t.x + t.y)` -> unchanged (starts with `(`)
- `email.lower().hash()` -> `t.email.lower().hash()` (bare column, gets prefix)

**Test 13: `test_computed_column_description_as_comment`**

Build entity with `ComputedColumn(name="x", expression="t.y", description="My description")`.

Assert: `# My description` appears on the line above `x=t.y` in generated code.

### Step 7: Verify and finalize

After all code is written:

1. Run `ast.parse()` on the test entity outputs to confirm syntactic validity.
2. Run the test suite: `uv run pytest tests/generators/test_prep_dimension_generation.py -v`.
3. Verify no import cycles between `fyrnheim.generators` and `fyrnheim.core`.
4. Verify `generate_module()` produces a complete, well-structured Python file when given
   an entity with source + prep + dimension layers.

### Files Changed

| File | Action | Description |
| --- | --- | --- |
| `src/fyrnheim/generators/ibis_code_generator.py` | Modify | Add `IBIS_TYPE_MAP`, `_map_ibis_type`, `_generate_source_transforms`; update `_generate_prep_function`, `_generate_dimension_function` |
| `tests/generators/test_prep_dimension_generation.py` | Create | 13 test functions covering all acceptance criteria |

### Acceptance Criteria Checklist

| # | Criterion | Satisfied By |
| --- | --- | --- |
| 1 | PrepLayer generation produces Ibis rename, type cast, and computed column operations | Steps 2-3, Tests 1-4 |
| 2 | DimensionLayer generation produces Ibis select with computed columns appended | Step 4, Tests 6-9 |
| 3 | Generated code for entity with PrepLayer + DimensionLayer is syntactically valid (ast.parse) | Step 5, Test 10 |
| 4 | Generated code contains correct function signatures referencing entity name | Steps 3-4, Tests 7-8, 11 |
