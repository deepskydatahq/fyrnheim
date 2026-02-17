# Design: Public `generate()` Function (M001-E003-S004)

**Story:** M001-E003-S004 -- Create public generate() function that writes transformation files
**Date:** 2026-02-17
**Status:** Plan

---

## Context

This story is the public API entry point for typedata's code generation pipeline. It sits on top of:

- **S001** -- `IbisCodeGenerator` base class (constructor, imports, source function generation)
- **S002** -- PrepLayer and DimensionLayer generation methods
- **S003** -- SnapshotLayer (SCD Type 2) generation

The reference implementation is `PydanticIbisGenerator.write_module()` and `generate_entities()` in `timo-data-stack/scripts/generate_pydantic_entities.py` (lines 1074-1160). That implementation has two levels: an instance method `write_module(output_dir)` that writes a single entity, and a standalone `generate_entities(entity_names, output_dir)` that loops over names, loads entities, and calls `write_module`.

Downstream, **M001-E004-S003** (`typedata.run()`) will call `generate()` as part of the full pipeline: discover entities, generate code, execute on backend, run quality checks.

---

## Decision 1: Public API Shape

### Options Considered

**A. Single-entity only:** `typedata.generate(entity, output_dir)`
**B. Batch only:** `typedata.generate_all(entities_dir, output_dir)`
**C. Both:** `typedata.generate(entity, output_dir)` + `typedata.generate_all(entities_dir, output_dir)`

### Decision: Option C -- provide both

Rationale:

- `generate(entity, output_dir)` is the composable primitive. It takes an already-loaded `Entity` instance, so the caller controls how entities are discovered and loaded. This is the testable, inspectable unit.
- `generate_all(entities_dir, output_dir)` is the convenience function for the common case: "I have a directory of entity definitions, generate all of them." It internally discovers entities (using the registry from E004-S001), loads each one, and calls `generate()` per entity.
- `run()` (E004-S003) will call `generate()` internally, not `generate_all()`, because `run()` also handles execution and quality checks per entity.

### Signatures

```python
def generate(
    entity: Entity,
    output_dir: str | Path = "generated",
) -> GenerateResult:
    """Generate Ibis transformation code for a single entity.

    Args:
        entity: Pydantic Entity instance defining the business object.
        output_dir: Directory to write the generated module into.
            Created if it does not exist.

    Returns:
        GenerateResult with the generated code and output file path.
    """

def generate_all(
    entities_dir: str | Path,
    output_dir: str | Path = "generated",
) -> list[GenerateResult]:
    """Discover and generate Ibis transformation code for all entities in a directory.

    Args:
        entities_dir: Directory containing entity definition Python files.
        output_dir: Directory to write generated modules into.

    Returns:
        List of GenerateResult, one per entity processed.
    """
```

Both are exported from the top-level `typedata` namespace:

```python
import typedata
result = typedata.generate(my_entity, "generated/")
results = typedata.generate_all("entities/", "generated/")
```

---

## Decision 2: Return Value -- String + Write

### Decision: Return a `GenerateResult` dataclass that contains the code AND writes to disk

The function does both: it writes the file (the primary side effect users want) and returns a result object that includes the generated source code as a string. This makes testing straightforward without needing to read files back from disk.

```python
from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class GenerateResult:
    """Result of generating transformation code for one entity."""
    entity_name: str
    code: str
    output_path: Path
    written: bool  # False if dry_run=True
```

For testing and CI inspection, add a `dry_run` parameter:

```python
def generate(
    entity: Entity,
    output_dir: str | Path = "generated",
    dry_run: bool = False,
) -> GenerateResult:
```

When `dry_run=True`, the function generates the code and returns it in `GenerateResult.code` but does not write to disk. `GenerateResult.written` is `False` and `output_path` is set to the path that *would* have been written.

This avoids the need for a separate `generate_to_string()` function while keeping the common path (write to disk) as the default.

---

## Decision 3: Overwrite Policy

### Options Considered

**A. Always overwrite** -- generated files are derived artifacts, never hand-edited.
**B. Check timestamps** -- skip if generated file is newer than entity definition.
**C. Prompt the user** -- ask before overwriting.
**D. Configurable** -- default to always overwrite, with an option to skip if unchanged.

### Decision: Always overwrite, with content-comparison skip

Rationale:

- Generated files are fully derived from entity definitions. They should never be hand-edited (see Decision 5 about auto-generated headers). Overwriting is always safe.
- Timestamp comparison is fragile (requires tracking the entity source file path, which is not always available when `generate()` receives an already-loaded `Entity` instance).
- Prompting is not appropriate for a library function.
- However, we do want to avoid unnecessary file writes that churn `git diff` when nothing has changed. So: compare the new code against the existing file content. If identical, skip the write and note it in the result.

```python
# Inside generate():
if output_path.exists() and output_path.read_text() == code:
    return GenerateResult(
        entity_name=entity.name,
        code=code,
        output_path=output_path,
        written=False,  # skipped, content unchanged
    )
```

This gives the user a clean signal: `result.written == True` means the file was actually modified on disk.

---

## Decision 4: File Naming Convention

### Decision: `{entity_name}_transforms.py`

This matches the existing timo-data-stack convention (`write_module` in the reference writes to `{entity_name}_transforms.py`). Consistency with the predecessor reduces migration friction.

The output directory defaults to `generated/` relative to the caller's working directory:

```
generated/
    newsletter_subscribers_transforms.py
    subscriptions_transforms.py
    customers_transforms.py
```

No `__init__.py` is generated. The generated modules are standalone and imported by path or by adding `generated/` to `sys.path`. The `run()` function (E004-S003) will handle the import mechanics.

We explicitly avoid:

- Nesting by layer (e.g., `generated/prep/`, `generated/dimension/`) -- a single file per entity is simpler and matches how dbt models work (one file = one model).
- Including layer names in the filename (e.g., `customers_prep.py`, `customers_dimension.py`) -- the entity file contains all layers for that entity, keeping the 1:1 mapping between entity definition and generated output.

---

## Decision 5: Auto-Generated Header

### Decision: Yes, include a header warning

Every generated file starts with a docstring header identifying it as auto-generated, referencing the entity it was derived from, and warning against manual edits:

```python
"""
{entity_name} entity transformations.

Auto-generated by typedata.generate() -- DO NOT EDIT.
Source entity: {entity_name}
Generated: {iso_timestamp}
"""

import ibis
```

Design details:

- The header is a module docstring (triple-quoted string), not a comment. This makes it accessible via `module.__doc__` and visible in IDE tooltips.
- The timestamp uses ISO 8601 format (`2026-02-17T14:30:00Z`). This is informational only -- it is not used for overwrite decisions (see Decision 3).
- The `DO NOT EDIT` wording is a standard convention that tools like `.gitattributes` patterns (`linguist-generated=true`) and editors can key off.
- The content-comparison skip from Decision 3 ignores the timestamp when comparing. To make this work cleanly, the timestamp is excluded from the comparison hash: we compare everything *except* the `Generated:` line when deciding whether to skip a write. Alternatively -- and more simply -- we can omit the timestamp from the generated code entirely and only include the static warning. This avoids the comparison complexity.

**Simplified decision:** Omit the timestamp. The header is:

```python
"""
{entity_name} entity transformations.

Auto-generated by typedata.generate() -- DO NOT EDIT.
Source entity: {entity_name}
"""
```

This makes content comparison trivial (byte-for-byte equality) and avoids unnecessary diffs from timestamp changes.

---

## Summary of Decisions

| # | Question | Decision |
|---|----------|----------|
| 1 | API shape | Both `generate(entity, output_dir)` and `generate_all(entities_dir, output_dir)` |
| 2 | Return value | `GenerateResult` dataclass with `.code` string; `dry_run=True` skips writing |
| 3 | Overwrite policy | Always overwrite, but skip write if content is identical |
| 4 | File naming | `{entity_name}_transforms.py` in a flat `generated/` directory |
| 5 | Auto-generated header | Module docstring with `DO NOT EDIT` warning, no timestamp |

---

## Implementation Sketch

```
src/typedata/
    __init__.py          # exports: generate, generate_all
    generators/
        __init__.py      # exports: IbisCodeGenerator
        ibis_code.py     # IbisCodeGenerator class (S001-S003)
    generate.py          # generate(), generate_all(), GenerateResult
```

The `generate.py` module is thin -- it creates an `IbisCodeGenerator`, calls `generate_module()`, and writes the result:

```python
# src/typedata/generate.py

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from typedata.core import Entity
from typedata.generators import IbisCodeGenerator


@dataclass(frozen=True)
class GenerateResult:
    entity_name: str
    code: str
    output_path: Path
    written: bool


def generate(
    entity: Entity,
    output_dir: str | Path = "generated",
    dry_run: bool = False,
) -> GenerateResult:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{entity.name}_transforms.py"

    generator = IbisCodeGenerator(entity)
    code = generator.generate_module()

    if dry_run:
        return GenerateResult(
            entity_name=entity.name,
            code=code,
            output_path=output_path,
            written=False,
        )

    # Skip write if content unchanged
    if output_path.exists() and output_path.read_text() == code:
        return GenerateResult(
            entity_name=entity.name,
            code=code,
            output_path=output_path,
            written=False,
        )

    output_path.write_text(code)
    return GenerateResult(
        entity_name=entity.name,
        code=code,
        output_path=output_path,
        written=True,
    )
```

---

## Acceptance Criteria Mapping

| AC from Story | Covered By |
|---------------|------------|
| `generate(entity, output_dir)` writes `{entity_name}_transforms.py` | Decision 1 + Decision 4 |
| Generated file contains `transform_{entity_name}()` function | Handled by `IbisCodeGenerator.generate_module()` (S001-S003) |
| Generated file has correct imports (ibis, os) | Handled by `IbisCodeGenerator.generate_imports()` (S001) |
| Generated file importable as Python module | Auto-generated header is valid Python docstring; code from S001-S003 is valid |
| Full round trip: define entity, generate, import, check functions | Integration test using `dry_run=False` + `importlib` |

---

## Implementation Plan

### Summary

Create `src/typedata/generate.py` containing the `GenerateResult` frozen dataclass, `generate()` single-entity function, and `generate_all()` batch convenience function. The `generate()` function instantiates `IbisCodeGenerator`, calls `generate_module()` to produce code with a static header (no timestamp), writes to `{entity_name}_transforms.py`, and returns a `GenerateResult`. It supports `dry_run=True` to skip disk writes and skips writing when the existing file content is byte-identical. `generate_all()` discovers entities via `EntityRegistry`, then calls `generate()` per entity. Both functions are re-exported from `typedata.__init__`.

### Acceptance Criteria

- [ ] `src/typedata/generate.py` exists and contains `GenerateResult`, `generate()`, and `generate_all()`
- [ ] `from typedata import generate, generate_all, GenerateResult` succeeds without error
- [ ] `generate(entity, output_dir)` writes `{output_dir}/{entity.name}_transforms.py` and returns `GenerateResult` with `written=True`, `code` containing the module source, and `output_path` pointing to the written file
- [ ] Generated file starts with a module docstring header: entity name, "Auto-generated by typedata.generate() -- DO NOT EDIT.", source entity name, and NO timestamp line
- [ ] Generated file contains `import ibis` and `import os`
- [ ] Generated file contains layer functions matching entity config (e.g., `def source_{name}(...)`, `def prep_{name}(...)`, `def dim_{name}(...)`, `def snapshot_{name}(...)`) as produced by `IbisCodeGenerator.generate_module()`
- [ ] `generate(entity, output_dir, dry_run=True)` returns `GenerateResult` with `written=False` and `code` populated, but does NOT create a file on disk
- [ ] When the output file already exists with identical content, `generate()` returns `GenerateResult` with `written=False` and does NOT rewrite the file (mtime unchanged)
- [ ] When the output file exists with different content, `generate()` overwrites it and returns `written=True`
- [ ] `generate()` creates `output_dir` (including parents) if it does not exist
- [ ] `generate_all(entities_dir, output_dir)` discovers all entities in `entities_dir`, calls `generate()` for each, and returns `list[GenerateResult]`
- [ ] Generated file can be imported via `importlib.import_module()` / `importlib.util.spec_from_file_location()` without `SyntaxError` or `ImportError` (assuming `ibis` is installed)
- [ ] Full round trip test: define an Entity, call `generate()`, dynamically import the generated module, verify the module has the expected function names
- [ ] `pytest tests/test_generate.py` exits with code 0

### Implementation Tasks

1. **Create `src/typedata/generate.py`**
   - File: `src/typedata/generate.py`
   - Define `GenerateResult` as a `@dataclass(frozen=True)` with fields: `entity_name: str`, `code: str`, `output_path: Path`, `written: bool`
   - Implement `generate(entity: Entity, output_dir: str | Path = "generated", dry_run: bool = False) -> GenerateResult`:
     - Convert `output_dir` to `Path`
     - Create output dir with `parents=True, exist_ok=True`
     - Compute `output_path = output_dir / f"{entity.name}_transforms.py"`
     - Instantiate `IbisCodeGenerator(entity)` and call `generator.generate_module()` to get `code`
     - Prepend the static header docstring (no timestamp):
       ```
       """
       {entity.name} entity transformations.

       Auto-generated by typedata.generate() -- DO NOT EDIT.
       Source entity: {entity.name}
       """
       ```
       Note: Check whether `IbisCodeGenerator.generate_module()` already includes the header via `_generate_imports()`. The S001 design shows `_generate_imports()` produces the header docstring. If `generate_module()` already includes the header, do NOT prepend a second one -- just use the code as-is. The key requirement is that the header has no timestamp.
     - If `dry_run=True`: return `GenerateResult(entity_name=entity.name, code=code, output_path=output_path, written=False)`
     - If `output_path.exists() and output_path.read_text() == code`: return with `written=False` (skip-if-identical)
     - Otherwise: `output_path.write_text(code)` and return with `written=True`
   - Implement `generate_all(entities_dir: str | Path, output_dir: str | Path = "generated") -> list[GenerateResult]`:
     - Import `EntityRegistry` from `typedata.engine`
     - Create registry, call `registry.discover(entities_dir)`
     - Iterate over `registry.items()`, call `generate(info.entity, output_dir)` for each
     - Collect and return the list of `GenerateResult`

2. **Update `IbisCodeGenerator._generate_imports()` to omit timestamp**
   - File: `src/typedata/generators/ibis_code_generator.py` (or wherever S001-S003 placed it)
   - Verify the header docstring produced by `_generate_imports()` does NOT include a `Generated: {timestamp}` line
   - If the reference implementation includes a timestamp, remove it so the header is static:
     ```python
     def _generate_imports(self) -> str:
         return f'"""\n{self.entity_name} entity transformations.\n\nAuto-generated by typedata.generate() -- DO NOT EDIT.\nSource entity: {self.entity_name}\n"""\n\nimport ibis\nimport os\n'
     ```
   - The static header ensures byte-for-byte comparison works for skip-if-identical without needing to strip timestamp lines

3. **Export from `typedata.generators.__init__`**
   - File: `src/typedata/generators/__init__.py`
   - Ensure `IbisCodeGenerator` is exported (should already be done by S001)

4. **Export `generate`, `generate_all`, `GenerateResult` from `typedata.__init__`**
   - File: `src/typedata/__init__.py`
   - Add imports:
     ```python
     from typedata.generate import generate, generate_all, GenerateResult
     ```
   - Add `"generate"`, `"generate_all"`, `"GenerateResult"` to `__all__`
   - Note: `generate.py` imports from `typedata.generators` which imports `ibis`. This means the top-level `typedata` import will pull in `ibis`. Per the E002-S004 design (Decision 5), engine/generator imports that require `ibis` should NOT be eagerly loaded from the top-level. Two options:
     - **Option A (simple):** Import eagerly. Accept that `import typedata` now requires `ibis`. This is fine if typedata always requires ibis anyway.
     - **Option B (lazy):** Use `__getattr__` lazy import for `generate`, `generate_all`, `GenerateResult` so they are only loaded on first access. This keeps `import typedata` lightweight for users who only define entities.
   - Decision: Use **Option B** (lazy `__getattr__`) since the E002-S004 design explicitly recommends this pattern for engine/generator symbols. Add to `__getattr__`:
     ```python
     def __getattr__(name):
         if name in ("generate", "generate_all", "GenerateResult"):
             from typedata.generate import generate, generate_all, GenerateResult
             globals()["generate"] = generate
             globals()["generate_all"] = generate_all
             globals()["GenerateResult"] = GenerateResult
             return globals()[name]
         raise AttributeError(f"module 'typedata' has no attribute {name}")
     ```

5. **Create unit tests**
   - File: `tests/test_generate.py`
   - Fixtures:
     - `sample_entity`: A minimal `Entity` instance with prep and dimension layers, enough to exercise `IbisCodeGenerator.generate_module()`
     - `tmp_output_dir`: A `tmp_path` based output directory
   - Tests:
     - `test_generate_writes_file`: Call `generate(entity, tmp_dir)`, assert file exists at `{tmp_dir}/{entity.name}_transforms.py`, assert `result.written is True`, assert `result.code` matches file contents
     - `test_generate_returns_correct_entity_name`: Assert `result.entity_name == entity.name`
     - `test_generate_output_path`: Assert `result.output_path == tmp_dir / f"{entity.name}_transforms.py"`
     - `test_generate_creates_output_dir`: Pass a non-existent nested dir, assert it is created
     - `test_generate_header_no_timestamp`: Assert `"Generated:" not in result.code`, assert `"Auto-generated by typedata.generate() -- DO NOT EDIT." in result.code`
     - `test_generate_has_imports`: Assert `"import ibis" in result.code`
     - `test_generate_has_layer_functions`: Assert `f"def source_{entity.name}" in result.code` or `f"def prep_{entity.name}" in result.code` (depending on entity config)
     - `test_generate_dry_run_no_file`: Call with `dry_run=True`, assert file does NOT exist, assert `result.written is False`, assert `result.code` is non-empty
     - `test_generate_skip_identical`: Call `generate()` twice, assert second call returns `written=False`, assert file mtime is unchanged (use `os.path.getmtime()`)
     - `test_generate_overwrites_different`: Write a dummy file first, call `generate()`, assert `result.written is True`, assert file content matches `result.code`
     - `test_generate_ast_parses`: `import ast; ast.parse(result.code)` does not raise
     - `test_generate_importable`: Write file, use `importlib.util.spec_from_file_location` to import, assert module has expected function attributes

6. **Create integration test for `generate_all`**
   - File: `tests/test_generate.py` (same file, or `tests/test_generate_all.py`)
   - This test depends on `EntityRegistry` (E004-S001). If E004-S001 is not yet implemented:
     - Write the test but mark it `@pytest.mark.skip(reason="depends on E004-S001 EntityRegistry")`
     - Or: write a simpler version that manually creates entity `.py` files in a tmp dir and tests that `generate_all()` processes them
   - Tests:
     - `test_generate_all_discovers_and_generates`: Create a tmp dir with 2 entity definition files, call `generate_all(entities_dir, output_dir)`, assert 2 `GenerateResult` objects returned, assert 2 output files exist

7. **Create round-trip integration test**
   - File: `tests/test_generate.py`
   - `test_full_round_trip`: Define entity -> `generate()` -> `importlib` import -> assert module has `source_{name}` and/or `prep_{name}` and/or `dim_{name}` functions
   - This is the key integration test from the story acceptance criteria

### Test Plan

- [ ] Run `pytest tests/test_generate.py -v` -- all tests pass, exit code 0
- [ ] Verify AC: `generate(entity, output_dir)` writes file -- `test_generate_writes_file`
- [ ] Verify AC: Generated file contains layer functions -- `test_generate_has_layer_functions`
- [ ] Verify AC: Generated file has correct imports -- `test_generate_has_imports`
- [ ] Verify AC: Generated file importable -- `test_generate_importable`
- [ ] Verify AC: Full round trip -- `test_full_round_trip`
- [ ] Verify AC: Header has no timestamp -- `test_generate_header_no_timestamp`
- [ ] Verify AC: dry_run skips write -- `test_generate_dry_run_no_file`
- [ ] Verify AC: skip-if-identical -- `test_generate_skip_identical`
- [ ] Verify AC: overwrites when different -- `test_generate_overwrites_different`
- [ ] Verify AC: output dir created -- `test_generate_creates_output_dir`
- [ ] Verify AC: `generate_all` works -- `test_generate_all_discovers_and_generates` (may be skipped if E004-S001 not ready)
- [ ] Run `python -c "from typedata import generate, generate_all, GenerateResult"` -- exits 0

### Dependencies and Sequencing

This story depends on:
- **M001-E003-S002** (PrepLayer + DimensionLayer generation) -- provides `_generate_prep_function()` and `_generate_dimension_function()` in `IbisCodeGenerator`
- **M001-E003-S003** (SnapshotLayer generation) -- provides `_generate_snapshot_function()` in `IbisCodeGenerator`
- **M001-E002-S004** (public API exports) -- provides `Entity` importable from `typedata.core`

For `generate_all()`, additionally depends on:
- **M001-E004-S001** (entity discovery) -- provides `EntityRegistry` for directory scanning

If E004-S001 is not yet implemented when this story is picked up, `generate_all()` can be stubbed with a `TODO` or implemented with a simpler inline discovery mechanism, and the full integration test deferred.

### Risks

1. **S001-S003 not yet implemented.** The `IbisCodeGenerator` class and its `generate_module()` method are prerequisites. If they do not exist yet, this story cannot produce working code. Mitigation: the story TOML explicitly lists S002 and S003 as dependencies.
2. **Header ownership ambiguity.** The S001 design shows `_generate_imports()` producing the header docstring. The S004 design also describes the header. Need to decide: does the header live in `_generate_imports()` (generator concern) or in `generate()` (public API concern)? Recommendation: keep it in `_generate_imports()` since the generator already produces it; `generate()` just passes through the code from `generate_module()`. Verify this during implementation.
3. **`ibis` import at package level.** If `generate` is eagerly imported in `typedata/__init__.py`, it forces `ibis` as a required dependency at import time. Use lazy `__getattr__` to avoid this.

---

*Implementation plan created 2026-02-17*
