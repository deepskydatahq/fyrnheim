# Design: M001-E004-S003 -- Create public run() function orchestrating the full pipeline

**Date:** 2026-02-17
**Story:** M001-E004-S003
**Status:** ready
**Depends on:** M001-E004-S001 (EntityRegistry, discovery, dependency resolution), M001-E004-S002 (DuckDB executor)

---

## 1. Summary

A single public function `fyrnheim.run()` orchestrates the full pipeline: discover entities from a directory, resolve dependency order, auto-generate transformation code if missing, execute each entity on a DuckDB backend, run quality checks, and return a structured summary. This is the top-level entry point that makes the entire framework usable with one call.

---

## 2. Reference Analysis: timo-data-stack `run_transformations.py`

The existing orchestration script (695 lines) follows this flow:

```
main()
  -> discover_entities()          # importlib scan of entities/entities/*.py
  -> regenerate_ibis_code()       # subprocess call to generate_pydantic_entities.py
  -> resolve_dependencies()       # Kahn's algorithm topological sort
  -> execute_transformations()    # per-entity: import module -> call transform_fn -> persist
```

Key observations for fyrnheim extraction:

| Aspect | timo-data-stack behavior | fyrnheim decision |
|--------|--------------------------|-------------------|
| Discovery | Hard-coded `entities/entities/` path | Parameterized `entities_dir` |
| Code gen | Subprocess call to external script | Call `fyrnheim.generate()` in-process |
| Backends | BigQuery + DuckDB with env vars | DuckDB-only in v0.1; backend param for future |
| Incremental | High water mark, timestamp columns | Skip for v0.1 (full refresh only) |
| Failure | `raise SystemExit(1)` on any error | Graceful: skip entity, continue, report |
| Output | `click.echo()` with emoji | Structured return + Python logging |
| Identity graphs | DerivedSource special handling | Defer to future story |
| Analytics layer | Post-transform analytics function | Included in generated code; run() executes it |
| Quality checks | Not integrated | Wire in via quality runner |

---

## 3. Design Decisions

### 3.1 Public API Signature

```python
def run(
    entities_dir: str | Path,
    data_dir: str | Path,
    *,
    backend: str = "duckdb",
    generated_dir: str | Path | None = None,
    auto_generate: bool = True,
    quality_checks: bool = True,
    on_error: Literal["skip", "stop"] = "skip",
) -> RunResult:
```

**Parameter decisions:**

| Parameter | Type | Default | Rationale |
|-----------|------|---------|-----------|
| `entities_dir` | `str \| Path` | (required) | Where entity `.py` files live. No default -- explicit is better than implicit. The user must know where their entities are. |
| `data_dir` | `str \| Path` | (required) | Where parquet source data lives. For DuckDB, this is the base directory used to resolve `duckdb_path` references. Separating it from `entities_dir` keeps entity definitions portable. |
| `backend` | `str` | `"duckdb"` | Backend engine. String rather than enum for simplicity at the public API boundary. Only `"duckdb"` supported in v0.1. |
| `generated_dir` | `str \| Path \| None` | `None` | Where generated transform code lives/is written. `None` means `{entities_dir}/../generated/` (sibling to entities dir). |
| `auto_generate` | `bool` | `True` | If `True`, regenerate transformation code before execution. If `False`, expect generated code to already exist. See Decision 3.3. |
| `quality_checks` | `bool` | `True` | If `True`, run quality checks defined on entities after transformation. |
| `on_error` | `Literal["skip", "stop"]` | `"skip"` | Error strategy. See Decision 3.5. |

**Why `data_dir` is a separate parameter (not baked into entity source config):**

In timo-data-stack, `duckdb_path` in source configs is an absolute path like `~/timo-data/signals/**/*.parquet`. This makes entity definitions non-portable across machines. By passing `data_dir` at run time, entity source configs can use relative paths (e.g., `signals/**/*.parquet`) and `run()` resolves them against `data_dir`. This supports the "define once, run anywhere" vision.

**Why no `db_path` parameter:**

For DuckDB, the database can be in-memory (for tests) or file-based (for persistence). Rather than adding a `db_path` parameter to `run()`, the DuckDBExecutor (from S002) handles this. `run()` creates the executor internally. For advanced use cases (custom connection, BigQuery), we will add `run_entity()` that takes a connection directly. Keep `run()` simple.

### 3.2 Return Value: RunResult

```python
@dataclass(frozen=True)
class EntityRunResult:
    """Result of running a single entity through the pipeline."""
    entity_name: str
    status: Literal["success", "skipped", "error"]
    row_count: int | None = None
    error: str | None = None
    duration_seconds: float = 0.0
    quality_results: list[CheckResult] | None = None

@dataclass(frozen=True)
class RunResult:
    """Result of running the full pipeline."""
    entities: list[EntityRunResult]
    total_duration_seconds: float
    backend: str

    @property
    def success_count(self) -> int:
        return sum(1 for e in self.entities if e.status == "success")

    @property
    def error_count(self) -> int:
        return sum(1 for e in self.entities if e.status == "error")

    @property
    def skipped_count(self) -> int:
        return sum(1 for e in self.entities if e.status == "skipped")

    @property
    def ok(self) -> bool:
        """True if no errors occurred."""
        return self.error_count == 0
```

**Why dataclass, not Pydantic model:**

`RunResult` is an output-only value object. It does not need validation, serialization, or schema generation. Using `@dataclass(frozen=True)` keeps it lightweight and avoids coupling the public API return type to Pydantic. Users get a simple, inspectable object with dot access.

**Why not just print:**

Printing is a side effect that cannot be tested, composed, or filtered. Returning a structured result lets users:
- Check `result.ok` in scripts
- Iterate `result.entities` to find failures
- Pass results to downstream logic (CI reporting, dashboards)
- Ignore output entirely when running in tests

Logging (see 3.6) handles human-readable output separately.

### 3.3 Auto-Generate Strategy

**Decision:** `auto_generate=True` by default. `run()` always regenerates before execution unless the caller explicitly opts out.

**Rationale from timo-data-stack:** The existing script always regenerates (`regenerate_ibis_code()` runs before `execute_transformations()`). This ensures generated code never drifts from entity definitions. The cost is low (code gen is fast, it is string manipulation) and the benefit is high (eliminates "forgot to regenerate" bugs).

**Behavior:**

```
if auto_generate:
    for entity in sorted_entities:
        fyrnheim.generate(entity, output_dir=generated_dir)
else:
    # Verify generated files exist; skip entities where they don't
    for entity in sorted_entities:
        path = generated_dir / f"{entity.name}_transforms.py"
        if not path.exists():
            # Mark as skipped in results with clear message
            log.warning("Generated file not found: %s (skipping)", path)
```

**Why in-process, not subprocess:**

timo-data-stack calls `subprocess.run(["uv", "run", "python", "scripts/generate_pydantic_entities.py", ...])` for each entity -- a hack needed because the generator and runner lived in different environments. In fyrnheim, `generate()` is a library function in the same package. Calling it directly is faster, simpler, and produces better error messages.

### 3.4 Quality Check Integration

**Decision:** Run quality checks after each entity's transformation completes (not in a separate pass after all entities).

**Flow per entity:**

```
1. Generate code (if auto_generate)
2. Load transform module
3. Execute transform -> Ibis table -> persist to DuckDB
4. If quality_checks and entity has checks defined:
   a. Create QualityRunner(connection)
   b. Run checks against the output table
   c. Attach CheckResult list to EntityRunResult
```

**Why per-entity, not a separate pass:**

- Fail-fast feedback: if entity A's quality checks fail, the user sees it before waiting for entities B through Z to execute.
- Dependency-aware: if entity B depends on entity A and A's quality checks fail, we can optionally skip B (future enhancement).
- Simpler mental model: each entity's result includes its quality results.

**Quality check results do NOT gate execution:**

Quality check failures are reported in `EntityRunResult.quality_results` but do NOT change the entity's `status` to `"error"`. A quality failure means "the data was produced but has issues" -- this is informational, not a pipeline blocker. The entity status reflects whether the transformation itself succeeded.

Rationale: In timo-data-stack, quality checks are run separately and do not block the pipeline. Keeping this behavior avoids surprise pipeline failures when adding new quality rules. Users who want strict quality gating can check `result.entities[i].quality_results` and act accordingly.

### 3.5 Graceful Failure Strategy

**Decision:** `on_error="skip"` by default -- an error in one entity does not prevent other entities from running.

**Comparison with timo-data-stack:**

timo-data-stack uses `raise SystemExit(1)` on any error, halting the entire pipeline. This is problematic for development workflows where you want to see which entities succeed and which fail. It also prevents independent entities from running when an unrelated entity has a bug.

**Behavior:**

| `on_error` | Entity A fails | Entity B (no dep on A) | Entity C (depends on A) |
|------------|----------------|----------------------|------------------------|
| `"skip"` | Recorded as `status="error"` | Runs normally | Skipped (dependency failed) |
| `"stop"` | Recorded as `status="error"` | Not attempted | Not attempted |

When `on_error="skip"`:
- The failed entity is recorded with `status="error"` and the exception message in `error`.
- Entities that depend on the failed entity are recorded with `status="skipped"` and `error="dependency failed: {dep_name}"`.
- Independent entities continue execution.
- `RunResult.ok` returns `False`.

When `on_error="stop"`:
- Execution halts immediately. Remaining entities get `status="skipped"`.
- This matches timo-data-stack's current behavior for users who prefer it.

### 3.6 Logging and Output Strategy

**Decision:** Use Python's standard `logging` module. No rich, no click, no callbacks.

**Rationale:**

| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| `click.echo()` (timo-data-stack) | Simple, colored output | CLI dependency for a library; untestable; can't redirect | **No** |
| `rich` | Beautiful tables, progress bars | Heavy dependency (67 sub-packages); overkill for a library | **No** |
| Callback protocol | Maximum flexibility | Over-engineered for v0.1; users can wrap `run()` themselves | **No** |
| `logging` module | Zero dependencies; standard; configurable by caller; testable | Less pretty by default | **Yes** |

**Logger name:** `fyrnheim.engine` (matches the sub-package).

**Log levels used:**

| Event | Level | Example |
|-------|-------|---------|
| Pipeline start/end | `INFO` | `"Running 5 entities on duckdb backend"` |
| Entity start | `INFO` | `"Transforming: customers"` |
| Entity success | `INFO` | `"customers: 1234 rows (0.8s)"` |
| Entity skip | `WARNING` | `"customers: skipped (generated file missing)"` |
| Entity error | `ERROR` | `"customers: failed: KeyError('email')"` |
| Quality check result | `INFO` | `"customers: 3 checks passed, 1 failed"` |
| Code generation | `DEBUG` | `"Generating: customers_transforms.py"` |
| Dependency resolution | `DEBUG` | `"Execution order: transactions, customers, accounts"` |

**No emoji in log messages.** The timo-data-stack script uses emoji extensively. Emoji renders poorly in log files, CI output, and non-UTF-8 terminals. Use plain text markers.

**Caller controls formatting:**

```python
# User wants verbose output:
import logging
logging.basicConfig(level=logging.DEBUG)
result = fyrnheim.run("entities/", "data/")

# User wants quiet:
logging.basicConfig(level=logging.WARNING)
result = fyrnheim.run("entities/", "data/")

# User wants JSON logs (e.g., for production):
import json_log_handler  # hypothetical
logging.getLogger("fyrnheim").addHandler(json_log_handler)
result = fyrnheim.run("entities/", "data/")
```

This is the standard Python pattern. Libraries log; applications configure logging. fyrnheim is a library.

---

## 4. Internal Architecture

### 4.1 Orchestration Flow

```
fyrnheim.run(entities_dir, data_dir, ...)
    |
    v
EntityRegistry.discover(entities_dir)       # From S001
    |-> list[EntityInfo]
    v
EntityRegistry.resolve_order(entities)      # From S001 (topological sort)
    |-> list[EntityInfo] (sorted)
    v
for entity in sorted_entities:
    |
    |-- [auto_generate] fyrnheim.generate(entity, generated_dir)    # From E003-S004
    |
    |-- DuckDBExecutor.execute(entity)                              # From S002
    |       |-> register source parquet from data_dir
    |       |-> load generated transform module
    |       |-> call transform_fn(source) -> result table
    |       |-> persist result to DuckDB
    |       |-> return row count
    |
    |-- [quality_checks] QualityRunner.run_checks(entity, connection)  # From E001-S005
    |       |-> list[CheckResult]
    |
    |-> EntityRunResult(name, status, row_count, quality_results)
    |
    v
RunResult(entities=[...], total_duration, backend)
```

### 4.2 Module Placement

```
src/fyrnheim/
    __init__.py          # re-exports: run, generate, Entity
    engine/
        __init__.py      # re-exports: run, RunResult, EntityRunResult
        runner.py        # run() function + RunResult/EntityRunResult (NEW - this story)
        registry.py      # EntityRegistry (from S001)
        executor.py      # DuckDBExecutor (from S002)
```

The `run()` function lives in `fyrnheim.engine.runner` and is re-exported from both `fyrnheim.engine` and `fyrnheim` (top-level).

### 4.3 Companion Function: run_entity()

In addition to `run()`, expose `run_entity()` for single-entity execution:

```python
def run_entity(
    entity: Entity,
    data_dir: str | Path,
    *,
    backend: str = "duckdb",
    generated_dir: str | Path | None = None,
    auto_generate: bool = True,
    quality_checks: bool = True,
) -> EntityRunResult:
```

**Rationale:** The story handoff hints mention this. It is useful for:
- Testing a single entity during development
- Programmatic execution where the caller has already resolved entities
- REPL/notebook workflows

`run()` internally calls `run_entity()` for each entity, so this is not code duplication -- it is exposing the inner loop.

---

## 5. Acceptance Criteria Verification

| Criterion | How it is satisfied |
|-----------|---------------------|
| `run(entities_dir, data_dir, backend='duckdb')` executes all entities in dependency order | `EntityRegistry.discover()` + `resolve_order()` + sequential execution loop |
| `run()` generates transformation code if not already generated | `auto_generate=True` default calls `fyrnheim.generate()` per entity before execution |
| `run()` reports success/failure per entity with row counts | `RunResult.entities` is a list of `EntityRunResult` with `status`, `row_count`, `error` |
| `run()` with quality checks reports check results per entity | `EntityRunResult.quality_results: list[CheckResult]` populated when `quality_checks=True` |
| Errors during one entity don't prevent other entities from running | `on_error="skip"` default catches exceptions, records error, continues to next entity |

---

## 6. Edge Cases

| Scenario | Behavior |
|----------|----------|
| `entities_dir` does not exist | Raise `FileNotFoundError` immediately (not a recoverable error) |
| `entities_dir` has no `.py` files | Return `RunResult` with empty `entities` list, log warning |
| Circular dependency detected | Raise `CyclicDependencyError` immediately (not recoverable) |
| Generated file missing + `auto_generate=False` | Entity gets `status="skipped"`, `error="generated file not found"` |
| DuckDB connection fails | Raise immediately (not per-entity; backend failure affects all entities) |
| Source parquet file missing | Per-entity error: `status="error"`, `error="FileNotFoundError: ..."` |
| Transform function raises exception | Per-entity error captured in `EntityRunResult` |
| Quality check fails (data issue, not crash) | `status="success"`, failures recorded in `quality_results` |
| Quality runner crashes (code bug) | Per-entity: quality_results is `None`, warning logged, entity status stays "success" |

---

## 7. What We Defer

| Topic | Why deferred | Future story |
|-------|-------------|--------------|
| BigQuery backend | v0.1 is DuckDB-only per vision; BigQuery support requires env var config, auth, dataset management | E005 or later |
| Incremental processing | High water marks, timestamp columns, append mode -- significant complexity with minimal value for local dev | E005 or later |
| DerivedSource / identity graphs | Multi-source joins need separate design; `run()` will skip DerivedSource entities with a warning in v0.1 | E005 or later |
| CLI wrapper | `fyrnheim run entities/ data/` command-line interface on top of `run()` | Separate epic |
| Parallel execution | Running independent entities concurrently | Future optimization |
| Progress callbacks / rich output | Caller can configure logging; rich UI is an application concern | CLI epic |

---

## 8. Implementation Plan

### Prerequisites

This story depends on:
- **M001-E004-S001** (EntityRegistry, discovery, dependency resolution): provides `EntityRegistry.discover()`, `EntityInfo`, `resolve_execution_order()`, `CircularDependencyError`
- **M001-E004-S002** (DuckDB executor): provides `DuckDBExecutor`, `ExecutionResult`, `FyrnheimEngineError`
- **M001-E003-S004** (generate function): provides `fyrnheim.generate()`, `GenerateResult`
- **M001-E001-S005** (quality framework): provides `QualityRunner`, `CheckResult`

If any dependency is not yet implemented, the corresponding integration points should be stubbed with clear interfaces matching the designs above. The plan is written assuming all dependencies exist.

---

### Step 1: Create result dataclasses (`src/fyrnheim/engine/runner.py`)

**File:** `src/fyrnheim/engine/runner.py` (new file)

Define two frozen dataclasses at the top of the module:

```python
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from fyrnheim.quality.results import CheckResult

log = logging.getLogger("fyrnheim.engine")


@dataclass(frozen=True)
class EntityRunResult:
    """Result of running a single entity through the pipeline."""
    entity_name: str
    status: Literal["success", "skipped", "error"]
    row_count: int | None = None
    error: str | None = None
    duration_seconds: float = 0.0
    quality_results: list[CheckResult] | None = None


@dataclass(frozen=True)
class RunResult:
    """Result of running the full pipeline."""
    entities: list[EntityRunResult]
    total_duration_seconds: float
    backend: str

    @property
    def success_count(self) -> int:
        return sum(1 for e in self.entities if e.status == "success")

    @property
    def error_count(self) -> int:
        return sum(1 for e in self.entities if e.status == "error")

    @property
    def skipped_count(self) -> int:
        return sum(1 for e in self.entities if e.status == "skipped")

    @property
    def ok(self) -> bool:
        """True if no errors occurred."""
        return self.error_count == 0
```

Key decisions reflected:
- `frozen=True` for immutability -- results are output-only value objects
- `CheckResult` imported under `TYPE_CHECKING` to avoid circular imports and keep quality as an optional dependency at import time
- Logger name `fyrnheim.engine` as specified in design section 3.6

**Verification:** Import the module; instantiate both dataclasses; confirm `ok`, `success_count`, `error_count`, `skipped_count` properties work.

---

### Step 2: Implement `_resolve_generated_dir()` helper

In `runner.py`, add a private helper to resolve the generated directory:

```python
def _resolve_generated_dir(
    entities_dir: Path,
    generated_dir: Path | None,
) -> Path:
    """Resolve the generated code directory.

    If generated_dir is None, default to {entities_dir}/../generated/
    (a sibling directory to the entities directory).
    """
    if generated_dir is not None:
        return Path(generated_dir)
    return entities_dir.parent / "generated"
```

This encapsulates the defaulting logic from design section 3.1 so both `run()` and `run_entity()` can share it.

**Verification:** Call with `None` and confirm sibling resolution; call with explicit path and confirm pass-through.

---

### Step 3: Implement `run_entity()` -- single entity execution

This is the inner loop extracted as a public function per design section 4.3.

```python
def run_entity(
    entity: Entity,
    data_dir: str | Path,
    *,
    backend: str = "duckdb",
    generated_dir: str | Path | None = None,
    auto_generate: bool = True,
    quality_checks: bool = True,
) -> EntityRunResult:
```

**Internal flow (pseudocode):**

```
start_time = time.monotonic()
data_dir = Path(data_dir)
gen_dir = _resolve_generated_dir(???, generated_dir)
# Note: for run_entity, generated_dir must be explicit or default to cwd/generated

1. AUTO-GENERATE (if auto_generate=True):
   - log.debug("Generating: %s_transforms.py", entity.name)
   - call fyrnheim.generate(entity, output_dir=gen_dir)
   - if generate raises, return EntityRunResult(status="error", error=str(e))

2. VERIFY GENERATED FILE EXISTS (if auto_generate=False):
   - path = gen_dir / f"{entity.name}_transforms.py"
   - if not path.exists():
       log.warning("Generated file not found: %s (skipping)", path)
       return EntityRunResult(status="skipped", error="generated file not found: {path}")

3. EXECUTE ON BACKEND:
   - if backend != "duckdb": raise ValueError(f"Unsupported backend: {backend}")
   - Create DuckDBExecutor (in-memory, scoped to this entity)
     OR accept an executor parameter for batched use by run()
   - Register parquet sources from entity.source config, resolving paths against data_dir
   - executor.execute(entity.name, generated_dir=gen_dir)
   - Capture ExecutionResult with row_count

4. RUN QUALITY CHECKS (if quality_checks=True and entity has checks):
   - Create QualityRunner(connection=executor.connection)
   - results = runner.run_checks(entity, table_name)
   - Capture list[CheckResult]
   - log quality summary: "entity: N checks passed, M failed"
   - If quality runner crashes (code bug, not data issue):
     log.warning("Quality check error for %s: %s", entity.name, e)
     quality_results = None (do not change entity status)

5. RETURN EntityRunResult:
   - status="success", row_count from execution, quality_results
   - duration_seconds = time.monotonic() - start_time
```

**Important implementation detail:** When called from `run()`, the executor should be shared across entities (same DuckDB connection so entity B can read entity A's output table). Therefore, `run_entity()` will accept an optional `_executor` private parameter:

```python
def run_entity(
    entity: Entity,
    data_dir: str | Path,
    *,
    backend: str = "duckdb",
    generated_dir: str | Path | None = None,
    auto_generate: bool = True,
    quality_checks: bool = True,
    _executor: DuckDBExecutor | None = None,  # internal: shared executor from run()
) -> EntityRunResult:
```

When `_executor` is `None` (public standalone usage), create a new executor internally. When provided (called from `run()`), use the shared one.

**Verification:** Write a test with a minimal entity + parquet file that calls `run_entity()` directly and checks the returned `EntityRunResult`.

---

### Step 4: Implement source registration helper

Add a private function that reads an entity's source config and registers the appropriate parquet files with the executor:

```python
def _register_sources(
    executor: DuckDBExecutor,
    entity: Entity,
    data_dir: Path,
) -> None:
    """Register an entity's source data with the executor.

    Resolves source paths relative to data_dir.
    """
    source = entity.source
    if source is None:
        return

    # For TableSource / BigQuerySource with duckdb_path:
    # resolve the path relative to data_dir
    if hasattr(source, "duckdb_path") and source.duckdb_path:
        resolved = data_dir / source.duckdb_path
        source_name = f"source_{entity.name}"
        executor.register_parquet(source_name, resolved)
```

The exact source attribute names depend on the Entity/Source types from E001/E002. The key principle: `data_dir` is the base, source config paths are relative, and the function resolves them.

**Verification:** Test with a mock executor; confirm `register_parquet` is called with the resolved path.

---

### Step 5: Implement `run()` -- full pipeline orchestration

```python
def run(
    entities_dir: str | Path,
    data_dir: str | Path,
    *,
    backend: str = "duckdb",
    generated_dir: str | Path | None = None,
    auto_generate: bool = True,
    quality_checks: bool = True,
    on_error: Literal["skip", "stop"] = "skip",
) -> RunResult:
```

**Internal flow (pseudocode):**

```
pipeline_start = time.monotonic()
entities_dir = Path(entities_dir)
data_dir = Path(data_dir)
gen_dir = _resolve_generated_dir(entities_dir, generated_dir)

# --- Validate inputs ---
if not entities_dir.is_dir():
    raise FileNotFoundError(f"Entities directory not found: {entities_dir}")
if backend != "duckdb":
    raise ValueError(f"Unsupported backend: {backend}. Supported: 'duckdb'")

# --- Phase 1: Discover ---
log.info("Discovering entities in %s", entities_dir)
registry = EntityRegistry()
registry.discover(entities_dir)

if len(registry) == 0:
    log.warning("No entities found in %s", entities_dir)
    return RunResult(entities=[], total_duration_seconds=0.0, backend=backend)

# --- Phase 2: Resolve dependency order ---
sorted_entities = resolve_execution_order(registry)
entity_names = [e.name for e in sorted_entities]
log.debug("Execution order: %s", ", ".join(entity_names))
log.info("Running %d entities on %s backend", len(sorted_entities), backend)

# --- Phase 3: Execute ---
results: list[EntityRunResult] = []
failed_entities: set[str] = set()  # track failures for dependency skip

with DuckDBExecutor() as executor:
    for entity_info in sorted_entities:
        entity = entity_info.entity
        entity_name = entity_info.name

        # Check if any dependency has failed (on_error="skip" mode)
        if on_error == "skip":
            deps = _extract_dependencies(entity)
            failed_deps = [d for d in deps if d in failed_entities]
            if failed_deps:
                result = EntityRunResult(
                    entity_name=entity_name,
                    status="skipped",
                    error=f"dependency failed: {', '.join(failed_deps)}",
                )
                results.append(result)
                log.warning("%s: skipped (dependency failed: %s)",
                           entity_name, ", ".join(failed_deps))
                continue

        # Execute the entity
        log.info("Transforming: %s", entity_name)
        try:
            result = run_entity(
                entity,
                data_dir,
                backend=backend,
                generated_dir=gen_dir,
                auto_generate=auto_generate,
                quality_checks=quality_checks,
                _executor=executor,
            )
        except Exception as exc:
            result = EntityRunResult(
                entity_name=entity_name,
                status="error",
                error=str(exc),
            )

        results.append(result)

        # Handle error outcomes
        if result.status == "error":
            failed_entities.add(entity_name)
            log.error("%s: failed: %s", entity_name, result.error)
            if on_error == "stop":
                # Mark remaining entities as skipped
                remaining_idx = entity_names.index(entity_name) + 1
                for remaining_name in entity_names[remaining_idx:]:
                    results.append(EntityRunResult(
                        entity_name=remaining_name,
                        status="skipped",
                        error="execution stopped due to previous error",
                    ))
                break
        elif result.status == "success":
            log.info("%s: %s rows (%.1fs)",
                    entity_name, result.row_count, result.duration_seconds)
            if result.quality_results:
                passed = sum(1 for qr in result.quality_results if qr.passed)
                failed = len(result.quality_results) - passed
                log.info("%s: %d checks passed, %d failed",
                        entity_name, passed, failed)

total_duration = time.monotonic() - pipeline_start
run_result = RunResult(
    entities=results,
    total_duration_seconds=total_duration,
    backend=backend,
)
log.info("Pipeline complete: %d success, %d errors, %d skipped (%.1fs)",
        run_result.success_count, run_result.error_count,
        run_result.skipped_count, total_duration)
return run_result
```

Key implementation details:
- `_extract_dependencies()` is imported from `fyrnheim.engine.resolution` (S001)
- The `DuckDBExecutor` is created once and shared across all entities via the `_executor` parameter
- The `on_error="stop"` path marks ALL remaining entities as skipped, not just dependents
- Logging follows the exact levels and messages from design section 3.6

**Verification:** Full test with 2-3 entities in dependency order; confirm execution order, result counts, and log output.

---

### Step 6: Update `fyrnheim/engine/__init__.py` exports

Add the new public names to the engine package re-exports:

```python
# In src/fyrnheim/engine/__init__.py
from fyrnheim.engine.runner import (
    EntityRunResult,
    RunResult,
    run,
    run_entity,
)
```

These join the existing exports from S001 (`EntityRegistry`, `EntityInfo`, `resolve_execution_order`, `CircularDependencyError`) and S002 (`DuckDBExecutor`, `ExecutionResult`).

**Verification:** `from fyrnheim.engine import run, RunResult, EntityRunResult, run_entity` succeeds.

---

### Step 7: Re-export `run` from `fyrnheim/__init__.py`

Add `run` to the top-level public API:

```python
# In src/fyrnheim/__init__.py
from fyrnheim.engine.runner import run
```

This enables the canonical usage: `import fyrnheim; result = fyrnheim.run("entities/", "data/")`.

**Verification:** `import fyrnheim; fyrnheim.run` is the `run` function.

---

### Step 8: Write unit tests

**File:** `tests/engine/test_runner.py` (new file)

All unit tests use mocks/stubs for the dependencies (EntityRegistry, DuckDBExecutor, generate, QualityRunner). No real parquet files or DuckDB connections needed.

| Test | What it verifies | AC |
|------|-----------------|-----|
| `test_run_result_properties` | `ok`, `success_count`, `error_count`, `skipped_count` compute correctly from entity list | -- |
| `test_run_discovers_and_executes_in_order` | Entities execute in topological order returned by `resolve_execution_order()` | AC1 |
| `test_run_auto_generates_when_enabled` | `fyrnheim.generate()` called for each entity when `auto_generate=True` | AC2 |
| `test_run_skips_generate_when_disabled` | `fyrnheim.generate()` NOT called when `auto_generate=False`; missing generated files cause skip | AC2 |
| `test_run_reports_per_entity_results` | Each entity has correct `status`, `row_count`, `error` in `RunResult.entities` | AC3 |
| `test_run_quality_checks_reported` | `quality_results` populated when `quality_checks=True`; None when False | AC4 |
| `test_run_quality_failure_does_not_change_status` | Entity with passing transform but failing quality check has `status="success"` | AC4 |
| `test_run_graceful_failure_skip` | Error in entity A does not prevent independent entity B; dependent entity C skipped | AC5 |
| `test_run_graceful_failure_stop` | `on_error="stop"`: error in entity A marks all remaining as skipped | AC5 |
| `test_run_entity_single_execution` | `run_entity()` works standalone for a single entity | -- |
| `test_run_empty_directory` | Empty entities dir returns `RunResult` with empty list, log warning | edge case |
| `test_run_missing_directory` | Non-existent `entities_dir` raises `FileNotFoundError` | edge case |
| `test_run_unsupported_backend` | `backend="bigquery"` raises `ValueError` | edge case |

**Mocking strategy:**

```python
# Mock EntityRegistry to return controlled entities
@patch("fyrnheim.engine.runner.EntityRegistry")
@patch("fyrnheim.engine.runner.resolve_execution_order")
@patch("fyrnheim.engine.runner.DuckDBExecutor")
def test_run_discovers_and_executes_in_order(
    mock_executor_cls, mock_resolve, mock_registry_cls, tmp_path
):
    # Setup: create entities_dir so FileNotFoundError is not raised
    entities_dir = tmp_path / "entities"
    entities_dir.mkdir()

    # Configure mocks to return entities in specific order
    entity_a = make_stub_entity("a")
    entity_b = make_stub_entity("b", depends_on=["a"])
    mock_resolve.return_value = [entity_a, entity_b]

    result = run(str(entities_dir), str(tmp_path / "data"))
    assert [e.entity_name for e in result.entities] == ["a", "b"]
```

---

### Step 9: Write integration test (pairs with E004-S005)

**File:** `tests/engine/test_runner_integration.py` (new file)

One integration test that exercises the real pipeline without mocks:

1. Create a temporary directory with a minimal entity `.py` file
2. Create a temporary parquet file with sample data
3. Call `run(entities_dir, data_dir)` with defaults
4. Assert `result.ok is True`
5. Assert `result.entities[0].row_count > 0`
6. Assert `result.entities[0].status == "success"`

This test requires all dependencies (S001, S002, E003-S004) to be implemented. Mark it with `@pytest.mark.integration` so it can be skipped when running fast unit tests.

---

### Implementation Order

Execute steps in this order to maintain a working state at each checkpoint:

1. **Step 1** -- Result dataclasses (no dependencies, immediately testable)
2. **Step 2** -- `_resolve_generated_dir` helper (pure function, no dependencies)
3. **Step 8 partial** -- Unit tests for `RunResult`/`EntityRunResult` properties
4. **Step 4** -- Source registration helper
5. **Step 3** -- `run_entity()` function
6. **Step 5** -- `run()` function
7. **Step 6 + 7** -- Re-exports
8. **Step 8 remainder** -- Full unit test suite
9. **Step 9** -- Integration test

### Lines of Code Estimate

| File | Estimated lines |
|------|----------------|
| `src/fyrnheim/engine/runner.py` | ~200 lines |
| `tests/engine/test_runner.py` | ~250 lines |
| `tests/engine/test_runner_integration.py` | ~50 lines |
| `src/fyrnheim/engine/__init__.py` changes | ~5 lines |
| `src/fyrnheim/__init__.py` changes | ~2 lines |
| **Total** | **~507 lines** |
