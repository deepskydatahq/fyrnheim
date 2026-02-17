# Design: DuckDB Execution Engine (M001-E004-S002)

**Date:** 2026-02-17
**Story:** M001-E004-S002 -- Build DuckDB execution engine
**Status:** Plan
**Depends on:** M001-E003-S004 (generate command)

## Context

The timo-data-stack `run_transformations.py` (695 lines) handles both DuckDB and BigQuery backends, incremental processing with high water marks, identity graph resolution, union sources, and timo-specific entity discovery. Fyrnheim needs the DuckDB path only, stripped of all timo-specific logic, as a clean reusable execution engine.

The executor is the critical bridge between code generation (E003) and the public `run()` API (S003). It must be simple enough to use standalone but composable enough to plug into the orchestration layer.

---

## Design Decisions

### 1. Executor Interface: Class-Based (DuckDBExecutor)

**Decision:** Class-based `DuckDBExecutor` with a clear lifecycle.

**Rationale:**

- The executor has natural state: a DuckDB connection, registered source tables, and configuration. A class models this lifecycle cleanly (connect, register, execute, close).
- The timo-data-stack code already follows this pattern implicitly -- `execute_transformations()` takes a connection and threads it through every call. A class makes that explicit.
- A functional `run_on_duckdb()` would need to accept or create a connection on every call, making multi-entity execution awkward (reconnecting each time, or passing a connection as a bag parameter).
- The class enables context manager protocol (`with DuckDBExecutor(...) as ex:`) which guarantees cleanup.
- Future backends (BigQueryExecutor, PostgresExecutor) can share a common protocol/ABC without forcing a specific inheritance hierarchy.

**Interface:**

```python
from pathlib import Path
from fyrnheim.engine.executor import DuckDBExecutor

# Context manager usage (preferred)
with DuckDBExecutor(db_path=":memory:") as executor:
    executor.register_parquet("raw_customers", Path("data/customers.parquet"))
    result = executor.execute("customers", generated_dir=Path("generated/"))
    # result is an ExecutionResult with .table, .row_count, .target_name

# Explicit lifecycle (for REPL / notebook use)
executor = DuckDBExecutor(db_path="warehouse.duckdb")
executor.register_parquet("raw_customers", Path("data/customers.parquet"))
result = executor.execute("customers")
executor.close()
```

**Protocol (for future backends):**

```python
from typing import Protocol, runtime_checkable

@runtime_checkable
class Executor(Protocol):
    def register_source(self, name: str, path: Path) -> None: ...
    def execute(self, entity_name: str, generated_dir: Path) -> ExecutionResult: ...
    def close(self) -> None: ...
```

The protocol is not implemented in S002. It is documented here to show the class design is forward-compatible. Implementing the protocol is deferred until a second backend is added.

### 2. Parquet Registration: Explicit API with Glob Support

**Decision:** Explicit `register_parquet(name, path)` method. No convention-based auto-discovery.

**Rationale:**

- The timo-data-stack uses convention-based paths (`~/timo-data/{source_name}/**/*.parquet`) which is tightly coupled to the timo directory layout. Fyrnheim should not assume any directory structure.
- Explicit registration makes the data contract visible: the caller says "this parquet file is the `raw_customers` table." No magic, no surprises.
- Convention-based discovery can be layered on top by the `run()` function (S003) without baking it into the executor. The executor stays a low-level building block.
- Glob support via `Path` means a caller can pass `Path("data/customers/*.parquet")` and DuckDB's `read_parquet` handles the glob natively.

**Interface:**

```python
# Single file
executor.register_parquet("raw_customers", Path("data/customers.parquet"))

# Glob pattern (DuckDB handles this natively)
executor.register_parquet("raw_events", Path("data/events/**/*.parquet"))

# Multiple sources before execution
executor.register_parquet("raw_customers", Path("data/customers.parquet"))
executor.register_parquet("raw_orders", Path("data/orders.parquet"))
result = executor.execute("customers")
```

**Implementation detail:** Under the hood, `register_parquet` calls `conn.read_parquet(str(path), table_name=name)` which creates a virtual table in DuckDB's catalog. The method stores the name in an internal `_registered_sources: dict[str, Path]` for introspection and error messages.

### 3. Generated Transform Module Loading

**Decision:** `importlib.util` dynamic loading with a strict naming convention, same as timo-data-stack.

**Rationale:**

- The timo-data-stack pattern works: load `generated/{entity_name}_transforms.py`, look for `transform_{entity_name}()` function. This is battle-tested and simple.
- The naming convention (`{name}_transforms.py` containing `transform_{name}(source) -> ibis.Table`) is the contract between the generator (E003) and the executor (E004). Both sides must agree on it.
- Dynamic `importlib` loading is required because generated files are not part of the installed package. They are written to a user-specified output directory at generation time.

**Module loading flow:**

```
execute("customers", generated_dir=Path("generated/"))
  -> resolve path: generated/customers_transforms.py
  -> importlib.util.spec_from_file_location("fyrnheim.generated.customers_transforms", path)
  -> module_from_spec + exec_module
  -> getattr(module, "transform_customers")
  -> call transform_fn(source_table) -> ibis.Table result
```

**What gets passed to the transform function:**

- For regular entities: `transform_fn(source: ibis.Table) -> ibis.Table` -- a single Ibis table expression representing the registered source.
- For derived/identity-graph entities (deferred, not in S002): `transform_fn(sources: dict[str, ibis.Table]) -> ibis.Table`.

S002 implements only the single-source path. Multi-source support is documented here but deferred until identity graph support is added.

**Generated dir default:** The `execute()` method accepts `generated_dir` as an optional parameter. If not provided, it falls back to the executor-level default set at construction (`DuckDBExecutor(generated_dir=Path("generated/"))`). This avoids passing the same path on every call.

### 4. In-Memory vs File-Based DuckDB

**Decision:** Constructor parameter `db_path` with `:memory:` as the default. The string `":memory:"` means in-memory; any other string or `Path` means file-based.

**Rationale:**

- In-memory is the right default for tests, CI, and quick local runs. It is fast and leaves no artifacts.
- File-based is needed when results must persist across executor invocations (e.g., running entity A, then entity B that depends on A's output, in separate sessions). It is also required for inspecting results after execution with external tools (DBeaver, DuckDB CLI).
- The timo-data-stack hardcodes `~/timo-data/timo.duckdb`. Fyrnheim should not impose a path convention but should make file-based easy when needed.
- Using the string `":memory:"` follows DuckDB's own convention (`duckdb.connect(":memory:")`), so it is unsurprising.

**Interface:**

```python
# In-memory (default) -- good for tests and single-shot runs
executor = DuckDBExecutor()
executor = DuckDBExecutor(db_path=":memory:")

# File-based -- persists across sessions
executor = DuckDBExecutor(db_path=Path("warehouse.duckdb"))
executor = DuckDBExecutor(db_path="warehouse.duckdb")  # str also accepted
```

**Implementation note:** `ibis.duckdb.connect()` with no arguments or `":memory:"` creates an in-memory database. With a file path, it creates or opens a persistent database. The executor passes `db_path` through directly.

### 5. Result Persistence: CREATE TABLE AS with Overwrite

**Decision:** Full refresh with `conn.create_table(name, result, overwrite=True)` for S002. No incremental INSERT.

**Rationale:**

- The epic notes explicitly say: "Skip for now: Incremental processing (high water marks) -- that's an optimization."
- `CREATE TABLE ... AS` with `overwrite=True` is the simplest correct approach. It replaces the entire table each run, which matches the expected local development workflow (define, run, inspect, iterate).
- The timo-data-stack uses both `create_table` (full refresh) and `insert` (incremental). Fyrnheim S002 needs only full refresh.
- Incremental mode can be added later as an `ExecutionMode` enum or a parameter on `execute()` without breaking the S002 interface.

**Target table naming:**

- Default: `dim_{entity_name}` (matching dbt/timo convention).
- Override: if the entity model defines a custom `model_name` in its layer config, use that. This is resolved by reading `entity.layers.dimension.model_name` if available.
- For S002, the simpler approach is sufficient: the executor receives a `target_name` parameter or derives it as `dim_{entity_name}`.

**Interface:**

```python
result = executor.execute("customers")
# Creates table "dim_customers" in DuckDB

result = executor.execute("customers", target_name="stg_customers")
# Creates table "stg_customers" instead
```

**ExecutionResult dataclass:**

```python
@dataclasses.dataclass(frozen=True)
class ExecutionResult:
    entity_name: str
    target_name: str
    row_count: int
    columns: list[str]
    success: bool
    error: str | None = None
```

### 6. Error Handling Strategy

**Decision:** Catch, wrap, and surface errors with context. Never swallow. Let the caller decide whether to halt or continue.

**Rationale:**

- The timo-data-stack does `raise SystemExit(1)` on any error, which is appropriate for a CLI script but wrong for a library. Fyrnheim is a library first.
- The S003 `run()` function (the orchestrator) is responsible for deciding whether to continue after one entity fails. The acceptance criteria for S003 say: "Errors during one entity don't prevent other entities from running." The executor should not make this decision.
- Errors fall into three categories, each needing different handling:

**Error taxonomy:**

| Error | Cause | Executor behavior |
|---|---|---|
| `SourceNotFoundError` | Parquet file does not exist or no table registered with that name | Raise immediately with path/name in message |
| `TransformModuleError` | Generated file missing, function not found, or import fails | Raise immediately with module path and entity name |
| `ExecutionError` | Ibis/DuckDB error during `transform_fn()` or `create_table()` | Wrap underlying exception with entity context, re-raise |
| `ConnectionError` | DuckDB cannot open file or connection dropped | Raise immediately (no point continuing) |

**All custom exceptions inherit from a common base:**

```python
class FyrnheimEngineError(Exception):
    """Base exception for engine errors."""
    pass

class SourceNotFoundError(FyrnheimEngineError):
    """Raised when a source table or parquet file cannot be found."""
    pass

class TransformModuleError(FyrnheimEngineError):
    """Raised when a generated transform module cannot be loaded."""
    pass

class ExecutionError(FyrnheimEngineError):
    """Raised when transform execution or result persistence fails."""
    pass
```

**Logging:** The executor uses `logging.getLogger("fyrnheim.engine")`. It logs at INFO for lifecycle events (connecting, registering sources, starting execution, completion with row count) and at ERROR for failures (with the original exception attached). It does not use `click.echo` -- that is the CLI layer's job.

---

## File Layout

```
src/fyrnheim/engine/
    __init__.py          # Public exports: DuckDBExecutor, ExecutionResult
    executor.py          # DuckDBExecutor class
    errors.py            # FyrnheimEngineError hierarchy
    _loader.py           # Internal: transform module loading via importlib
```

`_loader.py` is a private module that isolates the `importlib` machinery. This keeps `executor.py` focused on the DuckDB lifecycle and makes the loading logic independently testable.

---

## Acceptance Criteria Mapping

| Criterion | How satisfied |
|---|---|
| DuckDBExecutor connects to DuckDB (in-memory or file-based) | Constructor with `db_path` parameter; `ibis.duckdb.connect()` |
| `register_parquet(name, path)` registers a parquet file as a named table | `conn.read_parquet(path, table_name=name)` |
| `execute(entity_name)` loads generated transform module and runs it | `_loader.load_transform_module()` + call `transform_{name}(source)` |
| Transform result is an Ibis table that can be `.to_pandas()` or persisted | `transform_fn` returns `ibis.Table`; executor calls `create_table` |
| Results persist to DuckDB as tables accessible via `conn.table(name)` | `conn.create_table(target, result, overwrite=True)` |

---

## What This Does NOT Cover

These are explicitly deferred to later stories or epics:

- **Incremental processing** (high water marks, INSERT mode) -- optimization, not in E004 scope
- **BigQuery or other backends** -- deferred until second backend is needed
- **Identity graph / multi-source execution** -- DerivedSource with `dict[str, ibis.Table]`
- **Analytics layer execution** -- `analytics_{name}()` function in generated modules
- **Post-processing hooks** -- `dim_{name}__post_processing()` in generated modules
- **Dependency-aware multi-entity execution** -- that is S003's job (the `run()` function)
- **Convention-based parquet discovery** -- that is S003's job (layered on top of explicit registration)

---

## Open Questions

1. **Should `register_parquet` validate that the file exists immediately, or defer to DuckDB?** Recommendation: validate immediately and raise `SourceNotFoundError` with the path. DuckDB's own error for missing files is not actionable ("IO Error: No files found that match the pattern"). Checking `Path.exists()` first (for non-glob paths) gives a better message. For glob patterns, defer to DuckDB.

2. **Should the executor expose the raw `ibis.BaseBackend` connection?** Recommendation: yes, as a read-only property `executor.connection`. Power users and tests need it for inspection (`conn.table("dim_customers").to_pandas()`). This is consistent with the acceptance criteria which mention `conn.table(name)`.
