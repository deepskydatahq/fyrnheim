# Design: M001-E001-S001 -- Package Structure

**Story:** M001-E001-S001 -- Create package structure with pyproject.toml and src layout
**Date:** 2026-02-17
**Status:** plan

---

## 1. Summary

Create a minimal, pip-installable Python package called `fyrnheim` using the `src/` layout and hatchling build system. The package will contain empty sub-package directories that mirror the architecture from `timo-data-stack/metadata/` and will be installable via `uv pip install -e .`.

---

## 2. Directory Structure

```
fyrnheim/                          # repo root (already exists)
├── pyproject.toml                 # package metadata + tool config
├── src/
│   └── fyrnheim/
│       ├── __init__.py            # version + top-level imports
│       ├── core/
│       │   └── __init__.py        # empty
│       ├── primitives/
│       │   └── __init__.py        # empty
│       ├── components/
│       │   └── __init__.py        # empty
│       ├── quality/
│       │   └── __init__.py        # empty
│       ├── generators/
│       │   └── __init__.py        # empty
│       └── engine/
│           └── __init__.py        # empty
├── tests/
│   ├── __init__.py
│   └── test_package.py            # smoke test for acceptance criteria
├── docs/
│   └── plans/                     # design docs (this file lives here)
└── product/                       # product layer (already exists)
```

### Sub-package purposes (for context; all empty in this story)

| Sub-package | Maps to in timo-data-stack | Purpose |
|---|---|---|
| `core/` | `metadata/core/` | Base Pydantic models: Entity, Layer, Source, types |
| `primitives/` | `metadata/primitives/` | Reusable Ibis expression functions (hashing, dates, strings) |
| `components/` | `metadata/components/` | Multi-column patterns (LifecycleFlags, TimeMetrics, Measures) |
| `quality/` | `metadata/quality/` | Data quality check definitions and runner |
| `generators/` | `metadata/generators/` | Code generators (Ibis, prep, dimension, activity) |
| `engine/` | (new) | Execution engine: backend connection, DAG resolution, runner |

---

## 3. pyproject.toml

```toml
[project]
name = "fyrnheim"
version = "0.1.0"
description = "Define typed Python entities, generate transformations, run anywhere. A dbt alternative built on Pydantic + Ibis."
requires-python = ">=3.11"
readme = "README.md"
license = "MIT"
authors = [
    { name = "Timo Dechau", email = "timo@deepskydata.com" }
]
keywords = ["data", "transformations", "pydantic", "ibis", "dbt-alternative", "elt"]
classifiers = [
    "Development Status :: 3 - Alpha",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: MIT License",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Programming Language :: Python :: 3.13",
    "Programming Language :: Python :: 3.14",
    "Topic :: Database",
    "Topic :: Software Development :: Libraries :: Python Modules",
    "Typing :: Typed",
]

dependencies = [
    "pydantic>=2.11.0",
    "pyyaml>=6.0.0",
    "ibis-framework>=11.0.0",
]

[project.optional-dependencies]
duckdb = [
    "ibis-framework[duckdb]>=11.0.0",
    "duckdb>=1.4.0",
]
bigquery = [
    "ibis-framework[bigquery]>=11.0.0",
    "google-cloud-bigquery>=3.35.0",
]
dev = [
    "pytest>=8.0.0",
    "pytest-cov>=6.0.0",
    "ruff>=0.12.0",
    "mypy>=1.17.0",
]

[project.urls]
Homepage = "https://github.com/timo-ai/fyrnheim"
Repository = "https://github.com/timo-ai/fyrnheim"
Issues = "https://github.com/timo-ai/fyrnheim/issues"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/fyrnheim"]

[tool.ruff]
line-length = 100
target-version = "py311"

[tool.ruff.lint]
select = [
    "E",   # pycodestyle errors
    "W",   # pycodestyle warnings
    "F",   # pyflakes
    "I",   # isort
    "B",   # flake8-bugbear
    "C4",  # flake8-comprehensions
    "UP",  # pyupgrade
]
ignore = [
    "E501",  # line too long
    "B008",  # do not perform function calls in argument defaults
]

[tool.mypy]
python_version = "3.11"
warn_return_any = true
warn_unused_configs = true
disallow_untyped_defs = true
ignore_missing_imports = true

[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
python_classes = ["Test*"]
python_functions = ["test_*"]
addopts = [
    "-v",
    "--strict-markers",
    "--tb=short",
]
```

---

## 4. Design Decisions

### 4.1 pyproject.toml contents

**name:** `fyrnheim` -- short, memorable, describes the core value (typed data transformations).

**version:** `0.1.0` -- standard alpha starting point. We are not at 1.0 yet.

**python-requires:** `>=3.11` -- matches timo-data-stack. Python 3.11 introduced `ExceptionGroup`, `TaskGroup`, and significant performance improvements. No reason to support older versions for a new project.

**Core dependencies (3 packages only):**

| Dependency | Why | Why this version floor |
|---|---|---|
| `pydantic>=2.11.0` | Entity schema definitions, validation. This is the foundation. | 2.11 has TypeAdapter improvements and is the version timo-data-stack pins. |
| `pyyaml>=6.0.0` | Config file parsing (entity YAML definitions). | 6.0 is the modern safe-by-default release. |
| `ibis-framework>=11.0.0` | Backend-portable expression compilation. Core to the "run anywhere" promise. | 11.0 is the version timo-data-stack uses; recent enough for all needed features. |

**What we deliberately excluded from core deps:**
- `pydantic-settings` -- not needed for the library; users configure their own settings
- `sqlglot` -- ibis-framework already depends on sqlglot internally
- `pandas` -- fyrnheim should not require pandas; Ibis works with its own Table type
- `dlt`, `dbt-core`, `dbt-bigquery`, `dbt-duckdb` -- these are what we are *replacing*
- `typer`, `rich` -- CLI is a future story, not part of the core library
- `sqlalchemy` -- not needed; Ibis handles backend connections
- `convex`, `httpx`, `pygithub` -- growth-stack-specific, not relevant to fyrnheim
- `python-dotenv`, `logtail-python` -- application concerns, not library concerns
- `google-api-python-client`, `rapidfuzz` -- growth-stack-specific

**Optional extras:**
- `[duckdb]` -- local development backend. Adds `ibis-framework[duckdb]` and `duckdb>=1.4.0`.
- `[bigquery]` -- production backend. Adds `ibis-framework[bigquery]` and `google-cloud-bigquery>=3.35.0`.

This lets users `pip install fyrnheim[duckdb]` for local dev and `pip install fyrnheim[bigquery]` for prod, without pulling in heavy cloud SDKs when not needed.

**Build system:** `hatchling` -- same as timo-data-stack. Lightweight, supports src layout natively via `tool.hatch.build.targets.wheel.packages`.

### 4.2 src/ layout rationale

Using `src/fyrnheim/` instead of a flat `fyrnheim/` directory because:
1. Prevents accidental imports from the working directory during testing
2. Forces `pip install -e .` to actually work (catches packaging bugs early)
3. Industry standard for library packages (recommended by PyPA)
4. Story explicitly requires it

### 4.3 Top-level `__init__.py`

```python
"""fyrnheim -- Define typed Python entities, generate transformations, run anywhere."""

__version__ = "0.1.0"
```

Minimal. Only exports `__version__` for now. As sub-packages mature (future stories), we will add convenience re-exports like:

```python
from fyrnheim.core.entity import Entity
from fyrnheim.core.layer import Layer
```

But for this story, only `__version__` is required by the acceptance criteria.

### 4.4 Sub-package `__init__.py` files

All empty (`""` or a one-line docstring). The acceptance criteria only require they exist so that `import fyrnheim.core`, `import fyrnheim.primitives`, etc. work. Content comes in subsequent stories.

### 4.5 Dev dependencies

| Dependency | Why |
|---|---|
| `pytest>=8.0.0` | Test runner. Bumped floor to 8.x (current stable) since this is a new project. |
| `pytest-cov>=6.0.0` | Coverage reporting. Same floor as timo-data-stack. |
| `ruff>=0.12.0` | Linter + formatter. Replaces black + flake8 + isort. Same floor as timo-data-stack. |
| `mypy>=1.17.0` | Static type checking. Same floor as timo-data-stack. |

**Excluded from timo-data-stack dev deps:**
- `pytest-asyncio` -- not needed yet; no async code in initial fyrnheim
- `black` -- ruff handles formatting now; no need for a separate formatter

### 4.6 Tool configuration

**Ruff:** Mirrors timo-data-stack config exactly (line-length 100, py311 target, same lint rules). This ensures code moving from timo-data-stack to fyrnheim passes the same checks.

**Mypy:** Same as timo-data-stack but with `disallow_untyped_defs = true` (stricter). Since fyrnheim is a new library with a "typed" identity, we should enforce type annotations from day one. The timo-data-stack uses `false` because it has legacy untyped code.

**Pytest:** Simplified to `testpaths = ["tests"]` only (timo-data-stack had `["tests", "entities/tests"]` for its multi-package layout). Added `--tb=short` for cleaner output.

---

## 5. Acceptance Criteria Verification

| Criterion | How it is satisfied |
|---|---|
| pyproject.toml exists with name='fyrnheim', python>=3.11, build-system=hatchling | See section 3 -- `name = "fyrnheim"`, `requires-python = ">=3.11"`, `build-backend = "hatchling.build"` |
| Core dependencies: pydantic>=2.11.0, pyyaml>=6.0.0 | Listed in `[project] dependencies` |
| Optional extras: [duckdb] and [bigquery] with correct packages | Defined in `[project.optional-dependencies]` |
| `uv pip install -e .` completes without errors | src layout + hatchling config + wheel packages setting ensures this |
| `import fyrnheim` succeeds and `fyrnheim.__version__` exists | `src/fyrnheim/__init__.py` exports `__version__ = "0.1.0"` |

---

## 6. Smoke Test

File: `tests/test_package.py`

```python
"""Smoke tests for fyrnheim package structure."""


def test_import_fyrnheim():
    """fyrnheim is importable."""
    import fyrnheim
    assert fyrnheim.__version__ == "0.1.0"


def test_import_subpackages():
    """All sub-packages are importable."""
    import fyrnheim.core
    import fyrnheim.primitives
    import fyrnheim.components
    import fyrnheim.quality
    import fyrnheim.generators
    import fyrnheim.engine


def test_version_is_string():
    """Version is a proper string."""
    import fyrnheim
    assert isinstance(fyrnheim.__version__, str)
    parts = fyrnheim.__version__.split(".")
    assert len(parts) == 3, "Version should be semver (major.minor.patch)"
```

---

## 7. Implementation Checklist

1. Create `pyproject.toml` at repo root with contents from section 3
2. Create `src/fyrnheim/__init__.py` with `__version__`
3. Create empty `__init__.py` in each sub-package: `core/`, `primitives/`, `components/`, `quality/`, `generators/`, `engine/`
4. Create `tests/__init__.py` (empty)
5. Create `tests/test_package.py` with smoke tests from section 6
6. Run `uv pip install -e .` and verify success
7. Run `pytest tests/` and verify all 3 tests pass
8. Run `ruff check src/` and verify no lint errors

---

## 8. Open Questions (resolved)

**Q: Should `ibis-framework` be a core dep or optional?**
A: Core. The story hints say "Add ibis-framework (without backend extras) as core dep, backends as extras." This makes sense because Ibis is fundamental to fyrnheim's value proposition -- every entity compiles to Ibis expressions. Without Ibis, the library has no purpose. Backend-specific drivers (duckdb, bigquery) are optional.

**Q: Should we include `sqlglot` as a direct dependency?**
A: No. `ibis-framework` already depends on `sqlglot`. Adding it directly would risk version conflicts. If we need sqlglot features directly in the future, we can add it then.

**Q: What about a `py.typed` marker file?**
A: Yes, we should include `src/fyrnheim/py.typed` (empty file) so that mypy recognizes fyrnheim as a typed package. This is low effort and aligns with our "typed" identity. Adding it to the implementation checklist.

**Q: GitHub org name for URLs?**
A: Using `timo-ai/fyrnheim` as placeholder. Can be updated before first publish.
