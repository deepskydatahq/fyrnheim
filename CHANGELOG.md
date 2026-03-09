# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-03-09

### Added

- `fyr docs generate` command — generates a self-contained HTML documentation site with interactive entity lineage DAG (dagre-d3), entity detail pages, and sidebar navigation
- `fyr docs serve` command — serves generated docs on a local HTTP server and opens the browser
- JSON catalog builder (`fyrnheim.docs.catalog`) for extracting entity metadata
- Inline identity graph sources — `IdentityGraphSource` now accepts an inline `TableSource` instead of requiring a named entity reference
- Optional `prep_columns` on `IdentityGraphSource` for lightweight transforms before identity graph joins
- Code generator and executor support for inline identity graph sources

### Changed

- `extract_dependencies` promoted from private to public API in `fyrnheim.engine.resolution`
- `IdentityGraphSource.entity` is now optional (exactly one of `entity` or `source` must be set)

## [0.2.0] - 2026-03-07

### Added

- Entity unit testing framework (`fyrnheim.testing`) with `EntityTest` base class
- `fyr test` CLI command with test discovery, pass/fail output, and `--entity` filter
- Postgres backend support via `create_connection('postgres', ...)`
- `[postgres]` optional dependency group in pyproject.toml
- Example test file in `fyr init` scaffold (`tests/test_customers.py`)
- CONTRIBUTING.md for open source contributors
- Documentation website (Astro Starlight)

### Changed

- README updated with testing workflow and `fyr test` documentation
- README Status section lists Postgres as supported backend

### Fixed

- Ruff import sorting in test_public_api.py

## [0.1.0] - 2026-03-04

### Added

- Entity definition system with Pydantic models and typed fields
- Source types: SourceMapping, DerivedSource, UnionSource
- Ibis-based executor for backend-agnostic transformations
- DuckDB and BigQuery backend support
- CLI (`fyr`) with `init`, `run`, `validate`, and `inspect` commands
- Scaffold project template with sample entity and data
- Field-level lineage tracking
- Connection factory for multi-backend configuration
- YAML-based project configuration (`fyrnheim.yaml`)
