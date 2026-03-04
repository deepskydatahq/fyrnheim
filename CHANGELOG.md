# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
