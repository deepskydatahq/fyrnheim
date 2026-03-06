---
title: Configuration
description: Configure your Fyrnheim project with YAML, CLI flags, and production deployment patterns.
---

## Project Configuration

Configure your project with `fyrnheim.yaml` at the project root:

```yaml
entities_dir: entities
data_dir: data
output_dir: generated
backend: duckdb
backend_config:
  db_path: my_project.duckdb

# Push results to a separate output backend after fyr run
output_backend: clickhouse
output_config:
  host: localhost
  port: "8123"
  database: default
  user: default
  password: ""
```

### Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `entities_dir` | Directory containing entity Python files | `entities` |
| `data_dir` | Directory containing local data files (parquet, CSV) | `data` |
| `output_dir` | Directory where generated transform code is written | `generated` |
| `backend` | Transformation backend (`duckdb`, `bigquery`) | `duckdb` |
| `backend_config` | Backend-specific configuration (see below) | `{}` |
| `output_backend` | Optional output backend for pushing results | none |
| `output_config` | Output backend-specific configuration | `{}` |

### Backend Configuration

**DuckDB:**

```yaml
backend: duckdb
backend_config:
  db_path: my_project.duckdb
```

**BigQuery:**

```yaml
backend: bigquery
backend_config:
  project: my-gcp-project
  dataset: analytics
```

## CLI Flags

All settings can be overridden via CLI flags. For example:

```bash
fyr run --backend bigquery
```

This runs on BigQuery regardless of what `fyrnheim.yaml` says.

Use `fyr --help` to see all available CLI options:

```bash
fyr --help
fyr generate --help
fyr run --help
```

## Production Deployment

A typical production pattern:

1. **Extract** raw data with DLT (or any EL tool) into parquet files or a warehouse
2. **Transform** with Fyrnheim: `fyr run --backend bigquery` (or duckdb for local)
3. **Push** results to an output backend for serving (ClickHouse, Postgres, etc.)

Configure the output backend in `fyrnheim.yaml`:

```yaml
backend: duckdb           # transform backend
output_backend: clickhouse # push dim/analytics tables here after run
output_config:
  host: ch.example.com
  port: "8123"
  database: analytics
```

This separation lets you develop locally on DuckDB while pushing production results to a fast query engine.
