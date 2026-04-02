---
title: Introduction
description: Get started with Fyrnheim -- an activities-first Python data pipeline tool.
---

Fyrnheim is a Python-native dbt alternative built on an activities-first architecture. You define sources, activity definitions, identity graphs, entity models, and analytics models in Python. Fyrnheim generates Ibis transformation code and runs it on DuckDB, BigQuery, ClickHouse, or Postgres.

## Install

```bash
pip install fyrnheim[duckdb]
```

Requires Python 3.11 or later.

## Quick Start

```bash
fyr init myproject && cd myproject
fyr generate
fyr run
```

See [Getting Started](/getting-started/) for a full walkthrough of the pipeline, or jump to [Core Concepts](/concepts/sources/) to learn about sources, activities, identity, and entity models.
