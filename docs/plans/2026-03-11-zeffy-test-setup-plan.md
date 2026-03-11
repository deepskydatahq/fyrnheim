# Zeffy Test Setup Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Create a local Fyrnheim attribution pipeline for the Zeffy client, extracting data from Snowflake via dlt and modeling it as 3 Fyrnheim entities on DuckDB.

**Architecture:** Two dlt pipelines extract from Snowflake to local parquet (amplitude events/merges + organizations). Three Fyrnheim entities (touchpoints, account, attribution) read the parquet and run the attribution pipeline on DuckDB.

**Tech Stack:** Python 3.11+, uv, dlt (sql_database + filesystem), Fyrnheim, DuckDB, Snowflake (key-pair auth)

**Working directory:** `/home/tmo/roadtothebeach/propel/client-zeffy-fyrnheim`

---

### Task 1: Scaffold the project

**Files:**
- Create: `pyproject.toml`
- Create: `.env`
- Create: `.gitignore`
- Create: `fyrnheim.yaml`
- Create: `src/zeffy/__init__.py`
- Create: `src/zeffy/pipelines/__init__.py`

**Step 1: Create the project directory and init git**

```bash
mkdir -p /home/tmo/roadtothebeach/propel/client-zeffy-fyrnheim
cd /home/tmo/roadtothebeach/propel/client-zeffy-fyrnheim
git init
```

**Step 2: Create pyproject.toml**

```toml
[project]
name = "client-zeffy-fyrnheim"
version = "0.1.0"
description = "Zeffy marketing attribution pipeline using Fyrnheim"
requires-python = ">=3.11"
dependencies = [
    "fyrnheim>=0.3.0",
    "dlt[duckdb,filesystem,parquet,sql_database]>=1.0.0",
    "duckdb>=1.0.0",
    "python-dotenv>=1.0.0",
    "snowflake-connector-python>=3.0.0",
    "cryptography>=42.0.0",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/zeffy"]
```

**Step 3: Create .env with Snowflake credentials**

```bash
SNOWFLAKE_ACCOUNT=kq13141.ca-central-1.aws
SNOWFLAKE_USER=attribution_consultant
SNOWFLAKE_PRIVATE_KEY_PATH=/home/tmo/roadtothebeach/propel/client-zeffy/.keys/snowflake_key.p8
SNOWFLAKE_DATABASE=DATABASE_PRODUCTION
SNOWFLAKE_WAREHOUSE=CONSULTANT_WH
SNOWFLAKE_ROLE=ATTRIBUTION_CONSULTANT_ROLE
```

**Step 4: Create .gitignore**

```
data/
generated/
.env
*.pyc
__pycache__/
.venv/
.dlt/
```

**Step 5: Create fyrnheim.yaml**

```yaml
backend: duckdb
data_dir: ./data
entities_dir: ./entities
output_dir: ./generated
```

**Step 6: Create package init files**

```bash
mkdir -p src/zeffy/pipelines
touch src/zeffy/__init__.py
touch src/zeffy/pipelines/__init__.py
mkdir -p entities
```

**Step 7: Install dependencies**

```bash
cd /home/tmo/roadtothebeach/propel/client-zeffy-fyrnheim
uv sync
```

**Step 8: Commit**

```bash
git add -A
git commit -m "chore: scaffold zeffy-fyrnheim project"
```

---

### Task 2: dlt pipeline — amplitude

**Files:**
- Create: `src/zeffy/pipelines/amplitude.py`

**Step 1: Write the amplitude pipeline**

This pipeline extracts `EVENTS_165674` (last 30 days) and `MERGE_IDS_165674` (full) from Snowflake to local parquet.

```python
"""Amplitude dlt pipeline — events and merge IDs from Snowflake to local parquet."""

import logging
import os
from datetime import datetime, timedelta, timezone

import dlt
from dlt.sources.sql_database import sql_database
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def _snowflake_connection_url() -> str:
    """Build Snowflake connection URL for key-pair auth."""
    account = os.environ["SNOWFLAKE_ACCOUNT"]
    user = os.environ["SNOWFLAKE_USER"]
    database = os.environ["SNOWFLAKE_DATABASE"]
    warehouse = os.environ["SNOWFLAKE_WAREHOUSE"]
    role = os.environ["SNOWFLAKE_ROLE"]
    key_path = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]

    from cryptography.hazmat.primitives import serialization

    with open(key_path, "rb") as f:
        private_key = serialization.load_pem_private_key(f.read(), password=None)

    # Encode private key as DER for snowflake-connector-python
    pkb = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    import snowflake.connector

    conn = snowflake.connector.connect(
        account=account,
        user=user,
        private_key=pkb,
        database=database,
        warehouse=warehouse,
        role=role,
    )
    return conn


def run_events_pipeline(
    data_dir: str = "./data",
    days_back: int = 30,
) -> None:
    """Extract amplitude events (last N days) to local parquet."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%d")
    logger.info(f"Extracting events since {cutoff}")

    conn = _snowflake_connection_url()

    source = sql_database(
        credentials=conn,
        schema="AMPLITUDE",
        table_names=["EVENTS_165674"],
    )

    # Add date filter for events
    source.resources["EVENTS_165674"].apply_hints(
        incremental=dlt.sources.incremental("EVENT_TIME", initial_value=cutoff),
        write_disposition="replace",
    )

    pipeline = dlt.pipeline(
        pipeline_name="amplitude_events",
        destination=dlt.destinations.filesystem(bucket_url=data_dir),
        dataset_name="amplitude",
    )

    info = pipeline.run(source, loader_file_format="parquet")
    logger.info(f"Events pipeline completed: {info}")


def run_merge_ids_pipeline(
    data_dir: str = "./data",
) -> None:
    """Extract amplitude merge IDs (full) to local parquet."""
    conn = _snowflake_connection_url()

    source = sql_database(
        credentials=conn,
        schema="AMPLITUDE",
        table_names=["MERGE_IDS_165674"],
    )

    pipeline = dlt.pipeline(
        pipeline_name="amplitude_merge_ids",
        destination=dlt.destinations.filesystem(bucket_url=data_dir),
        dataset_name="amplitude",
    )

    info = pipeline.run(source, loader_file_format="parquet")
    logger.info(f"Merge IDs pipeline completed: {info}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_events_pipeline()
    run_merge_ids_pipeline()
```

**Step 2: Test the pipeline runs**

```bash
cd /home/tmo/roadtothebeach/propel/client-zeffy-fyrnheim
uv run python -m zeffy.pipelines.amplitude
```

Expected: parquet files appear in `data/amplitude/events_165674/` and `data/amplitude/merge_ids_165674/`.

**Step 3: Verify data landed**

```bash
uv run python -c "
import duckdb
conn = duckdb.connect()
events = conn.read_parquet('data/amplitude/events_165674/**/*.parquet')
print(f'Events: {events.count(\"*\").fetchone()[0]} rows')
print(events.columns)
merges = conn.read_parquet('data/amplitude/merge_ids_165674/**/*.parquet')
print(f'Merge IDs: {merges.count(\"*\").fetchone()[0]} rows')
"
```

**Step 4: Commit**

```bash
git add src/zeffy/pipelines/amplitude.py
git commit -m "feat: add amplitude dlt pipeline (events + merge IDs)"
```

---

### Task 3: dlt pipeline — organizations

**Files:**
- Create: `src/zeffy/pipelines/organizations.py`

**Step 1: Write the organizations pipeline**

```python
"""Organizations dlt pipeline — org data from Snowflake to local parquet."""

import logging
import os

import dlt
from dlt.sources.sql_database import sql_database
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def _get_snowflake_conn():
    """Build Snowflake connection with key-pair auth."""
    from zeffy.pipelines.amplitude import _snowflake_connection_url
    return _snowflake_connection_url()


def run_pipeline(
    data_dir: str = "./data",
) -> None:
    """Extract organization tables to local parquet."""
    conn = _get_snowflake_conn()

    source = sql_database(
        credentials=conn,
        schema="PRODUCTION_ALL",
        table_names=["SQL_ORGANIZATION", "SQL_ORGANIZATION_QUALIFICATION_QUESTIONS"],
    )

    pipeline = dlt.pipeline(
        pipeline_name="organizations",
        destination=dlt.destinations.filesystem(bucket_url=data_dir),
        dataset_name="organizations",
    )

    info = pipeline.run(source, loader_file_format="parquet")
    logger.info(f"Organizations pipeline completed: {info}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_pipeline()
```

**Step 2: Test the pipeline**

```bash
cd /home/tmo/roadtothebeach/propel/client-zeffy-fyrnheim
uv run python -m zeffy.pipelines.organizations
```

**Step 3: Verify**

```bash
uv run python -c "
import duckdb
conn = duckdb.connect()
orgs = conn.read_parquet('data/organizations/sql_organization/**/*.parquet')
print(f'Organizations: {orgs.count(\"*\").fetchone()[0]} rows')
quals = conn.read_parquet('data/organizations/sql_organization_qualification_questions/**/*.parquet')
print(f'Qualification questions: {quals.count(\"*\").fetchone()[0]} rows')
"
```

**Step 4: Commit**

```bash
git add src/zeffy/pipelines/organizations.py
git commit -m "feat: add organizations dlt pipeline"
```

---

### Task 4: Entity — touchpoints

**Files:**
- Create: `entities/touchpoints.py`

**Step 1: Explore the actual parquet columns**

Before writing the entity, inspect what dlt actually extracted:

```bash
uv run python -c "
import duckdb
conn = duckdb.connect()
t = conn.read_parquet('data/amplitude/events_165674/**/*.parquet')
for col in sorted(t.columns):
    print(col)
"
```

This tells us the exact column names (Snowflake may uppercase them; dlt may normalize).

**Step 2: Write the touchpoints entity**

Create `entities/touchpoints.py`. The exact column references will depend on what Step 1 reveals, but the structure follows this pattern:

```python
"""Touchpoints entity — acquisition-relevant amplitude events with channel classification."""

from fyrnheim import Entity, LayersConfig, PrepLayer, DimensionLayer
from fyrnheim.components.computed_column import ComputedColumn
from fyrnheim.components.expressions import CaseColumn, contains_any, isin_literal
from fyrnheim.core.source import TableSource

touchpoints = Entity(
    name="touchpoints",
    description="Acquisition-relevant amplitude events with channel classification",
    source=TableSource(
        table="events_165674",
        duckdb_path="./data/amplitude/events_165674/**/*.parquet",
    ),
    layers=LayersConfig(
        prep=PrepLayer(
            model_name="prep_touchpoints",
            computed_columns=[
                # JSON extraction — adjust field names based on Step 1
                # These use json_extract_scalar from primitives or raw ibis expressions
                ComputedColumn(
                    name="gclid",
                    expression="t.event_properties.json_extract_scalar('gclid')",
                    description="Google click ID",
                ),
                ComputedColumn(
                    name="fbclid",
                    expression="t.event_properties.json_extract_scalar('fbclid')",
                    description="Facebook click ID",
                ),
                ComputedColumn(
                    name="utm_source",
                    expression="t.event_properties.json_extract_scalar('utm_source')",
                ),
                ComputedColumn(
                    name="utm_medium",
                    expression="t.event_properties.json_extract_scalar('utm_medium')",
                ),
                ComputedColumn(
                    name="utm_campaign",
                    expression="t.event_properties.json_extract_scalar('utm_campaign')",
                ),
                ComputedColumn(
                    name="referring_domain",
                    expression="t.event_properties.json_extract_scalar('referring_domain')",
                ),
                ComputedColumn(
                    name="organization_id",
                    expression="t.user_properties.json_extract_scalar('organization_zeffy_id')",
                ),
                ComputedColumn(
                    name="user_agent_str",
                    expression="t.event_properties.json_extract_scalar('user_agent')",
                ),
                # Channel classification — cascading priority rules
                CaseColumn(
                    name="channel",
                    cases=[
                        ("t.event_properties.json_extract_scalar('gclid').notnull()", "paid_search_google"),
                        ("t.event_properties.json_extract_scalar('fbclid').notnull()", "paid_social_meta"),
                        (isin_literal("t.event_properties.json_extract_scalar('utm_medium')", ["cpc", "ppc", "paid", "paidsocial"]), "paid_other"),
                        # Add more rules as needed based on data exploration
                    ],
                    default="direct_or_unknown",
                ),
            ],
        ),
    ),
)
```

NOTE: The exact column names and JSON paths depend on Task 4 Step 1 output. Adjust the entity definition to match. The channel classification rules should follow the Zeffy dbt project's logic in `models/attribution/fact/fct_signal.sql` and `fct_signal_enriched.sql`.

**Step 3: Test the entity loads**

```bash
cd /home/tmo/roadtothebeach/propel/client-zeffy-fyrnheim
uv run fyr list
```

Expected: touchpoints entity appears in the list.

**Step 4: Generate and run**

```bash
uv run fyr generate --entities-dir entities/
uv run fyr run --entity touchpoints
```

**Step 5: Verify output**

```bash
uv run python -c "
import duckdb
conn = duckdb.connect()
t = conn.read_parquet('generated/touchpoints_transforms.py')  # or check DuckDB
# Actually verify via executor output
"
```

**Step 6: Commit**

```bash
git add entities/touchpoints.py
git commit -m "feat: add touchpoints entity with channel classification"
```

---

### Task 5: Entity — account

**Files:**
- Create: `entities/account.py`

**Step 1: Explore organization parquet columns**

```bash
uv run python -c "
import duckdb
conn = duckdb.connect()
t = conn.read_parquet('data/organizations/sql_organization/**/*.parquet')
for col in sorted(t.columns):
    print(col)
"
```

**Step 2: Write the account entity**

```python
"""Account entity — identity graph merging organizations + amplitude data."""

from fyrnheim import Entity, LayersConfig, DimensionLayer
from fyrnheim.components.computed_column import ComputedColumn
from fyrnheim.core.source import (
    DerivedSource,
    IdentityGraphConfig,
    IdentityGraphSource,
    TableSource,
)
from fyrnheim.core.types import SourcePriority

account = Entity(
    name="account",
    description="Unified account from organization data + amplitude identity resolution",
    source=DerivedSource(
        identity_graph_config=IdentityGraphConfig(
            match_key="organization_id",
            priority=["organizations", "amplitude_merges"],
            sources=[
                IdentityGraphSource(
                    name="organizations",
                    source=TableSource(
                        table="sql_organization",
                        duckdb_path="./data/organizations/sql_organization/**/*.parquet",
                    ),
                    fields=["name", "region", "referral_question"],
                    match_key_field="id",
                    id_field="id",
                    date_field="created_at_utc",
                ),
                IdentityGraphSource(
                    name="amplitude_merges",
                    source=TableSource(
                        table="merge_ids_165674",
                        duckdb_path="./data/amplitude/merge_ids_165674/**/*.parquet",
                    ),
                    fields=["merged_amplitude_id"],
                    match_key_field="amplitude_id",
                ),
            ],
        ),
    ),
    layers=LayersConfig(
        dimension=DimensionLayer(
            model_name="dim_account",
            computed_columns=[
                ComputedColumn(
                    name="account_name",
                    expression="t.name",
                ),
            ],
        ),
    ),
)
```

NOTE: Column names depend on what dlt produces (Task 5 Step 1). The identity graph config may need adjustment based on actual Snowflake column names vs what dlt normalizes to.

**Step 3: Test**

```bash
uv run fyr generate --entities-dir entities/
uv run fyr run --entity account
```

**Step 4: Commit**

```bash
git add entities/account.py
git commit -m "feat: add account entity with identity graph"
```

---

### Task 6: Entity — attribution

**Files:**
- Create: `entities/attribution.py`

**Step 1: Write the attribution entity**

```python
"""Attribution entity — first touch + paid priority models per account."""

from fyrnheim import Entity, LayersConfig, DimensionLayer
from fyrnheim.components.computed_column import ComputedColumn
from fyrnheim.components.expressions import CaseColumn, dedup_by, first_value_by
from fyrnheim.core.source import AggregationSource

attribution = Entity(
    name="attribution",
    description="Marketing attribution models: first touch and paid priority per account",
    source=AggregationSource(
        source_entity="touchpoints",
        group_by_column="organization_id",
        filter_expression="t.event_type != 'signup'",  # pre-signup signals only
        aggregations=[
            # These will use the window function helpers from M021
        ],
    ),
    layers=LayersConfig(
        dimension=DimensionLayer(
            model_name="dim_attribution",
            computed_columns=[
                # First touch: earliest channel
                ComputedColumn(
                    name="first_touch_channel",
                    expression=first_value_by("t.channel", "t.organization_id", "t.event_time"),
                    description="Channel of first touchpoint",
                ),
                # Paid priority: cascading rules
                CaseColumn(
                    name="paid_priority_channel",
                    cases=[
                        ("t.gclid.notnull().any()", "paid_search_google"),
                        ("t.fbclid.notnull().any()", "paid_social_meta"),
                    ],
                    default="first_touch_channel",
                    description="Channel with paid priority override",
                ),
            ],
        ),
    ),
)
```

NOTE: The exact AggregationSource config and computed column expressions will need tuning based on what the touchpoints entity actually produces. The attribution logic should reference the Zeffy dbt models in `models/attribution/attributed/` for the rule details.

**Step 2: Test**

```bash
uv run fyr generate --entities-dir entities/
uv run fyr run --entity attribution
```

**Step 3: Verify the full pipeline**

```bash
uv run fyr run  # runs all entities in dependency order
```

**Step 4: Commit**

```bash
git add entities/attribution.py
git commit -m "feat: add attribution entity with first touch + paid priority"
```

---

### Task 7: Verify end-to-end and document

**Step 1: Run full pipeline**

```bash
cd /home/tmo/roadtothebeach/propel/client-zeffy-fyrnheim
uv run fyr run
```

Verify all 3 entities execute without errors.

**Step 2: Check output tables**

```bash
uv run python -c "
from fyrnheim.engine.executor import IbisExecutor

with IbisExecutor.duckdb() as executor:
    # Check each table exists and has rows
    for table in ['dim_touchpoints', 'dim_account', 'dim_attribution']:
        try:
            t = executor.connection.table(table)
            print(f'{table}: {t.count().execute()} rows, {len(t.columns)} columns')
        except Exception as e:
            print(f'{table}: MISSING - {e}')
"
```

**Step 3: Final commit**

```bash
git add -A
git commit -m "chore: finalize zeffy attribution pipeline"
```
