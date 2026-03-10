"""E2E tests for incremental materialization strategies.

Tests APPEND and MERGE strategies via IbisExecutor on DuckDB.
Also verifies backward compatibility: non-incremental entities
still use full refresh (overwrite).
"""

from __future__ import annotations

import pandas as pd

from fyrnheim import (
    DimensionLayer,
    Entity,
    LayersConfig,
    TableSource,
)
from fyrnheim._generate import generate
from fyrnheim.core.types import IncrementalStrategy, MaterializationType
from fyrnheim.engine.connection import create_connection
from fyrnheim.engine.executor import IbisExecutor

# ---------------------------------------------------------------------------
# APPEND strategy tests
# ---------------------------------------------------------------------------


class TestAppendStrategy:
    """APPEND incremental strategy E2E tests."""

    def test_first_run_creates_table(self, tmp_path):
        """First run with APPEND creates table normally."""
        parquet_path, generated_dir = _setup_append_entity(tmp_path)

        # Write initial data
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "event_id": [100, 200, 300],
        })
        df.to_parquet(parquet_path, index=False)

        entity = _make_append_entity(str(parquet_path))
        generate(entity, output_dir=generated_dir)

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=generated_dir) as executor:
            result = executor.execute("events", entity=entity)

            assert result.success is True
            assert result.row_count == 3

    def test_second_run_appends_only_new_rows(self, tmp_path):
        """Second run inserts only rows with event_id > max existing."""
        parquet_path, generated_dir = _setup_append_entity(tmp_path)

        # Initial data
        df1 = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "event_id": [100, 200, 300],
        })
        df1.to_parquet(parquet_path, index=False)

        entity = _make_append_entity(str(parquet_path))
        generate(entity, output_dir=generated_dir)

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=generated_dir) as executor:
            # First run
            executor.execute("events", entity=entity)

            # Update source with new + overlapping data
            df2 = pd.DataFrame({
                "id": [3, 4, 5],
                "name": ["Carol", "Dave", "Eve"],
                "event_id": [300, 400, 500],
            })
            df2.to_parquet(parquet_path, index=False)
            # Re-register the source
            conn.read_parquet(str(parquet_path), table_name="source_events")

            # Second run
            result = executor.execute("events", entity=entity)

            assert result.success is True
            # Should have original 3 + 2 new (event_id 400, 500)
            assert result.row_count == 5

            # Verify no duplicates
            final_df = conn.table("dim_events").to_pandas()
            assert len(final_df) == 5
            assert set(final_df["event_id"]) == {100, 200, 300, 400, 500}

    def test_second_run_no_new_rows(self, tmp_path):
        """Second run with no new rows doesn't add anything."""
        parquet_path, generated_dir = _setup_append_entity(tmp_path)

        df = pd.DataFrame({
            "id": [1, 2],
            "name": ["Alice", "Bob"],
            "event_id": [100, 200],
        })
        df.to_parquet(parquet_path, index=False)

        entity = _make_append_entity(str(parquet_path))
        generate(entity, output_dir=generated_dir)

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=generated_dir) as executor:
            executor.execute("events", entity=entity)

            # Same data, re-register
            conn.read_parquet(str(parquet_path), table_name="source_events")
            result = executor.execute("events", entity=entity)

            assert result.row_count == 2


# ---------------------------------------------------------------------------
# MERGE strategy tests
# ---------------------------------------------------------------------------


class TestMergeStrategy:
    """MERGE incremental strategy E2E tests."""

    def test_first_run_creates_table(self, tmp_path):
        """First run with MERGE creates table normally."""
        parquet_path, generated_dir = _setup_merge_entity(tmp_path)

        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "email": ["a@x.com", "b@x.com", "c@x.com"],
        })
        df.to_parquet(parquet_path, index=False)

        entity = _make_merge_entity(str(parquet_path))
        generate(entity, output_dir=generated_dir)

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=generated_dir) as executor:
            result = executor.execute("users", entity=entity)

            assert result.success is True
            assert result.row_count == 3

    def test_second_run_adds_new_rows(self, tmp_path):
        """Second run with new unique_key rows adds them."""
        parquet_path, generated_dir = _setup_merge_entity(tmp_path)

        df1 = pd.DataFrame({
            "id": [1, 2],
            "name": ["Alice", "Bob"],
            "email": ["a@x.com", "b@x.com"],
        })
        df1.to_parquet(parquet_path, index=False)

        entity = _make_merge_entity(str(parquet_path))
        generate(entity, output_dir=generated_dir)

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=generated_dir) as executor:
            executor.execute("users", entity=entity)

            # New source with additional rows
            df2 = pd.DataFrame({
                "id": [3, 4],
                "name": ["Carol", "Dave"],
                "email": ["c@x.com", "d@x.com"],
            })
            df2.to_parquet(parquet_path, index=False)
            conn.read_parquet(str(parquet_path), table_name="source_users")

            result = executor.execute("users", entity=entity)

            assert result.row_count == 4
            final_df = conn.table("dim_users").to_pandas()
            assert set(final_df["id"]) == {1, 2, 3, 4}

    def test_second_run_updates_existing_rows(self, tmp_path):
        """Second run with same unique_key updates those rows."""
        parquet_path, generated_dir = _setup_merge_entity(tmp_path)

        df1 = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "email": ["a@x.com", "b@x.com", "c@x.com"],
        })
        df1.to_parquet(parquet_path, index=False)

        entity = _make_merge_entity(str(parquet_path))
        generate(entity, output_dir=generated_dir)

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=generated_dir) as executor:
            executor.execute("users", entity=entity)

            # Update Bob's email, add Dave
            df2 = pd.DataFrame({
                "id": [2, 4],
                "name": ["Bob", "Dave"],
                "email": ["bob_new@x.com", "d@x.com"],
            })
            df2.to_parquet(parquet_path, index=False)
            conn.read_parquet(str(parquet_path), table_name="source_users")

            result = executor.execute("users", entity=entity)

            assert result.row_count == 4
            final_df = conn.table("dim_users").to_pandas()
            bob = final_df[final_df["id"] == 2].iloc[0]
            assert bob["email"] == "bob_new@x.com"
            # Alice and Carol preserved
            assert set(final_df["id"]) == {1, 2, 3, 4}

    def test_existing_rows_not_in_new_result_are_preserved(self, tmp_path):
        """Rows not in new result are kept (not deleted)."""
        parquet_path, generated_dir = _setup_merge_entity(tmp_path)

        df1 = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "email": ["a@x.com", "b@x.com", "c@x.com"],
        })
        df1.to_parquet(parquet_path, index=False)

        entity = _make_merge_entity(str(parquet_path))
        generate(entity, output_dir=generated_dir)

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=generated_dir) as executor:
            executor.execute("users", entity=entity)

            # Second run only has id=2 updated
            df2 = pd.DataFrame({
                "id": [2],
                "name": ["Bob Updated"],
                "email": ["bob2@x.com"],
            })
            df2.to_parquet(parquet_path, index=False)
            conn.read_parquet(str(parquet_path), table_name="source_users")

            result = executor.execute("users", entity=entity)

            assert result.row_count == 3
            final_df = conn.table("dim_users").to_pandas()
            assert set(final_df["id"]) == {1, 2, 3}
            alice = final_df[final_df["id"] == 1].iloc[0]
            assert alice["name"] == "Alice"


# ---------------------------------------------------------------------------
# Backward compatibility: non-incremental still does full refresh
# ---------------------------------------------------------------------------


class TestFullRefreshBackwardCompat:
    """Non-incremental entities still overwrite on each run."""

    def test_full_refresh_overwrites(self, tmp_path):
        """TABLE materialization overwrites on second run."""
        parquet_path = tmp_path / "data" / "items.parquet"
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        generated_dir = tmp_path / "generated"

        df1 = pd.DataFrame({
            "id": [1, 2, 3],
            "value": [10, 20, 30],
        })
        df1.to_parquet(parquet_path, index=False)

        entity = Entity(
            name="items",
            description="Non-incremental entity",
            source=TableSource(
                project="test", dataset="test", table="items",
                duckdb_path=str(parquet_path),
            ),
            layers=LayersConfig(
                dimension=DimensionLayer(
                    model_name="dim_items",
                    materialization=MaterializationType.TABLE,
                ),
            ),
        )
        generate(entity, output_dir=generated_dir)

        conn = create_connection("duckdb")
        with IbisExecutor(conn=conn, backend="duckdb", generated_dir=generated_dir) as executor:
            executor.execute("items", entity=entity)

            # Second run with different data
            df2 = pd.DataFrame({
                "id": [4, 5],
                "value": [40, 50],
            })
            df2.to_parquet(parquet_path, index=False)
            conn.read_parquet(str(parquet_path), table_name="source_items")

            result = executor.execute("items", entity=entity)

            # Should be 2, not 5 — full overwrite
            assert result.row_count == 2


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _setup_append_entity(tmp_path):
    """Create dirs and return (parquet_path, generated_dir)."""
    parquet_path = tmp_path / "data" / "events.parquet"
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    generated_dir = tmp_path / "generated"
    return parquet_path, generated_dir


def _setup_merge_entity(tmp_path):
    """Create dirs and return (parquet_path, generated_dir)."""
    parquet_path = tmp_path / "data" / "users.parquet"
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    generated_dir = tmp_path / "generated"
    return parquet_path, generated_dir


def _make_append_entity(parquet_path: str) -> Entity:
    """Create an entity with APPEND incremental strategy."""
    return Entity(
        name="events",
        description="Incremental append entity",
        source=TableSource(
            project="test", dataset="test", table="events",
            duckdb_path=parquet_path,
        ),
        layers=LayersConfig(
            dimension=DimensionLayer(
                model_name="dim_events",
                materialization=MaterializationType.INCREMENTAL,
                incremental_strategy=IncrementalStrategy.APPEND,
                incremental_key="event_id",
            ),
        ),
    )


def _make_merge_entity(parquet_path: str) -> Entity:
    """Create an entity with MERGE incremental strategy."""
    return Entity(
        name="users",
        description="Incremental merge entity",
        source=TableSource(
            project="test", dataset="test", table="users",
            duckdb_path=parquet_path,
        ),
        layers=LayersConfig(
            dimension=DimensionLayer(
                model_name="dim_users",
                materialization=MaterializationType.INCREMENTAL,
                incremental_strategy=IncrementalStrategy.MERGE,
                unique_key="id",
            ),
        ),
    )
