"""End-to-end test: StagingView + StateSource(upstream=view) via run_pipeline."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd

from fyrnheim.cli import _discover_assets
from fyrnheim.config import ResolvedConfig
from fyrnheim.engine.executor import IbisExecutor
from fyrnheim.engine.pipeline import run_pipeline

ENTITIES_SRC = '''
from fyrnheim.core.staging_view import StagingView
from fyrnheim.core.source import StateSource

users_view = StagingView(
    name="stg_users",
    project="main",
    dataset="analytics",
    sql="SELECT 1 AS id, 'alice' AS name UNION ALL SELECT 2 AS id, 'bob' AS name",
)

users_source = StateSource(
    upstream=users_view,
    name="users",
    id_field="id",
    snapshot_grain="daily",
    duckdb_path="users.parquet",
)
'''


def test_staging_view_feeds_state_source_e2e() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        entities_dir = root / "entities"
        entities_dir.mkdir()
        data_dir = root / "data"
        data_dir.mkdir()
        output_dir = root / "out"
        output_dir.mkdir()

        # Write a parquet file for the StateSource's duckdb_path.
        # (In duckdb backend, StateSource still loads from parquet;
        # upstream= provides the (project, dataset, table) metadata.)
        pd.DataFrame({"id": [1, 2], "name": ["alice", "bob"]}).to_parquet(
            str(data_dir / "users.parquet")
        )

        (entities_dir / "entities.py").write_text(ENTITIES_SRC)

        assets = _discover_assets(entities_dir)
        assert len(assets["staging_views"]) == 1
        assert len(assets["sources"]) == 1

        # Verify upstream resolution filled (project, dataset, table)
        source = assets["sources"][0]
        assert source.project == "main"
        assert source.dataset == "analytics"
        assert source.table == "stg_users"

        config = ResolvedConfig(
            project_root=root,
            entities_dir=entities_dir,
            data_dir=data_dir,
            output_dir=output_dir,
            backend="duckdb",
            backend_config={},
        )

        with IbisExecutor.duckdb() as ex:
            result = run_pipeline(assets, config, ex)

            # Phase 0: staging view was materialized
            assert "stg_users" in result.staging_materialized
            assert ex.view_exists("main", "analytics", "stg_users")

            # The materialized view returns non-empty data
            view_df = ex.connection.table(
                "stg_users", database="analytics"
            ).execute()
            assert len(view_df) == 2
            assert set(view_df["name"].tolist()) == {"alice", "bob"}

            # The StateSource can read its upstream parquet (non-empty)
            loaded = source.read_table(
                ex.connection, config.backend, data_dir=config.data_dir
            )
            df = loaded.execute()
            assert len(df) == 2
