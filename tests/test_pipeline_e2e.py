"""E2E integration tests for the full pipeline and CLI.

Tests with temp dirs, sample parquet, and entity definitions.
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pytest
from click.testing import CliRunner

from fyrnheim.cli import main
from fyrnheim.config import ResolvedConfig
from fyrnheim.core.activity import ActivityDefinition, EventOccurred, RowAppeared
from fyrnheim.core.analytics_entity import AnalyticsEntity, Measure, StateField
from fyrnheim.core.metrics_model import MetricField, MetricsModel
from fyrnheim.core.source import EventSource, StateSource
from fyrnheim.engine.executor import IbisExecutor
from fyrnheim.engine.pipeline import run_pipeline


def _make_config(tmp_path: Path) -> ResolvedConfig:
    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    return ResolvedConfig(
        entities_dir=tmp_path / "entities",
        data_dir=tmp_path / "data",
        output_dir=output_dir,
        backend="duckdb",
        project_root=tmp_path,
    )


def _write_parquet(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(str(path))


class TestStateSourceOnlyPipeline:
    """StateSource-only: creates snapshot, produces events, writes metrics parquet."""

    def test_state_source_pipeline_end_to_end(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        data_dir = tmp_path / "data"
        data_dir.mkdir(exist_ok=True)

        parquet_path = data_dir / "customers.parquet"
        _write_parquet(
            parquet_path,
            pd.DataFrame(
                {
                    "id": ["1", "2", "3"],
                    "name": ["alice", "bob", "charlie"],
                    "plan": ["free", "pro", "free"],
                    "email": ["a@test.com", "b@test.com", "c@test.com"],
                }
            ),
        )

        source = StateSource(
            name="customers",
            project="test",
            dataset="test",
            table="customers",
            duckdb_path=str(parquet_path),
            id_field="id",
        )

        signup = ActivityDefinition(
            name="signup",
            source="customers",
            trigger=RowAppeared(),
            entity_id_field="id",
        )

        metrics = MetricsModel(
            name="customer_metrics",
            sources=["customers"],
            grain="daily",
            metric_fields=[
                MetricField(field_name="signup", aggregation="count"),
            ],
        )

        assets = {
            "sources": [source],
            "activities": [signup],
            "identity_graphs": [],
            "analytics_entities": [],
            "metrics_models": [metrics],
        }

        executor = IbisExecutor.duckdb()
        result = run_pipeline(assets, config, executor)

        assert result.source_count == 1
        assert result.output_count == 1
        assert not result.errors

        # Verify output parquet
        out_file = config.output_dir / "customer_metrics.parquet"
        assert out_file.exists()
        df = pd.read_parquet(str(out_file))
        assert len(df) > 0
        assert "signup_count" in df.columns

        # Verify snapshot was created
        snapshot_dir = config.output_dir / "snapshots" / "customers"
        assert snapshot_dir.exists()
        snapshots = list(snapshot_dir.glob("*.parquet"))
        assert len(snapshots) == 1


class TestEventSourceOnlyPipeline:
    """EventSource-only: loads events, writes metrics parquet."""

    def test_event_source_pipeline_end_to_end(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        data_dir = tmp_path / "data"
        data_dir.mkdir(exist_ok=True)

        parquet_path = data_dir / "page_views.parquet"
        _write_parquet(
            parquet_path,
            pd.DataFrame(
                {
                    "user_id": ["u1", "u2", "u1", "u3"],
                    "viewed_at": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02"],
                    "page": ["/home", "/about", "/pricing", "/home"],
                    "referrer": ["google", "direct", "google", "twitter"],
                }
            ),
        )

        source = EventSource(
            name="page_views",
            project="test",
            dataset="test",
            table="page_views",
            duckdb_path=str(parquet_path),
            entity_id_field="user_id",
            timestamp_field="viewed_at",
            event_type="page_view",
        )

        activity = ActivityDefinition(
            name="page_viewed",
            source="page_views",
            trigger=EventOccurred(event_type="page_view"),
            entity_id_field="user_id",
        )

        metrics = MetricsModel(
            name="page_metrics",
            sources=["page_views"],
            grain="daily",
            metric_fields=[
                MetricField(field_name="page_viewed", aggregation="count"),
            ],
        )

        assets = {
            "sources": [source],
            "activities": [activity],
            "identity_graphs": [],
            "analytics_entities": [],
            "metrics_models": [metrics],
        }

        executor = IbisExecutor.duckdb()
        result = run_pipeline(assets, config, executor)

        assert result.source_count == 1
        assert result.output_count == 1
        assert not result.errors

        out_file = config.output_dir / "page_metrics.parquet"
        assert out_file.exists()
        df = pd.read_parquet(str(out_file))
        assert len(df) > 0
        assert "page_viewed_count" in df.columns


class TestMixedPipeline:
    """Mixed pipeline: both source types produce correct output."""

    def test_mixed_sources_end_to_end(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        data_dir = tmp_path / "data"
        data_dir.mkdir(exist_ok=True)

        # State source
        customers_path = data_dir / "customers.parquet"
        _write_parquet(
            customers_path,
            pd.DataFrame(
                {
                    "id": ["1", "2"],
                    "name": ["alice", "bob"],
                    "plan": ["free", "pro"],
                }
            ),
        )

        state_source = StateSource(
            name="customers",
            project="test",
            dataset="test",
            table="customers",
            duckdb_path=str(customers_path),
            id_field="id",
        )

        # Event source
        events_path = data_dir / "page_views.parquet"
        _write_parquet(
            events_path,
            pd.DataFrame(
                {
                    "user_id": ["u1", "u2"],
                    "viewed_at": ["2024-01-01", "2024-01-01"],
                    "page": ["/home", "/about"],
                }
            ),
        )

        event_source = EventSource(
            name="page_views",
            project="test",
            dataset="test",
            table="page_views",
            duckdb_path=str(events_path),
            entity_id_field="user_id",
            timestamp_field="viewed_at",
            event_type="page_view",
        )

        signup = ActivityDefinition(
            name="signup",
            source="customers",
            trigger=RowAppeared(),
            entity_id_field="id",
        )

        page_viewed = ActivityDefinition(
            name="page_viewed",
            source="page_views",
            trigger=EventOccurred(event_type="page_view"),
            entity_id_field="user_id",
        )

        customer_metrics = MetricsModel(
            name="customer_metrics",
            sources=["customers"],
            grain="daily",
            metric_fields=[
                MetricField(field_name="signup", aggregation="count"),
            ],
        )

        page_metrics = MetricsModel(
            name="page_metrics",
            sources=["page_views"],
            grain="daily",
            metric_fields=[
                MetricField(field_name="page_viewed", aggregation="count"),
            ],
        )

        assets = {
            "sources": [state_source, event_source],
            "activities": [signup, page_viewed],
            "identity_graphs": [],
            "analytics_entities": [],
            "metrics_models": [customer_metrics, page_metrics],
        }

        executor = IbisExecutor.duckdb()
        result = run_pipeline(assets, config, executor)

        assert result.source_count == 2
        assert result.output_count == 2
        assert not result.errors

        # Both output files exist with data
        for name in ["customer_metrics", "page_metrics"]:
            out_file = config.output_dir / f"{name}.parquet"
            assert out_file.exists(), f"{name}.parquet missing"
            df = pd.read_parquet(str(out_file))
            assert len(df) > 0, f"{name}.parquet is empty"


class TestAnalyticsEntityOutput:
    """Verify analytics entity projection output."""

    def test_analytics_entity_parquet_output(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        data_dir = tmp_path / "data"
        data_dir.mkdir(exist_ok=True)

        parquet_path = data_dir / "customers.parquet"
        _write_parquet(
            parquet_path,
            pd.DataFrame(
                {
                    "id": ["1", "2"],
                    "name": ["alice", "bob"],
                    "plan": ["free", "pro"],
                }
            ),
        )

        source = StateSource(
            name="customers",
            project="test",
            dataset="test",
            table="customers",
            duckdb_path=str(parquet_path),
            id_field="id",
        )

        signup = ActivityDefinition(
            name="signup",
            source="customers",
            trigger=RowAppeared(),
            entity_id_field="id",
        )

        ae = AnalyticsEntity(
            name="customer_entity",
            state_fields=[
                StateField(name="name", source="customers", field="name", strategy="latest"),
                StateField(name="plan", source="customers", field="plan", strategy="latest"),
            ],
            measures=[
                Measure(name="signup_count", activity="signup", aggregation="count"),
            ],
        )

        assets = {
            "sources": [source],
            "activities": [signup],
            "identity_graphs": [],
            "analytics_entities": [ae],
            "metrics_models": [],
        }

        executor = IbisExecutor.duckdb()
        result = run_pipeline(assets, config, executor)

        assert result.output_count == 1
        assert not result.errors

        out_file = config.output_dir / "customer_entity.parquet"
        assert out_file.exists()
        df = pd.read_parquet(str(out_file))
        assert len(df) == 2
        assert "name" in df.columns
        assert "plan" in df.columns
        assert "signup_count" in df.columns


class TestCLIEndToEnd:
    """CLI test via CliRunner: fyr run exits 0 and produces output files."""

    def test_fyr_run_e2e_with_dynamic_entities(self, tmp_path: Path) -> None:
        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        # Write sample data
        df = pd.DataFrame(
            {
                "user_id": ["u1", "u2", "u3"],
                "event_time": ["2024-01-01", "2024-01-01", "2024-01-02"],
                "page": ["/home", "/pricing", "/home"],
            }
        )
        df.to_parquet(str(data_dir / "events.parquet"))

        # Write entity definition
        entity_code = f'''
from fyrnheim.core.source import EventSource
from fyrnheim.core.activity import ActivityDefinition, EventOccurred
from fyrnheim.core.metrics_model import MetricsModel, MetricField

events_source = EventSource(
    name="web_events",
    project="test",
    dataset="test",
    table="events",
    duckdb_path="{data_dir / 'events.parquet'}",
    entity_id_field="user_id",
    timestamp_field="event_time",
    event_type="page_view",
)

page_viewed = ActivityDefinition(
    name="page_viewed",
    source="web_events",
    trigger=EventOccurred(event_type="page_view"),
    entity_id_field="user_id",
)

daily_metrics = MetricsModel(
    name="daily_page_views",
    sources=["web_events"],
    grain="daily",
    metric_fields=[
        MetricField(field_name="page_viewed", aggregation="count"),
    ],
)
'''
        (entities_dir / "web_events.py").write_text(entity_code)

        # Write fyrnheim.yaml
        yaml_content = f"""
entities_dir: {entities_dir}
data_dir: {data_dir}
output_dir: {output_dir}
backend: duckdb
"""
        (tmp_path / "fyrnheim.yaml").write_text(yaml_content)

        runner = CliRunner()
        original_dir = os.getcwd()
        try:
            os.chdir(str(tmp_path))
            result = runner.invoke(main, ["run"])
        finally:
            os.chdir(original_dir)

        assert result.exit_code == 0, f"Output: {result.output}\nException: {result.exception}"
        assert "Sources processed:" in result.output
        assert "Outputs written:" in result.output

        # Verify output file
        out_file = output_dir / "daily_page_views.parquet"
        assert out_file.exists()
        df = pd.read_parquet(str(out_file))
        assert len(df) > 0
