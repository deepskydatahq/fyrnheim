"""Tests for pipeline orchestrator."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from fyrnheim.config import ResolvedConfig
from fyrnheim.core.activity import ActivityDefinition, EventOccurred, RowAppeared
from fyrnheim.core.metrics_model import MetricField, MetricsModel
from fyrnheim.core.source import EventSource, StateSource
from fyrnheim.engine.executor import IbisExecutor
from fyrnheim.engine.pipeline import PipelineResult, run_pipeline


def _make_config(tmp_path: Path) -> ResolvedConfig:
    return ResolvedConfig(
        entities_dir=tmp_path / "entities",
        data_dir=tmp_path / "data",
        output_dir=tmp_path / "output",
        backend="duckdb",
        project_root=tmp_path,
    )


def _write_parquet(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(str(path))


class TestRunPipelineStateSources:
    """Pipeline with StateSource only."""

    def test_state_source_produces_events_and_metrics(self, tmp_path: Path) -> None:
        """StateSource through SnapshotDiffPipeline produces events."""
        config = _make_config(tmp_path)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # Write sample data
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

        activity = ActivityDefinition(
            name="signup",
            source="customers",
            trigger=RowAppeared(),
            entity_id_field="id",
        )

        metrics = MetricsModel(
            name="signup_metrics",
            sources=["customers"],
            grain="daily",
            metric_fields=[
                MetricField(field_name="signup", aggregation="count"),
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
        assert "signup_metrics" in result.outputs
        assert result.outputs["signup_metrics"] > 0
        assert not result.errors

        # Verify output file exists
        out_file = config.output_dir / "signup_metrics.parquet"
        assert out_file.exists()


class TestRunPipelineEventSources:
    """Pipeline with EventSource only."""

    def test_event_source_loads_and_produces_metrics(self, tmp_path: Path) -> None:
        """EventSource through event_source_loader produces events."""
        config = _make_config(tmp_path)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        parquet_path = data_dir / "page_views.parquet"
        _write_parquet(
            parquet_path,
            pd.DataFrame(
                {
                    "user_id": ["u1", "u2", "u1"],
                    "viewed_at": ["2024-01-01", "2024-01-01", "2024-01-02"],
                    "page": ["/home", "/about", "/pricing"],
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
            name="page_view_metrics",
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

        out_file = config.output_dir / "page_view_metrics.parquet"
        assert out_file.exists()
        df = pd.read_parquet(str(out_file))
        assert len(df) > 0


class TestRunPipelineMixed:
    """Pipeline with both StateSource and EventSource."""

    def test_mixed_sources_concatenated(self, tmp_path: Path) -> None:
        """Both source types produce events that are concatenated."""
        config = _make_config(tmp_path)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # State source
        customers_path = data_dir / "customers.parquet"
        _write_parquet(
            customers_path,
            pd.DataFrame({"id": ["1"], "name": ["alice"], "plan": ["free"]}),
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
                    "user_id": ["u1"],
                    "viewed_at": ["2024-01-01"],
                    "page": ["/home"],
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

        assets = {
            "sources": [state_source, event_source],
            "activities": [],
            "identity_graphs": [],
            "analytics_entities": [],
            "metrics_models": [],
        }

        executor = IbisExecutor.duckdb()
        result = run_pipeline(assets, config, executor)

        assert result.source_count == 2
        assert not result.errors


class TestRunPipelineEmptyAndErrors:
    """Edge cases: empty sources, zero sources."""

    def test_zero_sources_returns_empty_result(self, tmp_path: Path) -> None:
        """Pipeline with no sources returns empty result, not exception."""
        config = _make_config(tmp_path)
        assets = {
            "sources": [],
            "activities": [],
            "identity_graphs": [],
            "analytics_entities": [],
            "metrics_models": [],
        }

        executor = IbisExecutor.duckdb()
        result = run_pipeline(assets, config, executor)

        assert result.source_count == 0
        assert result.output_count == 0
        assert not result.errors

    def test_returns_pipeline_result_type(self, tmp_path: Path) -> None:
        """run_pipeline returns a PipelineResult."""
        config = _make_config(tmp_path)
        assets = {
            "sources": [],
            "activities": [],
            "identity_graphs": [],
            "analytics_entities": [],
            "metrics_models": [],
        }
        executor = IbisExecutor.duckdb()
        result = run_pipeline(assets, config, executor)
        assert isinstance(result, PipelineResult)
