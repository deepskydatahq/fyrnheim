"""Shared pytest fixtures for the fyrnheim test suite.

Defines the ``benchmark_result`` fixture: a session-scoped
:class:`fyrnheim.engine.pipeline.PipelineResult` produced by running the
smallest duckdb-backed pipeline we have. It exists so perf-oriented tests
(see M058 / M059 / M060) can assert on :attr:`PipelineResult.timings`
without each spinning up their own pipeline.

The underlying example mirrors ``tests/test_pipeline_e2e.py``'s
``TestEventSourceOnlyPipeline`` — an EventSource + activity + metrics
model — so this fixture tracks the same canonical smallest example as
the rest of the suite.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from fyrnheim.config import ResolvedConfig
from fyrnheim.core.activity import ActivityDefinition, EventOccurred
from fyrnheim.core.metrics_model import MetricField, MetricsModel
from fyrnheim.core.source import EventSource
from fyrnheim.engine.executor import IbisExecutor
from fyrnheim.engine.pipeline import PipelineResult, run_pipeline


@pytest.fixture(scope="session")
def benchmark_result(tmp_path_factory: pytest.TempPathFactory) -> PipelineResult:
    """Run a minimal duckdb-backed pipeline once per session.

    Mirrors the EventSource-only example in
    ``tests/test_pipeline_e2e.py`` so the benchmark harness is exercised
    against the same canonical small fixture as the rest of the suite.
    """
    tmp_path = tmp_path_factory.mktemp("benchmark_result")

    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    parquet_path = data_dir / "page_views.parquet"
    pd.DataFrame(
        {
            "user_id": ["u1", "u2", "u1", "u3"],
            "viewed_at": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02"],
            "page": ["/home", "/about", "/pricing", "/home"],
            "referrer": ["google", "direct", "google", "twitter"],
        }
    ).to_parquet(str(parquet_path))

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
        metric_fields=[MetricField(field_name="page_viewed", aggregation="count")],
    )

    assets: dict[str, list] = {
        "sources": [source],
        "activities": [activity],
        "identity_graphs": [],
        "analytics_entities": [],
        "metrics_models": [metrics],
    }

    config = ResolvedConfig(
        entities_dir=tmp_path / "entities",
        data_dir=data_dir,
        output_dir=output_dir,
        backend="duckdb",
        project_root=Path(tmp_path),
    )

    executor = IbisExecutor.duckdb()
    try:
        return run_pipeline(assets, config, executor)
    finally:
        executor.close()
