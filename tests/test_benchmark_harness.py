"""Tests for the M058 benchmark harness.

Covers :class:`PipelineTimings`, the ``fyr bench`` CLI subcommand and the
session-scoped ``benchmark_result`` pytest fixture.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
from click.testing import CliRunner

from fyrnheim.cli import main
from fyrnheim.engine.pipeline import PipelineResult, PipelineTimings


def _flatten_timings(timings: PipelineTimings) -> list[float]:
    """Flatten a PipelineTimings into a list of float seconds."""
    values: list[float] = [timings.staging_views_s, timings.activities_s]
    values.extend(timings.source_loads.values())
    values.extend(timings.identity_graphs.values())
    for parts in timings.analytics_entities.values():
        values.extend(parts.values())
    for parts in timings.metrics_models.values():
        values.extend(parts.values())
    return values


# ---------------------------------------------------------------------------
# benchmark_result fixture smoke test
# ---------------------------------------------------------------------------


def test_benchmark_fixture_provides_populated_timings(
    benchmark_result: PipelineResult,
) -> None:
    """The session fixture actually populates per-source timings."""
    assert benchmark_result.timings.source_loads


def test_benchmark_result_has_no_errors(benchmark_result: PipelineResult) -> None:
    """Sanity: the underlying pipeline succeeded."""
    assert benchmark_result.errors == []
    assert benchmark_result.output_count >= 1


# ---------------------------------------------------------------------------
# Approximate-sum invariant
# ---------------------------------------------------------------------------


def test_timing_sum_close_to_elapsed(benchmark_result: PipelineResult) -> None:
    """Sum of per-phase timings stays within 0.15s of ``elapsed_seconds``.

    Phases are wrapped in sequence (no parallelism yet), so the sum of
    all timing values must track top-level wall-clock time closely —
    this is the invariant future M059 work will relax once per-source
    loads run in parallel.
    """
    timings = benchmark_result.timings
    total = sum(_flatten_timings(timings))
    assert abs(total - benchmark_result.elapsed_seconds) < 0.15, (
        f"sum={total:.6f}s elapsed={benchmark_result.elapsed_seconds:.6f}s "
        f"diff={abs(total - benchmark_result.elapsed_seconds):.6f}s"
    )


def test_metrics_model_split_shape(benchmark_result: PipelineResult) -> None:
    """Each metrics_models entry has the required project_s/write_s split."""
    assert benchmark_result.timings.metrics_models, (
        "fixture should exercise at least one metrics model"
    )
    for parts in benchmark_result.timings.metrics_models.values():
        assert set(parts) == {"project_s", "write_s"}


# ---------------------------------------------------------------------------
# `fyr bench` CLI
# ---------------------------------------------------------------------------


def _setup_bench_project(tmp_path: Path) -> None:
    """Minimal project on disk for the bench CLI tests."""
    entities_dir = tmp_path / "entities"
    entities_dir.mkdir()
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    pd.DataFrame(
        {
            "user_id": ["u1", "u2"],
            "event_time": ["2024-01-01", "2024-01-02"],
            "page": ["/home", "/about"],
        }
    ).to_parquet(str(data_dir / "page_views.parquet"))

    entity_code = f'''
from fyrnheim.core.source import EventSource
from fyrnheim.core.activity import ActivityDefinition, EventOccurred
from fyrnheim.core.metrics_model import MetricsModel, MetricField

page_views_source = EventSource(
    name="page_views",
    project="test",
    dataset="test",
    table="page_views",
    duckdb_path="{data_dir / 'page_views.parquet'}",
    entity_id_field="user_id",
    timestamp_field="event_time",
    event_type="page_view",
)

page_viewed = ActivityDefinition(
    name="page_viewed",
    source="page_views",
    trigger=EventOccurred(event_type="page_view"),
    entity_id_field="user_id",
)

page_view_metrics = MetricsModel(
    name="page_view_metrics",
    sources=["page_views"],
    grain="daily",
    metric_fields=[
        MetricField(field_name="page_viewed", aggregation="count"),
    ],
)
'''
    (entities_dir / "page_views.py").write_text(entity_code)

    yaml_content = f"""
entities_dir: {entities_dir}
data_dir: {data_dir}
output_dir: {output_dir}
backend: duckdb
"""
    (tmp_path / "fyrnheim.yaml").write_text(yaml_content)


class TestFyrBenchCommand:
    """CLI tests for ``fyr bench``."""

    def test_bench_appears_in_help(self) -> None:
        runner = CliRunner()
        result = runner.invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "bench" in result.output

    def test_bench_human_readable(self, tmp_path: Path) -> None:
        _setup_bench_project(tmp_path)

        runner = CliRunner()
        original_dir = os.getcwd()
        try:
            os.chdir(str(tmp_path))
            result = runner.invoke(main, ["bench"])
        finally:
            os.chdir(original_dir)

        assert result.exit_code == 0, f"Output: {result.output}"
        assert "Pipeline bench" in result.output
        assert "source_loads" in result.output
        assert "metrics_models" in result.output

    def test_bench_json_is_parseable(self, tmp_path: Path) -> None:
        _setup_bench_project(tmp_path)

        runner = CliRunner()
        original_dir = os.getcwd()
        try:
            os.chdir(str(tmp_path))
            result = runner.invoke(main, ["bench", "--json"])
        finally:
            os.chdir(original_dir)

        assert result.exit_code == 0, f"Output: {result.output}"
        payload = json.loads(result.output.strip())
        # Shape mirrors PipelineTimings
        assert set(payload) == {
            "staging_views_s",
            "source_loads",
            "activities_s",
            "identity_graphs",
            "analytics_entities",
            "metrics_models",
        }
        assert "page_views" in payload["source_loads"]
        assert payload["metrics_models"]["page_view_metrics"].keys() == {
            "project_s",
            "write_s",
        }

    def test_bench_no_assets_exits_nonzero(self, tmp_path: Path) -> None:
        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["bench", "--entities-dir", str(entities_dir)],
        )
        assert result.exit_code != 0
