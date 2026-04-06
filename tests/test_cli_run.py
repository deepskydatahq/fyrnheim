"""Tests for the fyr run CLI command."""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from click.testing import CliRunner

from fyrnheim.cli import main


def _setup_project(tmp_path: Path) -> None:
    """Set up a minimal project with entities, data, and config."""
    entities_dir = tmp_path / "entities"
    entities_dir.mkdir()
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    # Write sample parquet data
    df = pd.DataFrame(
        {
            "user_id": ["u1", "u2"],
            "event_time": ["2024-01-01", "2024-01-02"],
            "page": ["/home", "/about"],
        }
    )
    df.to_parquet(str(data_dir / "page_views.parquet"))

    # Write entity definition
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

    # Write fyrnheim.yaml
    yaml_content = f"""
entities_dir: {entities_dir}
data_dir: {data_dir}
output_dir: {output_dir}
backend: duckdb
"""
    (tmp_path / "fyrnheim.yaml").write_text(yaml_content)


class TestFyrRunCommand:
    """Tests for fyr run CLI command."""

    def test_run_appears_in_help(self) -> None:
        """fyr run appears in fyr --help output."""
        runner = CliRunner()
        result = runner.invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "run" in result.output

    def test_run_with_explicit_dirs(self, tmp_path: Path) -> None:
        """fyr run --entities-dir --data-dir --output-dir executes pipeline."""
        _setup_project(tmp_path)

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "run",
                "--entities-dir", str(tmp_path / "entities"),
                "--data-dir", str(tmp_path / "data"),
                "--output-dir", str(tmp_path / "output"),
            ],
        )

        assert result.exit_code == 0, f"Output: {result.output}"
        assert "Sources processed:" in result.output
        assert "Outputs written:" in result.output
        assert "Completed in" in result.output

    def test_run_with_yaml_defaults(self, tmp_path: Path) -> None:
        """fyr run with no args uses fyrnheim.yaml defaults."""
        _setup_project(tmp_path)

        runner = CliRunner()
        original_dir = os.getcwd()
        try:
            os.chdir(str(tmp_path))
            result = runner.invoke(main, ["run"])
        finally:
            os.chdir(original_dir)

        assert result.exit_code == 0, f"Output: {result.output}"
        assert "Sources processed:" in result.output

    def test_run_produces_output_files(self, tmp_path: Path) -> None:
        """fyr run produces parquet output files."""
        _setup_project(tmp_path)

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "run",
                "--entities-dir", str(tmp_path / "entities"),
                "--data-dir", str(tmp_path / "data"),
                "--output-dir", str(tmp_path / "output"),
            ],
        )

        assert result.exit_code == 0, f"Output: {result.output}"

        # Check output file exists
        output_file = tmp_path / "output" / "page_view_metrics.parquet"
        assert output_file.exists()
        df = pd.read_parquet(str(output_file))
        assert len(df) > 0

    def test_run_no_assets_exits_1(self, tmp_path: Path) -> None:
        """fyr run with empty entities dir exits with code 1."""
        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "run",
                "--entities-dir", str(entities_dir),
            ],
        )

        assert result.exit_code == 1
        assert "No assets found" in result.output

    def test_run_prints_discovery_summary(self, tmp_path: Path) -> None:
        """fyr run prints discovered asset counts."""
        _setup_project(tmp_path)

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "run",
                "--entities-dir", str(tmp_path / "entities"),
                "--data-dir", str(tmp_path / "data"),
                "--output-dir", str(tmp_path / "output"),
            ],
        )

        assert result.exit_code == 0, f"Output: {result.output}"
        assert "Discovered:" in result.output
        assert "sources" in result.output
