"""Tests for fyr run command."""

from unittest.mock import patch

from click.testing import CliRunner

from fyrnheim.cli import main
from fyrnheim.engine.runner import EntityRunResult, RunResult
from fyrnheim.quality.results import CheckResult

MINIMAL_ENTITY = """\
from fyrnheim import Entity, LayersConfig, PrepLayer, TableSource

entity = Entity(
    name="{name}",
    description="Test entity",
    source=TableSource(project="test", dataset="raw", table="{name}", duckdb_path="{name}.parquet"),
    layers=LayersConfig(prep=PrepLayer(model_name="prep_{name}")),
)
"""


def _make_project(tmp_path, entities=None):
    """Create a project with config, entity files, and data dir."""
    entities_dir = tmp_path / "entities"
    entities_dir.mkdir()
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    gen_dir = tmp_path / "generated"
    gen_dir.mkdir()
    (tmp_path / "fyrnheim.yaml").write_text(
        f"entities_dir: {entities_dir}\ndata_dir: {data_dir}\noutput_dir: {gen_dir}\n"
    )
    for name in (entities or []):
        (entities_dir / f"{name}.py").write_text(MINIMAL_ENTITY.format(name=name))
    return tmp_path


def _success_result(names=None, backend="duckdb"):
    entities = [
        EntityRunResult(
            entity_name=n, status="success", row_count=10, duration_seconds=0.1
        )
        for n in (names or ["test"])
    ]
    return RunResult(
        entities=entities,
        total_duration_seconds=0.3,
        backend=backend,
    )


def _error_result():
    return RunResult(
        entities=[
            EntityRunResult(entity_name="bad", status="error", error="boom", duration_seconds=0.1),
        ],
        total_duration_seconds=0.1,
        backend="duckdb",
    )


def _quality_fail_result():
    return RunResult(
        entities=[
            EntityRunResult(
                entity_name="test",
                status="success",
                row_count=10,
                duration_seconds=0.1,
                quality_results=[
                    CheckResult(check_name="not_null(email)", passed=True, failure_count=0, sample_failures=[]),
                    CheckResult(check_name="unique(email_hash)", passed=False, failure_count=2, sample_failures=[]),
                ],
            ),
        ],
        total_duration_seconds=0.1,
        backend="duckdb",
    )


class TestRunOutput:
    def test_discovery_count(self, tmp_path, monkeypatch):
        _make_project(tmp_path, ["test"])
        monkeypatch.chdir(tmp_path)
        with patch("fyrnheim.engine.runner.run", return_value=_success_result()):
            result = CliRunner().invoke(main, ["run"])
        assert "Discovering entities... 1 found" in result.output

    def test_shows_backend(self, tmp_path, monkeypatch):
        _make_project(tmp_path, ["test"])
        monkeypatch.chdir(tmp_path)
        with patch("fyrnheim.engine.runner.run", return_value=_success_result()):
            result = CliRunner().invoke(main, ["run"])
        assert "Running on duckdb" in result.output

    def test_per_entity_format(self, tmp_path, monkeypatch):
        _make_project(tmp_path, ["alpha"])
        monkeypatch.chdir(tmp_path)
        with patch("fyrnheim.engine.runner.run", return_value=_success_result(["alpha"])):
            result = CliRunner().invoke(main, ["run"])
        assert "alpha" in result.output
        assert "rows" in result.output
        assert "ok" in result.output

    def test_summary_line(self, tmp_path, monkeypatch):
        _make_project(tmp_path, ["a"])
        monkeypatch.chdir(tmp_path)
        with patch("fyrnheim.engine.runner.run", return_value=_success_result(["a"])):
            result = CliRunner().invoke(main, ["run"])
        assert "Done:" in result.output
        assert "1 success" in result.output


class TestRunExitCodes:
    def test_exit_0_on_success(self, tmp_path, monkeypatch):
        _make_project(tmp_path, ["test"])
        monkeypatch.chdir(tmp_path)
        with patch("fyrnheim.engine.runner.run", return_value=_success_result()):
            result = CliRunner().invoke(main, ["run"])
        assert result.exit_code == 0

    def test_exit_1_on_error(self, tmp_path, monkeypatch):
        _make_project(tmp_path, ["bad"])
        monkeypatch.chdir(tmp_path)
        with patch("fyrnheim.engine.runner.run", return_value=_error_result()):
            result = CliRunner().invoke(main, ["run"])
        assert result.exit_code == 1

    def test_exit_2_on_quality_failure(self, tmp_path, monkeypatch):
        _make_project(tmp_path, ["test"])
        monkeypatch.chdir(tmp_path)
        with patch("fyrnheim.engine.runner.run", return_value=_quality_fail_result()):
            result = CliRunner().invoke(main, ["run"])
        assert result.exit_code == 2

    def test_exit_1_overrides_2(self, tmp_path, monkeypatch):
        """Runtime errors (1) take priority over quality failures (2)."""
        mixed = RunResult(
            entities=[
                EntityRunResult(entity_name="bad", status="error", error="boom", duration_seconds=0.1),
                EntityRunResult(
                    entity_name="test", status="success", row_count=10, duration_seconds=0.1,
                    quality_results=[
                        CheckResult(check_name="unique(x)", passed=False, failure_count=1, sample_failures=[]),
                    ],
                ),
            ],
            total_duration_seconds=0.2,
            backend="duckdb",
        )
        _make_project(tmp_path, ["bad", "test"])
        monkeypatch.chdir(tmp_path)
        with patch("fyrnheim.engine.runner.run", return_value=mixed):
            result = CliRunner().invoke(main, ["run"])
        assert result.exit_code == 1


class TestRunQualityOutput:
    def test_quality_failure_output(self, tmp_path, monkeypatch):
        _make_project(tmp_path, ["test"])
        monkeypatch.chdir(tmp_path)
        with patch("fyrnheim.engine.runner.run", return_value=_quality_fail_result()):
            result = CliRunner().invoke(main, ["run"])
        assert "checks:" in result.output
        assert "1 passed" in result.output
        assert "1 failed" in result.output
        assert "unique(email_hash)" in result.output
        assert "FAIL" in result.output
        assert "Quality:" in result.output


class TestRunSingleEntity:
    def test_unknown_entity(self, tmp_path, monkeypatch):
        _make_project(tmp_path, ["real"])
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(main, ["run", "--entity", "bogus"])
        assert result.exit_code == 1
        assert "Entity 'bogus' not found" in result.output
        assert "real" in result.output

    def test_single_entity_runs(self, tmp_path, monkeypatch):
        _make_project(tmp_path, ["alpha"])
        monkeypatch.chdir(tmp_path)
        er = EntityRunResult(entity_name="alpha", status="success", row_count=5, duration_seconds=0.2)
        with patch("fyrnheim.engine.runner.run_entity", return_value=er):
            result = CliRunner().invoke(main, ["run", "--entity", "alpha"])
        assert result.exit_code == 0
        assert "Running alpha on duckdb" in result.output
        assert "alpha" in result.output
        assert "Done" in result.output


class TestRunBackendConfig:
    def _make_project_with_backend_config(self, tmp_path, backend="bigquery"):
        """Create a project with backend_config in fyrnheim.yaml."""
        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        gen_dir = tmp_path / "generated"
        gen_dir.mkdir()
        (tmp_path / "fyrnheim.yaml").write_text(
            f"entities_dir: {entities_dir}\ndata_dir: {data_dir}\noutput_dir: {gen_dir}\n"
            f"backend: {backend}\n"
            "backend_config:\n"
            "  project_id: my-project\n"
            "  dataset_id: my_dataset\n"
        )
        (entities_dir / "test.py").write_text(MINIMAL_ENTITY.format(name="test"))
        return tmp_path

    def test_backend_config_passed_to_run(self, tmp_path, monkeypatch):
        self._make_project_with_backend_config(tmp_path)
        monkeypatch.chdir(tmp_path)
        with patch("fyrnheim.engine.runner.run", return_value=_success_result(backend="bigquery")) as mock_run:
            CliRunner().invoke(main, ["run"])
        assert mock_run.call_args.kwargs["backend_config"] == {
            "project_id": "my-project",
            "dataset_id": "my_dataset",
        }

    def test_backend_config_passed_to_run_entity(self, tmp_path, monkeypatch):
        self._make_project_with_backend_config(tmp_path)
        monkeypatch.chdir(tmp_path)
        er = EntityRunResult(entity_name="test", status="success", row_count=5, duration_seconds=0.2)
        with patch("fyrnheim.engine.runner.run_entity", return_value=er) as mock_run_entity:
            CliRunner().invoke(main, ["run", "--entity", "test"])
        assert mock_run_entity.call_args.kwargs["backend_config"] == {
            "project_id": "my-project",
            "dataset_id": "my_dataset",
        }

    def test_no_backend_config_passes_none(self, tmp_path, monkeypatch):
        _make_project(tmp_path, ["test"])
        monkeypatch.chdir(tmp_path)
        with patch("fyrnheim.engine.runner.run", return_value=_success_result()) as mock_run:
            CliRunner().invoke(main, ["run"])
        assert mock_run.call_args.kwargs["backend_config"] is None

    def test_duckdb_regression(self, tmp_path, monkeypatch):
        """fyr run with backend=duckdb still works end-to-end."""
        _make_project(tmp_path, ["test"])
        monkeypatch.chdir(tmp_path)
        with patch("fyrnheim.engine.runner.run", return_value=_success_result()) as mock_run:
            result = CliRunner().invoke(main, ["run"])
        assert result.exit_code == 0
        assert mock_run.call_args.kwargs["backend"] == "duckdb"


class TestRunBackendFlag:
    def test_backend_flag_overrides_yaml(self, tmp_path, monkeypatch):
        _make_project(tmp_path, ["test"])
        monkeypatch.chdir(tmp_path)
        with patch("fyrnheim.engine.runner.run", return_value=_success_result(backend="bigquery")) as mock_run:
            CliRunner().invoke(main, ["run", "--backend", "bigquery"])
        assert mock_run.call_args.kwargs["backend"] == "bigquery"

    def test_backend_flag_shows_in_output(self, tmp_path, monkeypatch):
        _make_project(tmp_path, ["test"])
        monkeypatch.chdir(tmp_path)
        with patch("fyrnheim.engine.runner.run", return_value=_success_result(backend="bigquery")):
            result = CliRunner().invoke(main, ["run", "--backend", "bigquery"])
        assert "Running on bigquery" in result.output

    def test_default_backend_is_duckdb(self, tmp_path, monkeypatch):
        _make_project(tmp_path, ["test"])
        monkeypatch.chdir(tmp_path)
        with patch("fyrnheim.engine.runner.run", return_value=_success_result()) as mock_run:
            CliRunner().invoke(main, ["run"])
        assert mock_run.call_args.kwargs["backend"] == "duckdb"

    def test_help_shows_backend_option(self):
        result = CliRunner().invoke(main, ["run", "--help"])
        assert "--backend" in result.output

    def test_single_entity_backend_flag(self, tmp_path, monkeypatch):
        _make_project(tmp_path, ["alpha"])
        monkeypatch.chdir(tmp_path)
        er = EntityRunResult(entity_name="alpha", status="success", row_count=5, duration_seconds=0.2)
        with patch("fyrnheim.engine.runner.run_entity", return_value=er) as mock_run_entity:
            result = CliRunner().invoke(main, ["run", "--entity", "alpha", "--backend", "bigquery"])
        assert mock_run_entity.call_args.kwargs["backend"] == "bigquery"
        assert "Running alpha on bigquery" in result.output


    def test_invalid_backend_rejected(self):
        result = CliRunner().invoke(main, ["run", "--backend", "postgres"])
        assert result.exit_code != 0
        assert "Invalid value" in result.output or "invalid choice" in result.output.lower()


class TestRunEdgeCases:
    def test_no_entities(self, tmp_path, monkeypatch):
        _make_project(tmp_path, [])
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(main, ["run"])
        assert result.exit_code == 0
        assert "Nothing to run" in result.output

    def test_missing_entities_dir(self):
        result = CliRunner().invoke(main, ["run", "--entities-dir", "/nonexistent_xyz"])
        assert result.exit_code == 1
        assert "Entities directory not found" in result.output
