"""Tests for run() and run_entity() pipeline orchestration."""

from pathlib import Path

import pandas as pd
import pytest

from fyrnheim import (
    Entity,
    LayersConfig,
    PrepLayer,
    TableSource,
)
from fyrnheim.engine.runner import (
    EntityRunResult,
    RunResult,
    _resolve_generated_dir,
    run,
    run_entity,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


ENTITY_WITH_SOURCE_TEMPLATE = """\
from fyrnheim import Entity, LayersConfig, PrepLayer, TableSource

entity = Entity(
    name="{name}",
    description="Test entity {name}",
    layers=LayersConfig(prep=PrepLayer(model_name="prep_{name}")),
    source=TableSource(
        project="p", dataset="d", table="{name}",
        duckdb_path="{duckdb_path}",
    ),
)
"""


def _create_parquet(directory: Path, name: str = "data") -> Path:
    """Create a minimal parquet file."""
    df = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
    path = directory / f"{name}.parquet"
    directory.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return path


def _setup_single_entity(tmp_path: Path) -> tuple[Path, Path, Path]:
    """Set up a single entity with parquet data for testing.

    Returns (entities_dir, data_dir, generated_dir).
    """
    entities_dir = tmp_path / "entities"
    entities_dir.mkdir()
    data_dir = tmp_path / "data"
    parquet_path = _create_parquet(data_dir, "orders")
    generated_dir = tmp_path / "generated"

    entity_code = ENTITY_WITH_SOURCE_TEMPLATE.format(
        name="orders", duckdb_path=str(parquet_path)
    )
    (entities_dir / "orders.py").write_text(entity_code)

    return entities_dir, data_dir, generated_dir


# ---------------------------------------------------------------------------
# RunResult / EntityRunResult property tests
# ---------------------------------------------------------------------------


class TestRunResultProperties:
    """Test RunResult computed properties."""

    def test_ok_when_all_success(self):
        result = RunResult(
            entities=[
                EntityRunResult(entity_name="a", status="success", row_count=10),
                EntityRunResult(entity_name="b", status="success", row_count=20),
            ],
            total_duration_seconds=1.0,
            backend="duckdb",
        )
        assert result.ok is True
        assert result.success_count == 2
        assert result.error_count == 0
        assert result.skipped_count == 0

    def test_not_ok_when_error(self):
        result = RunResult(
            entities=[
                EntityRunResult(entity_name="a", status="success", row_count=10),
                EntityRunResult(entity_name="b", status="error", error="boom"),
            ],
            total_duration_seconds=1.0,
            backend="duckdb",
        )
        assert result.ok is False
        assert result.error_count == 1

    def test_counts_skipped(self):
        result = RunResult(
            entities=[
                EntityRunResult(entity_name="a", status="skipped", error="dep failed"),
            ],
            total_duration_seconds=0.5,
            backend="duckdb",
        )
        assert result.skipped_count == 1
        assert result.ok is True  # skipped != error

    def test_empty_result_is_ok(self):
        result = RunResult(entities=[], total_duration_seconds=0.0)
        assert result.ok is True
        assert result.success_count == 0


class TestEntityRunResultDataclass:
    """Test EntityRunResult frozen dataclass."""

    def test_frozen(self):
        result = EntityRunResult(entity_name="x", status="success")
        with pytest.raises(AttributeError):
            result.status = "error"  # type: ignore[misc]

    def test_defaults(self):
        result = EntityRunResult(entity_name="x", status="success")
        assert result.row_count is None
        assert result.activity_row_count is None
        assert result.analytics_row_count is None
        assert result.error is None
        assert result.duration_seconds == 0.0
        assert result.quality_results is None


# ---------------------------------------------------------------------------
# _resolve_generated_dir tests
# ---------------------------------------------------------------------------


class TestResolveGeneratedDir:
    """Test _resolve_generated_dir helper."""

    def test_explicit_dir(self, tmp_path):
        result = _resolve_generated_dir(tmp_path / "entities", tmp_path / "custom")
        assert result == tmp_path / "custom"

    def test_default_sibling(self, tmp_path):
        result = _resolve_generated_dir(tmp_path / "entities", None)
        assert result == tmp_path / "generated"


# ---------------------------------------------------------------------------
# run() tests
# ---------------------------------------------------------------------------


class TestRun:
    """Test the run() orchestration function."""

    def test_missing_directory_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="Entities directory not found"):
            run(tmp_path / "nonexistent", tmp_path / "data")

    def test_unsupported_backend_raises(self, tmp_path):
        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()
        # Create at least one entity so pipeline reaches connection creation
        parquet_path = _create_parquet(tmp_path / "data", "test")
        entity_code = ENTITY_WITH_SOURCE_TEMPLATE.format(
            name="test", duckdb_path=str(parquet_path)
        )
        (entities_dir / "test.py").write_text(entity_code)
        with pytest.raises(ValueError, match="Unsupported backend"):
            run(entities_dir, tmp_path / "data", backend="postgres")

    def test_empty_directory(self, tmp_path):
        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()
        result = run(entities_dir, tmp_path / "data")
        assert result.ok is True
        assert len(result.entities) == 0

    def test_discovers_and_executes(self, tmp_path):
        entities_dir, data_dir, generated_dir = _setup_single_entity(tmp_path)
        result = run(entities_dir, data_dir, generated_dir=generated_dir)
        assert len(result.entities) == 1
        assert result.entities[0].entity_name == "orders"
        assert result.entities[0].status == "success"
        assert result.entities[0].row_count == 3
        assert result.ok is True

    def test_auto_generate_creates_files(self, tmp_path):
        entities_dir, data_dir, generated_dir = _setup_single_entity(tmp_path)
        result = run(entities_dir, data_dir, generated_dir=generated_dir, auto_generate=True)
        assert result.ok is True
        assert (generated_dir / "orders_transforms.py").exists()

    def test_auto_generate_false_skips_missing(self, tmp_path):
        entities_dir, data_dir, generated_dir = _setup_single_entity(tmp_path)
        generated_dir.mkdir(parents=True, exist_ok=True)
        result = run(
            entities_dir, data_dir, generated_dir=generated_dir, auto_generate=False
        )
        assert result.entities[0].status == "skipped"
        assert "not found" in result.entities[0].error

    def test_on_error_stop(self, tmp_path):
        """When on_error=stop and first entity fails, remaining are skipped."""
        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        generated_dir = tmp_path / "generated"
        generated_dir.mkdir()

        # Two entities, first has a bad source path
        bad_code = ENTITY_WITH_SOURCE_TEMPLATE.format(
            name="alpha", duckdb_path="/nonexistent/path.parquet"
        )
        good_parquet = _create_parquet(data_dir, "beta")
        good_code = ENTITY_WITH_SOURCE_TEMPLATE.format(
            name="beta", duckdb_path=str(good_parquet)
        )
        (entities_dir / "alpha.py").write_text(bad_code)
        (entities_dir / "beta.py").write_text(good_code)

        result = run(
            entities_dir, data_dir, generated_dir=generated_dir, on_error="stop"
        )
        statuses = {e.entity_name: e.status for e in result.entities}
        # At least one error and one skipped
        assert "error" in statuses.values() or "skipped" in statuses.values()
        assert result.ok is False


class TestRunEntity:
    """Test run_entity() for single entity execution."""

    def test_run_entity_success(self, tmp_path):
        data_dir = tmp_path / "data"
        parquet_path = _create_parquet(data_dir, "items")
        generated_dir = tmp_path / "generated"

        entity = Entity(
            name="items",
            description="Test items",
            layers=LayersConfig(prep=PrepLayer(model_name="prep_items")),
            source=TableSource(
                project="p", dataset="d", table="items",
                duckdb_path=str(parquet_path),
            ),
        )

        result = run_entity(entity, data_dir, generated_dir=generated_dir)
        assert result.status == "success"
        assert result.row_count == 3
        assert result.entity_name == "items"

    def test_run_entity_activity_analytics_row_counts_none_by_default(self, tmp_path):
        """Entities without activity/analytics layers should have None row counts."""
        data_dir = tmp_path / "data"
        parquet_path = _create_parquet(data_dir, "items")
        generated_dir = tmp_path / "generated"

        entity = Entity(
            name="items",
            description="Test items",
            layers=LayersConfig(prep=PrepLayer(model_name="prep_items")),
            source=TableSource(
                project="p", dataset="d", table="items",
                duckdb_path=str(parquet_path),
            ),
        )

        result = run_entity(entity, data_dir, generated_dir=generated_dir)
        assert result.status == "success"
        assert result.activity_row_count is None
        assert result.analytics_row_count is None

    def test_run_entity_unsupported_backend(self, tmp_path):
        entity = Entity(
            name="x",
            description="test",
            layers=LayersConfig(prep=PrepLayer(model_name="prep_x")),
            source=TableSource(project="p", dataset="d", table="x"),
        )
        result = run_entity(entity, tmp_path, backend="postgres")
        assert result.status == "error"
        assert "Unsupported backend" in result.error


class TestLazyImports:
    """Test lazy imports from top-level fyrnheim package."""

    def test_run_importable(self):
        import fyrnheim

        assert callable(fyrnheim.run)

    def test_run_entity_importable(self):
        import fyrnheim

        assert callable(fyrnheim.run_entity)

    def test_run_result_importable(self):
        import fyrnheim

        assert fyrnheim.RunResult is not None

    def test_entity_run_result_importable(self):
        import fyrnheim

        assert fyrnheim.EntityRunResult is not None

    def test_ibis_executor_importable(self):
        import fyrnheim

        assert fyrnheim.IbisExecutor is not None
