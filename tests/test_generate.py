"""Tests for public generate() function and GenerateResult."""

import ast
import importlib.util
import os

import pytest

from fyrnheim import (
    ComputedColumn,
    DimensionLayer,
    Entity,
    LayersConfig,
    PrepLayer,
    SnapshotLayer,
    TableSource,
)
from fyrnheim._generate import generate


@pytest.fixture()
def sample_entity():
    """Entity with prep + dimension + snapshot layers."""
    return Entity(
        name="users",
        description="User records",
        layers=LayersConfig(
            prep=PrepLayer(model_name="prep_users"),
            dimension=DimensionLayer(
                model_name="dim_users",
                computed_columns=[
                    ComputedColumn(name="full_name", expression="t.first.concat(t.last)"),
                ],
            ),
            snapshot=SnapshotLayer(natural_key="user_id"),
        ),
        source=TableSource(
            project="warehouse",
            dataset="app",
            table="users",
            duckdb_path="data/users/*.parquet",
        ),
    )


@pytest.fixture()
def minimal_entity():
    """Minimal entity with prep only."""
    return Entity(
        name="events",
        description="Raw events",
        layers=LayersConfig(prep=PrepLayer(model_name="prep_events")),
        source=TableSource(project="p", dataset="d", table="events", duckdb_path="data/events/*.parquet"),
    )


class TestGenerateWritesFile:
    """Test that generate() writes the correct file."""

    def test_writes_file(self, sample_entity, tmp_path):
        result = generate(sample_entity, tmp_path)
        assert result.output_path.exists()
        assert result.written is True
        assert result.output_path.read_text() == result.code

    def test_correct_filename(self, sample_entity, tmp_path):
        result = generate(sample_entity, tmp_path)
        assert result.output_path.name == "users_transforms.py"

    def test_correct_entity_name(self, sample_entity, tmp_path):
        result = generate(sample_entity, tmp_path)
        assert result.entity_name == "users"

    def test_output_path(self, sample_entity, tmp_path):
        result = generate(sample_entity, tmp_path)
        assert result.output_path == tmp_path / "users_transforms.py"


class TestGenerateCreatesDir:
    """Test that generate() creates output directory."""

    def test_creates_nested_dir(self, sample_entity, tmp_path):
        nested = tmp_path / "a" / "b" / "c"
        result = generate(sample_entity, nested)
        assert result.output_path.exists()
        assert nested.exists()


class TestGenerateHeader:
    """Test auto-generated header."""

    def test_has_do_not_edit(self, sample_entity, tmp_path):
        result = generate(sample_entity, tmp_path)
        assert "DO NOT EDIT" in result.code

    def test_no_timestamp(self, sample_entity, tmp_path):
        result = generate(sample_entity, tmp_path)
        assert "Generated:" not in result.code

    def test_has_entity_name_in_header(self, sample_entity, tmp_path):
        result = generate(sample_entity, tmp_path)
        assert "users entity transformations" in result.code


class TestGenerateImports:
    """Test generated file has correct imports."""

    def test_has_ibis(self, sample_entity, tmp_path):
        result = generate(sample_entity, tmp_path)
        assert "import ibis" in result.code

    def test_has_os(self, sample_entity, tmp_path):
        result = generate(sample_entity, tmp_path)
        assert "import os" in result.code


class TestGenerateLayerFunctions:
    """Test generated file has layer functions."""

    def test_has_source_function(self, sample_entity, tmp_path):
        result = generate(sample_entity, tmp_path)
        assert "def source_users(" in result.code

    def test_has_prep_function(self, sample_entity, tmp_path):
        result = generate(sample_entity, tmp_path)
        assert "def prep_users(" in result.code

    def test_has_dimension_function(self, sample_entity, tmp_path):
        result = generate(sample_entity, tmp_path)
        assert "def dim_users(" in result.code

    def test_has_snapshot_function(self, sample_entity, tmp_path):
        result = generate(sample_entity, tmp_path)
        assert "def snapshot_users(" in result.code


class TestGenerateDryRun:
    """Test dry_run mode."""

    def test_no_file_written(self, sample_entity, tmp_path):
        result = generate(sample_entity, tmp_path, dry_run=True)
        assert not result.output_path.exists()
        assert result.written is False

    def test_code_populated(self, sample_entity, tmp_path):
        result = generate(sample_entity, tmp_path, dry_run=True)
        assert result.code
        assert "import ibis" in result.code

    def test_dir_not_created(self, sample_entity, tmp_path):
        nested = tmp_path / "does_not_exist"
        generate(sample_entity, nested, dry_run=True)
        assert not nested.exists()


class TestGenerateSkipIdentical:
    """Test content-comparison skip."""

    def test_skip_when_identical(self, sample_entity, tmp_path):
        result1 = generate(sample_entity, tmp_path)
        assert result1.written is True
        mtime1 = os.path.getmtime(result1.output_path)

        result2 = generate(sample_entity, tmp_path)
        assert result2.written is False
        mtime2 = os.path.getmtime(result2.output_path)
        assert mtime1 == mtime2

    def test_overwrite_when_different(self, sample_entity, tmp_path):
        # Write a dummy file first
        output_path = tmp_path / "users_transforms.py"
        tmp_path.mkdir(parents=True, exist_ok=True)
        output_path.write_text("# old content")

        result = generate(sample_entity, tmp_path)
        assert result.written is True
        assert result.code != "# old content"
        assert output_path.read_text() == result.code


class TestGenerateAstParseable:
    """Test generated code is syntactically valid."""

    def test_ast_parses(self, sample_entity, tmp_path):
        result = generate(sample_entity, tmp_path)
        ast.parse(result.code)

    def test_minimal_entity_parses(self, minimal_entity, tmp_path):
        result = generate(minimal_entity, tmp_path)
        ast.parse(result.code)


class TestGenerateImportable:
    """Test generated file can be loaded as a module."""

    def test_importable(self, sample_entity, tmp_path):
        result = generate(sample_entity, tmp_path)
        spec = importlib.util.spec_from_file_location(
            "users_transforms", result.output_path
        )
        assert spec is not None


class TestGenerateResultDataclass:
    """Test GenerateResult is a frozen dataclass."""

    def test_frozen(self, sample_entity, tmp_path):
        result = generate(sample_entity, tmp_path)
        with pytest.raises(AttributeError):
            result.written = False  # type: ignore[misc]

    def test_fields(self, sample_entity, tmp_path):
        result = generate(sample_entity, tmp_path)
        assert hasattr(result, "entity_name")
        assert hasattr(result, "code")
        assert hasattr(result, "output_path")
        assert hasattr(result, "written")


class TestLazyImports:
    """Test lazy imports from top-level fyrnheim package."""

    def test_generate_importable(self):
        import fyrnheim

        gen_func = fyrnheim.generate
        assert callable(gen_func)

    def test_generate_result_importable(self):
        import fyrnheim

        assert fyrnheim.GenerateResult is not None

    def test_ibis_code_generator_importable(self):
        from fyrnheim import IbisCodeGenerator

        assert IbisCodeGenerator is not None
