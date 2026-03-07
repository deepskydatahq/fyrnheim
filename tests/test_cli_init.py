"""Tests for fyr init command."""

from pathlib import Path

from click.testing import CliRunner

from fyrnheim.cli import main


class TestInitNamedProject:
    def test_creates_dir_with_all_files(self, tmp_path):
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(main, ["init", "myproject"])
            assert result.exit_code == 0
            root = Path("myproject")
            assert (root / "fyrnheim.yaml").is_file()
            assert (root / "entities" / "customers.py").is_file()
            assert (root / "data" / "customers.parquet").is_file()
            assert (root / "generated").is_dir()

    def test_output_includes_project_name(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(main, ["init", "myproject"])
            assert "Created myproject/" in result.output

    def test_output_next_steps_include_cd(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(main, ["init", "myproject"])
            assert "cd myproject" in result.output
            assert "fyr generate" in result.output
            assert "fyr run" in result.output


class TestInitCwd:
    def test_creates_files_in_cwd(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(main, ["init"])
            assert result.exit_code == 0
            assert Path("fyrnheim.yaml").is_file()
            assert Path("entities/customers.py").is_file()
            assert Path("data/customers.parquet").is_file()
            assert Path("generated").is_dir()

    def test_output_says_initializing(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(main, ["init"])
            assert "Initializing in current directory..." in result.output

    def test_next_steps_no_cd(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(main, ["init"])
            assert "cd " not in result.output
            assert "fyr generate" in result.output


class TestInitSafety:
    def test_existing_config_not_overwritten(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            Path("fyrnheim.yaml").write_text("backend: bigquery\n")
            result = runner.invoke(main, ["init"])
            assert result.exit_code == 0
            assert "skipped" in result.output
            assert Path("fyrnheim.yaml").read_text() == "backend: bigquery\n"

    def test_existing_entity_preserved(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            Path("entities").mkdir()
            Path("entities/customers.py").write_text("# my entity\n")
            result = runner.invoke(main, ["init"])
            assert result.exit_code == 0
            assert Path("entities/customers.py").read_text() == "# my entity\n"

    def test_existing_data_preserved(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            Path("data").mkdir()
            Path("data/customers.parquet").write_bytes(b"fake")
            result = runner.invoke(main, ["init"])
            assert result.exit_code == 0
            assert Path("data/customers.parquet").read_bytes() == b"fake"

    def test_idempotent(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            result1 = runner.invoke(main, ["init"])
            assert result1.exit_code == 0
            result2 = runner.invoke(main, ["init"])
            assert result2.exit_code == 0
            assert "skipped" in result2.output


class TestInitOutputFormat:
    def test_lists_created_files(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(main, ["init"])
            assert "fyrnheim.yaml" in result.output
            assert "customers.py" in result.output
            assert "customers.parquet" in result.output

    def test_next_steps_present(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(main, ["init"])
            assert "Next steps:" in result.output


class TestScaffoldTestFile:
    def test_init_creates_test_customers(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(main, ["init"])
            assert result.exit_code == 0
            assert Path("tests/test_customers.py").is_file()

    def test_scaffold_test_imports_entity_test(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            runner.invoke(main, ["init"])
            content = Path("tests/test_customers.py").read_text()
            assert "from fyrnheim.testing import EntityTest" in content

    def test_scaffold_test_has_given_run_assert(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            runner.invoke(main, ["init"])
            content = Path("tests/test_customers.py").read_text()
            assert "def test_basic_transform" in content
            assert ".given(" in content
            assert ".run()" in content
            assert "assert result.row_count" in content

    def test_scaffold_test_uses_entity_test_base_class(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            runner.invoke(main, ["init"])
            content = Path("tests/test_customers.py").read_text()
            assert "class TestCustomers(EntityTest):" in content

    def test_named_project_creates_test_file(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(main, ["init", "myproject"])
            assert result.exit_code == 0
            assert Path("myproject/tests/test_customers.py").is_file()

    def test_pyproject_includes_scaffold_test(self):
        """Verify pyproject.toml includes the scaffold test file in hatch build."""
        import tomllib

        pyproject = Path(__file__).resolve().parent.parent / "pyproject.toml"
        with open(pyproject, "rb") as f:
            data = tomllib.load(f)
        includes = data["tool"]["hatch"]["build"]["include"]
        assert "src/fyrnheim/_scaffold/test_customers.py" in includes


class TestScaffoldValidity:
    def test_sample_entity_importable(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            runner.invoke(main, ["init"])
            import importlib.util

            spec = importlib.util.spec_from_file_location(
                "customers", str(Path("entities/customers.py").resolve())
            )
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            assert hasattr(mod, "entity")

    def test_sample_data_has_rows(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            runner.invoke(main, ["init"])
            # Verify parquet file is non-trivial (> 100 bytes means real data)
            data = Path("data/customers.parquet").read_bytes()
            assert len(data) > 100
