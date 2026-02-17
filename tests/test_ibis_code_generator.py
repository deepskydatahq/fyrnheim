"""Tests for IbisCodeGenerator base class and source function generation."""

import ast

import pytest

from typedata import (
    Entity,
    Field,
    LayersConfig,
    PrepLayer,
    TableSource,
    UnionSource,
)
from typedata.generators import IbisCodeGenerator


@pytest.fixture()
def simple_entity():
    """Entity with a single TableSource."""
    return Entity(
        name="transactions",
        description="Transaction records",
        layers=LayersConfig(prep=PrepLayer(model_name="prep_transactions")),
        source=TableSource(
            project="warehouse",
            dataset="stripe",
            table="charges",
            duckdb_path="~/timo-data/stripe/charges/*.parquet",
        ),
    )


@pytest.fixture()
def union_entity():
    """Entity with a UnionSource containing two sub-sources."""
    return Entity(
        name="signals",
        description="Unified signals",
        layers=LayersConfig(prep=PrepLayer(model_name="prep_signals")),
        source=UnionSource(
            sources=[
                TableSource(
                    project="warehouse",
                    dataset="hubspot",
                    table="contacts",
                    duckdb_path="~/timo-data/hubspot/contacts/*.parquet",
                ),
                TableSource(
                    project="warehouse",
                    dataset="stripe",
                    table="customers",
                    duckdb_path="~/timo-data/stripe/customers/*.parquet",
                ),
            ]
        ),
        required_fields=[Field(name="id", type="STRING")],
    )


class TestImports:
    """Test IbisCodeGenerator is importable."""

    def test_importable(self):
        from typedata.generators import IbisCodeGenerator

        assert IbisCodeGenerator is not None


class TestConstructor:
    """Test IbisCodeGenerator constructor."""

    def test_constructor_accepts_entity(self, simple_entity):
        gen = IbisCodeGenerator(simple_entity)
        assert gen.entity_name == "transactions"
        assert gen.entity is simple_entity


class TestGenerateImports:
    """Test _generate_imports method."""

    def test_has_ibis_import(self, simple_entity):
        gen = IbisCodeGenerator(simple_entity)
        code = gen._generate_imports()
        assert "import ibis" in code

    def test_has_os_import(self, simple_entity):
        gen = IbisCodeGenerator(simple_entity)
        code = gen._generate_imports()
        assert "import os" in code

    def test_no_hamilton_import(self, simple_entity):
        gen = IbisCodeGenerator(simple_entity)
        code = gen._generate_imports()
        assert "hamilton" not in code.lower()

    def test_has_docstring(self, simple_entity):
        gen = IbisCodeGenerator(simple_entity)
        code = gen._generate_imports()
        assert "transactions entity transformations" in code
        assert "DO NOT EDIT" in code

    def test_is_valid_python(self, simple_entity):
        gen = IbisCodeGenerator(simple_entity)
        code = gen._generate_imports()
        ast.parse(code)


class TestBindExpression:
    """Test _bind_expression helper."""

    def test_passthrough_with_t_prefix(self, simple_entity):
        gen = IbisCodeGenerator(simple_entity)
        assert gen._bind_expression("t.status == 'active'") == "t.status == 'active'"

    def test_passthrough_with_ibis_prefix(self, simple_entity):
        gen = IbisCodeGenerator(simple_entity)
        assert gen._bind_expression("ibis.cases(...)") == "ibis.cases(...)"

    def test_passthrough_with_paren_prefix(self, simple_entity):
        gen = IbisCodeGenerator(simple_entity)
        assert gen._bind_expression("(t.x + t.y)") == "(t.x + t.y)"

    def test_prefixes_bare_column(self, simple_entity):
        gen = IbisCodeGenerator(simple_entity)
        assert gen._bind_expression("email.lower().hash()") == "t.email.lower().hash()"


class TestSingleSourceGeneration:
    """Test source function generation for TableSource."""

    def test_function_signature(self, simple_entity):
        gen = IbisCodeGenerator(simple_entity)
        code = gen._generate_source_functions()
        assert "def source_transactions(conn: ibis.BaseBackend, backend: str)" in code

    def test_duckdb_branch(self, simple_entity):
        gen = IbisCodeGenerator(simple_entity)
        code = gen._generate_source_functions()
        assert 'if backend == "duckdb"' in code
        assert "conn.read_parquet" in code

    def test_bigquery_branch(self, simple_entity):
        gen = IbisCodeGenerator(simple_entity)
        code = gen._generate_source_functions()
        assert 'elif backend == "bigquery"' in code
        assert "conn.table" in code

    def test_raises_on_unsupported(self, simple_entity):
        gen = IbisCodeGenerator(simple_entity)
        code = gen._generate_source_functions()
        assert "raise ValueError" in code

    def test_no_hamilton_decorators(self, simple_entity):
        gen = IbisCodeGenerator(simple_entity)
        code = gen._generate_source_functions()
        assert "@config.when" not in code
        assert "__duckdb" not in code
        assert "__bigquery" not in code

    def test_is_valid_python(self, simple_entity):
        gen = IbisCodeGenerator(simple_entity)
        code = gen._generate_imports() + gen._generate_source_functions()
        ast.parse(code)

    def test_uses_duckdb_path(self, simple_entity):
        gen = IbisCodeGenerator(simple_entity)
        code = gen._generate_source_functions()
        assert "stripe/charges" in code


class TestUnionSourceGeneration:
    """Test source function generation for UnionSource."""

    def test_per_source_functions(self, union_entity):
        gen = IbisCodeGenerator(union_entity)
        code = gen._generate_source_functions()
        assert "def source_signals_contacts" in code
        assert "def source_signals_customers" in code

    def test_union_aggregator(self, union_entity):
        gen = IbisCodeGenerator(union_entity)
        code = gen._generate_source_functions()
        assert "def source_signals(" in code
        assert "ibis.union" in code

    def test_no_hamilton_patterns(self, union_entity):
        gen = IbisCodeGenerator(union_entity)
        code = gen._generate_source_functions()
        assert "@config.when" not in code
        assert "__duckdb" not in code
        assert "__bigquery" not in code

    def test_is_valid_python(self, union_entity):
        gen = IbisCodeGenerator(union_entity)
        code = gen._generate_imports() + gen._generate_source_functions()
        ast.parse(code)


class TestGenerateModule:
    """Test generate_module produces a valid Python module."""

    def test_is_parseable(self, simple_entity):
        gen = IbisCodeGenerator(simple_entity)
        code = gen.generate_module()
        tree = ast.parse(code)
        func_names = [n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]
        assert "source_transactions" in func_names

    def test_union_module_parseable(self, union_entity):
        gen = IbisCodeGenerator(union_entity)
        code = gen.generate_module()
        tree = ast.parse(code)
        func_names = [n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]
        assert "source_signals" in func_names
        assert "source_signals_contacts" in func_names
        assert "source_signals_customers" in func_names


class TestWriteModule:
    """Test write_module writes to disk."""

    def test_creates_file(self, simple_entity, tmp_path):
        gen = IbisCodeGenerator(simple_entity)
        path = gen.write_module(tmp_path)
        assert path.exists()
        assert path.name == "transactions_transforms.py"
        assert "import ibis" in path.read_text()

    def test_creates_dir(self, simple_entity, tmp_path):
        output = tmp_path / "nested" / "dir"
        gen = IbisCodeGenerator(simple_entity)
        path = gen.write_module(output)
        assert path.exists()
