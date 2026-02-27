"""Tests for IbisCodeGenerator base class and source function generation."""

import ast

import pytest

from fyrnheim import (
    ActivityConfig,
    ActivityType,
    AnalyticsLayer,
    AnalyticsMetric,
    DimensionLayer,
    Entity,
    Field,
    LayersConfig,
    PrepLayer,
    TableSource,
    UnionSource,
)
from fyrnheim.generators import IbisCodeGenerator


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
        from fyrnheim.generators import IbisCodeGenerator

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


# ---------------------------------------------------------------------------
# Activity code generation tests
# ---------------------------------------------------------------------------


def _activity_entity(trigger="row_appears", **trigger_kwargs):
    """Create an entity with activity config for testing."""
    at_kwargs = dict(
        name="created",
        trigger=trigger,
        timestamp_field="created_at",
    )
    at_kwargs.update(trigger_kwargs)
    return Entity(
        name="users",
        description="Users with activity",
        source=TableSource(project="p", dataset="d", table="users", duckdb_path="users.parquet"),
        layers=LayersConfig(
            prep=PrepLayer(model_name="prep_users"),
            dimension=DimensionLayer(model_name="dim_users"),
            activity=ActivityConfig(
                model_name="activity_users",
                entity_id_field="id",
                person_id_field="email",
                types=[ActivityType(**at_kwargs)],
            ),
        ),
    )


class TestActivityCodeGeneration:
    """Test _generate_activity_function code generation."""

    def test_row_appears_generates_select(self):
        entity = _activity_entity("row_appears")
        gen = IbisCodeGenerator(entity)
        code = gen._generate_activity_function()
        assert "def activity_users(" in code
        assert "row_appears" in code
        assert "t.select(" in code
        assert 'entity_id=t.id.cast("string")' in code
        assert 'identity=t.email.cast("string")' in code
        assert "ts=t.created_at" in code
        assert 'activity_type=ibis.literal("created")' in code

    def test_status_becomes_generates_filter_and_select(self):
        entity = _activity_entity(
            "status_becomes",
            name="activated",
            field="status",
            values=["active"],
        )
        gen = IbisCodeGenerator(entity)
        code = gen._generate_activity_function()
        assert "def activity_users(" in code
        assert "status_becomes" in code
        assert "t.filter(" in code
        assert "t.status.isin(" in code
        assert "['active']" in code

    def test_status_becomes_uses_filtered_ref(self):
        """select() should reference _filtered, not the unfiltered t."""
        entity = _activity_entity(
            "status_becomes",
            name="activated",
            field="status",
            values=["active"],
        )
        gen = IbisCodeGenerator(entity)
        code = gen._generate_activity_function()
        assert "_filtered = t.filter(" in code
        assert "_filtered.select(" in code
        assert "_filtered.id.cast" in code
        assert "_filtered.email.cast" in code
        assert "_filtered.created_at" in code

    def test_multiple_triggers_generates_union(self):
        entity = Entity(
            name="users",
            description="Users",
            source=TableSource(project="p", dataset="d", table="users", duckdb_path="u.parquet"),
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_users"),
                dimension=DimensionLayer(model_name="dim_users"),
                activity=ActivityConfig(
                    model_name="activity_users",
                    entity_id_field="id",
                    person_id_field="email",
                    types=[
                        ActivityType(name="created", trigger="row_appears", timestamp_field="created_at"),
                        ActivityType(name="activated", trigger="status_becomes", timestamp_field="updated_at", field="status", values=["active"]),
                    ],
                ),
            ),
        )
        gen = IbisCodeGenerator(entity)
        code = gen._generate_activity_function()
        assert "act_created" in code
        assert "act_activated" in code
        assert "ibis.union(act_created, act_activated)" in code

    def test_field_changes_generates_lag_and_filter(self):
        entity = _activity_entity(
            "field_changes",
            name="plan_changed",
            field="plan",
        )
        gen = IbisCodeGenerator(entity)
        code = gen._generate_activity_function()
        assert "def activity_users(" in code
        assert "field_changes" in code
        assert "t.plan.lag()" in code
        assert "ibis.window(" in code
        assert "group_by=t.id" in code
        assert "order_by=t.created_at" in code
        assert "_prev.notnull()" in code
        assert "_prev != t.plan" in code
        assert "_changed.select(" in code

    def test_field_changes_is_valid_python(self):
        entity = _activity_entity("field_changes", name="plan_changed", field="plan")
        gen = IbisCodeGenerator(entity)
        code = gen.generate_module()
        ast.parse(code)

    def test_single_trigger_no_union(self):
        entity = _activity_entity("row_appears")
        gen = IbisCodeGenerator(entity)
        code = gen._generate_activity_function()
        assert "return act_created" in code
        assert "ibis.union" not in code

    def test_activity_input_is_dim(self):
        """Activity function input should be dim when dimension layer exists."""
        entity = _activity_entity("row_appears")
        gen = IbisCodeGenerator(entity)
        code = gen._generate_activity_function()
        assert "dim_users: ibis.Table" in code

    def test_activity_is_valid_python(self):
        entity = _activity_entity("row_appears")
        gen = IbisCodeGenerator(entity)
        code = gen.generate_module()
        ast.parse(code)

    def test_activity_in_generate_module(self):
        entity = _activity_entity("row_appears")
        gen = IbisCodeGenerator(entity)
        code = gen.generate_module()
        tree = ast.parse(code)
        func_names = [n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]
        assert "activity_users" in func_names


# ---------------------------------------------------------------------------
# Analytics code generation tests
# ---------------------------------------------------------------------------


def _analytics_entity(dimensions=None):
    """Create an entity with analytics config for testing."""
    return Entity(
        name="users",
        description="Users with analytics",
        source=TableSource(project="p", dataset="d", table="users", duckdb_path="users.parquet"),
        layers=LayersConfig(
            prep=PrepLayer(model_name="prep_users"),
            dimension=DimensionLayer(model_name="dim_users"),
            analytics=AnalyticsLayer(
                model_name="analytics_users",
                date_expression="t.created_at.date()",
                metrics=[
                    AnalyticsMetric(name="user_count", expression="t.id.count()", metric_type="event"),
                    AnalyticsMetric(name="total_amount", expression="t.amount.sum()", metric_type="event"),
                ],
                dimensions=dimensions or [],
            ),
        ),
    )


class TestAnalyticsCodeGeneration:
    """Test _generate_analytics_function code generation."""

    def test_generates_function(self):
        entity = _analytics_entity()
        gen = IbisCodeGenerator(entity)
        code = gen._generate_analytics_function()
        assert "def analytics_users(" in code

    def test_has_date_mutate(self):
        entity = _analytics_entity()
        gen = IbisCodeGenerator(entity)
        code = gen._generate_analytics_function()
        assert "t = t.mutate(_date=t.created_at.date())" in code

    def test_has_metrics(self):
        entity = _analytics_entity()
        gen = IbisCodeGenerator(entity)
        code = gen._generate_analytics_function()
        assert "user_count=t.id.count()" in code
        assert "total_amount=t.amount.sum()" in code

    def test_group_by_date(self):
        entity = _analytics_entity()
        gen = IbisCodeGenerator(entity)
        code = gen._generate_analytics_function()
        assert 'group_by("_date")' in code

    def test_with_dimensions(self):
        entity = _analytics_entity(dimensions=["plan", "region"])
        gen = IbisCodeGenerator(entity)
        code = gen._generate_analytics_function()
        assert '"plan"' in code
        assert '"region"' in code
        assert 'group_by("_date", "plan", "region")' in code

    def test_analytics_input_is_dim(self):
        entity = _analytics_entity()
        gen = IbisCodeGenerator(entity)
        code = gen._generate_analytics_function()
        assert "dim_users: ibis.Table" in code

    def test_analytics_without_dim_uses_prep(self):
        entity = Entity(
            name="users",
            description="Users",
            source=TableSource(project="p", dataset="d", table="users", duckdb_path="u.parquet"),
            layers=LayersConfig(
                prep=PrepLayer(model_name="prep_users"),
                analytics=AnalyticsLayer(
                    model_name="analytics_users",
                    date_expression="t.created_at.date()",
                    metrics=[AnalyticsMetric(name="cnt", expression="t.id.count()", metric_type="event")],
                    dimensions=[],
                ),
            ),
        )
        gen = IbisCodeGenerator(entity)
        code = gen._generate_analytics_function()
        assert "prep_users: ibis.Table" in code

    def test_analytics_is_valid_python(self):
        entity = _analytics_entity()
        gen = IbisCodeGenerator(entity)
        code = gen.generate_module()
        ast.parse(code)

    def test_analytics_in_generate_module(self):
        entity = _analytics_entity()
        gen = IbisCodeGenerator(entity)
        code = gen.generate_module()
        tree = ast.parse(code)
        func_names = [n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]
        assert "analytics_users" in func_names
