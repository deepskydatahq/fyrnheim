"""Tests for IbisCodeGenerator base class and source function generation."""

import ast

import pytest

from fyrnheim import (
    ActivityConfig,
    ActivityType,
    AggregationSource,
    AnalyticsLayer,
    AnalyticsMetric,
    ComputedColumn,
    DerivedSource,
    DimensionLayer,
    Entity,
    Field,
    LayersConfig,
    PrepLayer,
    SourceMapping,
    TableSource,
    UnionSource,
)
from fyrnheim.core.source import IdentityGraphConfig, IdentityGraphSource
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
            duckdb_path="data/charges/*.parquet",
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
                    duckdb_path="data/contacts/*.parquet",
                ),
                TableSource(
                    project="warehouse",
                    dataset="stripe",
                    table="customers",
                    duckdb_path="data/customers/*.parquet",
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


class TestConstructorWithSourceMapping:
    """Test IbisCodeGenerator accepts optional SourceMapping."""

    def test_source_mapping_defaults_to_none(self, simple_entity):
        gen = IbisCodeGenerator(simple_entity)
        assert gen.source_mapping is None

    def test_source_mapping_stored_when_provided(self, simple_entity):
        sm = SourceMapping(
            entity=simple_entity,
            source=simple_entity.source,
            field_mappings={},
        )
        gen = IbisCodeGenerator(simple_entity, source_mapping=sm)
        assert gen.source_mapping is sm

    def test_generate_module_unchanged_without_source_mapping(self, simple_entity):
        gen_without = IbisCodeGenerator(simple_entity)
        gen_with_none = IbisCodeGenerator(simple_entity, source_mapping=None)
        assert gen_without.generate_module() == gen_with_none.generate_module()


class TestRenameGeneration:
    """Test .rename() generation from SourceMapping.field_mappings."""

    def test_no_rename_when_no_source_mapping(self, simple_entity):
        gen = IbisCodeGenerator(simple_entity)
        code = gen._generate_source_functions()
        assert ".rename(" not in code

    def test_no_rename_when_empty_field_mappings(self, simple_entity):
        sm = SourceMapping(
            entity=simple_entity,
            source=simple_entity.source,
            field_mappings={},
        )
        gen = IbisCodeGenerator(simple_entity, source_mapping=sm)
        code = gen._generate_source_functions()
        assert ".rename(" not in code

    def test_single_rename_in_duckdb_branch(self, simple_entity):
        sm = SourceMapping(
            entity=simple_entity,
            source=simple_entity.source,
            field_mappings={"transaction_id": "id"},
        )
        gen = IbisCodeGenerator(simple_entity, source_mapping=sm)
        code = gen._generate_source_functions()
        assert "return raw.rename(" in code

    def test_single_rename_in_bigquery_branch(self, simple_entity):
        sm = SourceMapping(
            entity=simple_entity,
            source=simple_entity.source,
            field_mappings={"transaction_id": "id"},
        )
        gen = IbisCodeGenerator(simple_entity, source_mapping=sm)
        code = gen._generate_source_functions()
        assert ").rename(" in code
        # bigquery branch also has .rename()
        lines = code.split("\n")
        rename_lines = [line for line in lines if ".rename(" in line]
        assert len(rename_lines) == 2  # duckdb + bigquery

    def test_multiple_renames(self, simple_entity):
        sm = SourceMapping(
            entity=simple_entity,
            source=simple_entity.source,
            field_mappings={"transaction_id": "id", "amount_cents": "subtotal"},
        )
        gen = IbisCodeGenerator(simple_entity, source_mapping=sm)
        code = gen._generate_source_functions()
        assert "'transaction_id': 'id'" in code
        assert "'amount_cents': 'subtotal'" in code

    def test_rename_dict_not_inverted(self, simple_entity):
        """field_mappings {entity_field: source_col} passed directly to .rename()."""
        sm = SourceMapping(
            entity=simple_entity,
            source=simple_entity.source,
            field_mappings={"transaction_id": "id"},
        )
        gen = IbisCodeGenerator(simple_entity, source_mapping=sm)
        code = gen._generate_source_functions()
        assert "{'transaction_id': 'id'}" in code

    def test_rename_is_valid_python(self, simple_entity):
        sm = SourceMapping(
            entity=simple_entity,
            source=simple_entity.source,
            field_mappings={"transaction_id": "id", "amount_cents": "subtotal"},
        )
        gen = IbisCodeGenerator(simple_entity, source_mapping=sm)
        code = gen._generate_imports() + gen._generate_source_functions()
        ast.parse(code)

    def test_full_module_with_rename_valid(self, simple_entity):
        sm = SourceMapping(
            entity=simple_entity,
            source=simple_entity.source,
            field_mappings={"transaction_id": "id"},
        )
        gen = IbisCodeGenerator(simple_entity, source_mapping=sm)
        code = gen.generate_module()
        ast.parse(code)

    def test_backward_compat_existing_tests_unaffected(self, simple_entity):
        """Existing usage without source_mapping produces identical code."""
        gen = IbisCodeGenerator(simple_entity)
        code = gen._generate_source_functions()
        assert ".rename(" not in code
        assert "conn.read_parquet" in code


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
        assert "data/charges" in code


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
    at_kwargs = {
        "name": "created",
        "trigger": trigger,
        "timestamp_field": "created_at",
    }
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


# ---------------------------------------------------------------------------
# Union source field_mappings / literal_columns codegen tests
# ---------------------------------------------------------------------------


class TestUnionSourceFieldMappingsCodegen:
    """Test codegen for UnionSource with field_mappings and literal_columns."""

    def _make_union_entity(self, sources):
        """Helper to create a union entity with given sources."""
        return Entity(
            name="signals",
            description="Unified signals",
            layers=LayersConfig(prep=PrepLayer(model_name="prep_signals")),
            source=UnionSource(sources=sources),
            required_fields=[Field(name="id", type="STRING")],
        )

    def test_rename_generated_for_field_mappings(self):
        entity = self._make_union_entity([
            TableSource(
                project="w", dataset="h", table="contacts",
                duckdb_path="~/data/contacts.parquet",
                field_mappings={"contact_email": "email"},
            ),
        ])
        gen = IbisCodeGenerator(entity)
        code = gen._generate_source_functions()
        # field_mappings={source: unified}, .rename() needs {new: old}
        assert ".rename({'email': 'contact_email'})" in code

    def test_mutate_literal_generated(self):
        entity = self._make_union_entity([
            TableSource(
                project="w", dataset="h", table="contacts",
                duckdb_path="~/data/contacts.parquet",
                literal_columns={"product_type": "video"},
            ),
        ])
        gen = IbisCodeGenerator(entity)
        code = gen._generate_source_functions()
        assert ".mutate(product_type=ibis.literal('video'))" in code

    def test_both_rename_and_mutate_chained(self):
        entity = self._make_union_entity([
            TableSource(
                project="w", dataset="h", table="contacts",
                duckdb_path="~/data/contacts.parquet",
                field_mappings={"contact_email": "email"},
                literal_columns={"source": "hubspot"},
            ),
        ])
        gen = IbisCodeGenerator(entity)
        code = gen._generate_source_functions()
        assert ".rename(" in code
        assert ".mutate(" in code
        # rename comes before mutate
        rename_pos = code.index(".rename(")
        mutate_pos = code.index(".mutate(")
        assert rename_pos < mutate_pos

    def test_empty_mappings_no_suffix(self):
        entity = self._make_union_entity([
            TableSource(
                project="w", dataset="h", table="contacts",
                duckdb_path="~/data/contacts.parquet",
            ),
        ])
        gen = IbisCodeGenerator(entity)
        code = gen._generate_source_functions()
        # Per-source functions should not have rename or mutate
        # (the union aggregator has schema normalization mutate which is expected)
        for line in code.split("\n"):
            if "return conn.read_parquet" in line or "return conn.table" in line:
                assert ".rename(" not in line
                assert ".mutate(" not in line

    def test_multiple_sources_different_mappings(self):
        entity = self._make_union_entity([
            TableSource(
                project="w", dataset="h", table="contacts",
                duckdb_path="~/data/contacts.parquet",
                field_mappings={"contact_email": "email"},
            ),
            TableSource(
                project="w", dataset="s", table="customers",
                duckdb_path="~/data/customers.parquet",
                field_mappings={"email_address": "email"},
            ),
        ])
        gen = IbisCodeGenerator(entity)
        code = gen._generate_source_functions()
        assert ".rename({'email': 'contact_email'})" in code
        assert ".rename({'email': 'email_address'})" in code

    def test_generated_module_valid_python(self):
        entity = self._make_union_entity([
            TableSource(
                project="w", dataset="h", table="contacts",
                duckdb_path="~/data/contacts.parquet",
                field_mappings={"contact_email": "email", "contact_name": "name"},
                literal_columns={"source": "hubspot", "priority": 1},
            ),
            TableSource(
                project="w", dataset="s", table="customers",
                duckdb_path="~/data/customers.parquet",
                field_mappings={"email_address": "email"},
                literal_columns={"source": "stripe"},
            ),
        ])
        gen = IbisCodeGenerator(entity)
        code = gen.generate_module()
        ast.parse(code)  # Must not raise

    def test_union_aggregator_unchanged(self):
        entity = self._make_union_entity([
            TableSource(
                project="w", dataset="h", table="contacts",
                duckdb_path="~/data/contacts.parquet",
                field_mappings={"contact_email": "email"},
            ),
            TableSource(
                project="w", dataset="s", table="customers",
                duckdb_path="~/data/customers.parquet",
            ),
        ])
        gen = IbisCodeGenerator(entity)
        code = gen._generate_source_functions()
        assert "def source_signals(" in code
        assert "ibis.union(*normalized)" in code


# ---------------------------------------------------------------------------
# DerivedSource identity graph codegen tests
# ---------------------------------------------------------------------------


class TestDerivedSourceCodeGeneration:
    """Test codegen for DerivedSource with identity graph config."""

    def _make_config(self, **kwargs):
        """Helper to create a minimal IdentityGraphConfig."""
        defaults = {
            "match_key": "email",
            "sources": [
                IdentityGraphSource(
                    name="hubspot",
                    entity="hubspot_person",
                    match_key_field="hs_email",
                    fields={"name": "full_name"},
                    id_field="person_id",
                ),
                IdentityGraphSource(
                    name="stripe",
                    entity="stripe_customer",
                    match_key_field="contact_email",
                    fields={"name": "cust_name"},
                    id_field="customer_id",
                ),
            ],
            "priority": ["hubspot", "stripe"],
        }
        defaults.update(kwargs)
        return IdentityGraphConfig(**defaults)

    def _make_entity(self, config=None):
        if config is None:
            config = self._make_config()
        return Entity(
            name="person",
            description="Unified person",
            layers=LayersConfig(prep=PrepLayer(model_name="prep_person")),
            source=DerivedSource(
                identity_graph="person_graph",
                identity_graph_config=config,
            ),
        )

    def test_non_empty_output(self):
        entity = self._make_entity()
        gen = IbisCodeGenerator(entity)
        code = gen._generate_source_functions()
        assert len(code) > 0

    def test_function_signature(self):
        entity = self._make_entity()
        gen = IbisCodeGenerator(entity)
        code = gen._generate_source_functions()
        assert "def source_person(sources: dict) -> ibis.Table:" in code

    def test_outer_join_present(self):
        entity = self._make_entity()
        gen = IbisCodeGenerator(entity)
        code = gen._generate_source_functions()
        assert ".outer_join(" in code

    def test_fill_null_chain_in_priority_order(self):
        entity = self._make_entity()
        gen = IbisCodeGenerator(entity)
        code = gen._generate_source_functions()
        assert "name_hubspot" in code
        assert "name_stripe" in code
        assert ".fill_null(" in code

    def test_source_flags(self):
        entity = self._make_entity()
        gen = IbisCodeGenerator(entity)
        code = gen._generate_source_functions()
        assert "is_hubspot" in code
        assert "is_stripe" in code
        assert ".notnull()" in code

    def test_source_ids_when_id_field_set(self):
        entity = self._make_entity()
        gen = IbisCodeGenerator(entity)
        code = gen._generate_source_functions()
        assert "hubspot_id" in code
        assert "stripe_id" in code

    def test_date_field_when_set(self):
        config = IdentityGraphConfig(
            match_key="email",
            sources=[
                IdentityGraphSource(
                    name="hubspot",
                    entity="hubspot_person",
                    match_key_field="email",
                    fields={"name": "full_name"},
                    date_field="signup_date",
                ),
                IdentityGraphSource(
                    name="stripe",
                    entity="stripe_customer",
                    match_key_field="contact_email",
                    fields={"name": "name"},
                ),
            ],
            priority=["hubspot", "stripe"],
        )
        entity = self._make_entity(config)
        gen = IbisCodeGenerator(entity)
        code = gen._generate_source_functions()
        assert "first_seen_hubspot" in code

    def test_ast_parse_valid(self):
        entity = self._make_entity()
        gen = IbisCodeGenerator(entity)
        code = gen.generate_module()
        ast.parse(code)

    def test_backward_compat_no_config(self):
        entity = Entity(
            name="person",
            description="Person",
            layers=LayersConfig(prep=PrepLayer(model_name="prep_person")),
            source=DerivedSource(identity_graph="person_graph"),
        )
        gen = IbisCodeGenerator(entity)
        code = gen._generate_source_functions()
        assert code == ""

    def test_three_sources_cascading_join(self):
        config = IdentityGraphConfig(
            match_key="email",
            sources=[
                IdentityGraphSource(
                    name="a",
                    entity="a_entity",
                    match_key_field="email",
                    fields={"name": "name_a"},
                ),
                IdentityGraphSource(
                    name="b",
                    entity="b_entity",
                    match_key_field="email",
                    fields={"name": "name_b"},
                ),
                IdentityGraphSource(
                    name="c",
                    entity="c_entity",
                    match_key_field="email",
                    fields={"name": "name_c"},
                ),
            ],
            priority=["a", "b", "c"],
        )
        entity = self._make_entity(config)
        gen = IbisCodeGenerator(entity)
        code = gen._generate_source_functions()
        # Should have 2 outer_join calls
        assert code.count(".outer_join(") == 2
        assert "name_a" in code
        assert "name_b" in code
        assert "name_c" in code
        ast.parse(gen.generate_module())

    def test_rename_uses_correct_direction(self):
        """Ibis .rename() takes {new_name: old_name}."""
        entity = self._make_entity()
        gen = IbisCodeGenerator(entity)
        code = gen._generate_source_functions()
        # hubspot: match_key_field="hs_email" -> unified "email"
        # So rename should be {'email': 'hs_email'}
        assert "{'email': 'hs_email'" in code

    def test_select_after_each_join(self):
        """Each outer_join() must be followed by .select() (Ibis bug #10293)."""
        entity = self._make_entity()
        gen = IbisCodeGenerator(entity)
        code = gen._generate_source_functions()
        assert ".outer_join(" in code
        assert ").select(" in code

    def test_drop_intermediate_columns(self):
        entity = self._make_entity()
        gen = IbisCodeGenerator(entity)
        code = gen._generate_source_functions()
        assert ".drop(" in code
        assert '"name_hubspot"' in code
        assert '"name_stripe"' in code
        assert '"_hubspot_match_key"' in code
        assert '"_stripe_match_key"' in code

    def test_coalesce_match_key(self):
        entity = self._make_entity()
        gen = IbisCodeGenerator(entity)
        code = gen._generate_source_functions()
        assert "ibis.coalesce(" in code

    def test_extract_source_tables(self):
        entity = self._make_entity()
        gen = IbisCodeGenerator(entity)
        code = gen._generate_source_functions()
        assert 't_hubspot = sources["hubspot"]' in code
        assert 't_stripe = sources["stripe"]' in code


# ---------------------------------------------------------------------------
# AggregationSource codegen tests
# ---------------------------------------------------------------------------


class TestAggregationSourceCodeGeneration:
    """Tests for AggregationSource code generation."""

    def _make_entity(self, aggregations=None, filter_expression=None):
        source = AggregationSource(
            source_entity="person",
            group_by_column="account_id",
            aggregations=aggregations or [],
            filter_expression=filter_expression,
        )
        return Entity(
            name="account",
            description="Account entity",
            source=source,
            layers=LayersConfig(prep=PrepLayer(model_name="prep_account")),
        )

    def test_function_signature(self):
        entity = self._make_entity()
        gen = IbisCodeGenerator(entity)
        code = gen.generate_module()
        assert "def source_account(source_person: ibis.Table) -> ibis.Table:" in code

    def test_aggregate_with_expressions(self):
        entity = self._make_entity(aggregations=[
            ComputedColumn(name="person_count", expression="t.person_id.count()"),
            ComputedColumn(name="total_amount", expression="t.amount.sum()"),
        ])
        gen = IbisCodeGenerator(entity)
        code = gen.generate_module()
        assert '.group_by("account_id").aggregate(' in code
        assert "person_count=t.person_id.count()," in code
        assert "total_amount=t.amount.sum()," in code

    def test_empty_aggregations(self):
        entity = self._make_entity()
        gen = IbisCodeGenerator(entity)
        code = gen.generate_module()
        assert '.group_by("account_id").aggregate()' in code

    def test_filter_expression_included(self):
        entity = self._make_entity(
            aggregations=[
                ComputedColumn(name="cnt", expression="t.id.count()"),
            ],
            filter_expression="t.status == 'active'",
        )
        gen = IbisCodeGenerator(entity)
        code = gen.generate_module()
        assert "t = t.filter(t.status == 'active')" in code
        assert '.group_by("account_id").aggregate(' in code

    def test_ast_parse_valid(self):
        entity = self._make_entity(aggregations=[
            ComputedColumn(name="person_count", expression="t.person_id.count()"),
            ComputedColumn(name="total_amount", expression="t.amount.sum()"),
        ])
        gen = IbisCodeGenerator(entity)
        code = gen.generate_module()
        ast.parse(code)  # Should not raise

    def test_ast_parse_valid_empty_aggregations(self):
        entity = self._make_entity()
        gen = IbisCodeGenerator(entity)
        code = gen.generate_module()
        ast.parse(code)

    def test_ast_parse_valid_with_filter(self):
        entity = self._make_entity(
            aggregations=[ComputedColumn(name="cnt", expression="t.id.count()")],
            filter_expression="t.active == True",
        )
        gen = IbisCodeGenerator(entity)
        code = gen.generate_module()
        ast.parse(code)

    def test_expression_binding(self):
        """Expressions without t. prefix get bound correctly."""
        entity = self._make_entity(aggregations=[
            ComputedColumn(name="cnt", expression="person_id.count()"),
        ])
        gen = IbisCodeGenerator(entity)
        code = gen.generate_module()
        assert "cnt=t.person_id.count()," in code
