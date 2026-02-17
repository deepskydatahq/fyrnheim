"""Tests for PrepLayer and DimensionLayer code generation."""

import ast

from fyrnheim import (
    ComputedColumn,
    DimensionLayer,
    Divide,
    Entity,
    LayersConfig,
    Multiply,
    PrepLayer,
    Rename,
    SourceTransforms,
    TableSource,
    TypeCast,
)
from fyrnheim.generators import IbisCodeGenerator


def _make_entity(
    name="transactions",
    source=None,
    prep=None,
    dimension=None,
    core_computed=None,
):
    """Helper to build Entity instances for testing."""
    layers_kwargs = {}
    if prep is not None:
        layers_kwargs["prep"] = prep
    elif dimension is None:
        layers_kwargs["prep"] = PrepLayer(model_name=f"prep_{name}")
    if dimension is not None:
        layers_kwargs["dimension"] = dimension

    return Entity(
        name=name,
        description=f"Test entity {name}",
        layers=LayersConfig(**layers_kwargs),
        source=source or TableSource(project="p", dataset="d", table="t"),
        core_computed=core_computed,
    )


class TestPrepWithSourceTransforms:
    """Test prep function generation with source transforms."""

    def test_generates_rename(self):
        entity = _make_entity(
            source=TableSource(
                project="p",
                dataset="d",
                table="t",
                transforms=SourceTransforms(
                    renames=[Rename(from_name="id", to_name="transaction_id")],
                ),
            ),
        )
        gen = IbisCodeGenerator(entity)
        code = gen._generate_prep_function()
        assert 't.rename(transaction_id="id")' in code

    def test_generates_type_cast(self):
        entity = _make_entity(
            source=TableSource(
                project="p",
                dataset="d",
                table="t",
                transforms=SourceTransforms(
                    type_casts=[TypeCast(field="created_at", target_type="timestamp")],
                ),
            ),
        )
        gen = IbisCodeGenerator(entity)
        code = gen._generate_prep_function()
        assert 't.created_at.cast("timestamp")' in code

    def test_generates_divide(self):
        entity = _make_entity(
            source=TableSource(
                project="p",
                dataset="d",
                table="t",
                transforms=SourceTransforms(
                    divides=[Divide(field="subtotal", divisor=100.0)],
                ),
            ),
        )
        gen = IbisCodeGenerator(entity)
        code = gen._generate_prep_function()
        assert "(t.subtotal / 100.0)" in code
        assert "subtotal_amount=" in code

    def test_generates_multiply(self):
        entity = _make_entity(
            source=TableSource(
                project="p",
                dataset="d",
                table="t",
                transforms=SourceTransforms(
                    multiplies=[Multiply(field="rate", multiplier=100.0, suffix="_pct")],
                ),
            ),
        )
        gen = IbisCodeGenerator(entity)
        code = gen._generate_prep_function()
        assert "(t.rate * 100.0)" in code
        assert "rate_pct=" in code

    def test_rename_cast_divide_order(self):
        entity = _make_entity(
            source=TableSource(
                project="p",
                dataset="d",
                table="t",
                transforms=SourceTransforms(
                    renames=[Rename(from_name="id", to_name="tx_id")],
                    type_casts=[TypeCast(field="created_at", target_type="timestamp")],
                    divides=[Divide(field="amt", divisor=100.0)],
                ),
            ),
        )
        gen = IbisCodeGenerator(entity)
        code = gen._generate_prep_function()
        # Renames before casts before divides
        rename_pos = code.index("t.rename(")
        cast_pos = code.index(".cast(")
        divide_pos = code.index("/ 100.0")
        assert rename_pos < cast_pos < divide_pos


class TestPrepWithComputedColumns:
    """Test prep function generation with computed columns."""

    def test_generates_mutate(self):
        entity = _make_entity(
            prep=PrepLayer(
                model_name="prep_transactions",
                computed_columns=[
                    ComputedColumn(name="email_hash", expression="email.lower().hash()"),
                ],
            ),
        )
        gen = IbisCodeGenerator(entity)
        code = gen._generate_prep_function()
        assert "t.mutate(" in code
        assert "email_hash=t.email.lower().hash()" in code

    def test_bind_expression_applied(self):
        entity = _make_entity(
            prep=PrepLayer(
                model_name="prep_transactions",
                computed_columns=[
                    ComputedColumn(name="x", expression="col.lower()"),
                ],
            ),
        )
        gen = IbisCodeGenerator(entity)
        code = gen._generate_prep_function()
        assert "x=t.col.lower()" in code

    def test_ibis_expression_passthrough(self):
        entity = _make_entity(
            prep=PrepLayer(
                model_name="prep_transactions",
                computed_columns=[
                    ComputedColumn(name="y", expression="ibis.now()"),
                ],
            ),
        )
        gen = IbisCodeGenerator(entity)
        code = gen._generate_prep_function()
        assert "y=ibis.now()" in code

    def test_description_as_comment(self):
        entity = _make_entity(
            prep=PrepLayer(
                model_name="prep_transactions",
                computed_columns=[
                    ComputedColumn(
                        name="x",
                        expression="t.y",
                        description="My description",
                    ),
                ],
            ),
        )
        gen = IbisCodeGenerator(entity)
        code = gen._generate_prep_function()
        assert "# My description" in code


class TestPrepWithTransformsAndComputed:
    """Test prep with both source transforms and computed columns."""

    def test_transforms_before_computed(self):
        entity = _make_entity(
            source=TableSource(
                project="p",
                dataset="d",
                table="t",
                transforms=SourceTransforms(
                    renames=[Rename(from_name="id", to_name="tx_id")],
                ),
            ),
            prep=PrepLayer(
                model_name="prep_transactions",
                computed_columns=[
                    ComputedColumn(name="x", expression="t.y"),
                ],
            ),
        )
        gen = IbisCodeGenerator(entity)
        code = gen._generate_prep_function()
        rename_pos = code.index("t.rename(")
        computed_pos = code.index("t.mutate(")
        assert rename_pos < computed_pos

    def test_ast_parses(self):
        entity = _make_entity(
            source=TableSource(
                project="p",
                dataset="d",
                table="t",
                transforms=SourceTransforms(
                    renames=[Rename(from_name="id", to_name="tx_id")],
                    type_casts=[TypeCast(field="created_at", target_type="timestamp")],
                ),
            ),
            prep=PrepLayer(
                model_name="prep_transactions",
                computed_columns=[
                    ComputedColumn(name="email_hash", expression="email.lower().hash()"),
                ],
            ),
        )
        gen = IbisCodeGenerator(entity)
        code = gen._generate_imports() + gen._generate_prep_function()
        ast.parse(code)


class TestPrepPassthrough:
    """Test prep passthrough when no transforms or computed columns."""

    def test_returns_t(self):
        entity = _make_entity(
            prep=PrepLayer(model_name="prep_things"),
        )
        gen = IbisCodeGenerator(entity)
        code = gen._generate_prep_function()
        assert "return t" in code

    def test_no_mutate(self):
        entity = _make_entity(
            prep=PrepLayer(model_name="prep_things"),
        )
        gen = IbisCodeGenerator(entity)
        code = gen._generate_prep_function()
        assert ".mutate(" not in code
        assert ".rename(" not in code


class TestDimensionGeneration:
    """Test dimension layer function generation."""

    def test_merges_core_and_layer_computed(self):
        entity = _make_entity(
            name="subscriptions",
            core_computed=[
                ComputedColumn(name="is_active", expression='t.status.isin(["active"])'),
            ],
            dimension=DimensionLayer(
                model_name="dim_subscriptions",
                computed_columns=[
                    ComputedColumn(name="days_active", expression="ibis.now()"),
                ],
            ),
        )
        gen = IbisCodeGenerator(entity)
        code = gen._generate_dimension_function()
        assert "is_active=" in code
        assert "days_active=" in code
        # Core first, then layer
        assert code.index("is_active=") < code.index("days_active=")

    def test_with_prep_takes_prep_input(self):
        entity = _make_entity(
            name="subscriptions",
            prep=PrepLayer(model_name="prep_subscriptions"),
            dimension=DimensionLayer(model_name="dim_subscriptions"),
        )
        gen = IbisCodeGenerator(entity)
        code = gen._generate_dimension_function()
        assert "prep_subscriptions: ibis.Table" in code

    def test_without_prep_takes_source_input(self):
        entity = _make_entity(
            name="leads",
            prep=None,
            dimension=DimensionLayer(model_name="dim_leads"),
        )
        gen = IbisCodeGenerator(entity)
        code = gen._generate_dimension_function()
        assert "source_leads: ibis.Table" in code

    def test_passthrough_when_no_computed(self):
        entity = _make_entity(
            name="leads",
            prep=None,
            dimension=DimensionLayer(model_name="dim_leads"),
        )
        gen = IbisCodeGenerator(entity)
        code = gen._generate_dimension_function()
        assert "return t" in code

    def test_description_as_comment(self):
        entity = _make_entity(
            name="users",
            prep=None,
            dimension=DimensionLayer(
                model_name="dim_users",
                computed_columns=[
                    ComputedColumn(
                        name="full_name",
                        expression="t.first || t.last",
                        description="Full name concatenation",
                    ),
                ],
            ),
        )
        gen = IbisCodeGenerator(entity)
        code = gen._generate_dimension_function()
        assert "# Full name concatenation" in code


class TestFunctionSignatures:
    """Test that function signatures use entity name correctly."""

    def test_prep_signature(self):
        entity = _make_entity(name="leads")
        gen = IbisCodeGenerator(entity)
        code = gen._generate_prep_function()
        assert "def prep_leads(source_leads: ibis.Table) -> ibis.Table:" in code

    def test_dimension_signature_with_prep(self):
        entity = _make_entity(
            name="leads",
            prep=PrepLayer(model_name="prep_leads"),
            dimension=DimensionLayer(model_name="dim_leads"),
        )
        gen = IbisCodeGenerator(entity)
        code = gen._generate_dimension_function()
        assert "def dim_leads(prep_leads: ibis.Table) -> ibis.Table:" in code


class TestFullModule:
    """Test full module generation with prep + dimension."""

    def test_ast_parses(self):
        entity = _make_entity(
            name="users",
            source=TableSource(
                project="p",
                dataset="d",
                table="users",
                transforms=SourceTransforms(
                    renames=[Rename(from_name="id", to_name="user_id")],
                    type_casts=[TypeCast(field="created_at", target_type="timestamp")],
                ),
            ),
            prep=PrepLayer(
                model_name="prep_users",
                computed_columns=[
                    ComputedColumn(name="email_hash", expression="email.lower().hash()"),
                ],
            ),
            dimension=DimensionLayer(
                model_name="dim_users",
                computed_columns=[
                    ComputedColumn(name="days_active", expression="ibis.now()"),
                ],
            ),
            core_computed=[
                ComputedColumn(name="is_active", expression='t.status == "active"'),
            ],
        )
        gen = IbisCodeGenerator(entity)
        code = gen.generate_module()
        tree = ast.parse(code)
        func_names = [n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]
        assert "source_users" in func_names
        assert "prep_users" in func_names
        assert "dim_users" in func_names


class TestMapIbisType:
    """Test _map_ibis_type helper."""

    def test_known_types(self):
        entity = _make_entity()
        gen = IbisCodeGenerator(entity)
        assert gen._map_ibis_type("timestamp") == "timestamp"
        assert gen._map_ibis_type("TIMESTAMP") == "timestamp"
        assert gen._map_ibis_type("integer") == "int64"
        assert gen._map_ibis_type("text") == "string"
        assert gen._map_ibis_type("numeric") == "decimal"

    def test_unknown_passthrough(self):
        entity = _make_entity()
        gen = IbisCodeGenerator(entity)
        assert gen._map_ibis_type("custom_type") == "custom_type"
