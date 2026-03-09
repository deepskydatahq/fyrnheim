"""Tests for code generator handling of inline identity graph sources."""

import ast

import pytest

from fyrnheim import (
    ComputedColumn,
    DerivedSource,
    Entity,
    LayersConfig,
    PrepLayer,
    TableSource,
)
from fyrnheim.core.source import IdentityGraphConfig, IdentityGraphSource
from fyrnheim.generators import IbisCodeGenerator


class TestInlineSourceCodegen:
    """Code generator produces correct source functions for inline TableSource."""

    def _make_entity(self, config):
        return Entity(
            name="person",
            description="Unified person",
            layers=LayersConfig(prep=PrepLayer(model_name="prep_person")),
            source=DerivedSource(
                identity_graph="person_graph",
                identity_graph_config=config,
            ),
        )

    def test_inline_source_generates_read_parquet(self):
        """Generated code includes read_parquet for inline DuckDB source."""
        config = IdentityGraphConfig(
            match_key="email",
            sources=[
                IdentityGraphSource(
                    name="src_a",
                    source=TableSource(
                        project="p", dataset="d", table="a",
                        duckdb_path="~/data/a.parquet",
                    ),
                    match_key_field="email",
                    fields={"name": "name_a"},
                ),
                IdentityGraphSource(
                    name="src_b",
                    source=TableSource(
                        project="p", dataset="d", table="b",
                        duckdb_path="~/data/b.parquet",
                    ),
                    match_key_field="email",
                    fields={"name": "name_b"},
                ),
            ],
            priority=["src_a", "src_b"],
        )
        entity = self._make_entity(config)
        gen = IbisCodeGenerator(entity)
        code = gen._generate_source_functions()
        assert 'conn.read_parquet(os.path.expanduser("~/data/a.parquet"))' in code
        assert 'conn.read_parquet(os.path.expanduser("~/data/b.parquet"))' in code

    def test_inline_source_generates_bigquery_table(self):
        """Generated code includes conn.table() for BigQuery backend."""
        config = IdentityGraphConfig(
            match_key="email",
            sources=[
                IdentityGraphSource(
                    name="src_a",
                    source=TableSource(
                        project="myproj", dataset="raw", table="leads",
                        duckdb_path="~/leads.parquet",
                    ),
                    match_key_field="email",
                    fields={"name": "name_a"},
                ),
                IdentityGraphSource(
                    name="src_b",
                    source=TableSource(
                        project="myproj", dataset="raw", table="contacts",
                        duckdb_path="~/contacts.parquet",
                    ),
                    match_key_field="email",
                    fields={"name": "name_b"},
                ),
            ],
            priority=["src_a", "src_b"],
        )
        entity = self._make_entity(config)
        gen = IbisCodeGenerator(entity)
        code = gen._generate_source_functions()
        assert 'conn.table("leads", database=("myproj", "raw"))' in code
        assert 'conn.table("contacts", database=("myproj", "raw"))' in code

    def test_inline_source_function_signature_has_conn_backend(self):
        """When inline sources present, signature includes conn and backend."""
        config = IdentityGraphConfig(
            match_key="email",
            sources=[
                IdentityGraphSource(
                    name="src_a",
                    source=TableSource(
                        project="p", dataset="d", table="a",
                        duckdb_path="~/a.parquet",
                    ),
                    match_key_field="email",
                    fields={"name": "n_a"},
                ),
                IdentityGraphSource(
                    name="src_b",
                    source=TableSource(
                        project="p", dataset="d", table="b",
                        duckdb_path="~/b.parquet",
                    ),
                    match_key_field="email",
                    fields={"name": "n_b"},
                ),
            ],
            priority=["src_a", "src_b"],
        )
        entity = self._make_entity(config)
        gen = IbisCodeGenerator(entity)
        code = gen._generate_source_functions()
        assert "def source_person(sources: dict, conn: ibis.BaseBackend, backend: str)" in code

    def test_entity_ref_only_no_conn_backend(self):
        """When all sources are entity-ref, signature does NOT include conn/backend."""
        config = IdentityGraphConfig(
            match_key="email",
            sources=[
                IdentityGraphSource(
                    name="hubspot", entity="hubspot_person",
                    match_key_field="email", fields={"name": "n"},
                ),
                IdentityGraphSource(
                    name="stripe", entity="stripe_person",
                    match_key_field="email", fields={"name": "n"},
                ),
            ],
            priority=["hubspot", "stripe"],
        )
        entity = self._make_entity(config)
        gen = IbisCodeGenerator(entity)
        code = gen._generate_source_functions()
        assert "def source_person(sources: dict) -> ibis.Table:" in code
        assert "conn" not in code.split("def source_person")[1].split("):")[0]

    def test_prep_columns_generate_mutate(self):
        """Inline source with prep_columns generates .mutate() calls."""
        config = IdentityGraphConfig(
            match_key="email",
            sources=[
                IdentityGraphSource(
                    name="raw_leads",
                    source=TableSource(
                        project="p", dataset="d", table="leads",
                        duckdb_path="~/leads.parquet",
                    ),
                    match_key_field="lead_email",
                    fields={"name": "lead_name"},
                    prep_columns=[
                        ComputedColumn(
                            name="email_lower",
                            expression="t.lead_email.lower()",
                        ),
                    ],
                ),
                IdentityGraphSource(
                    name="other",
                    source=TableSource(
                        project="p", dataset="d", table="other",
                        duckdb_path="~/other.parquet",
                    ),
                    match_key_field="email",
                    fields={"name": "other_name"},
                ),
            ],
            priority=["raw_leads", "other"],
        )
        entity = self._make_entity(config)
        gen = IbisCodeGenerator(entity)
        code = gen._generate_source_functions()
        assert "email_lower=t.lead_email.lower()" in code
        assert ".mutate(" in code

    def test_mixed_inline_and_entity_ref(self):
        """Mixed inline + entity-reference sources produce correct code."""
        config = IdentityGraphConfig(
            match_key="email",
            sources=[
                IdentityGraphSource(
                    name="hubspot",
                    entity="hubspot_person",
                    match_key_field="hs_email",
                    fields={"name": "full_name"},
                    id_field="person_id",
                ),
                IdentityGraphSource(
                    name="raw_leads",
                    source=TableSource(
                        project="p", dataset="d", table="leads",
                        duckdb_path="~/leads.parquet",
                    ),
                    match_key_field="lead_email",
                    fields={"name": "lead_name"},
                ),
            ],
            priority=["hubspot", "raw_leads"],
        )
        entity = self._make_entity(config)
        gen = IbisCodeGenerator(entity)
        code = gen._generate_source_functions()
        # Entity ref reads from sources dict
        assert 't_hubspot = sources["hubspot"]' in code
        # Inline reads from conn
        assert "conn.read_parquet" in code
        # Signature includes conn/backend because of inline source
        assert "conn: ibis.BaseBackend" in code

    def test_ast_parse_valid_inline(self):
        """Generated module with inline sources is valid Python."""
        config = IdentityGraphConfig(
            match_key="email",
            sources=[
                IdentityGraphSource(
                    name="hubspot",
                    entity="hubspot_person",
                    match_key_field="email",
                    fields={"name": "full_name"},
                ),
                IdentityGraphSource(
                    name="raw_leads",
                    source=TableSource(
                        project="p", dataset="d", table="leads",
                        duckdb_path="~/leads.parquet",
                    ),
                    match_key_field="lead_email",
                    fields={"name": "lead_name"},
                    prep_columns=[
                        ComputedColumn(name="email_clean", expression="t.lead_email.lower()"),
                    ],
                ),
            ],
            priority=["hubspot", "raw_leads"],
        )
        entity = self._make_entity(config)
        gen = IbisCodeGenerator(entity)
        code = gen.generate_module()
        ast.parse(code)

    def test_ast_parse_valid_all_inline(self):
        """Generated module with all-inline sources is valid Python."""
        config = IdentityGraphConfig(
            match_key="email",
            sources=[
                IdentityGraphSource(
                    name="src_a",
                    source=TableSource(
                        project="p", dataset="d", table="a",
                        duckdb_path="~/a.parquet",
                    ),
                    match_key_field="email",
                    fields={"name": "n_a"},
                ),
                IdentityGraphSource(
                    name="src_b",
                    source=TableSource(
                        project="p", dataset="d", table="b",
                        duckdb_path="~/b.parquet",
                    ),
                    match_key_field="email",
                    fields={"name": "n_b"},
                ),
            ],
            priority=["src_a", "src_b"],
        )
        entity = self._make_entity(config)
        gen = IbisCodeGenerator(entity)
        code = gen.generate_module()
        ast.parse(code)

    def test_inline_source_without_duckdb_path_raises(self):
        """Inline source without duckdb_path raises ValueError during codegen."""
        config = IdentityGraphConfig(
            match_key="email",
            sources=[
                IdentityGraphSource(
                    name="src_a",
                    source=TableSource(
                        project="p", dataset="d", table="a",
                        # no duckdb_path
                    ),
                    match_key_field="email",
                    fields={"name": "n_a"},
                ),
                IdentityGraphSource(
                    name="src_b",
                    source=TableSource(
                        project="p", dataset="d", table="b",
                        duckdb_path="~/b.parquet",
                    ),
                    match_key_field="email",
                    fields={"name": "n_b"},
                ),
            ],
            priority=["src_a", "src_b"],
        )
        entity = self._make_entity(config)
        gen = IbisCodeGenerator(entity)
        with pytest.raises(ValueError, match="duckdb_path"):
            gen._generate_source_functions()
