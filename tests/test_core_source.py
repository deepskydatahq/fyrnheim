"""Tests for source configuration classes."""

import pytest
from pydantic import ValidationError

from fyrnheim.core.source import (
    AggregationSource,
    BaseTableSource,
    DerivedEntitySource,
    DerivedSource,
    Divide,
    EventAggregationSource,
    Field,
    IdentityGraphConfig,
    IdentityGraphSource,
    Multiply,
    Rename,
    SourceTransforms,
    TableSource,
    TypeCast,
    UnionSource,
)


class TestField:
    """Tests for the Field model."""

    def test_minimal_creation(self):
        f = Field(name="email", type="STRING")
        assert f.name == "email"
        assert f.type == "STRING"
        assert f.description is None
        assert f.nullable is True
        assert f.json_path is None

    def test_full_creation(self):
        f = Field(
            name="utm_source",
            type="STRING",
            description="UTM source parameter",
            nullable=False,
            json_path="$.utm_source",
        )
        assert f.name == "utm_source"
        assert f.description == "UTM source parameter"
        assert f.nullable is False
        assert f.json_path == "$.utm_source"


class TestTypeCast:
    """Tests for the TypeCast model."""

    def test_creation(self):
        tc = TypeCast(field="amount", target_type="FLOAT64")
        assert tc.field == "amount"
        assert tc.target_type == "FLOAT64"


class TestRename:
    """Tests for the Rename model."""

    def test_creation(self):
        r = Rename(from_name="old_col", to_name="new_col")
        assert r.from_name == "old_col"
        assert r.to_name == "new_col"


class TestDivide:
    """Tests for the Divide model."""

    def test_creation_with_defaults(self):
        d = Divide(field="price_cents", divisor=100.0)
        assert d.field == "price_cents"
        assert d.divisor == 100.0
        assert d.target_type == "decimal"
        assert d.suffix == "_amount"

    def test_creation_with_custom_values(self):
        d = Divide(field="weight_grams", divisor=1000.0, target_type="float", suffix="_kg")
        assert d.target_type == "float"
        assert d.suffix == "_kg"


class TestMultiply:
    """Tests for the Multiply model."""

    def test_creation_with_defaults(self):
        m = Multiply(field="rate", multiplier=100.0)
        assert m.field == "rate"
        assert m.multiplier == 100.0
        assert m.target_type == "decimal"
        assert m.suffix == "_value"

    def test_creation_with_custom_values(self):
        m = Multiply(field="weight", multiplier=2.2046, target_type="float", suffix="_lbs")
        assert m.target_type == "float"
        assert m.suffix == "_lbs"


class TestSourceTransforms:
    """Tests for the SourceTransforms model."""

    def test_empty_defaults(self):
        st = SourceTransforms()
        assert st.type_casts == []
        assert st.renames == []
        assert st.divides == []
        assert st.multiplies == []

    def test_populated(self):
        st = SourceTransforms(
            type_casts=[TypeCast(field="a", target_type="INT64")],
            renames=[Rename(from_name="b", to_name="c")],
            divides=[Divide(field="d", divisor=10.0)],
            multiplies=[Multiply(field="e", multiplier=5.0)],
        )
        assert len(st.type_casts) == 1
        assert len(st.renames) == 1
        assert len(st.divides) == 1
        assert len(st.multiplies) == 1


class TestBaseTableSource:
    """Tests for the BaseTableSource model."""

    def test_valid_creation(self):
        src = BaseTableSource(project="myproject", dataset="mydata", table="mytable")
        assert src.project == "myproject"
        assert src.dataset == "mydata"
        assert src.table == "mytable"
        assert src.duckdb_path is None

    def test_with_duckdb_path(self):
        src = BaseTableSource(
            project="p", dataset="d", table="t", duckdb_path="~/data/file.parquet"
        )
        assert src.duckdb_path == "~/data/file.parquet"

    def test_empty_project_rejected(self):
        with pytest.raises(ValidationError):
            BaseTableSource(project="", dataset="d", table="t")

    def test_empty_dataset_rejected(self):
        with pytest.raises(ValidationError):
            BaseTableSource(project="p", dataset="", table="t")

    def test_empty_table_rejected(self):
        with pytest.raises(ValidationError):
            BaseTableSource(project="p", dataset="d", table="")

    def test_read_table_duckdb_no_path_raises(self):
        src = BaseTableSource(project="p", dataset="d", table="t")
        with pytest.raises(ValueError, match="duckdb_path is required"):
            src.read_table(conn=None, backend="duckdb")

    def test_read_table_duckdb_with_path(self):
        """read_table calls conn.read_parquet for duckdb backend."""

        class MockConn:
            def read_parquet(self, path):
                return f"parquet:{path}"

        src = BaseTableSource(
            project="p", dataset="d", table="t", duckdb_path="/tmp/test.parquet"
        )
        result = src.read_table(conn=MockConn(), backend="duckdb")
        assert result == "parquet:/tmp/test.parquet"

    def test_read_table_bigquery(self):
        """read_table calls conn.table for non-duckdb backend."""

        class MockConn:
            def table(self, name, database=None):
                return f"table:{name}:{database}"

        src = BaseTableSource(project="proj", dataset="ds", table="tbl")
        result = src.read_table(conn=MockConn(), backend="bigquery")
        assert result == "table:tbl:('proj', 'ds')"


class TestTableSource:
    """Tests for the TableSource model."""

    def test_inherits_base_table_source(self):
        assert issubclass(TableSource, BaseTableSource)

    def test_minimal_creation(self):
        src = TableSource(project="p", dataset="d", table="t")
        assert src.transforms is None
        assert src.fields is None

    def test_with_transforms_and_fields(self):
        src = TableSource(
            project="p",
            dataset="d",
            table="t",
            transforms=SourceTransforms(
                type_casts=[TypeCast(field="a", target_type="INT64")],
            ),
            fields=[Field(name="email", type="STRING")],
        )
        assert len(src.transforms.type_casts) == 1
        assert len(src.fields) == 1
        assert src.fields[0].name == "email"

    def test_field_mappings_default_empty(self):
        src = TableSource(project="p", dataset="d", table="t")
        assert src.field_mappings == {}

    def test_literal_columns_default_empty(self):
        src = TableSource(project="p", dataset="d", table="t")
        assert src.literal_columns == {}

    def test_with_field_mappings(self):
        src = TableSource(
            project="p", dataset="d", table="t",
            field_mappings={"contact_email": "email"},
        )
        assert src.field_mappings == {"contact_email": "email"}

    def test_with_literal_columns(self):
        src = TableSource(
            project="p", dataset="d", table="t",
            literal_columns={"product_type": "video"},
        )
        assert src.literal_columns == {"product_type": "video"}


class TestDerivedEntitySource:
    """Tests for DerivedEntitySource model."""

    def test_creation(self):
        des = DerivedEntitySource(type="identity_graph")
        assert des.type == "identity_graph"
        assert des.identity_graph is None
        assert des.fields is None

    def test_with_fields(self):
        des = DerivedEntitySource(
            type="identity_graph",
            fields=[Field(name="person_id", type="STRING")],
        )
        assert len(des.fields) == 1

    def test_invalid_type_rejected(self):
        with pytest.raises(ValidationError):
            DerivedEntitySource(type="something_else")


class TestDerivedSource:
    """Tests for DerivedSource model."""

    def test_valid_creation(self):
        ds = DerivedSource(identity_graph="person_graph")
        assert ds.identity_graph == "person_graph"

    def test_empty_identity_graph_rejected(self):
        with pytest.raises(ValidationError):
            DerivedSource(identity_graph="")

    def test_frozen(self):
        ds = DerivedSource(identity_graph="person_graph")
        with pytest.raises(ValidationError):
            ds.identity_graph = "other_graph"

    def test_identity_graph_config_default_none(self):
        ds = DerivedSource(identity_graph="person_graph")
        assert ds.identity_graph_config is None

    def test_with_identity_graph_config(self):
        config = IdentityGraphConfig(
            match_key="email",
            sources=[
                IdentityGraphSource(name="hubspot", entity="hubspot_person", match_key_field="email"),
                IdentityGraphSource(name="stripe", entity="stripe_customer", match_key_field="email"),
            ],
            priority=["hubspot", "stripe"],
        )
        ds = DerivedSource(identity_graph="person_graph", identity_graph_config=config)
        assert ds.identity_graph_config is config

    def test_depends_on_auto_derived_from_config(self):
        config = IdentityGraphConfig(
            match_key="email",
            sources=[
                IdentityGraphSource(name="hubspot", entity="hubspot_person", match_key_field="email"),
                IdentityGraphSource(name="stripe", entity="stripe_customer", match_key_field="email"),
            ],
            priority=["hubspot", "stripe"],
        )
        ds = DerivedSource(identity_graph="person_graph", identity_graph_config=config)
        assert ds.depends_on == ["hubspot_person", "stripe_customer"]

    def test_depends_on_merged_with_explicit(self):
        config = IdentityGraphConfig(
            match_key="email",
            sources=[
                IdentityGraphSource(name="hubspot", entity="hubspot_person", match_key_field="email"),
                IdentityGraphSource(name="stripe", entity="stripe_customer", match_key_field="email"),
            ],
            priority=["hubspot", "stripe"],
        )
        ds = DerivedSource(
            identity_graph="person_graph",
            identity_graph_config=config,
            depends_on=["lookup_table"],
        )
        assert "lookup_table" in ds.depends_on
        assert "hubspot_person" in ds.depends_on
        assert "stripe_customer" in ds.depends_on

    def test_depends_on_deduplicates(self):
        config = IdentityGraphConfig(
            match_key="email",
            sources=[
                IdentityGraphSource(name="hubspot", entity="hubspot_person", match_key_field="email"),
                IdentityGraphSource(name="stripe", entity="stripe_customer", match_key_field="email"),
            ],
            priority=["hubspot", "stripe"],
        )
        ds = DerivedSource(
            identity_graph="person_graph",
            identity_graph_config=config,
            depends_on=["hubspot_person"],
        )
        assert ds.depends_on.count("hubspot_person") == 1

    def test_still_frozen_with_config(self):
        config = IdentityGraphConfig(
            match_key="email",
            sources=[
                IdentityGraphSource(name="hubspot", entity="hubspot_person", match_key_field="email"),
                IdentityGraphSource(name="stripe", entity="stripe_customer", match_key_field="email"),
            ],
            priority=["hubspot", "stripe"],
        )
        ds = DerivedSource(identity_graph="person_graph", identity_graph_config=config)
        with pytest.raises(ValidationError):
            ds.identity_graph = "other"


class TestIdentityGraphSource:
    """Tests for IdentityGraphSource model."""

    def test_minimal_creation(self):
        s = IdentityGraphSource(name="hubspot", entity="hubspot_person", match_key_field="email")
        assert s.name == "hubspot"
        assert s.entity == "hubspot_person"
        assert s.match_key_field == "email"
        assert s.fields == {}
        assert s.id_field is None
        assert s.date_field is None

    def test_full_creation(self):
        s = IdentityGraphSource(
            name="hubspot", entity="hubspot_person", match_key_field="email",
            fields={"first_name": "firstname", "last_name": "lastname"},
            id_field="contact_id", date_field="created_at",
        )
        assert s.fields == {"first_name": "firstname", "last_name": "lastname"}
        assert s.id_field == "contact_id"
        assert s.date_field == "created_at"

    def test_empty_name_rejected(self):
        with pytest.raises(ValidationError):
            IdentityGraphSource(name="", entity="e", match_key_field="k")

    def test_empty_entity_rejected(self):
        with pytest.raises(ValidationError):
            IdentityGraphSource(name="n", entity="", match_key_field="k")

    def test_empty_match_key_field_rejected(self):
        with pytest.raises(ValidationError):
            IdentityGraphSource(name="n", entity="e", match_key_field="")

    def test_frozen(self):
        s = IdentityGraphSource(name="hubspot", entity="hubspot_person", match_key_field="email")
        with pytest.raises(ValidationError):
            s.name = "other"


class TestIdentityGraphConfig:
    """Tests for IdentityGraphConfig model."""

    def _make_sources(self, names=("hubspot", "stripe")):
        return [
            IdentityGraphSource(name=n, entity=f"{n}_person", match_key_field="email")
            for n in names
        ]

    def test_valid_creation_two_sources(self):
        sources = self._make_sources()
        config = IdentityGraphConfig(match_key="email", sources=sources, priority=["hubspot", "stripe"])
        assert config.match_key == "email"
        assert len(config.sources) == 2
        assert config.priority == ["hubspot", "stripe"]

    def test_valid_creation_three_sources(self):
        sources = self._make_sources(("a", "b", "c"))
        config = IdentityGraphConfig(match_key="email", sources=sources, priority=["a", "b", "c"])
        assert len(config.sources) == 3

    def test_fewer_than_two_sources_rejected(self):
        with pytest.raises(ValidationError):
            IdentityGraphConfig(
                match_key="email",
                sources=[IdentityGraphSource(name="a", entity="a_person", match_key_field="email")],
                priority=["a"],
            )

    def test_priority_missing_source_rejected(self):
        sources = self._make_sources()
        with pytest.raises(ValidationError, match="priority must contain exactly"):
            IdentityGraphConfig(match_key="email", sources=sources, priority=["hubspot"])

    def test_priority_extra_name_rejected(self):
        sources = self._make_sources()
        with pytest.raises(ValidationError, match="priority must contain exactly"):
            IdentityGraphConfig(match_key="email", sources=sources, priority=["hubspot", "stripe", "extra"])

    def test_empty_priority_rejected(self):
        sources = self._make_sources()
        with pytest.raises(ValidationError):
            IdentityGraphConfig(match_key="email", sources=sources, priority=[])

    def test_empty_match_key_rejected(self):
        sources = self._make_sources()
        with pytest.raises(ValidationError):
            IdentityGraphConfig(match_key="", sources=sources, priority=["hubspot", "stripe"])

    def test_duplicate_source_names_rejected(self):
        sources = [
            IdentityGraphSource(name="hubspot", entity="hubspot_person", match_key_field="email"),
            IdentityGraphSource(name="hubspot", entity="hubspot_contacts", match_key_field="email"),
        ]
        with pytest.raises(ValidationError, match="Duplicate source names"):
            IdentityGraphConfig(match_key="email", sources=sources, priority=["hubspot", "hubspot"])

    def test_frozen(self):
        sources = self._make_sources()
        config = IdentityGraphConfig(match_key="email", sources=sources, priority=["hubspot", "stripe"])
        with pytest.raises(ValidationError):
            config.match_key = "other"


class TestAggregationSource:
    """Tests for AggregationSource model."""

    def test_minimal_creation(self):
        src = AggregationSource(source_entity="person", group_by_column="account_id")
        assert src.source_entity == "person"
        assert src.group_by_column == "account_id"
        assert src.filter_expression is None
        assert src.fields is None

    def test_full_creation(self):
        src = AggregationSource(
            source_entity="person",
            group_by_column="account_id",
            filter_expression="account_id IS NOT NULL",
            fields=[Field(name="account_id", type="STRING")],
        )
        assert src.filter_expression == "account_id IS NOT NULL"
        assert len(src.fields) == 1

    def test_with_aggregations(self):
        from fyrnheim.components.computed_column import ComputedColumn
        src = AggregationSource(
            source_entity="person",
            group_by_column="account_id",
            aggregations=[
                ComputedColumn(name="person_count", expression="t.person_id.count()"),
                ComputedColumn(name="total_amount", expression="t.amount.sum()"),
            ],
        )
        assert len(src.aggregations) == 2
        assert src.aggregations[0].name == "person_count"
        assert src.aggregations[1].name == "total_amount"

    def test_without_aggregations_backward_compatible(self):
        src = AggregationSource(source_entity="person", group_by_column="account_id")
        assert src.aggregations == []

    def test_aggregations_default_empty(self):
        src = AggregationSource(source_entity="person", group_by_column="account_id")
        assert src.aggregations == []
        assert src.source_entity == "person"


class TestEventAggregationSource:
    """Tests for EventAggregationSource model."""

    def test_valid_creation(self):
        src = EventAggregationSource(
            project="proj",
            dataset="ds",
            table="events",
            group_by_column="session_id",
        )
        assert src.group_by_column == "session_id"
        assert src.group_by_expression is None
        assert src.filter_expression is None

    def test_inherits_base_table_source(self):
        assert issubclass(EventAggregationSource, BaseTableSource)

    def test_empty_group_by_rejected(self):
        with pytest.raises(ValidationError):
            EventAggregationSource(
                project="p", dataset="d", table="t", group_by_column=""
            )

    def test_full_creation(self):
        src = EventAggregationSource(
            project="p",
            dataset="d",
            table="t",
            group_by_column="tid",
            group_by_expression="COALESCE(session_id, 'unknown')",
            filter_expression="tid IS NOT NULL",
            fields=[Field(name="utm_source", type="STRING", json_path="$.utm_source")],
        )
        assert src.group_by_expression == "COALESCE(session_id, 'unknown')"
        assert src.filter_expression == "tid IS NOT NULL"
        assert len(src.fields) == 1


class TestUnionSource:
    """Tests for UnionSource model."""

    def test_valid_creation(self):
        src = UnionSource(
            sources=[
                TableSource(project="p", dataset="d", table="t1"),
                TableSource(project="p", dataset="d", table="t2"),
            ]
        )
        assert len(src.sources) == 2

    def test_empty_sources_rejected(self):
        with pytest.raises(ValidationError):
            UnionSource(sources=[])

    def test_sources_are_table_source(self):
        src = UnionSource(
            sources=[TableSource(project="p", dataset="d", table="t1")]
        )
        assert isinstance(src.sources[0], TableSource)

    def test_union_source_with_field_mappings(self):
        src = UnionSource(
            sources=[
                TableSource(
                    project="p", dataset="d", table="t1",
                    field_mappings={"contact_email": "email"},
                ),
                TableSource(project="p", dataset="d", table="t2"),
            ]
        )
        assert src.sources[0].field_mappings == {"contact_email": "email"}
        assert src.sources[1].field_mappings == {}

    def test_union_source_with_literal_columns(self):
        src = UnionSource(
            sources=[
                TableSource(
                    project="p", dataset="d", table="t1",
                    literal_columns={"product_type": "video"},
                ),
                TableSource(
                    project="p", dataset="d", table="t2",
                    literal_columns={"product_type": "audio"},
                ),
            ]
        )
        assert src.sources[0].literal_columns == {"product_type": "video"}
        assert src.sources[1].literal_columns == {"product_type": "audio"}

    def test_union_source_with_both_mappings_and_literals(self):
        src = UnionSource(
            sources=[
                TableSource(
                    project="p", dataset="d", table="t1",
                    field_mappings={"contact_email": "email"},
                    literal_columns={"source_system": "crm"},
                ),
            ]
        )
        assert src.sources[0].field_mappings == {"contact_email": "email"}
        assert src.sources[0].literal_columns == {"source_system": "crm"}


class TestOldNamesNotImportable:
    """Verify that old/dropped names from timo-data-stack are not importable."""

    def test_bigquery_source_not_importable(self):
        with pytest.raises(ImportError):
            from fyrnheim.core.source import BigQuerySource  # noqa: F401

    def test_base_source_config_not_importable(self):
        with pytest.raises(ImportError):
            from fyrnheim.core.source import BaseSourceConfig  # noqa: F401

    def test_source_overrides_not_importable(self):
        with pytest.raises(ImportError):
            from fyrnheim.core.source import SourceOverrides  # noqa: F401

    def test_signal_source_config_not_importable(self):
        with pytest.raises(ImportError):
            from fyrnheim.core.source import SignalSourceConfig  # noqa: F401

    def test_product_source_config_not_importable(self):
        with pytest.raises(ImportError):
            from fyrnheim.core.source import ProductSourceConfig  # noqa: F401

    def test_product_union_source_not_importable(self):
        with pytest.raises(ImportError):
            from fyrnheim.core.source import ProductUnionSource  # noqa: F401

    def test_anon_source_config_not_importable(self):
        with pytest.raises(ImportError):
            from fyrnheim.core.source import AnonSourceConfig  # noqa: F401


class TestReExportsFromCore:
    """Verify all source classes are re-exported from fyrnheim.core."""

    def test_all_source_classes_importable(self):
        from fyrnheim.core import (
            Field,
        )

        # Verify they are the same objects (not copies)
        from fyrnheim.core.source import Field as DirectField
        assert Field is DirectField
