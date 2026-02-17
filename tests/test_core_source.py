"""Tests for source configuration classes."""

import pytest
from pydantic import ValidationError

from typedata.core.source import (
    AggregationSource,
    BaseTableSource,
    DerivedEntitySource,
    DerivedSource,
    Divide,
    EventAggregationSource,
    Field,
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


class TestOldNamesNotImportable:
    """Verify that old/dropped names from timo-data-stack are not importable."""

    def test_bigquery_source_not_importable(self):
        with pytest.raises(ImportError):
            from typedata.core.source import BigQuerySource  # noqa: F401

    def test_base_source_config_not_importable(self):
        with pytest.raises(ImportError):
            from typedata.core.source import BaseSourceConfig  # noqa: F401

    def test_source_overrides_not_importable(self):
        with pytest.raises(ImportError):
            from typedata.core.source import SourceOverrides  # noqa: F401

    def test_signal_source_config_not_importable(self):
        with pytest.raises(ImportError):
            from typedata.core.source import SignalSourceConfig  # noqa: F401

    def test_product_source_config_not_importable(self):
        with pytest.raises(ImportError):
            from typedata.core.source import ProductSourceConfig  # noqa: F401

    def test_product_union_source_not_importable(self):
        with pytest.raises(ImportError):
            from typedata.core.source import ProductUnionSource  # noqa: F401

    def test_anon_source_config_not_importable(self):
        with pytest.raises(ImportError):
            from typedata.core.source import AnonSourceConfig  # noqa: F401


class TestReExportsFromCore:
    """Verify all source classes are re-exported from typedata.core."""

    def test_all_source_classes_importable(self):
        from typedata.core import (
            Field,
        )

        # Verify they are the same objects (not copies)
        from typedata.core.source import Field as DirectField
        assert Field is DirectField
