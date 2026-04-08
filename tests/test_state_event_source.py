"""Tests for StateSource and EventSource models."""

import pytest
from pydantic import ValidationError

from fyrnheim.components.computed_column import ComputedColumn
from fyrnheim.core.source import (
    EventSource,
    Field,
    SourceTransforms,
    StateSource,
    TypeCast,
)


class TestStateSource:
    """Tests for the StateSource model."""

    def test_minimal_creation(self):
        s = StateSource(
            name="crm",
            project="p",
            dataset="d",
            table="t",
            id_field="id",
        )
        assert s.name == "crm"
        assert s.project == "p"
        assert s.dataset == "d"
        assert s.table == "t"
        assert s.id_field == "id"
        assert s.transforms is None
        assert s.fields is None

    def test_without_id_field_raises(self):
        with pytest.raises(ValidationError):
            StateSource(
                name="crm",
                project="p",
                dataset="d",
                table="t",
            )

    def test_without_name_raises(self):
        with pytest.raises(ValidationError):
            StateSource(
                project="p",
                dataset="d",
                table="t",
                id_field="id",
            )

    def test_accepts_optional_transforms(self):
        transforms = SourceTransforms(
            type_casts=[TypeCast(field="age", target_type="INT64")]
        )
        s = StateSource(
            name="crm",
            project="p",
            dataset="d",
            table="t",
            id_field="id",
            transforms=transforms,
        )
        assert s.transforms is not None
        assert len(s.transforms.type_casts) == 1

    def test_accepts_optional_fields(self):
        fields = [Field(name="email", type="STRING")]
        s = StateSource(
            name="crm",
            project="p",
            dataset="d",
            table="t",
            id_field="id",
            fields=fields,
        )
        assert s.fields is not None
        assert len(s.fields) == 1
        assert s.fields[0].name == "email"

    def test_inherits_base_table_source_validation(self):
        """Empty project/dataset/table should fail."""
        with pytest.raises(ValidationError):
            StateSource(
                name="crm",
                project="",
                dataset="d",
                table="t",
                id_field="id",
            )

    def test_empty_name_raises(self):
        with pytest.raises(ValidationError):
            StateSource(
                name="",
                project="p",
                dataset="d",
                table="t",
                id_field="id",
            )

    def test_empty_id_field_raises(self):
        with pytest.raises(ValidationError):
            StateSource(
                name="crm",
                project="p",
                dataset="d",
                table="t",
                id_field="",
            )


class TestEventSource:
    """Tests for the EventSource model."""

    def test_minimal_creation(self):
        e = EventSource(
            name="views",
            project="p",
            dataset="d",
            table="t",
            entity_id_field="user_id",
            timestamp_field="ts",
        )
        assert e.name == "views"
        assert e.entity_id_field == "user_id"
        assert e.timestamp_field == "ts"
        assert e.event_type is None
        assert e.event_type_field is None
        assert e.transforms is None
        assert e.fields is None

    def test_without_entity_id_field_raises(self):
        with pytest.raises(ValidationError):
            EventSource(
                name="views",
                project="p",
                dataset="d",
                table="t",
                timestamp_field="ts",
            )

    def test_without_timestamp_field_raises(self):
        with pytest.raises(ValidationError):
            EventSource(
                name="views",
                project="p",
                dataset="d",
                table="t",
                entity_id_field="user_id",
            )

    def test_accepts_optional_event_type(self):
        e = EventSource(
            name="views",
            project="p",
            dataset="d",
            table="t",
            entity_id_field="user_id",
            timestamp_field="ts",
            event_type="page_view",
        )
        assert e.event_type == "page_view"
        assert e.event_type_field is None

    def test_accepts_optional_event_type_field(self):
        e = EventSource(
            name="events",
            project="p",
            dataset="d",
            table="t",
            entity_id_field="user_id",
            timestamp_field="ts",
            event_type_field="event_name",
        )
        assert e.event_type is None
        assert e.event_type_field == "event_name"

    def test_both_event_type_and_field_raises(self):
        with pytest.raises(ValidationError, match="cannot have both"):
            EventSource(
                name="events",
                project="p",
                dataset="d",
                table="t",
                entity_id_field="user_id",
                timestamp_field="ts",
                event_type="page_view",
                event_type_field="event_name",
            )

    def test_accepts_optional_transforms_and_fields(self):
        e = EventSource(
            name="events",
            project="p",
            dataset="d",
            table="t",
            entity_id_field="user_id",
            timestamp_field="ts",
            transforms=SourceTransforms(),
            fields=[Field(name="url", type="STRING")],
        )
        assert e.transforms is not None
        assert e.fields is not None
        assert len(e.fields) == 1

    def test_without_name_raises(self):
        with pytest.raises(ValidationError):
            EventSource(
                project="p",
                dataset="d",
                table="t",
                entity_id_field="user_id",
                timestamp_field="ts",
            )

    def test_inherits_base_table_source_validation(self):
        with pytest.raises(ValidationError):
            EventSource(
                name="views",
                project="",
                dataset="d",
                table="t",
                entity_id_field="user_id",
                timestamp_field="ts",
            )


class TestReadTableDataDir:
    """Tests for BaseTableSource.read_table() with data_dir."""

    def test_relative_duckdb_path_resolved_against_data_dir(self, tmp_path):
        data_dir = tmp_path / "mydata"
        data_dir.mkdir()
        parquet_file = data_dir / "customers.parquet"
        import pandas as pd
        pd.DataFrame({"id": [1], "name": ["Alice"]}).to_parquet(str(parquet_file))

        source = StateSource(
            name="crm",
            project="p",
            dataset="d",
            table="t",
            id_field="id",
            duckdb_path="customers.parquet",
        )
        import ibis
        conn = ibis.duckdb.connect()
        result = source.read_table(conn, "duckdb", data_dir=data_dir)
        assert result.execute().iloc[0]["name"] == "Alice"

    def test_absolute_duckdb_path_ignores_data_dir(self, tmp_path):
        parquet_file = tmp_path / "abs.parquet"
        import pandas as pd
        pd.DataFrame({"id": [1]}).to_parquet(str(parquet_file))

        source = StateSource(
            name="crm",
            project="p",
            dataset="d",
            table="t",
            id_field="id",
            duckdb_path=str(parquet_file),
        )
        import ibis
        conn = ibis.duckdb.connect()
        # data_dir should be ignored for absolute paths
        result = source.read_table(conn, "duckdb", data_dir="/nonexistent")
        assert len(result.execute()) == 1

    def test_no_data_dir_resolves_from_cwd(self, tmp_path):
        """Without data_dir, relative paths resolve from CWD (existing behavior)."""
        source = StateSource(
            name="crm",
            project="p",
            dataset="d",
            table="t",
            id_field="id",
            duckdb_path="nonexistent_file_xyz.parquet",
        )
        import ibis
        conn = ibis.duckdb.connect()
        # DuckDB raises IOException for missing files
        with pytest.raises(Exception, match="No files found"):  # noqa: B017
            source.read_table(conn, "duckdb")


class TestSourceExports:
    """Tests for public exports."""

    def test_import_from_core(self):
        """Verify public API exports work."""
        from fyrnheim.core import EventSource as ES, StateSource as SS

        assert SS is StateSource
        assert ES is EventSource


class TestStateSourceComputedColumns:
    """Tests for StateSource.computed_columns field."""

    def test_state_source_with_computed_columns(self):
        src = StateSource(
            project="proj",
            dataset="ds",
            table="users",
            name="users",
            id_field="user_id",
            computed_columns=[
                ComputedColumn(name="email_lower", expression="t.email.lower()"),
            ],
        )
        assert len(src.computed_columns) == 1
        assert src.computed_columns[0].name == "email_lower"
        assert src.computed_columns[0].expression == "t.email.lower()"

    def test_state_source_defaults_to_empty_list(self):
        src = StateSource(
            project="proj",
            dataset="ds",
            table="users",
            name="users",
            id_field="user_id",
        )
        assert src.computed_columns == []

    def test_state_source_multiple_computed_columns(self):
        src = StateSource(
            project="proj",
            dataset="ds",
            table="users",
            name="users",
            id_field="user_id",
            computed_columns=[
                ComputedColumn(name="email_lower", expression="t.email.lower()"),
                ComputedColumn(name="signup_date", expression='t.created_at.cast("date")'),
            ],
        )
        assert len(src.computed_columns) == 2


class TestEventSourcePayloadExclude:
    """Tests for EventSource.payload_exclude field."""

    def test_default_is_empty_list(self):
        es = EventSource(
            name="views",
            project="p",
            dataset="d",
            table="t",
            entity_id_field="user_id",
            timestamp_field="ts",
        )
        assert es.payload_exclude == []

    def test_accepts_list_of_column_names(self):
        es = EventSource(
            name="ga4_events",
            project="p",
            dataset="d",
            table="t",
            entity_id_field="user_pseudo_id",
            timestamp_field="event_timestamp",
            payload_exclude=["event_params", "user_properties", "items"],
        )
        assert es.payload_exclude == ["event_params", "user_properties", "items"]


class TestEventSourceComputedColumns:
    """Tests for EventSource.computed_columns field."""

    def test_event_source_with_computed_columns(self):
        src = EventSource(
            project="proj",
            dataset="ds",
            table="events",
            name="page_views",
            entity_id_field="user_id",
            timestamp_field="ts",
            computed_columns=[
                ComputedColumn(name="domain", expression='t.url.split("/")[2]'),
            ],
        )
        assert len(src.computed_columns) == 1
        assert src.computed_columns[0].name == "domain"

    def test_event_source_defaults_to_empty_list(self):
        src = EventSource(
            project="proj",
            dataset="ds",
            table="events",
            name="page_views",
            entity_id_field="user_id",
            timestamp_field="ts",
        )
        assert src.computed_columns == []

    def test_event_source_with_event_type_and_computed_columns(self):
        src = EventSource(
            project="proj",
            dataset="ds",
            table="events",
            name="page_views",
            entity_id_field="user_id",
            timestamp_field="ts",
            event_type="pageview",
            computed_columns=[
                ComputedColumn(name="email_lower", expression="t.email.lower()"),
            ],
        )
        assert src.event_type == "pageview"
        assert len(src.computed_columns) == 1


class TestStateSourceSnapshotGrain:
    """Tests for StateSource.snapshot_grain field."""

    def test_snapshot_grain_defaults_to_daily(self):
        src = StateSource(
            name="crm",
            project="p",
            dataset="d",
            table="t",
            id_field="id",
        )
        assert src.snapshot_grain == "daily"

    def test_snapshot_grain_hourly(self):
        src = StateSource(
            name="crm",
            project="p",
            dataset="d",
            table="t",
            id_field="id",
            snapshot_grain="hourly",
        )
        assert src.snapshot_grain == "hourly"

    def test_snapshot_grain_daily(self):
        src = StateSource(
            name="crm",
            project="p",
            dataset="d",
            table="t",
            id_field="id",
            snapshot_grain="daily",
        )
        assert src.snapshot_grain == "daily"

    def test_snapshot_grain_weekly(self):
        src = StateSource(
            name="crm",
            project="p",
            dataset="d",
            table="t",
            id_field="id",
            snapshot_grain="weekly",
        )
        assert src.snapshot_grain == "weekly"

    def test_snapshot_grain_invalid_raises(self):
        with pytest.raises(ValidationError):
            StateSource(
                name="crm",
                project="p",
                dataset="d",
                table="t",
                id_field="id",
                snapshot_grain="minutely",
            )
