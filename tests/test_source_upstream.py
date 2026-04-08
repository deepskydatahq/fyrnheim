"""Tests for optional upstream=StagingView field on BaseTableSource."""

import pytest
from pydantic import ValidationError

from fyrnheim.core.source import EventSource, StateSource, TableSource
from fyrnheim.core.staging_view import StagingView


@pytest.fixture
def sv() -> StagingView:
    return StagingView(
        name="stg_users",
        project="proj_a",
        dataset="ds_a",
        sql="SELECT 1 AS id",
    )


class TestUpstreamResolution:
    def test_state_source_with_upstream(self, sv: StagingView) -> None:
        s = StateSource(upstream=sv, name="users", id_field="id", snapshot_grain="daily")  # type: ignore[arg-type]
        assert s.project == "proj_a"
        assert s.dataset == "ds_a"
        assert s.table == "stg_users"

    def test_event_source_with_upstream(self, sv: StagingView) -> None:
        e = EventSource(
            upstream=sv,
            name="ev",
            entity_id_field="uid",
            timestamp_field="ts",
            event_type_field="ev",
        )
        assert e.project == "proj_a"
        assert e.dataset == "ds_a"
        assert e.table == "stg_users"

    def test_table_source_with_upstream(self, sv: StagingView) -> None:
        t = TableSource(upstream=sv)
        assert (t.project, t.dataset, t.table) == ("proj_a", "ds_a", "stg_users")

    def test_explicit_values_win_over_upstream(self, sv: StagingView) -> None:
        t = TableSource(
            upstream=sv,
            project="override_proj",
            dataset="override_ds",
            table="override_tbl",
        )
        assert t.project == "override_proj"
        assert t.dataset == "override_ds"
        assert t.table == "override_tbl"

    def test_partial_override(self, sv: StagingView) -> None:
        t = TableSource(upstream=sv, table="custom_tbl")
        assert t.project == "proj_a"
        assert t.dataset == "ds_a"
        assert t.table == "custom_tbl"

    def test_no_upstream_no_coords_raises(self) -> None:
        with pytest.raises(ValidationError):
            TableSource()

    def test_no_upstream_partial_coords_raises(self) -> None:
        with pytest.raises(ValidationError):
            TableSource(project="p", dataset="d")

    def test_existing_behavior_preserved(self) -> None:
        t = TableSource(project="p", dataset="d", table="t")
        assert t.upstream is None
        assert t.project == "p"

    def test_read_table_with_upstream_bigquery(self, sv: StagingView) -> None:
        t = TableSource(upstream=sv)

        class FakeConn:
            def table(self, name: str, database: tuple[str, str]) -> tuple[str, tuple[str, str]]:
                return (name, database)

        result = t.read_table(FakeConn(), "bigquery")
        assert result == ("stg_users", ("proj_a", "ds_a"))
