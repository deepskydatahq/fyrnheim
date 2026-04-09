"""Tests for AnalyticsEntity and Measure types."""

import pytest

from fyrnheim.components.computed_column import ComputedColumn
from fyrnheim.core import AnalyticsEntity, Measure
from fyrnheim.core.analytics_entity import StateField
from fyrnheim.quality.checks import NotNull


class TestMeasure:
    """Tests for the Measure type."""

    def test_count_measure_validates(self):
        m = Measure(name="workshop_count", activity="workshop_attended", aggregation="count")
        assert m.name == "workshop_count"
        assert m.activity == "workshop_attended"
        assert m.aggregation == "count"
        assert m.field is None

    def test_sum_measure_validates_with_field(self):
        m = Measure(name="total_revenue", activity="purchase", aggregation="sum", field="amount")
        assert m.aggregation == "sum"
        assert m.field == "amount"

    def test_latest_measure_validates_with_field(self):
        m = Measure(name="last_plan", activity="plan_changed", aggregation="latest", field="plan_name")
        assert m.aggregation == "latest"
        assert m.field == "plan_name"

    def test_sum_without_field_raises(self):
        with pytest.raises(ValueError, match="'sum' aggregation requires a 'field'"):
            Measure(name="total", activity="purchase", aggregation="sum")

    def test_latest_without_field_raises(self):
        with pytest.raises(ValueError, match="'latest' aggregation requires a 'field'"):
            Measure(name="last", activity="event", aggregation="latest")

    def test_count_without_field_is_ok(self):
        m = Measure(name="n", activity="ev", aggregation="count")
        assert m.field is None

    def test_invalid_aggregation_raises(self):
        with pytest.raises(ValueError):
            Measure(name="bad", activity="ev", aggregation="mean")  # type: ignore[arg-type]

    def test_empty_name_raises(self):
        with pytest.raises(ValueError):
            Measure(name="", activity="ev", aggregation="count")

    def test_empty_activity_raises(self):
        with pytest.raises(ValueError):
            Measure(name="n", activity="", aggregation="count")


class TestAnalyticsEntity:
    """Tests for the AnalyticsEntity type."""

    def test_basic_with_state_fields(self):
        ae = AnalyticsEntity(
            name="accounts",
            state_fields=[
                StateField(name="company_name", source="crm", field="name", strategy="latest"),
            ],
        )
        assert ae.name == "accounts"
        assert len(ae.state_fields) == 1
        assert ae.measures == []
        assert ae.identity_graph is None

    def test_basic_with_measures(self):
        ae = AnalyticsEntity(
            name="accounts",
            measures=[
                Measure(name="workshop_count", activity="workshop_attended", aggregation="count"),
            ],
        )
        assert len(ae.measures) == 1
        assert ae.state_fields == []

    def test_with_both_state_fields_and_measures(self):
        ae = AnalyticsEntity(
            name="accounts",
            state_fields=[
                StateField(name="company_name", source="crm", field="name", strategy="latest"),
            ],
            measures=[
                Measure(name="workshop_count", activity="workshop_attended", aggregation="count"),
            ],
        )
        assert len(ae.state_fields) == 1
        assert len(ae.measures) == 1

    def test_identity_graph_none_validates(self):
        ae = AnalyticsEntity(
            name="accounts",
            identity_graph=None,
            measures=[
                Measure(name="n", activity="ev", aggregation="count"),
            ],
        )
        assert ae.identity_graph is None

    def test_identity_graph_set(self):
        ae = AnalyticsEntity(
            name="accounts",
            identity_graph="account_graph",
            measures=[
                Measure(name="n", activity="ev", aggregation="count"),
            ],
        )
        assert ae.identity_graph == "account_graph"

    def test_empty_state_fields_and_measures_raises(self):
        with pytest.raises(ValueError, match="at least one state_field or measure"):
            AnalyticsEntity(name="accounts")

    def test_accepts_computed_fields(self):
        ae = AnalyticsEntity(
            name="accounts",
            measures=[
                Measure(name="n", activity="ev", aggregation="count"),
            ],
            computed_fields=[
                ComputedColumn(name="doubled", expression="n * 2"),
            ],
        )
        assert len(ae.computed_fields) == 1
        assert ae.computed_fields[0].name == "doubled"

    def test_accepts_quality_checks(self):
        ae = AnalyticsEntity(
            name="accounts",
            measures=[
                Measure(name="n", activity="ev", aggregation="count"),
            ],
            quality_checks=[NotNull("n")],
        )
        assert len(ae.quality_checks) == 1

    def test_empty_name_raises(self):
        with pytest.raises(ValueError):
            AnalyticsEntity(
                name="",
                measures=[Measure(name="n", activity="ev", aggregation="count")],
            )


class TestStateField:
    """Tests for the StateField in analytics_entity module."""

    def test_latest_strategy(self):
        sf = StateField(name="company_name", source="crm", field="name", strategy="latest")
        assert sf.strategy == "latest"

    def test_coalesce_requires_priority(self):
        with pytest.raises(ValueError, match="coalesce strategy requires a priority list"):
            StateField(name="x", source="a", field="f", strategy="coalesce")

    def test_coalesce_with_priority(self):
        sf = StateField(name="x", source="a", field="f", strategy="coalesce", priority=["a", "b"])
        assert sf.priority == ["a", "b"]


class TestImports:
    """Test that AnalyticsEntity and Measure are importable from fyrnheim.core."""

    def test_import_from_core(self):
        from fyrnheim.core import AnalyticsEntity, Measure
        assert AnalyticsEntity is not None
        assert Measure is not None

    def test_import_from_top_level(self):
        from fyrnheim import AnalyticsEntity, Measure
        assert AnalyticsEntity is not None
        assert Measure is not None


# ---------------------------------------------------------------------------
# M051 regression tests (issue #94)
# ---------------------------------------------------------------------------


class TestM051ExtractFieldValue:
    """Issue #94: _extract_field_value must handle arbitrary event types,
    not just row_appeared and field_changed. EventSource events and
    activity-rewritten events carry flat payloads under different types."""

    def test_extract_field_value_handles_arbitrary_event_type_with_flat_payload(
        self,
    ) -> None:
        import json as _json

        import pandas as pd

        from fyrnheim.engine.analytics_entity_engine import _extract_field_value

        row = pd.Series(
            {
                "event_type": "anon_attrs_loaded",
                "payload": _json.dumps(
                    {"company_name": "Northwind Bank", "region": "EU"}
                ),
            }
        )
        assert _extract_field_value(row, "company_name") == "Northwind Bank"
        assert _extract_field_value(row, "region") == "EU"
        assert _extract_field_value(row, "missing") is None

    def test_extract_field_value_still_handles_field_changed(self) -> None:
        import json as _json

        import pandas as pd

        from fyrnheim.engine.analytics_entity_engine import _extract_field_value

        row = pd.Series(
            {
                "event_type": "field_changed",
                "payload": _json.dumps(
                    {
                        "field_name": "company_name",
                        "old_value": "Old",
                        "new_value": "New",
                    }
                ),
            }
        )
        # Matching field name returns new_value
        assert _extract_field_value(row, "company_name") == "New"
        # Non-matching field name returns None (not new_value by accident)
        assert _extract_field_value(row, "other") is None


class TestMaterializationField:
    """Tests for the materialization / project / dataset / table fields."""

    def _make_measure(self):
        return Measure(name="n", activity="a", aggregation="count")

    def test_materialization_defaults_to_parquet(self):
        ae = AnalyticsEntity(name="users", measures=[self._make_measure()])
        assert ae.materialization == "parquet"
        assert ae.project is None
        assert ae.dataset is None
        assert ae.table is None

    def test_materialization_parquet_no_coords_required(self):
        ae = AnalyticsEntity(
            name="users",
            measures=[self._make_measure()],
            materialization="parquet",
        )
        assert ae.materialization == "parquet"

    def test_materialization_table_requires_project(self):
        with pytest.raises(ValueError, match="project"):
            AnalyticsEntity(
                name="users",
                measures=[self._make_measure()],
                materialization="table",
                dataset="marts",
            )

    def test_materialization_table_requires_dataset(self):
        with pytest.raises(ValueError, match="dataset"):
            AnalyticsEntity(
                name="users",
                measures=[self._make_measure()],
                materialization="table",
                project="my-proj",
            )

    def test_materialization_table_defaults_table_to_name(self):
        ae = AnalyticsEntity(
            name="users",
            measures=[self._make_measure()],
            materialization="table",
            project="my-proj",
            dataset="marts",
        )
        assert ae.table == "users"

    def test_materialization_table_with_explicit_table_name(self):
        ae = AnalyticsEntity(
            name="users",
            measures=[self._make_measure()],
            materialization="table",
            project="my-proj",
            dataset="marts",
            table="dim_users",
        )
        assert ae.table == "dim_users"
