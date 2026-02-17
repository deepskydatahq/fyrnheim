"""Tests for fyrnheim.components package."""

import pytest
from pydantic import ValidationError


class TestImports:
    """Test that all 5 classes are importable from fyrnheim.components."""

    def test_computed_column_importable(self):
        from fyrnheim.components import ComputedColumn

        assert ComputedColumn is not None

    def test_measure_importable(self):
        from fyrnheim.components import Measure

        assert Measure is not None

    def test_lifecycle_flags_importable(self):
        from fyrnheim.components import LifecycleFlags

        assert LifecycleFlags is not None

    def test_time_based_metrics_importable(self):
        from fyrnheim.components import TimeBasedMetrics

        assert TimeBasedMetrics is not None

    def test_data_quality_checks_importable(self):
        from fyrnheim.components import DataQualityChecks

        assert DataQualityChecks is not None


class TestComputedColumn:
    """Tests for ComputedColumn validation and behavior."""

    def test_valid_creation(self):
        from fyrnheim.components import ComputedColumn

        col = ComputedColumn(
            name="total",
            expression="SUM(amount)",
            description="Total amount",
        )
        assert col.name == "total"
        assert col.expression == "SUM(amount)"
        assert col.description == "Total amount"

    def test_empty_name_fails(self):
        from fyrnheim.components import ComputedColumn

        with pytest.raises(ValidationError):
            ComputedColumn(name="", expression="SUM(amount)")

    def test_empty_expression_fails(self):
        from fyrnheim.components import ComputedColumn

        with pytest.raises(ValidationError):
            ComputedColumn(name="total", expression="")

    def test_to_sql_works(self):
        from fyrnheim.components import ComputedColumn

        col = ComputedColumn(name="total", expression="SUM(amount)")
        sql = col.to_sql()
        assert "SUM(amount) AS total" in sql

    def test_to_sql_custom_indent(self):
        from fyrnheim.components import ComputedColumn

        col = ComputedColumn(name="total", expression="SUM(amount)")
        sql = col.to_sql(indent=8)
        assert sql.startswith("        ")

    def test_expression_is_stripped(self):
        from fyrnheim.components import ComputedColumn

        col = ComputedColumn(name="total", expression="  SUM(amount)  ")
        assert col.expression == "SUM(amount)"


class TestMeasure:
    """Tests for Measure validation."""

    def test_valid_creation(self):
        from fyrnheim.components import Measure

        m = Measure(
            name="total_revenue",
            expression="SUM(amount_cents)",
            description="Total revenue in cents",
        )
        assert m.name == "total_revenue"
        assert m.expression == "SUM(amount_cents)"

    def test_empty_name_fails(self):
        from fyrnheim.components import Measure

        with pytest.raises(ValidationError):
            Measure(name="", expression="SUM(amount)")

    def test_empty_expression_fails(self):
        from fyrnheim.components import Measure

        with pytest.raises(ValidationError):
            Measure(name="total", expression="")

    def test_expression_is_stripped(self):
        from fyrnheim.components import Measure

        m = Measure(name="total", expression="  COUNT(*)  ")
        assert m.expression == "COUNT(*)"


class TestLifecycleFlags:
    """Tests for LifecycleFlags component."""

    def test_generates_is_active_and_is_churned(self):
        from fyrnheim.components import LifecycleFlags

        flags = LifecycleFlags(
            status_column="status",
            active_states=["active", "on_trial"],
            churned_states=["cancelled", "expired"],
        )
        columns = flags.to_computed_columns()
        names = [c.name for c in columns]
        assert "is_active" in names
        assert "is_churned" in names
        assert len(columns) == 2

    def test_generates_is_at_risk_when_provided(self):
        from fyrnheim.components import LifecycleFlags

        flags = LifecycleFlags(
            status_column="status",
            active_states=["active"],
            churned_states=["cancelled"],
            at_risk_states=["past_due", "delinquent"],
        )
        columns = flags.to_computed_columns()
        names = [c.name for c in columns]
        assert "is_at_risk" in names
        assert len(columns) == 3

    def test_computed_columns_have_expressions(self):
        from fyrnheim.components import LifecycleFlags

        flags = LifecycleFlags(
            status_column="status",
            active_states=["active"],
            churned_states=["cancelled"],
        )
        columns = flags.to_computed_columns()
        for col in columns:
            assert col.expression
            assert "isin" in col.expression


class TestTimeBasedMetrics:
    """Tests for TimeBasedMetrics component."""

    def test_generates_basic_time_columns(self):
        from fyrnheim.components import TimeBasedMetrics

        metrics = TimeBasedMetrics(created_at_col="created_at")
        columns = metrics.to_computed_columns()
        names = [c.name for c in columns]
        assert "days_since_created" in names
        assert "created_month" in names
        assert "created_year" in names
        assert len(columns) == 3

    def test_generates_updated_columns_when_provided(self):
        from fyrnheim.components import TimeBasedMetrics

        metrics = TimeBasedMetrics(
            created_at_col="created_at",
            updated_at_col="updated_at",
        )
        columns = metrics.to_computed_columns()
        names = [c.name for c in columns]
        assert "days_since_updated" in names
        assert "days_between_created_and_updated" in names
        assert len(columns) == 5

    def test_computed_columns_have_expressions(self):
        from fyrnheim.components import TimeBasedMetrics

        metrics = TimeBasedMetrics(created_at_col="created_at")
        columns = metrics.to_computed_columns()
        for col in columns:
            assert col.expression
            assert isinstance(col.expression, str)


class TestDataQualityChecks:
    """Tests for DataQualityChecks component."""

    def test_generates_flag_columns(self):
        from fyrnheim.components import DataQualityChecks

        checks = DataQualityChecks(
            checks={
                "missing_email": "email IS NULL",
                "future_date": "created_at > CURRENT_TIMESTAMP()",
            }
        )
        columns = checks.to_computed_columns()
        names = [c.name for c in columns]
        assert "has_missing_email" in names
        assert "has_future_date" in names
        assert len(columns) == 2

    def test_custom_prefix(self):
        from fyrnheim.components import DataQualityChecks

        checks = DataQualityChecks(
            checks={"null_name": "name IS NULL"},
            prefix="flag_",
        )
        columns = checks.to_computed_columns()
        assert columns[0].name == "flag_null_name"

    def test_empty_checks_returns_empty_list(self):
        from fyrnheim.components import DataQualityChecks

        checks = DataQualityChecks()
        columns = checks.to_computed_columns()
        assert columns == []

    def test_columns_are_computed_columns(self):
        from fyrnheim.components import ComputedColumn, DataQualityChecks

        checks = DataQualityChecks(
            checks={"missing_email": "email IS NULL"},
        )
        columns = checks.to_computed_columns()
        assert all(isinstance(c, ComputedColumn) for c in columns)
