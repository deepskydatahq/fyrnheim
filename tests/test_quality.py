"""Tests for quality check models and runner."""

import ibis
import pytest

from fyrnheim.quality import CheckResult, NotNull, QualityCheck, QualityRunner, Unique


class TestNotNull:
    def test_notnull_creates_with_single_column(self):
        check = NotNull("email")
        assert check.columns == ("email",)

    def test_notnull_creates_with_multiple_columns(self):
        check = NotNull("email", "name")
        assert check.columns == ("email", "name")

    def test_notnull_requires_at_least_one_column(self):
        with pytest.raises(ValueError, match="At least one column"):
            NotNull()

    def test_notnull_is_quality_check(self):
        check = NotNull("email")
        assert isinstance(check, QualityCheck)

    def test_notnull_display_name(self):
        check = NotNull("email")
        assert check.display_name == "NotNull: email"


class TestUnique:
    def test_unique_creates_with_single_column(self):
        check = Unique("email")
        assert check.columns == ("email",)

    def test_unique_creates_with_multiple_columns(self):
        check = Unique("email", "name")
        assert check.columns == ("email", "name")

    def test_unique_requires_at_least_one_column(self):
        with pytest.raises(ValueError, match="At least one column"):
            Unique()

    def test_unique_is_quality_check(self):
        check = Unique("email")
        assert isinstance(check, QualityCheck)

    def test_unique_display_name(self):
        check = Unique("email")
        assert check.display_name == "Unique: email"


class TestQualityRunnerRun:
    """Tests for QualityRunner.run() static method with ibis tables."""

    def _make_table(self, data: dict) -> ibis.Table:
        return ibis.memtable(data)

    def test_notnull_passes_when_no_nulls(self):
        table = self._make_table({"email": ["a@b.com", "c@d.com", "e@f.com"]})
        results = QualityRunner.run(table, [NotNull("email")])
        assert len(results) == 1
        assert results[0].passed is True
        assert results[0].check_name == "not_null:email"
        assert results[0].error is None

    def test_notnull_fails_when_nulls_present(self):
        table = self._make_table({"email": ["a@b.com", None, "e@f.com"]})
        results = QualityRunner.run(table, [NotNull("email")])
        assert len(results) == 1
        assert results[0].passed is False
        assert results[0].check_name == "not_null:email"
        assert "1 null values" in results[0].error

    def test_unique_passes_when_all_distinct(self):
        table = self._make_table({"email": ["a@b.com", "c@d.com", "e@f.com"]})
        results = QualityRunner.run(table, [Unique("email")])
        assert len(results) == 1
        assert results[0].passed is True
        assert results[0].check_name == "unique:email"
        assert results[0].error is None

    def test_unique_fails_when_duplicates_present(self):
        table = self._make_table({"email": ["a@b.com", "a@b.com", "e@f.com"]})
        results = QualityRunner.run(table, [Unique("email")])
        assert len(results) == 1
        assert results[0].passed is False
        assert results[0].check_name == "unique:email"
        assert "1 duplicates" in results[0].error

    def test_run_returns_list_of_check_result(self):
        table = self._make_table({"email": ["a@b.com"]})
        results = QualityRunner.run(table, [NotNull("email"), Unique("email")])
        assert len(results) == 2
        assert all(isinstance(r, CheckResult) for r in results)
        assert results[0].check_name == "not_null:email"
        assert results[1].check_name == "unique:email"

    def test_run_with_multiple_columns(self):
        table = self._make_table({
            "email": ["a@b.com", "c@d.com"],
            "name": ["Alice", None],
        })
        results = QualityRunner.run(table, [NotNull("email", "name")])
        assert len(results) == 2
        assert results[0].check_name == "not_null:email"
        assert results[0].passed is True
        assert results[1].check_name == "not_null:name"
        assert results[1].passed is False

    def test_run_empty_checks_returns_empty(self):
        table = self._make_table({"email": ["a@b.com"]})
        results = QualityRunner.run(table, [])
        assert results == []
