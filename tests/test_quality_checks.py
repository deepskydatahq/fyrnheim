"""Unit tests for quality check classes."""

import typing

import pytest

from fyrnheim.quality import (
    CustomSQL,
    ForeignKey,
    InRange,
    InSet,
    MatchesPattern,
    MaxAge,
    NotEmpty,
    NotNull,
    QualityCheck,
    QualityConfig,
    Unique,
)


class TestImports:
    """All check types should be importable from fyrnheim.quality."""

    def test_quality_check_importable(self) -> None:
        assert QualityCheck is not None

    def test_quality_config_importable(self) -> None:
        assert QualityConfig is not None

    def test_not_null_importable(self) -> None:
        assert NotNull is not None

    def test_not_empty_importable(self) -> None:
        assert NotEmpty is not None

    def test_in_range_importable(self) -> None:
        assert InRange is not None

    def test_in_set_importable(self) -> None:
        assert InSet is not None

    def test_matches_pattern_importable(self) -> None:
        assert MatchesPattern is not None

    def test_foreign_key_importable(self) -> None:
        assert ForeignKey is not None

    def test_unique_importable(self) -> None:
        assert Unique is not None

    def test_max_age_importable(self) -> None:
        assert MaxAge is not None

    def test_custom_sql_importable(self) -> None:
        assert CustomSQL is not None


class TestNotNull:
    def test_get_where_clause_single_column(self) -> None:
        check = NotNull("email")
        assert check.get_where_clause() == "email IS NULL"

    def test_get_where_clause_multiple_columns(self) -> None:
        check = NotNull("email", "name")
        assert check.get_where_clause() == "email IS NULL OR name IS NULL"

    def test_display_name(self) -> None:
        check = NotNull("email")
        assert check.display_name == "NotNull: email"

    def test_columns_to_show(self) -> None:
        check = NotNull("email", "name")
        assert check.columns_to_show == ["email", "name"]

    def test_requires_at_least_one_column(self) -> None:
        with pytest.raises(ValueError, match="At least one column"):
            NotNull()


class TestNotEmpty:
    def test_get_where_clause_single_column(self) -> None:
        check = NotEmpty("name")
        assert check.get_where_clause() == "(name IS NULL OR TRIM(name) = '')"

    def test_get_where_clause_multiple_columns(self) -> None:
        check = NotEmpty("name", "email")
        result = check.get_where_clause()
        assert "(name IS NULL OR TRIM(name) = '')" in result
        assert "(email IS NULL OR TRIM(email) = '')" in result
        assert " OR " in result

    def test_display_name(self) -> None:
        check = NotEmpty("name")
        assert check.display_name == "NotEmpty: name"


class TestInRange:
    def test_get_where_clause_min_and_max(self) -> None:
        check = InRange("amount", min=0, max=10000)
        assert check.get_where_clause() == "NOT (amount >= 0 AND amount <= 10000)"

    def test_get_where_clause_min_only(self) -> None:
        check = InRange("amount", min=0)
        assert check.get_where_clause() == "NOT (amount >= 0)"

    def test_get_where_clause_max_only(self) -> None:
        check = InRange("amount", max=10000)
        assert check.get_where_clause() == "NOT (amount <= 10000)"

    def test_requires_min_or_max(self) -> None:
        with pytest.raises(ValueError, match="At least one of min or max"):
            InRange("amount")

    def test_display_name(self) -> None:
        check = InRange("amount", min=0, max=10000)
        assert check.display_name == "InRange: amount >= 0 AND amount <= 10000"


class TestInSet:
    def test_get_where_clause(self) -> None:
        check = InSet("status", ["A", "B"])
        assert check.get_where_clause() == "status NOT IN ('A', 'B')"

    def test_display_name(self) -> None:
        check = InSet("status", ["A", "B"])
        assert check.display_name == "InSet: status in [A, B]"

    def test_columns_to_show(self) -> None:
        check = InSet("status", ["A", "B"])
        assert check.columns_to_show == ["status"]


class TestMatchesPattern:
    def test_get_where_clause(self) -> None:
        check = MatchesPattern("email", r"^.+@.+$")
        result = check.get_where_clause()
        assert "REGEXP_CONTAINS" in result
        assert "email" in result
        assert "^.+@.+$" in result

    def test_display_name(self) -> None:
        check = MatchesPattern("email", r"^.+@.+$")
        assert check.display_name == "MatchesPattern: email"

    def test_columns_to_show(self) -> None:
        check = MatchesPattern("email", r"^.+@.+$")
        assert check.columns_to_show == ["email"]


class TestForeignKey:
    def test_get_where_clause(self) -> None:
        check = ForeignKey("user_id", "user.id")
        result = check.get_where_clause()
        assert "user_id IS NOT NULL" in result
        assert "NOT IN" in result
        assert "SELECT id FROM user" in result

    def test_custom_ref_table(self) -> None:
        check = ForeignKey("user_id", "user.id", ref_table="dim_user")
        result = check.get_where_clause()
        assert "SELECT id FROM dim_user" in result

    def test_default_ref_table_no_prefix(self) -> None:
        """Default ref_table should use entity name without hard-coded prefix."""
        check = ForeignKey("user_id", "user.id")
        assert check._resolved_ref_table == "user"

    def test_requires_dot_in_references(self) -> None:
        with pytest.raises(ValueError, match="references must be in format"):
            ForeignKey("user_id", "user_id")

    def test_display_name(self) -> None:
        check = ForeignKey("user_id", "user.id")
        assert check.display_name == "ForeignKey: user_id -> user"


class TestUnique:
    def test_requires_special_query(self) -> None:
        check = Unique("email")
        assert check.requires_special_query is True

    def test_get_where_clause_raises(self) -> None:
        check = Unique("email")
        with pytest.raises(NotImplementedError):
            check.get_where_clause()

    def test_display_name(self) -> None:
        check = Unique("email")
        assert check.display_name == "Unique: email"

    def test_columns_to_show(self) -> None:
        check = Unique("email", "name")
        assert check.columns_to_show == ["email", "name"]

    def test_requires_at_least_one_column(self) -> None:
        with pytest.raises(ValueError, match="At least one column"):
            Unique()


class TestMaxAge:
    def test_requires_special_query(self) -> None:
        check = MaxAge("updated_at", days=7)
        assert check.requires_special_query is True

    def test_get_where_clause_raises(self) -> None:
        check = MaxAge("updated_at", days=7)
        with pytest.raises(NotImplementedError):
            check.get_where_clause()

    def test_display_name(self) -> None:
        check = MaxAge("updated_at", days=7)
        assert check.display_name == "MaxAge: updated_at within 7 days"

    def test_columns_to_show(self) -> None:
        check = MaxAge("updated_at", days=7)
        assert check.columns_to_show == ["updated_at"]


class TestCustomSQL:
    def test_get_where_clause(self) -> None:
        check = CustomSQL(name="test", sql="amount > 0")
        assert check.get_where_clause() == "NOT (amount > 0)"

    def test_display_name(self) -> None:
        check = CustomSQL(name="test", sql="amount > 0")
        assert check.display_name == "Custom: test"

    def test_requires_special_query_default(self) -> None:
        check = CustomSQL(name="test", sql="amount > 0")
        assert check.requires_special_query is False


class TestQualityConfig:
    def test_default_empty_checks(self) -> None:
        config = QualityConfig()
        assert config.checks == []

    def test_with_checks_list(self) -> None:
        checks = [NotNull("email"), NotEmpty("name")]
        config = QualityConfig(checks=checks)
        assert len(config.checks) == 2

    def test_checks_type_annotation_is_quality_check(self) -> None:
        """QualityConfig.checks type annotation should be list[QualityCheck], not list[Any]."""
        hints = typing.get_type_hints(QualityConfig)
        checks_type = hints["checks"]
        origin = typing.get_origin(checks_type)
        args = typing.get_args(checks_type)
        assert origin is list
        assert args == (QualityCheck,)

    def test_default_primary_key(self) -> None:
        config = QualityConfig()
        assert config.primary_key == "id"

    def test_custom_primary_key(self) -> None:
        config = QualityConfig(primary_key="user_id")
        assert config.primary_key == "user_id"
