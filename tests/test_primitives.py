"""Tests for typedata.primitives package."""



class TestHashing:
    """Tests for hashing primitives."""

    def test_hash_email_importable_and_returns_string(self):
        from typedata.primitives import hash_email

        result = hash_email("email")
        assert isinstance(result, str)
        assert "hash()" in result
        assert "lower()" in result
        assert "strip()" in result

    def test_concat_hash_importable_and_returns_string(self):
        from typedata.primitives import concat_hash

        result = concat_hash("col1", "col2")
        assert isinstance(result, str)
        assert "ibis.concat" in result
        assert "hash()" in result

    def test_hash_md5_importable_and_returns_string(self):
        from typedata.primitives import hash_md5

        result = hash_md5("col")
        assert isinstance(result, str)
        assert "hashbytes" in result
        assert "md5" in result

    def test_hash_sha256_importable_and_returns_string(self):
        from typedata.primitives import hash_sha256

        result = hash_sha256("col")
        assert isinstance(result, str)
        assert "hash()" in result
        assert 'cast("string")' in result


class TestCategorization:
    """Tests for categorization primitives."""

    def test_categorize_importable_and_returns_string(self):
        from typedata.primitives import categorize

        result = categorize("revenue", [(1000, "small"), (10000, "medium")], "large")
        assert isinstance(result, str)
        assert "ibis.cases" in result
        assert "small" in result
        assert "medium" in result
        assert "large" in result

    def test_lifecycle_flag_importable_and_returns_string(self):
        from typedata.primitives import lifecycle_flag

        result = lifecycle_flag("status", ["active", "on_trial"])
        assert isinstance(result, str)
        assert "isin" in result
        assert "active" in result
        assert "on_trial" in result

    def test_boolean_to_int_importable_and_returns_string(self):
        from typedata.primitives import boolean_to_int

        result = boolean_to_int("is_active")
        assert isinstance(result, str)
        assert 'cast("int64")' in result


class TestDates:
    """Tests for date primitives."""

    def test_date_diff_days_importable_and_returns_string(self):
        from typedata.primitives import date_diff_days

        result = date_diff_days("created_at")
        assert isinstance(result, str)
        assert "delta" in result
        assert "day" in result

    def test_days_since_importable_and_returns_string(self):
        from typedata.primitives import days_since

        result = days_since("created_at")
        assert isinstance(result, str)
        assert "delta" in result

    def test_extract_year_importable_and_returns_string(self):
        from typedata.primitives import extract_year

        result = extract_year("created_at")
        assert isinstance(result, str)
        assert "year()" in result

    def test_date_trunc_month_importable_and_returns_string(self):
        from typedata.primitives import date_trunc_month

        result = date_trunc_month("created_at")
        assert isinstance(result, str)
        assert 'truncate("M")' in result


class TestAggregations:
    """Tests for aggregation primitives."""

    def test_sum_returns_expected_sql(self):
        from typedata.primitives import sum_

        assert sum_("amount") == "SUM(amount)"

    def test_count_returns_expected_sql(self):
        from typedata.primitives import count_

        assert count_() == "COUNT(*)"

    def test_count_distinct_returns_expected_sql(self):
        from typedata.primitives import count_distinct

        assert count_distinct("user_id") == "COUNT(DISTINCT user_id)"

    def test_avg_returns_expected_sql(self):
        from typedata.primitives import avg_

        assert avg_("amount") == "AVG(amount)"

    def test_row_number_by_returns_expected_sql(self):
        from typedata.primitives import row_number_by

        result = row_number_by("user_id", "created_at DESC")
        assert isinstance(result, str)
        assert "ROW_NUMBER()" in result
        assert "PARTITION BY user_id" in result
        assert "ORDER BY created_at DESC" in result


class TestJsonOps:
    """Tests for JSON operation primitives."""

    def test_to_json_struct_returns_string(self):
        from typedata.primitives import to_json_struct

        result = to_json_struct({"amount": "total_amount"})
        assert isinstance(result, str)
        assert "TO_JSON_STRING" in result
        assert "STRUCT" in result

    def test_json_extract_scalar_returns_string(self):
        from typedata.primitives import json_extract_scalar

        result = json_extract_scalar("data", "$.amount")
        assert isinstance(result, str)
        assert "JSON_EXTRACT_SCALAR" in result

    def test_json_value_returns_string(self):
        from typedata.primitives import json_value

        result = json_value("data", "$.name")
        assert isinstance(result, str)
        assert "JSON_VALUE" in result


class TestStrings:
    """Tests for string primitives."""

    def test_extract_email_domain_returns_string(self):
        from typedata.primitives import extract_email_domain

        result = extract_email_domain("email")
        assert isinstance(result, str)
        assert "split_part" in result

    def test_is_personal_email_domain_returns_string(self):
        from typedata.primitives import is_personal_email_domain

        result = is_personal_email_domain("domain")
        assert isinstance(result, str)
        assert "IN" in result
        assert "gmail.com" in result

    def test_account_id_from_domain_returns_string(self):
        from typedata.primitives import account_id_from_domain

        result = account_id_from_domain("domain", "is_personal")
        assert isinstance(result, str)
        assert "CASE WHEN" in result
        assert "MD5" in result


class TestTime:
    """Tests for time primitives."""

    def test_parse_iso8601_duration_returns_string(self):
        from typedata.primitives import parse_iso8601_duration

        result = parse_iso8601_duration("duration")
        assert isinstance(result, str)
        assert "3600" in result
        assert "60" in result


class TestPrefixInjection:
    """Tests for t. prefix injection behavior."""

    def test_hash_email_adds_t_prefix(self):
        from typedata.primitives import hash_email

        result = hash_email("email")
        assert result.startswith("t.email")

    def test_hash_email_no_double_prefix(self):
        from typedata.primitives import hash_email

        result = hash_email("t.email")
        assert "t.t.email" not in result
        assert result.startswith("t.email")
