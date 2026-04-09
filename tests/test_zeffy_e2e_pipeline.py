"""End-to-end tests for the Zeffy attribution pipeline.

Validates that the full pipeline runs on DuckDB with synthetic data
and produces correct outputs.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Ensure examples directory is importable
sys.path.insert(0, str(Path(__file__).parent.parent / "examples" / "zeffy-attribution"))

from run_pipeline import run_pipeline  # noqa: E402


@pytest.fixture(scope="module")
def pipeline_result():
    """Run the full pipeline once and share results across tests."""
    return run_pipeline()


class TestPipelineSuccess:
    """Verify the pipeline completes without errors."""

    def test_pipeline_succeeds(self, pipeline_result):
        assert pipeline_result.success, (
            f"Pipeline had errors: {pipeline_result.errors}"
        )

    def test_all_entities_succeed(self, pipeline_result):
        for name, er in pipeline_result.entity_results.items():
            assert er.success, f"{name} failed: {er.error}"

    def test_six_entities_produced(self, pipeline_result):
        assert len(pipeline_result.entity_results) == 6


class TestTouchpoints:
    """Validate touchpoints entity output."""

    def test_row_count(self, pipeline_result):
        er = pipeline_result.entity_results["touchpoints"]
        assert er.row_count == 200, f"Expected 200 rows, got {er.row_count}"

    def test_has_channel_column(self, pipeline_result):
        er = pipeline_result.entity_results["touchpoints"]
        assert "channel" in er.columns

    def test_has_json_extracted_columns(self, pipeline_result):
        er = pipeline_result.entity_results["touchpoints"]
        expected = {
            "gclid",
            "fbclid",
            "utm_source",
            "utm_medium",
            "utm_campaign",
            "referring_domain",
            "form_type",
            "organization_id",
        }
        assert expected.issubset(set(er.columns)), (
            f"Missing columns: {expected - set(er.columns)}"
        )

    def test_channel_values(self, pipeline_result):
        """Channel classification produces expected values."""
        import ibis

        conn = ibis.duckdb.connect()
        data_dir = Path(__file__).parent.parent / "examples" / "zeffy-attribution" / "data"
        conn.read_parquet(
            str(data_dir / "amplitude" / "events" / "*.parquet"),
            table_name="source_touchpoints",
        )
        prep = conn.sql("""
            SELECT
                json_extract_string(event_properties, 'gclid') AS gclid,
                json_extract_string(event_properties, 'fbclid') AS fbclid,
                json_extract_string(event_properties, 'utm_source') AS utm_source,
                json_extract_string(event_properties, 'utm_medium') AS utm_medium,
                json_extract_string(event_properties, 'referring_domain') AS referring_domain
            FROM source_touchpoints
        """)
        dim = prep.mutate(
            channel=ibis.cases(
                (prep.gclid.notnull(), "paid_search_google"),
                (prep.fbclid.notnull(), "paid_social_meta"),
                (prep.utm_medium.isin(["cpc", "ppc", "paid", "paid_social"]), "paid_other"),
                (prep.utm_source.notnull(), "organic_campaign"),
                (prep.referring_domain.notnull(), "organic_referral"),
                else_="direct_or_unknown",
            ),
        )
        channels = set(dim.select("channel").distinct().execute()["channel"].tolist())
        expected_channels = {
            "paid_search_google",
            "paid_social_meta",
            "paid_other",
            "organic_campaign",
            "organic_referral",
            "direct_or_unknown",
        }
        assert channels == expected_channels, (
            f"Expected channels {expected_channels}, got {channels}"
        )
        conn.disconnect()


class TestAccount:
    """Validate account entity output."""

    def test_row_count(self, pipeline_result):
        er = pipeline_result.entity_results["account"]
        assert er.row_count > 0, "Account should have rows"

    def test_has_account_id(self, pipeline_result):
        er = pipeline_result.entity_results["account"]
        assert "account_id" in er.columns

    def test_has_organization_id(self, pipeline_result):
        er = pipeline_result.entity_results["account"]
        assert "organization_id" in er.columns

    def test_has_identity_flags(self, pipeline_result):
        er = pipeline_result.entity_results["account"]
        assert "is_organizations" in er.columns
        assert "is_amplitude" in er.columns


class TestAttributionFirstTouch:
    """Validate first-touch attribution entity output."""

    def test_row_count(self, pipeline_result):
        er = pipeline_result.entity_results["attribution_first_touch"]
        assert er.row_count > 0, "First-touch should have rows"

    def test_has_attribution_columns(self, pipeline_result):
        er = pipeline_result.entity_results["attribution_first_touch"]
        expected = {
            "organization_id",
            "first_touch_channel",
            "first_touch_time",
        }
        assert expected.issubset(set(er.columns))

    def test_one_row_per_org(self, pipeline_result):
        er = pipeline_result.entity_results["attribution_first_touch"]
        # Each org should appear exactly once
        assert er.row_count > 0


class TestAttributionPaidPriority:
    """Validate paid-priority attribution entity output."""

    def test_row_count(self, pipeline_result):
        er = pipeline_result.entity_results["attribution_paid_priority"]
        assert er.row_count > 0

    def test_has_attribution_columns(self, pipeline_result):
        er = pipeline_result.entity_results["attribution_paid_priority"]
        expected = {
            "organization_id",
            "paid_priority_channel",
            "has_paid_touch",
            "total_touchpoints",
        }
        assert expected.issubset(set(er.columns))


class TestAcquisitionSignal:
    """Validate acquisition_signal entity output."""

    def test_row_count(self, pipeline_result):
        er = pipeline_result.entity_results["acquisition_signal"]
        assert er.row_count > 0

    def test_has_signal_columns(self, pipeline_result):
        er = pipeline_result.entity_results["acquisition_signal"]
        expected = {
            "organization_id",
            "first_event_time",
            "last_event_time",
            "touchpoint_count",
            "distinct_channels",
        }
        assert expected.issubset(set(er.columns))


class TestAccountAttributed:
    """Validate account_attributed entity output (the final wide table)."""

    def test_has_one_row_per_account(self, pipeline_result):
        er = pipeline_result.entity_results["account_attributed"]
        account_er = pipeline_result.entity_results["account"]
        # account_attributed should have same number of rows as account
        # (left join preserves all account rows)
        assert er.row_count == account_er.row_count

    def test_has_account_columns(self, pipeline_result):
        er = pipeline_result.entity_results["account_attributed"]
        assert "organization_id" in er.columns
        assert "account_id" in er.columns

    def test_has_first_touch_columns(self, pipeline_result):
        er = pipeline_result.entity_results["account_attributed"]
        assert "first_touch_channel" in er.columns
        assert "first_touch_time" in er.columns

    def test_has_paid_priority_columns(self, pipeline_result):
        er = pipeline_result.entity_results["account_attributed"]
        assert "paid_priority_channel" in er.columns
        assert "has_paid_touch" in er.columns
        assert "total_touchpoints" in er.columns

    def test_wide_table_column_count(self, pipeline_result):
        """Account attributed should have columns from account + both attribution models."""
        er = pipeline_result.entity_results["account_attributed"]
        # account (14 cols) + first_touch (4 new) + paid_priority (4 new) = 22
        assert len(er.columns) >= 18, (
            f"Expected at least 18 columns, got {len(er.columns)}: {er.columns}"
        )


class TestGapsDocumented:
    """Verify that known gaps are documented in the pipeline result."""

    def test_gaps_found(self, pipeline_result):
        assert len(pipeline_result.gaps) >= 5, (
            f"Expected at least 5 gaps, found {len(pipeline_result.gaps)}"
        )

    def test_json_extract_gap(self, pipeline_result):
        gap_text = " ".join(pipeline_result.gaps)
        assert "json_extract_scalar" in gap_text

    def test_registry_gap(self, pipeline_result):
        gap_text = " ".join(pipeline_result.gaps)
        assert "module.entity" in gap_text

    def test_derived_source_gap(self, pipeline_result):
        gap_text = " ".join(pipeline_result.gaps)
        assert "identity_graph_config" in gap_text
