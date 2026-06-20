from pathlib import Path

import ibis
import pytest

from fyrnheim.core.multi_model import (
    MultiModelDefinition,
    load_multi_model,
    load_multi_model_flow,
)

PAID_PRIORITY = """
name: paid-priority
description: Prioritize paid touchpoints.
rules:
  - name: Google Ads
    target_channel: paid_search_google
    conditions:
      - field: gclid
        operator: is_not_empty
  - name: CPC
    target_channel: paid_search
    conditions:
      - field: utm_medium
        operator: equals
        value: cpc
"""

SURVEY_PRIORITY = """
name: survey-priority
rules:
  - name: Survey social
    target_channel: organic_social
    sub_channel: survey_social_media
    conditions:
      - field: referral_source
        operator: equals
        value: SocialMedia
  - name: Paid fallback
    target_channel: paid_search
    conditions:
      - field: utm_medium
        operator: in
        value: cpc,ppc
"""


def test_loads_and_evaluates_ordered_model(tmp_path: Path) -> None:
    path = tmp_path / "paid-priority.yaml"
    path.write_text(PAID_PRIORITY)

    model = load_multi_model(path, allowed_fields=["gclid", "utm_medium"])

    assert model.name == "paid-priority"
    assert model.filename == "paid-priority.yaml"
    assert model.evaluate_record({"gclid": "abc", "utm_medium": "cpc"}) == {
        "model": "paid-priority",
        "channel": "paid_search_google",
        "sub_channel": None,
        "rule": "Google Ads",
    }
    assert model.evaluate_record({"gclid": "", "utm_medium": "cpc"})["channel"] == "paid_search"
    assert model.evaluate_record({"gclid": "", "utm_medium": "email"}, default_channel="direct") == {
        "model": "paid-priority",
        "channel": "direct",
        "sub_channel": None,
        "rule": None,
    }


def test_load_flow_from_directory_and_project_model_outputs(tmp_path: Path) -> None:
    (tmp_path / "paid-priority.yaml").write_text(PAID_PRIORITY)
    (tmp_path / "survey-priority.yaml").write_text(SURVEY_PRIORITY)
    flow = load_multi_model_flow(tmp_path)

    table = ibis.memtable(
        [
            {"gclid": "g-1", "utm_medium": "email", "referral_source": "SocialMedia"},
            {"gclid": "", "utm_medium": "cpc", "referral_source": ""},
            {"gclid": "", "utm_medium": "email", "referral_source": ""},
        ]
    )

    result = flow.project(table, default_channel="direct_or_unknown").execute().to_dict("records")

    assert result[0]["model_paid_priority_channel"] == "paid_search_google"
    assert result[0]["model_survey_priority_channel"] == "organic_social"
    assert result[0]["model_survey_priority_sub_channel"] == "survey_social_media"
    assert result[1]["model_paid_priority_channel"] == "paid_search"
    assert result[1]["model_survey_priority_channel"] == "paid_search"
    assert result[2]["model_paid_priority_channel"] == "direct_or_unknown"
    assert result[2]["model_survey_priority_rule"] is None


def test_rejects_unknown_allowed_fields(tmp_path: Path) -> None:
    path = tmp_path / "model.yaml"
    path.write_text(PAID_PRIORITY)

    with pytest.raises(ValueError, match="unknown field 'gclid'"):
        load_multi_model(path, allowed_fields=["utm_medium"])


def test_rejects_conditions_without_required_values() -> None:
    with pytest.raises(ValueError, match="requires a value"):
        MultiModelDefinition.model_validate(
            {
                "name": "bad",
                "rules": [
                    {
                        "name": "bad rule",
                        "target_channel": "paid",
                        "conditions": [{"field": "utm_medium", "operator": "equals"}],
                    }
                ],
            }
        )
