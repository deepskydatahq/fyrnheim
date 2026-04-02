"""E2e tests for multi-source entity projection."""

import json

import ibis

from fyrnheim.components.computed_column import ComputedColumn
from fyrnheim.core.entity_model import EntityModel, StateField
from fyrnheim.engine.entity_projection import project_entity


def _make_enriched_events():
    """Create enriched events from two sources (crm and billing) with shared canonical_ids.

    Timeline for canonical_id c1 (alice):
      - crm row_appeared at 2024-01-01: name=alice, email_hash=abc
      - billing row_appeared at 2024-01-02: plan=free
      - billing field_changed at 2024-01-05: plan free->pro

    Timeline for canonical_id c2 (bob):
      - billing row_appeared at 2024-01-01: plan=free
      - crm row_appeared at 2024-01-03: name=bob, email_hash=def
    """
    rows = [
        # c1 events
        {
            "source": "crm",
            "entity_id": "crm_u1",
            "ts": "2024-01-01",
            "event_type": "row_appeared",
            "payload": json.dumps({"name": "alice", "email_hash": "abc"}),
            "canonical_id": "c1",
        },
        {
            "source": "billing",
            "entity_id": "bill_u1",
            "ts": "2024-01-02",
            "event_type": "row_appeared",
            "payload": json.dumps({"name": "alice_billing", "email_hash": "abc_billing", "plan": "free"}),
            "canonical_id": "c1",
        },
        {
            "source": "billing",
            "entity_id": "bill_u1",
            "ts": "2024-01-05",
            "event_type": "field_changed",
            "payload": json.dumps({"field_name": "plan", "old_value": "free", "new_value": "pro"}),
            "canonical_id": "c1",
        },
        # c2 events
        {
            "source": "billing",
            "entity_id": "bill_u2",
            "ts": "2024-01-01",
            "event_type": "row_appeared",
            "payload": json.dumps({"name": "bob_billing", "email_hash": "def_billing", "plan": "free"}),
            "canonical_id": "c2",
        },
        {
            "source": "crm",
            "entity_id": "crm_u2",
            "ts": "2024-01-03",
            "event_type": "row_appeared",
            "payload": json.dumps({"name": "bob", "email_hash": "def"}),
            "canonical_id": "c2",
        },
    ]
    return ibis.memtable(rows)


def _make_entity_model():
    """Create an EntityModel using all three strategies plus a computed field."""
    return EntityModel(
        name="user",
        identity_graph="user_graph",
        state_fields=[
            # latest: get the most recent name from crm
            StateField(name="name", source="crm", field="name", strategy="latest"),
            # first: get the earliest observed plan from billing
            StateField(name="original_plan", source="billing", field="plan", strategy="first"),
            # latest: get current plan from billing
            StateField(name="plan", source="billing", field="plan", strategy="latest"),
            # coalesce: prefer crm email_hash, fall back to billing
            StateField(
                name="email_hash",
                source="crm",
                field="email_hash",
                strategy="coalesce",
                priority=["crm", "billing"],
            ),
        ],
        computed_fields=[
            ComputedColumn(name="is_paying", expression="plan != 'free'"),
        ],
    )


class TestE2eMultiSourceProjection:
    """E2e: projection from events across 2 sources produces correct entity table."""

    def test_produces_one_row_per_entity(self):
        events = _make_enriched_events()
        model = _make_entity_model()
        result = project_entity(events, model).execute()
        assert len(result) == 2

    def test_latest_strategy_returns_correct_values(self):
        events = _make_enriched_events()
        model = _make_entity_model()
        result = project_entity(events, model).execute()
        result = result.sort_values("canonical_id").reset_index(drop=True)

        # c1: latest name from crm is "alice" (only one crm event)
        assert result.iloc[0]["name"] == "alice"
        # c1: latest plan from billing is "pro" (field_changed at 2024-01-05)
        assert result.iloc[0]["plan"] == "pro"

        # c2: latest name from crm is "bob"
        assert result.iloc[1]["name"] == "bob"
        # c2: latest plan from billing is "free" (only one billing event)
        assert result.iloc[1]["plan"] == "free"

    def test_first_strategy_returns_earliest_value(self):
        events = _make_enriched_events()
        model = _make_entity_model()
        result = project_entity(events, model).execute()
        result = result.sort_values("canonical_id").reset_index(drop=True)

        # c1: first plan from billing is "free" (row_appeared at 2024-01-02)
        assert result.iloc[0]["original_plan"] == "free"
        # c2: first plan from billing is "free"
        assert result.iloc[1]["original_plan"] == "free"

    def test_coalesce_strategy_prefers_higher_priority(self):
        events = _make_enriched_events()
        model = _make_entity_model()
        result = project_entity(events, model).execute()
        result = result.sort_values("canonical_id").reset_index(drop=True)

        # c1: crm has email_hash="abc", billing has "abc_billing" -> prefer crm
        assert result.iloc[0]["email_hash"] == "abc"
        # c2: crm has email_hash="def", billing has "def_billing" -> prefer crm
        assert result.iloc[1]["email_hash"] == "def"

    def test_computed_field_is_paying(self):
        events = _make_enriched_events()
        model = _make_entity_model()
        result = project_entity(events, model).execute()
        result = result.sort_values("canonical_id").reset_index(drop=True)

        # c1: plan=pro -> is_paying=True
        assert bool(result.iloc[0]["is_paying"]) is True
        # c2: plan=free -> is_paying=False
        assert bool(result.iloc[1]["is_paying"]) is False

    def test_result_columns_are_complete(self):
        events = _make_enriched_events()
        model = _make_entity_model()
        result = project_entity(events, model).execute()
        expected_cols = {"canonical_id", "name", "original_plan", "plan", "email_hash", "is_paying"}
        assert set(result.columns) == expected_cols
