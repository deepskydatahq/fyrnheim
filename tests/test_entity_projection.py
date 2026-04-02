"""Tests for the entity projection engine."""

import json

import ibis
import pytest

from fyrnheim.components.computed_column import ComputedColumn
from fyrnheim.core.entity_model import EntityModel, StateField
from fyrnheim.engine.entity_projection import project_entity


def _make_enriched_events(rows: list[dict]) -> ibis.expr.types.Table:
    """Helper to create an enriched events memtable."""
    return ibis.memtable(rows)


class TestProjectEntityOneRowPerCanonicalId:
    """project_entity returns one row per canonical_id."""

    def test_single_canonical_id(self):
        events = _make_enriched_events([
            {
                "source": "crm",
                "entity_id": "u1",
                "ts": "2024-01-01",
                "event_type": "row_appeared",
                "payload": json.dumps({"name": "alice"}),
                "canonical_id": "c1",
            },
        ])
        model = EntityModel(
            name="user",
            identity_graph="user_graph",
            state_fields=[
                StateField(name="name", source="crm", field="name", strategy="latest"),
            ],
        )
        result = project_entity(events, model).execute()
        assert len(result) == 1
        assert result.iloc[0]["canonical_id"] == "c1"

    def test_multiple_canonical_ids(self):
        events = _make_enriched_events([
            {
                "source": "crm",
                "entity_id": "u1",
                "ts": "2024-01-01",
                "event_type": "row_appeared",
                "payload": json.dumps({"name": "alice"}),
                "canonical_id": "c1",
            },
            {
                "source": "crm",
                "entity_id": "u2",
                "ts": "2024-01-01",
                "event_type": "row_appeared",
                "payload": json.dumps({"name": "bob"}),
                "canonical_id": "c2",
            },
        ])
        model = EntityModel(
            name="user",
            identity_graph="user_graph",
            state_fields=[
                StateField(name="name", source="crm", field="name", strategy="latest"),
            ],
        )
        result = project_entity(events, model).execute()
        assert len(result) == 2
        cids = set(result["canonical_id"].tolist())
        assert cids == {"c1", "c2"}


class TestLatestStrategy:
    """latest strategy returns the most recent value."""

    def test_returns_latest_value(self):
        events = _make_enriched_events([
            {
                "source": "crm",
                "entity_id": "u1",
                "ts": "2024-01-01",
                "event_type": "row_appeared",
                "payload": json.dumps({"name": "alice_old"}),
                "canonical_id": "c1",
            },
            {
                "source": "crm",
                "entity_id": "u1",
                "ts": "2024-01-02",
                "event_type": "row_appeared",
                "payload": json.dumps({"name": "alice_new"}),
                "canonical_id": "c1",
            },
        ])
        model = EntityModel(
            name="user",
            identity_graph="user_graph",
            state_fields=[
                StateField(name="name", source="crm", field="name", strategy="latest"),
            ],
        )
        result = project_entity(events, model).execute()
        assert result.iloc[0]["name"] == "alice_new"

    def test_uses_field_changed_events(self):
        events = _make_enriched_events([
            {
                "source": "billing",
                "entity_id": "u1",
                "ts": "2024-01-01",
                "event_type": "row_appeared",
                "payload": json.dumps({"plan": "free"}),
                "canonical_id": "c1",
            },
            {
                "source": "billing",
                "entity_id": "u1",
                "ts": "2024-01-02",
                "event_type": "field_changed",
                "payload": json.dumps({"field_name": "plan", "old_value": "free", "new_value": "pro"}),
                "canonical_id": "c1",
            },
        ])
        model = EntityModel(
            name="user",
            identity_graph="user_graph",
            state_fields=[
                StateField(name="plan", source="billing", field="plan", strategy="latest"),
            ],
        )
        result = project_entity(events, model).execute()
        assert result.iloc[0]["plan"] == "pro"


class TestFirstStrategy:
    """first strategy returns the earliest observed value."""

    def test_returns_first_value(self):
        events = _make_enriched_events([
            {
                "source": "crm",
                "entity_id": "u1",
                "ts": "2024-01-01",
                "event_type": "row_appeared",
                "payload": json.dumps({"name": "alice_first"}),
                "canonical_id": "c1",
            },
            {
                "source": "crm",
                "entity_id": "u1",
                "ts": "2024-01-02",
                "event_type": "row_appeared",
                "payload": json.dumps({"name": "alice_updated"}),
                "canonical_id": "c1",
            },
        ])
        model = EntityModel(
            name="user",
            identity_graph="user_graph",
            state_fields=[
                StateField(name="name", source="crm", field="name", strategy="first"),
            ],
        )
        result = project_entity(events, model).execute()
        assert result.iloc[0]["name"] == "alice_first"


class TestCoalesceStrategy:
    """coalesce strategy uses priority-ordered source fallback."""

    def test_uses_highest_priority_source(self):
        events = _make_enriched_events([
            {
                "source": "crm",
                "entity_id": "u1",
                "ts": "2024-01-01",
                "event_type": "row_appeared",
                "payload": json.dumps({"email_hash": "crm_hash"}),
                "canonical_id": "c1",
            },
            {
                "source": "billing",
                "entity_id": "b1",
                "ts": "2024-01-01",
                "event_type": "row_appeared",
                "payload": json.dumps({"email_hash": "billing_hash"}),
                "canonical_id": "c1",
            },
        ])
        model = EntityModel(
            name="user",
            identity_graph="user_graph",
            state_fields=[
                StateField(
                    name="email_hash",
                    source="crm",
                    field="email_hash",
                    strategy="coalesce",
                    priority=["crm", "billing"],
                ),
            ],
        )
        result = project_entity(events, model).execute()
        assert result.iloc[0]["email_hash"] == "crm_hash"

    def test_falls_back_to_lower_priority(self):
        events = _make_enriched_events([
            {
                "source": "billing",
                "entity_id": "b1",
                "ts": "2024-01-01",
                "event_type": "row_appeared",
                "payload": json.dumps({"email_hash": "billing_hash"}),
                "canonical_id": "c1",
            },
        ])
        model = EntityModel(
            name="user",
            identity_graph="user_graph",
            state_fields=[
                StateField(
                    name="email_hash",
                    source="crm",
                    field="email_hash",
                    strategy="coalesce",
                    priority=["crm", "billing"],
                ),
            ],
        )
        result = project_entity(events, model).execute()
        assert result.iloc[0]["email_hash"] == "billing_hash"


class TestComputedFields:
    """Computed fields are evaluated on top of projected state."""

    def test_computed_field_evaluation(self):
        events = _make_enriched_events([
            {
                "source": "billing",
                "entity_id": "u1",
                "ts": "2024-01-01",
                "event_type": "row_appeared",
                "payload": json.dumps({"plan": "pro"}),
                "canonical_id": "c1",
            },
            {
                "source": "billing",
                "entity_id": "u2",
                "ts": "2024-01-01",
                "event_type": "row_appeared",
                "payload": json.dumps({"plan": "free"}),
                "canonical_id": "c2",
            },
        ])
        model = EntityModel(
            name="user",
            identity_graph="user_graph",
            state_fields=[
                StateField(name="plan", source="billing", field="plan", strategy="latest"),
            ],
            computed_fields=[
                ComputedColumn(name="is_paying", expression="plan != 'free'"),
            ],
        )
        result = project_entity(events, model).execute()
        result = result.sort_values("canonical_id").reset_index(drop=True)
        assert bool(result.iloc[0]["is_paying"]) is True  # c1: pro
        assert bool(result.iloc[1]["is_paying"]) is False  # c2: free
