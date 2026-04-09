"""End-to-end regression test for ADR-0001 (M053).

Mirrors the customers.py example: a StateSource produces a row_appeared
event followed by a field_changed event on the email column. A `signup`
ActivityDefinition matches RowAppeared. Before v0.5.0, the field_changed
event was silently dropped by apply_activity_definitions, causing the
state field projection to return the stale signup-time email. After
v0.5.0 (this ADR), the field_changed event passes through and the
projection returns the latest value.
"""

from __future__ import annotations

import json

import ibis
import pandas as pd

from fyrnheim.core.activity import ActivityDefinition, RowAppeared
from fyrnheim.engine.activity_engine import apply_activity_definitions
from fyrnheim.engine.analytics_entity_engine import _resolve_latest


def test_e2e_customers_example_state_fields_stay_fresh():
    # Two events for the same entity on a StateSource:
    # 1) row_appeared at t=1 with the original email
    # 2) field_changed at t=2 updating the email
    raw = ibis.memtable(
        pd.DataFrame(
            [
                {
                    "source": "crm_contacts",
                    "entity_id": "c1",
                    "ts": "2024-01-01T00:00:00",
                    "event_type": "row_appeared",
                    "payload": json.dumps(
                        {"email": "a@b.com", "name": "Alice"}
                    ),
                },
                {
                    "source": "crm_contacts",
                    "entity_id": "c1",
                    "ts": "2024-01-02T00:00:00",
                    "event_type": "field_changed",
                    "payload": json.dumps(
                        {
                            "field_name": "email",
                            "old_value": "a@b.com",
                            "new_value": "a@c.com",
                        }
                    ),
                },
            ]
        )
    )

    defns = [
        ActivityDefinition(
            name="signup",
            source="crm_contacts",
            trigger=RowAppeared(),
            entity_id_field="id",
        )
    ]

    result_df = apply_activity_definitions(raw, defns).execute()

    # Both events must be present: the renamed signup AND the
    # passed-through field_changed event. Pre-v0.5.0, only signup
    # would appear (field_changed would be silently dropped).
    event_types = set(result_df["event_type"])
    assert "signup" in event_types
    assert "field_changed" in event_types
    assert len(result_df) == 2

    # Apply the StateField "latest" resolution that AnalyticsEntity
    # projection would use. Pre-v0.5.0 this returned "a@b.com" (the
    # signup-time value, baked into the rewritten signup payload).
    # After v0.5.0 it must return "a@c.com" — the latest field_changed
    # value — because the field_changed event is no longer dropped.
    latest_email = _resolve_latest(result_df, "email")
    assert latest_email == "a@c.com"
