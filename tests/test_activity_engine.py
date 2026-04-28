"""Tests for the activity engine (apply_activity_definitions)."""

from __future__ import annotations

import inspect
import json

import ibis
import pandas as pd

from fyrnheim.core.activity import (
    ActivityDefinition,
    EventOccurred,
    FieldChanged,
    RowAppeared,
    RowDisappeared,
)
from fyrnheim.engine.activity_engine import apply_activity_definitions


def _make_raw_events(rows: list[dict[str, str]]) -> ibis.Table:
    """Helper to create a raw events Ibis table from dicts."""
    if not rows:
        schema = ibis.schema(
            {
                "source": "string",
                "entity_id": "string",
                "ts": "string",
                "event_type": "string",
                "payload": "string",
            }
        )
        return ibis.memtable([], schema=schema)
    return ibis.memtable(pd.DataFrame(rows))


def test_activity_derivation_compiles_to_bigquery_predicates_and_union() -> None:
    raw = _make_raw_events(
        [
            {
                "source": "customers",
                "entity_id": "1",
                "ts": "2024-01-01",
                "event_type": "row_appeared",
                "payload": json.dumps({"name": "alice", "plan": "free"}),
            },
            {
                "source": "customers",
                "entity_id": "1",
                "ts": "2024-01-02",
                "event_type": "field_changed",
                "payload": json.dumps(
                    {"field_name": "plan", "old_value": "free", "new_value": "pro"}
                ),
            },
        ]
    )
    defns = [
        ActivityDefinition(
            name="signup",
            source="customers",
            trigger=RowAppeared(),
            entity_id_field="id",
            include_fields=["name"],
        ),
        ActivityDefinition(
            name="became_paying",
            source="customers",
            trigger=FieldChanged(field="plan", to_values=["pro"]),
            entity_id_field="id",
        ),
    ]

    sql = ibis.to_sql(apply_activity_definitions(raw, defns), dialect="bigquery")

    assert "UNION ALL" in sql
    assert "field_changed" in sql
    assert "CAST" in sql
    assert "JSON" in sql
    assert "became_paying" in sql


def test_activity_engine_does_not_materialize_inputs_in_core_path() -> None:
    source = inspect.getsource(apply_activity_definitions)

    assert ".execute(" not in source
    assert "iterrows(" not in source


class TestRowAppearedTrigger:
    def test_matches_row_appeared_events_from_source(self):
        raw = _make_raw_events(
            [
                {
                    "source": "customers",
                    "entity_id": "1",
                    "ts": "2024-01-01",
                    "event_type": "row_appeared",
                    "payload": json.dumps({"name": "alice", "plan": "free"}),
                },
                {
                    "source": "customers",
                    "entity_id": "1",
                    "ts": "2024-01-02",
                    "event_type": "field_changed",
                    "payload": json.dumps(
                        {"field_name": "plan", "old_value": "free", "new_value": "pro"}
                    ),
                },
            ]
        )

        defns = [
            ActivityDefinition(
                name="signup",
                source="customers",
                trigger=RowAppeared(),
                entity_id_field="id",
            )
        ]

        result = apply_activity_definitions(raw, defns).execute()
        signup_rows = result[result["event_type"] == "signup"]
        assert len(signup_rows) == 1
        assert signup_rows.iloc[0]["entity_id"] == "1"

    def test_does_not_match_other_sources(self):
        raw = _make_raw_events(
            [
                {
                    "source": "orders",
                    "entity_id": "1",
                    "ts": "2024-01-01",
                    "event_type": "row_appeared",
                    "payload": json.dumps({"item": "widget"}),
                },
            ]
        )

        defns = [
            ActivityDefinition(
                name="signup",
                source="customers",
                trigger=RowAppeared(),
                entity_id_field="id",
            )
        ]

        result = apply_activity_definitions(raw, defns).execute()
        # Per ADR-0001 (v0.5.0): unmatched events pass through unchanged
        # rather than being dropped. The single input event has no matching
        # definition, so it appears in the output as-is.
        assert len(result) == 1
        row = result.iloc[0]
        assert row["source"] == "orders"
        assert row["entity_id"] == "1"
        assert row["event_type"] == "row_appeared"
        assert json.loads(row["payload"]) == {"item": "widget"}


class TestUnmatchedPreserved:
    def test_unmatched_events_pass_through(self):
        raw = _make_raw_events(
            [
                {
                    "source": "customers",
                    "entity_id": "1",
                    "ts": "2024-01-01",
                    "event_type": "row_appeared",
                    "payload": json.dumps({"name": "alice", "email": "a@b.com"}),
                },
                {
                    "source": "customers",
                    "entity_id": "1",
                    "ts": "2024-01-02",
                    "event_type": "field_changed",
                    "payload": json.dumps(
                        {
                            "field_name": "email",
                            "old_value": "a@b.com",
                            "new_value": "a@c.com",
                        }
                    ),
                },
                {
                    "source": "orders",
                    "entity_id": "9",
                    "ts": "2024-01-03",
                    "event_type": "row_appeared",
                    "payload": json.dumps({"item": "widget"}),
                },
            ]
        )

        defns = [
            ActivityDefinition(
                name="signup",
                source="customers",
                trigger=RowAppeared(),
                entity_id_field="id",
            )
        ]

        result = apply_activity_definitions(raw, defns).execute()
        assert len(result) == 3
        event_types = sorted(result["event_type"].tolist())
        assert event_types == ["field_changed", "row_appeared", "signup"]

        signup_row = result[result["event_type"] == "signup"].iloc[0]
        assert signup_row["entity_id"] == "1"

        fc_row = result[result["event_type"] == "field_changed"].iloc[0]
        assert fc_row["source"] == "customers"
        assert json.loads(fc_row["payload"])["new_value"] == "a@c.com"

        ord_row = result[result["event_type"] == "row_appeared"].iloc[0]
        assert ord_row["source"] == "orders"
        assert ord_row["entity_id"] == "9"

    def test_unmatched_events_preserve_original_event_type(self):
        raw = _make_raw_events(
            [
                {
                    "source": "customers",
                    "entity_id": "1",
                    "ts": "2024-01-01",
                    "event_type": "row_appeared",
                    "payload": json.dumps({"name": "alice"}),
                },
                {
                    "source": "customers",
                    "entity_id": "1",
                    "ts": "2024-01-02",
                    "event_type": "field_changed",
                    "payload": json.dumps(
                        {"field_name": "x", "old_value": "a", "new_value": "b"}
                    ),
                },
                {
                    "source": "page_events",
                    "entity_id": "u1",
                    "ts": "2024-01-03",
                    "event_type": "session_start",
                    "payload": json.dumps({"device": "mobile"}),
                },
            ]
        )

        # No definitions match anything
        defns = [
            ActivityDefinition(
                name="became_pro",
                source="customers",
                trigger=FieldChanged(field="plan"),
                entity_id_field="id",
            )
        ]

        result = apply_activity_definitions(raw, defns).execute()
        assert len(result) == 3
        event_types = set(result["event_type"])
        assert event_types == {"row_appeared", "field_changed", "session_start"}

    def test_unmatched_events_preserve_original_payload(self):
        original_payload = json.dumps(
            {"name": "alice", "email": "a@b.com", "plan": "free"}
        )
        raw = _make_raw_events(
            [
                {
                    "source": "customers",
                    "entity_id": "1",
                    "ts": "2024-01-01",
                    "event_type": "row_appeared",
                    "payload": original_payload,
                },
            ]
        )

        # Definition would match, but include_fields would normally filter.
        # Since this definition is for a different source, the row passes
        # through and the payload must be byte-identical.
        defns = [
            ActivityDefinition(
                name="signup",
                source="other_source",
                trigger=RowAppeared(),
                entity_id_field="id",
                include_fields=["name"],
            )
        ]

        result = apply_activity_definitions(raw, defns).execute()
        assert len(result) == 1
        assert result.iloc[0]["payload"] == original_payload

    def test_mixed_matched_unmatched_in_same_source(self):
        raw = _make_raw_events(
            [
                {
                    "source": "customers",
                    "entity_id": "1",
                    "ts": "2024-01-01",
                    "event_type": "row_appeared",
                    "payload": json.dumps({"name": "alice", "email": "a@b.com"}),
                },
                {
                    "source": "customers",
                    "entity_id": "1",
                    "ts": "2024-01-02",
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

        defns = [
            ActivityDefinition(
                name="signup",
                source="customers",
                trigger=RowAppeared(),
                entity_id_field="id",
            )
        ]

        result = apply_activity_definitions(raw, defns).execute()
        assert len(result) == 2
        event_types = set(result["event_type"])
        assert event_types == {"signup", "field_changed"}

        fc_row = result[result["event_type"] == "field_changed"].iloc[0]
        payload = json.loads(fc_row["payload"])
        assert payload["new_value"] == "a@c.com"


def test_incomplete_field_changed_payload_passes_through_unmatched() -> None:
    """NULL predicate results must not drop unmatched events."""
    raw = _make_raw_events(
        [
            {
                "source": "customers",
                "entity_id": "1",
                "ts": "2024-01-01",
                "event_type": "field_changed",
                "payload": json.dumps({"field_name": "plan"}),
            }
        ]
    )
    defns = [
        ActivityDefinition(
            name="became_paying",
            source="customers",
            trigger=FieldChanged(field="plan", to_values=["pro"]),
            entity_id_field="id",
        )
    ]

    result = apply_activity_definitions(raw, defns).execute()

    assert len(result) == 1
    assert result.iloc[0]["event_type"] == "field_changed"


def test_field_changed_values_preserve_embedded_quotes() -> None:
    raw = _make_raw_events(
        [
            {
                "source": "customers",
                "entity_id": "1",
                "ts": "2024-01-01",
                "event_type": "field_changed",
                "payload": json.dumps(
                    {
                        "field_name": "plan",
                        "old_value": "free",
                        "new_value": 'He said "pro"',
                    }
                ),
            }
        ]
    )
    defns = [
        ActivityDefinition(
            name="quoted_plan",
            source="customers",
            trigger=FieldChanged(field="plan", to_values=['He said "pro"']),
            entity_id_field="id",
        )
    ]

    result = apply_activity_definitions(raw, defns).execute()

    assert len(result) == 1
    assert result.iloc[0]["event_type"] == "quoted_plan"


class TestFieldChangedTrigger:
    def test_matches_field_changed_by_field_name(self):
        raw = _make_raw_events(
            [
                {
                    "source": "customers",
                    "entity_id": "1",
                    "ts": "2024-01-02",
                    "event_type": "field_changed",
                    "payload": json.dumps(
                        {"field_name": "plan", "old_value": "free", "new_value": "pro"}
                    ),
                },
                {
                    "source": "customers",
                    "entity_id": "1",
                    "ts": "2024-01-02",
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

        defns = [
            ActivityDefinition(
                name="plan_changed",
                source="customers",
                trigger=FieldChanged(field="plan"),
                entity_id_field="id",
            )
        ]

        result = apply_activity_definitions(raw, defns).execute()
        matched = result[result["event_type"] == "plan_changed"]
        assert len(matched) == 1

    def test_to_values_filter(self):
        raw = _make_raw_events(
            [
                {
                    "source": "customers",
                    "entity_id": "1",
                    "ts": "2024-01-02",
                    "event_type": "field_changed",
                    "payload": json.dumps(
                        {"field_name": "plan", "old_value": "free", "new_value": "pro"}
                    ),
                },
                {
                    "source": "customers",
                    "entity_id": "2",
                    "ts": "2024-01-02",
                    "event_type": "field_changed",
                    "payload": json.dumps(
                        {
                            "field_name": "plan",
                            "old_value": "pro",
                            "new_value": "enterprise",
                        }
                    ),
                },
            ]
        )

        defns = [
            ActivityDefinition(
                name="became_pro",
                source="customers",
                trigger=FieldChanged(field="plan", to_values=["pro"]),
                entity_id_field="id",
            )
        ]

        result = apply_activity_definitions(raw, defns).execute()
        matched = result[result["event_type"] == "became_pro"]
        assert len(matched) == 1
        assert matched.iloc[0]["entity_id"] == "1"

    def test_from_values_filter(self):
        raw = _make_raw_events(
            [
                {
                    "source": "customers",
                    "entity_id": "1",
                    "ts": "2024-01-02",
                    "event_type": "field_changed",
                    "payload": json.dumps(
                        {"field_name": "plan", "old_value": "free", "new_value": "pro"}
                    ),
                },
                {
                    "source": "customers",
                    "entity_id": "2",
                    "ts": "2024-01-02",
                    "event_type": "field_changed",
                    "payload": json.dumps(
                        {
                            "field_name": "plan",
                            "old_value": "trial",
                            "new_value": "pro",
                        }
                    ),
                },
            ]
        )

        defns = [
            ActivityDefinition(
                name="converted_from_free",
                source="customers",
                trigger=FieldChanged(field="plan", from_values=["free"]),
                entity_id_field="id",
            )
        ]

        result = apply_activity_definitions(raw, defns).execute()
        matched = result[result["event_type"] == "converted_from_free"]
        assert len(matched) == 1
        assert matched.iloc[0]["entity_id"] == "1"

    def test_from_and_to_values_combined(self):
        raw = _make_raw_events(
            [
                {
                    "source": "customers",
                    "entity_id": "1",
                    "ts": "2024-01-02",
                    "event_type": "field_changed",
                    "payload": json.dumps(
                        {"field_name": "plan", "old_value": "free", "new_value": "pro"}
                    ),
                },
                {
                    "source": "customers",
                    "entity_id": "2",
                    "ts": "2024-01-02",
                    "event_type": "field_changed",
                    "payload": json.dumps(
                        {
                            "field_name": "plan",
                            "old_value": "trial",
                            "new_value": "pro",
                        }
                    ),
                },
            ]
        )

        defns = [
            ActivityDefinition(
                name="free_to_pro",
                source="customers",
                trigger=FieldChanged(
                    field="plan", from_values=["free"], to_values=["pro"]
                ),
                entity_id_field="id",
            )
        ]

        result = apply_activity_definitions(raw, defns).execute()
        matched = result[result["event_type"] == "free_to_pro"]
        assert len(matched) == 1
        assert matched.iloc[0]["entity_id"] == "1"


class TestRowDisappearedTrigger:
    def test_matches_row_disappeared_events(self):
        raw = _make_raw_events(
            [
                {
                    "source": "customers",
                    "entity_id": "1",
                    "ts": "2024-01-03",
                    "event_type": "row_disappeared",
                    "payload": json.dumps({"name": "alice", "plan": "pro"}),
                },
                {
                    "source": "customers",
                    "entity_id": "2",
                    "ts": "2024-01-03",
                    "event_type": "row_appeared",
                    "payload": json.dumps({"name": "bob", "plan": "free"}),
                },
            ]
        )

        defns = [
            ActivityDefinition(
                name="churned",
                source="customers",
                trigger=RowDisappeared(),
                entity_id_field="id",
            )
        ]

        result = apply_activity_definitions(raw, defns).execute()
        matched = result[result["event_type"] == "churned"]
        assert len(matched) == 1
        assert matched.iloc[0]["entity_id"] == "1"


class TestEventOccurredTrigger:
    def test_matches_all_events_from_source(self):
        raw = _make_raw_events(
            [
                {
                    "source": "page_events",
                    "entity_id": "user1",
                    "ts": "2024-01-01",
                    "event_type": "page_view",
                    "payload": json.dumps({"url": "/home"}),
                },
                {
                    "source": "page_events",
                    "entity_id": "user1",
                    "ts": "2024-01-01",
                    "event_type": "click",
                    "payload": json.dumps({"element": "button"}),
                },
                {
                    "source": "other_source",
                    "entity_id": "user2",
                    "ts": "2024-01-01",
                    "event_type": "page_view",
                    "payload": json.dumps({"url": "/other"}),
                },
            ]
        )

        defns = [
            ActivityDefinition(
                name="web_interaction",
                source="page_events",
                trigger=EventOccurred(),
                entity_id_field="user_id",
            )
        ]

        result = apply_activity_definitions(raw, defns).execute()
        matched = result[result["event_type"] == "web_interaction"]
        assert len(matched) == 2
        assert all(r["source"] == "page_events" for _, r in matched.iterrows())

    def test_with_event_type_filter(self):
        raw = _make_raw_events(
            [
                {
                    "source": "page_events",
                    "entity_id": "user1",
                    "ts": "2024-01-01",
                    "event_type": "page_view",
                    "payload": json.dumps({"url": "/home"}),
                },
                {
                    "source": "page_events",
                    "entity_id": "user1",
                    "ts": "2024-01-01",
                    "event_type": "click",
                    "payload": json.dumps({"element": "button"}),
                },
            ]
        )

        defns = [
            ActivityDefinition(
                name="viewed_page",
                source="page_events",
                trigger=EventOccurred(event_type="page_view"),
                entity_id_field="user_id",
            )
        ]

        result = apply_activity_definitions(raw, defns).execute()
        matched = result[result["event_type"] == "viewed_page"]
        assert len(matched) == 1


class TestOutputEventFormat:
    def test_event_type_replaced_with_definition_name(self):
        raw = _make_raw_events(
            [
                {
                    "source": "customers",
                    "entity_id": "1",
                    "ts": "2024-01-01",
                    "event_type": "row_appeared",
                    "payload": json.dumps({"name": "alice"}),
                },
            ]
        )

        defns = [
            ActivityDefinition(
                name="signup",
                source="customers",
                trigger=RowAppeared(),
                entity_id_field="id",
            )
        ]

        result = apply_activity_definitions(raw, defns).execute()
        assert result.iloc[0]["event_type"] == "signup"

    def test_include_fields_filters_payload(self):
        raw = _make_raw_events(
            [
                {
                    "source": "customers",
                    "entity_id": "1",
                    "ts": "2024-01-01",
                    "event_type": "row_appeared",
                    "payload": json.dumps(
                        {"name": "alice", "plan": "free", "email": "a@b.com"}
                    ),
                },
            ]
        )

        defns = [
            ActivityDefinition(
                name="signup",
                source="customers",
                trigger=RowAppeared(),
                entity_id_field="id",
                include_fields=["name", "plan"],
            )
        ]

        result = apply_activity_definitions(raw, defns).execute()
        payload = json.loads(result.iloc[0]["payload"])
        assert set(payload.keys()) == {"name", "plan"}

    def test_empty_result_returns_correct_schema(self):
        raw = _make_raw_events([])

        defns = [
            ActivityDefinition(
                name="signup",
                source="customers",
                trigger=RowAppeared(),
                entity_id_field="id",
            )
        ]

        result = apply_activity_definitions(raw, defns)
        cols = result.columns
        assert set(cols) == {"source", "entity_id", "ts", "event_type", "payload"}

    def test_multiple_definitions_produce_union(self):
        raw = _make_raw_events(
            [
                {
                    "source": "customers",
                    "entity_id": "1",
                    "ts": "2024-01-01",
                    "event_type": "row_appeared",
                    "payload": json.dumps({"name": "alice", "plan": "free"}),
                },
                {
                    "source": "customers",
                    "entity_id": "1",
                    "ts": "2024-01-02",
                    "event_type": "field_changed",
                    "payload": json.dumps(
                        {"field_name": "plan", "old_value": "free", "new_value": "pro"}
                    ),
                },
            ]
        )

        defns = [
            ActivityDefinition(
                name="signup",
                source="customers",
                trigger=RowAppeared(),
                entity_id_field="id",
            ),
            ActivityDefinition(
                name="became_paying",
                source="customers",
                trigger=FieldChanged(field="plan", to_values=["pro"]),
                entity_id_field="id",
            ),
        ]

        result = apply_activity_definitions(raw, defns).execute()
        assert len(result) == 2
        event_types = set(result["event_type"])
        assert event_types == {"signup", "became_paying"}
