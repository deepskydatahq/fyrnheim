# ADR-0001: Preserve unmatched events in `apply_activity_definitions`

- **Status:** Accepted
- **Date:** 2026-04-09
- **Deciders:** Fyrnheim maintainers
- **Mission:** M052
- **Supersedes:** none
- **Superseded by:** none
- **Implementation mission:** M053 (to be filed)
- **Target release:** v0.5.0

## Context

### The current behavior

`apply_activity_definitions` in `src/fyrnheim/engine/activity_engine.py` (lines 23-61) runs as Phase 2 of the Fyrnheim pipeline, between raw event production (Phase 1 — `SnapshotDiff` + EventSource loading) and identity resolution (Phase 3). Its current contract is:

> For each raw event, check it against every `ActivityDefinition`. For each matching definition, emit a renamed copy (with the activity's `name` as `event_type`). **Drop every raw event that no definition matched.**

Concretely, the function builds `all_matched` from per-definition matches and returns only `ibis.memtable(pd.DataFrame(all_matched))` — the raw input is never unioned back in.

### Why this is a problem

The drop-unmatched contract creates a silent-wrong-answer failure mode for any project that mixes `ActivityDefinition`s with `StateField`s on the same source. Events from sources with no matching definition, or events from a defined source that didn't match the trigger, vanish before Phase 4 state-field projection ever sees them. The failure is:

- **Silent** — no error, no warning, no log line
- **Correlated with a natural modeling pattern** — "I want activities AND state fields on the same source" is not exotic; it's the default for any Activity Schema-style model
- **Only visible at the end of the pipeline** — the materialized parquet has stale or missing columns after several phases of apparently-correct computation

### Proof the current example is broken

`examples/entities/customers.py` — the repo's flagship example — reproduces this silently today:

- `crm_contacts` is a `StateSource` with `StateField`s on `email`, `name`, `plan`, and `first_seen`.
- Two `ActivityDefinition`s target this source: `signup` (`RowAppeared`) and `became_paying` (`FieldChanged` on `plan`).
- There is no activity definition for `FieldChanged` on `email` or `name`.

Trace:

1. Phase 1 produces `row_appeared` and `field_changed` events for every column change.
2. Phase 2 keeps only events matching `signup` or `became_paying` and drops the rest — including every `field_changed` event for email, name, and first_seen.
3. Phase 4 `strategy="latest"` for `email` sees only the rewritten `signup` event (which carries the email value from snapshot-at-signup-time via its flat payload) and returns that.

Result: when a user updates their email after signup, the materialized `email` column silently shows the **original** email, not the latest. Same for `name`. `first_seen` is accidentally correct because "first" happens to equal "at row_appeared". `plan` is correct because `became_paying` happens to cover it.

The canonical documented example silently demonstrates a wrong-answer bug. Users following this example as a template get output that looks plausible and is subtly wrong in exactly the way the silent-failure mode is hardest to diagnose.

### Discovery context

Bug #94 (v0.4.2) fixed `_extract_field_value` to handle events with arbitrary `event_type` values, including activity-rewritten ones. That fix was triggered by client-flowable mission M006, where the user had added no-op passthrough `ActivityDefinition`s to work around the current drop contract. The passthrough idiom is itself a workaround for this contract. v0.4.2 makes the projection-side change needed to support any future decision here, but does not address the underlying Phase 2 semantics question — that's this ADR.

## Decision

**Change `apply_activity_definitions` to preserve unmatched events (the "d1" semantics described below), shipped as a breaking change in v0.5.0.**

The precise new rule:

> For each raw event in the input stream:
> - If one or more `ActivityDefinition`s match it, emit one renamed copy per matching definition (current behavior, unchanged).
> - If no `ActivityDefinition` matches it, emit the raw event unchanged (new).
>
> No raw event is ever silently dropped. A raw event matched by two definitions still produces two output rows. A raw event matched by zero definitions produces exactly one output row (itself).

## Alternatives considered

### Option A — DROP (keep current behavior)

Keep `apply_activity_definitions` dropping unmatched events; document the "add a passthrough activity" idiom as the recommended pattern for mixed projects.

- **Rejected because:** the workaround is invisible to new users until their state fields silently produce `None`. The flagship example would stay broken without an explicit per-field workaround per source, multiplying the cognitive tax. The v0.4.2 fix to `_extract_field_value` made the workaround functionally viable, but the cost of teaching every user about it is higher than the cost of a one-time semantic pivot.

### Option C — HYBRID (preserve by default, opt-in drop)

Preserve unmatched events by default; add a `drop_unmatched=True` flag (or a pipeline-level config) to restore the current behavior for projects that want a pure activity stream.

- **Rejected because:** no concrete use case surfaced for strict drop semantics. The client-flowable engagement, the canonical example, and every expected user pattern want preserve. Adding a flag now would reintroduce the complexity we're eliminating, double the test matrix, and lock us into supporting a mode nobody asked for. If a real drop-dependent use case emerges post-v0.5.0, the flag can be added as an additive change.

### Option d2 — Full union (preserve d selected; sub-option rejected)

Within the preserve decision, a full-union variant would emit BOTH the raw event AND each activity-renamed copy for matched events. A raw `row_appeared` matching a `signup` activity would produce two output rows: the raw `row_appeared` and the renamed `signup`.

- **Rejected because:** this would double-count matched events in measures that count "all events on source X", silently breaking existing correctness in the other direction. d1 preserves the existing matched-event counting contract while only adding the missing unmatched rows — the smallest possible semantic move.

## Consequences

### Bugs fixed

1. `examples/entities/customers.py` becomes correct without modification: state fields on `email`, `name`, `first_seen` now see the `field_changed` events for those columns, and `strategy="latest"` returns the actual latest value.
2. Mixed `ActivityDefinition` + `StateField` projects on the same source work with no workaround. The no-op passthrough idiom becomes unnecessary and should be removed from any project that adopted it.
3. The cognitive trap behind bug #94 is eliminated: users no longer reach for passthrough activities, no longer inadvertently rewrite `event_type`, and never hit the class of surprise that bug addressed.

### What changes observably (the breaking part)

1. **`apply_activity_definitions` output changes shape** for any project whose raw event stream contains events that no definition matches — which is essentially every mixed-source project including the flagship example. Output row count increases by the number of previously-dropped events; output `event_type` column now contains a mix of activity names and raw types (`row_appeared`, `field_changed`, arbitrary EventSource `event_type` values).

2. **Internal pipeline consumers are unaffected:**
   - Phase 3 (`resolve_identities`) handles mixed event_type streams post-v0.4.2 (#92 fix).
   - Phase 4 (`_extract_field_value`) handles flat-payload events with arbitrary `event_type` post-v0.4.2 (#94 fix).
   - Phase 5 (metrics models) filters by activity name, so mixed streams are ignored by non-matching measures.

3. **External (user) code reaching into the Phase 2 output** will see the new shape and must update if it was iterating the stream assuming only activity-named rows. No such code is known in the Fyrnheim codebase or any adopter project we've seen.

4. **One existing test must be updated**: `test_does_not_match_other_sources` currently asserts `len(result) == 0` when the raw input has one event on a source with no matching definition. Under d1, result has 1 row (the pass-through). The test must be updated to assert the new contract with a reference comment to this ADR.

### Migration story for v0.5.0 release notes

Three lines for users:

- **Project has only `ActivityDefinition`s, no state fields:** no action needed. Your activities still fire the same way.
- **Project uses state fields on sources that also have activity definitions:** remove any no-op passthrough activities you added as a workaround. Your state fields will now see all events automatically.
- **Project relies on `apply_activity_definitions` dropping unmatched events for downstream correctness:** add an explicit filter on the output (`events.filter(event_type.isin([...activity_names...]))`).

### Scope this ADR does NOT change

- The `AnalyticsEntity`, `StateField`, or `Measure` APIs
- The shape of `ActivityDefinition` or any trigger type
- `resolve_identities` or `_extract_field_value` (already correct post-v0.4.2)
- Phase 1, 3, 4, or 5 pipeline wiring
- The `_apply_single_definition` helper (per-definition matching is unchanged)

The change is localized to the outer loop of `apply_activity_definitions` plus a docstring rewrite.

## Implementation notes (high level, not a plan)

To be detailed in M053. Sketch:

1. In `apply_activity_definitions`, track which raw event indices were matched by any definition during the per-definition loop. After the loop, append the raw events whose index was never touched (ordering within groups doesn't matter — downstream doesn't care).
2. Preserve the empty-input case (return the empty-schema memtable).
3. Rewrite the function's docstring to state the new contract: "activities annotate where they apply; unmatched events pass through unchanged."
4. No changes to `_apply_single_definition`, `_match_field_changed`, `_match_event_occurred`, `_filter_payload`, or `pipeline.py`.

### Testing strategy

New tests in `tests/test_activity_engine.py`:

- `test_unmatched_events_pass_through` — 3 raw, 1 matches, 2 don't → output is 1 renamed + 2 raw.
- `test_unmatched_events_preserve_original_event_type` — pass-through rows keep their original `event_type` string (`row_appeared`, `field_changed`, arbitrary EventSource types).
- `test_unmatched_events_preserve_original_payload` — pass-through payload is byte-identical to the input (not filtered through `include_fields` on any definition).
- `test_mixed_matched_unmatched_in_same_source` — a single source has both a row matched by a definition and a row not matched; both appear correctly.

Updated test:

- `test_does_not_match_other_sources` — update assertion from `len(result) == 0` to `len(result) == 1` (the pass-through row); add a comment referencing this ADR.

End-to-end regression test (new integration test):

- `test_e2e_customers_example_state_fields_stay_fresh` — mirrors `customers.py`: a `crm_contacts` StateSource, a `row_appeared` event, a subsequent `field_changed` event on `email`, a `signup` activity on `RowAppeared`, and a `StateField` on `email` with `strategy="latest"`. Assert the resolved email equals the post-`field_changed` value. This test would fail on pre-v0.5.0 (silent wrong answer) and pass after the change — the regression guard for the bug this ADR fixes.

### Explicitly out of scope

- No opt-in flags (`drop_unmatched=...`). The decision is a hard pivot.
- No changes to `ActivityDefinition` shape or new primitive types.
- No backport to v0.4.x. Release is v0.5.0, forward-only.

## Follow-up implementation mission

M053 — Implement ADR-0001: preserve unmatched events in Phase 2 (v0.5.0). Single epic, single story, one PR. Deliverables:

1. Engine change (~30 lines) + docstring rewrite
2. 4 new unit tests + 1 updated unit test + 1 e2e regression test (~150 lines)
3. Version bump 0.4.2 → 0.5.0, `uv lock`
4. Release notes entry titled "Breaking: activity definitions now preserve unmatched events" with the three-line migration guide above
5. CHANGELOG / release notes link back to this ADR

## References

- **Bug report that surfaced the cognitive trap:** deepskydatahq/fyrnheim#94 (fixed in v0.4.2, Related section flagged this contract question as the root cause)
- **v0.4.2 projection fixes that unblock this decision:** deepskydatahq/fyrnheim#91, #92, #93, #94 (merged in #95, release v0.4.2)
- **Current implementation:** `src/fyrnheim/engine/activity_engine.py:23-61`
- **Flagship example demonstrating the bug:** `examples/entities/customers.py`
- **Mission TOML:** `product/missions/M052-activity-definitions-drop-vs-preserve.toml`
