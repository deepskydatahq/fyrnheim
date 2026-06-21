---
name: product-story-execution
description: Implements Fyrnheim product stories directly from product/stories TOML files without Beads. Use when selecting, claiming, implementing, testing, and completing a story.
---

# Product Story Execution

Stories are the canonical implementation tasks. Do not create Beads tasks.

## Status model

Use story `status` values:

- `draft` — not ready for implementation
- `ready` — ready and unclaimed
- `in_progress` — currently being implemented
- `blocked` — cannot proceed without a decision or prerequisite
- `complete` — implemented, tested, and committed
- `failed` — attempted but not completed; include a failure reason

Use story `triage` values:

- `ready` — implement directly
- `plan` — explore relevant files, make a short plan, then implement
- `brainstorm` — compare 2-3 approaches, pick the simplest, then plan and implement

## Implementing a story

1. Read `HOW_WE_WORK.md`, `CLAUDE.md`, the story TOML, parent epic, and parent mission.
2. Verify dependencies in `[context].depends_on` are complete.
3. Set the story to `status = "in_progress"`.
4. Read optional `[execution_strategy]` from the story, then the parent mission. If absent, use `mode = "single_agent"`.
5. Follow the triage path:
   - `ready`: implement directly.
   - `plan`: inspect relevant paths and write a concise plan in your response before editing.
   - `brainstorm`: compare approaches briefly and choose the simplest.
6. Follow the execution strategy:
   - `single_agent`: implement production code and tests in this session.
   - `recommend_multi_model`: use the multi-model workflow when multiple approaches or model comparisons would materially reduce risk; if you downgrade to single-agent, document why in the story execution metadata or final report.
   - `multi_model`: use `scripts/multi_model_coding.py prepare`, candidate execution, `check-artifacts`, evaluator `judge`, promotion if a candidate wins, and `cleanup` before marking the story complete.
7. Implement production code and tests for each acceptance criterion, or promote the accepted/synthesized candidate output.
8. Run focused tests, then relevant quality gates:
   - `uv run pytest`
   - `uv run ruff check src/ tests/`
   - `uv run mypy src/`
9. Commit the story with a descriptive message.
10. Update the story to `status = "complete"` and record useful execution metadata if fields exist:
   - branch
   - commit
   - PR URL if known
   - completed date

## Multi-model completion requirements

A story using `mode = "multi_model"` is complete only when the story or handoff links:

- `.pi/coding-runs/<run-id>/run.toml`
- candidate `candidate.toml` files and artifact directories
- candidate `execution.toml`, `status.toml`, `notes.md`, `quality-gates.txt`, and `diff.patch` where applicable
- `.pi/coding-runs/<run-id>/judgement.toml`
- selected or synthesized outcome, or `all_failed` rationale
- provider blockers such as missing credentials
- cleanup dry-run and cleanup output, or an explicit no-worktree/dry-run rationale

Do not silently skip multi-model execution because a provider is unavailable. Record the provider blocker as a candidate failure or story blocker and decide whether a dry-run/fallback candidate satisfies the story acceptance criteria.

## If blocked or failed

If implementation cannot continue:

1. Set `status = "blocked"` or `status = "failed"`.
2. Add or update `[execution].failure_reason` with a specific explanation.
3. Create or update follow-up product story TOML if more work is needed.
4. Do not mark the story complete.

## Completion rule

A story is complete only when:

- all acceptance criteria are satisfied
- tests were added or updated where appropriate
- quality gates pass, or failures are documented as out of scope
- changes are committed
