# Candidate Coding Run: m113-openrouter-dogfood / fallback

You are implementing one candidate solution for Fyrnheim story `M113-E001-S001`.

## Variant

- Name: fallback
- Model: openai-codex
- Build: gpt-5.5-high

## Story

- Path: `product/stories/M113-E001-S001-openrouter-provider-docs-dogfood.toml`
- Title: OpenRouter provider docs dogfood
- Triage: plan

### Outcome

Use provider-backed candidate execution to produce and evaluate a tiny documentation improvement about OpenRouter candidate selection and provider command behavior.

### Acceptance Criteria

- Candidate selection notes document qwen/openrouter-capped and fallback model choices from Propel Explorer configuration.
- A run under .pi/coding-runs contains qwen and fallback candidates with execution artifacts from run-candidates.
- check-artifacts is run and its output is preserved.
- judgement.toml explains provider success/blockers, quality, cost/token observations, and selected outcome.
- docs/multi-model-coding-workflow.md receives a small promoted clarification if a candidate/fallback output is usable.
- cleanup removes candidate worktrees or documents an exact blocker.

## Isolation Rules

- Work only in this candidate worktree.
- Do not mutate sibling worktrees or the source checkout.
- Keep commits scoped to this candidate branch.
- Leave notes and command output under `/home/tmo/roadtothebeach/open/fyrnheim/.pi/coding-runs/m113-openrouter-dogfood/candidates/fallback/artifacts`.

## Required Artifacts

Write or update these files before handing off to the evaluator:

- `/home/tmo/roadtothebeach/open/fyrnheim/.pi/coding-runs/m113-openrouter-dogfood/candidates/fallback/artifacts/notes.md` — implementation summary, tradeoffs, risks.
- `/home/tmo/roadtothebeach/open/fyrnheim/.pi/coding-runs/m113-openrouter-dogfood/candidates/fallback/artifacts/quality-gates.txt` — focused tests and quality gate output.
- `/home/tmo/roadtothebeach/open/fyrnheim/.pi/coding-runs/m113-openrouter-dogfood/candidates/fallback/artifacts/status.toml` — final status such as `complete`, `failed`, or `blocked`.

## Quality Gates

Run the normal gates before declaring the candidate complete:

```bash
uv run pytest
uv run ruff check src/ tests/
uv run mypy src/
```

## Product Workflow

Product TOML remains the source of truth. Do not mark the shared story complete from
inside this candidate unless this candidate has been selected and promoted by the
evaluator. Commit and push the candidate branch when finished.
