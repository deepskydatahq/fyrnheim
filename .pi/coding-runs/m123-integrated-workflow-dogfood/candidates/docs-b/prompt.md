# Candidate Coding Run: m123-integrated-workflow-dogfood / docs-b

You are implementing one candidate solution for Fyrnheim story `M123-E002-S001`.

## Variant

- Name: docs-b
- Model: local
- Build: dry-run

## Story

- Path: `product/stories/M123-E002-S001-dogfood-integrated-multi-model-strategy.toml`
- Title: Dogfood integrated multi-model strategy
- Triage: ready

### Outcome

Validate the multi-model execution strategy convention with a dry-run candidate workflow and preserved artifact links.

### Acceptance Criteria

- A run exists under `.pi/coding-runs/m123-integrated-workflow-dogfood` with two dry-run candidates.
- Candidate artifacts, judgement.toml, check-artifacts output, and cleanup evidence are preserved.
- Story execution metadata links the run directory and selected outcome.

## Isolation Rules

- Work only in this candidate worktree.
- Do not mutate sibling worktrees or the source checkout.
- Keep commits scoped to this candidate branch.
- Leave notes and command output under `/home/tmo/roadtothebeach/open/fyrnheim/.pi/coding-runs/m123-integrated-workflow-dogfood/candidates/docs-b/artifacts`.

## Required Artifacts

Write or update these files before handing off to the evaluator:

- `/home/tmo/roadtothebeach/open/fyrnheim/.pi/coding-runs/m123-integrated-workflow-dogfood/candidates/docs-b/artifacts/notes.md` — implementation summary, tradeoffs, risks.
- `/home/tmo/roadtothebeach/open/fyrnheim/.pi/coding-runs/m123-integrated-workflow-dogfood/candidates/docs-b/artifacts/quality-gates.txt` — focused tests and quality gate output.
- `/home/tmo/roadtothebeach/open/fyrnheim/.pi/coding-runs/m123-integrated-workflow-dogfood/candidates/docs-b/artifacts/status.toml` — final status such as `complete`, `failed`, or `blocked`.

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
