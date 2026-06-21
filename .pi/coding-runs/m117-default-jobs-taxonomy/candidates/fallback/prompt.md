# Candidate Coding Run: m117-default-jobs-taxonomy / fallback

You are implementing one candidate solution for Fyrnheim story `M117-E001-S001`.

## Variant

- Name: fallback
- Model: openai-codex
- Build: gpt-5.5-high

## Story

- Path: `product/stories/M117-E001-S001-add-default-jobs-taxonomy.toml`
- Title: Add default jobs taxonomy
- Triage: brainstorm

### Outcome

Implement a default jobs taxonomy for dbt model classification with docs and tests.

### Acceptance Criteria

- The classifier can load and apply default jobs taxonomy rules without a custom rules file.
- Classification output includes model-level evidence for default job labels and can flag layer/job mismatch evidence where configured.
- Docs describe each default job with description, signals, examples, anti-examples, suggested principles, and relation to common layer labels.

## Isolation Rules

- Work only in this candidate worktree.
- Do not mutate sibling worktrees or the source checkout.
- Keep commits scoped to this candidate branch.
- Leave notes and command output under `/home/tmo/roadtothebeach/open/fyrnheim/.pi/coding-runs/m117-default-jobs-taxonomy/candidates/fallback/artifacts`.

## Required Artifacts

Write or update these files before handing off to the evaluator:

- `/home/tmo/roadtothebeach/open/fyrnheim/.pi/coding-runs/m117-default-jobs-taxonomy/candidates/fallback/artifacts/notes.md` — implementation summary, tradeoffs, risks.
- `/home/tmo/roadtothebeach/open/fyrnheim/.pi/coding-runs/m117-default-jobs-taxonomy/candidates/fallback/artifacts/quality-gates.txt` — focused tests and quality gate output.
- `/home/tmo/roadtothebeach/open/fyrnheim/.pi/coding-runs/m117-default-jobs-taxonomy/candidates/fallback/artifacts/status.toml` — final status such as `complete`, `failed`, or `blocked`.

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
