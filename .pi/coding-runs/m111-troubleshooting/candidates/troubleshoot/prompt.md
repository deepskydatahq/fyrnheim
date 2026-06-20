# Candidate Coding Run: m111-troubleshooting / troubleshoot

You are implementing one candidate solution for Fyrnheim story `M111-E001-S001`.

## Variant

- Name: troubleshoot
- Model: manual
- Build: troubleshooting

## Story

- Path: `product/stories/M111-E001-S001-dogfood-docs-troubleshooting.toml`
- Title: Dogfood docs troubleshooting
- Triage: ready

### Outcome

Use the M110 multi-model coding workflow to add troubleshooting guidance to the workflow docs and capture the dogfood judgement artifacts.

### Acceptance Criteria

- A multi-candidate run is prepared with two distinct variants for this story.
- Each candidate artifact directory contains notes.md, quality-gates.txt, and status.toml.
- judgement.toml records an evaluator decision with rationale and candidate dispositions.
- docs/multi-model-coding-workflow.md gains concise troubleshooting guidance informed by the candidate run.
- A retro note or follow-up story captures friction discovered while dogfooding.

## Isolation Rules

- Work only in this candidate worktree.
- Do not mutate sibling worktrees or the source checkout.
- Keep commits scoped to this candidate branch.
- Leave notes and command output under `/home/tmo/roadtothebeach/open/fyrnheim/.pi/coding-runs/m111-troubleshooting/candidates/troubleshoot/artifacts`.

## Required Artifacts

Write or update these files before handing off to the evaluator:

- `/home/tmo/roadtothebeach/open/fyrnheim/.pi/coding-runs/m111-troubleshooting/candidates/troubleshoot/artifacts/notes.md` — implementation summary, tradeoffs, risks.
- `/home/tmo/roadtothebeach/open/fyrnheim/.pi/coding-runs/m111-troubleshooting/candidates/troubleshoot/artifacts/quality-gates.txt` — focused tests and quality gate output.
- `/home/tmo/roadtothebeach/open/fyrnheim/.pi/coding-runs/m111-troubleshooting/candidates/troubleshoot/artifacts/status.toml` — final status such as `complete`, `failed`, or `blocked`.

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
