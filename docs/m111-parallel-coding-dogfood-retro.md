# M111 parallel coding dogfood retro

Date: 2026-06-20
Run: `.pi/coding-runs/m111-troubleshooting`
Story: `M111-E001-S001`

## What happened

We used the M110 workflow on a small docs improvement for `docs/multi-model-coding-workflow.md`.

Candidates:

- `troubleshoot=manual:troubleshooting` — added prose troubleshooting guidance for auth, missing artifacts, worktree cleanup, and synthesis decisions.
- `cleanup=manual:cleanup` — added a compact cleanup/recovery table with strong worktree and PR-auth recovery commands.
- `synthesis=manual:synthesis` — combined the stronger coverage from `troubleshoot` with the scannable recovery commands from `cleanup`.

Evaluator decision:

- `synthesize`, recorded in `.pi/coding-runs/m111-troubleshooting/judgement.toml`.
- Rationale: neither first-pass candidate was clearly best; the synthesis candidate produced the smallest complete promoted docs change.

## What worked

- `prepare` created separate candidate metadata, prompts, artifact directories, branches, and worktrees.
- The prompt gave enough instruction for manual/simulated candidate execution.
- Candidate artifacts were sufficient for evaluator comparison once `notes.md`, `quality-gates.txt`, `status.toml`, and `diff.patch` were filled.
- The synthesis path was useful and prevented blindly choosing between two partial docs approaches.

## Friction

- Artifact completeness is manual; the evaluator had to know which files to expect.
- `judgement.toml` records the synthesis decision, but there is no second built-in final-accept judgement after a synthesis candidate is promoted.
- Worktree cleanup is not yet part of the CLI.
- Candidate execution is still provider/manual; the script prepares work but does not launch agents.

## Follow-up filed

- `M110-E002-S002` — add a candidate artifact completeness/check command for evaluator readiness.
