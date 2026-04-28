# Fyrnheim Pi Setup

This directory contains project-local Pi resources for the Fyrnheim workflow.

## Prompts

Prompt templates in `.pi/prompts/` become slash commands:

- `/plan-mission <idea>` — explore code and create a mission TOML
- `/product-mission-breakdown <Mxxx>` — create epics from a mission
- `/product-epic-breakdown <Mxxx-Exxx>` — create stories from an epic
- `/execute-mission <Mxxx>` — execute a mission using its `[execution].mode`
- `/product-judgment <artifact>` — validate story/epic/mission outcomes
- `/fix-pr-feedback <pr>` — fix actionable PR review feedback
- `/retro` — discover follow-up product stories
- `/product-values` — review/update the value ladder
- `/product-vision` — review/update the vision

## Skills

Skills in `.pi/skills/` provide on-demand workflow instructions:

- `product-mission-breakdown`
- `product-epic-breakdown`
- `product-story-execution`
- `product-judgment`
- `fyrnheim-testing`

## Extension

`.pi/extensions/fyrnheim-workflow/index.ts` adds project commands:

- `/fyrnheim-status`
- `/mission-status <Mxxx>`
- `/story-list [status]`
- `/story-set-status <story-id> <status>`
- `/quality-gates`
- `/pr-status [pr-number]`
- `/pr-merge-if-ready [pr-number]`

It also reminds the model that product TOML files are the canonical task system and Beads should not be used unless explicitly requested. When docs conflict, use: mission/product TOML workflow rules > `AGENTS.md` > `HOW_WE_WORK.md` > `CLAUDE.md`/legacy docs.

## Task Model

Stories in `product/stories/*.toml` are implementation tasks. Use:

- `status = "draft" | "ready" | "in_progress" | "blocked" | "complete" | "failed"`
- `triage = "ready" | "plan" | "brainstorm"`

## Mission Modes

Missions can include:

```toml
[execution]
mode = "implementation"  # implementation | audit | planning | docs | release
outputs = ["code", "stories"]
```

Audit/planning missions should produce audit docs, decision records, or follow-up mission TOMLs. They do not need implementation stories unless the mission explicitly asks for them.

## Quality Gates

Use:

```bash
scripts/quality-gates.sh
```

The script runs pytest, ruff, and mypy with this fallback order:

1. `uv run ...` when `uv` is available
2. `.venv/bin/...` when the project virtualenv is available
3. plain `python`/`ruff`/`mypy` from `PATH`

## PR Review / Merge

Use `/pr-status [pr-number]` to inspect CI, CodeRabbit, mergeability, reviews, and comments.

Use `/pr-merge-if-ready [pr-number]` only when explicitly asked to merge. It refuses to merge when checks are pending/failing or actionable CodeRabbit comments remain.

See `HOW_WE_WORK.md` for the full workflow.
