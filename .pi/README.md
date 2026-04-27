# Fyrnheim Pi Setup

This directory contains project-local Pi resources for the Fyrnheim workflow.

## Prompts

Prompt templates in `.pi/prompts/` become slash commands:

- `/plan-mission <idea>` — explore code and create a mission TOML
- `/product-mission-breakdown <Mxxx>` — create epics from a mission
- `/product-epic-breakdown <Mxxx-Exxx>` — create stories from an epic
- `/execute-mission <Mxxx>` — execute a mission using stories as tasks
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

It also reminds the model that product TOML files are the canonical task system and Beads should not be used unless explicitly requested.

## Task Model

Stories in `product/stories/*.toml` are implementation tasks. Use:

- `status = "draft" | "ready" | "in_progress" | "blocked" | "complete" | "failed"`
- `triage = "ready" | "plan" | "brainstorm"`

See `HOW_WE_WORK.md` for the full workflow.
