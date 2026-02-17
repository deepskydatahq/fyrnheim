---
description: Break an epic into implementable stories using the product-epic-breakdown skill
allowed-tools: Bash(git:*), Read, Write, Glob, Grep, Skill
---

# Product Epic Breakdown

Break a product epic into implementable stories with acceptance criteria.

## Arguments

- Epic ID (e.g., `/product-epic-breakdown M001-E001`)

## Instructions

Invoke the `product-epic-breakdown` skill with the provided epic ID.

The skill will:
1. Read the epic TOML file from `product/epics/`
2. Read the parent mission for context
3. Analyze existing code patterns
4. Create story TOML files in `product/stories/` with acceptance criteria
5. Report implementation order and any stories needing refinement

## After Running

- Review story acceptance criteria for specificity
- Set story status to `ready` when criteria are complete
- Run `/product-story-handoff` to create Beads tasks from ready stories
