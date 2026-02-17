---
description: Hand off ready stories to Beads tasks for implementation
allowed-tools: Bash(bd:*), Read, Write, Glob, Grep, Skill
---

# Product Story Handoff

Transform product stories into Beads tasks, bridging the product layer to implementation.

## Arguments

- Story ID: `/product-story-handoff M001-E001-S001` - Hand off specific story
- Epic flag: `/product-story-handoff --epic M001-E001` - Hand off all ready stories in epic
- Ready flag: `/product-story-handoff --ready` - Hand off all ready stories

## Instructions

Invoke the `product-story-handoff` skill with the provided arguments.

The skill will:
1. Validate story readiness (status = `ready`, dependencies met)
2. Gather context from mission -> epic -> story hierarchy
3. Create Beads task with rich context: `bd create "Title" --labels brainstorm -d "context"`
4. Update story status to `in-progress`

## After Running

- Stories are now Beads tasks with `brainstorm` label
- Run `/brainstorm` or `/brainstorm-auto` to design the implementation
- Or run `/plan-issue` if the design is already clear
