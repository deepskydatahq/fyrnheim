---
description: Break a mission into an epic with child tasks for testing
allowed-tools: Bash(git:*), Bash(bd:*), Skill, Read, Write, Glob, Grep
---

# Product Epic

Break a mission into an epic with child tasks for implementation.

## Purpose

Bridges strategy -> tactics. Takes a mission from `product/missions/` and creates:
- One epic task (the mission outcome)
- Multiple child tasks (work needed to deliver it)

## When to Use

- After creating a new mission TOML
- When ready to start working on a mission
- When current epic is complete and you need the next one

## Prerequisites

- A mission TOML must exist in `product/missions/` with status "active" or "draft"

## Instructions

### 1. Select Mission

1. Read mission TOMLs in `product/missions/`
2. Filter to active/draft missions
3. Score by priority:
   - **Impact:** How much does this serve the transformation?
   - **Uncertainty:** How unsure are we? (higher = more valuable to test)
   - **Effort:** How hard to deliver?
4. Present top choice to user with reasoning
5. Confirm before proceeding (or let user pick different one)

### 2. Brainstorm Work Items

Invoke `superpowers:brainstorming` skill to identify work needed to deliver the mission outcome.

Consider:
- What do we need to **prepare** before building?
- What do we need to **build or change** to deliver the outcome?
- How will we **test** it works?
- How will we **measure** results?
- How will we **validate** against the mission's testing criteria?

Aim for 3-7 concrete work items. Each should be:
- Small enough to complete in a focused session
- Clear about what "done" looks like
- Necessary to deliver the mission outcome (no nice-to-haves)

### 3. Create Epic Task

```bash
bd create "Epic: [Mission ID] - [Mission Name]" --labels brainstorm -d "## Mission\n\n**Outcome:** [From mission TOML]\n\n**Testing Criteria:** [From mission TOML]\n\n---\n\n## Child Tasks\n\n(Child tasks will be linked here after creation)\n\n---\n\n*Created via /product-epic*"
```

Note the epic task ID for the next step.

### 4. Create Child Tasks

For each work item identified in step 2:

```bash
bd create "[Task title]" --labels brainstorm -d "## Context\n\nPart of epic task [EPIC_ID]: [Mission ID] - [Mission Name]\n\n## Goal\n\n[What this task accomplishes toward the mission outcome]\n\n## Done When\n\n[Clear completion criteria]\n\n---\n\n*Created via /product-epic*"
```

Collect all child task IDs.

### 5. Update Epic with Tasklist

Update the epic task body to include the child task links:

```
## Mission

**Outcome:** [From mission TOML]

**Testing Criteria:** [From mission TOML]

---

## Child Tasks

- [ ] [CHILD_1_ID] - [Task 1 title]
- [ ] [CHILD_2_ID] - [Task 2 title]
- [ ] [CHILD_3_ID] - [Task 3 title]
...

---

*Created via /product-epic*
```

### 6. Update Mission Status

If the mission was in draft status, update it to active:

```toml
status = "active"
```

### 7. Commit Changes

```bash
git add product/missions/
git commit -m "docs: start working on [Mission ID] - [Mission Name]"
```

## Pipeline Integration

After `/product-epic` completes, child tasks enter the normal pipeline:

```
/product-epic
    |
    v
brainstorm --> plan --> ready
     |            |        |
/brainstorm  /plan-issue  /pick-issue
```

When all child tasks are complete, run `/product-judgment` to validate the mission outcome.

## Example Output

**Epic:** `Epic: M009 - Refinement Loop` (task 01KFJ4YM...)

**Children:**
- `Design MCP update tool interface` (01KFJ4YN...) - brainstorm
- `Implement update_definition tool` (01KFJ4YP...) - brainstorm
- `Add cascading update logic` (01KFJ4YQ...) - brainstorm
- `Build confidence tracking system` (01KFJ4YR...) - brainstorm
- `Test refinement with real profile` (01KFJ4YS...) - brainstorm

**Mission updated:** M009 status changed to active
