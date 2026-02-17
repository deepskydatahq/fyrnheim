---
description: Break a hypothesis into an epic with child tasks for testing
allowed-tools: Bash(git:*), Bash(hte tasks:*), Skill, Read, Write, Glob, Grep
---

# Product Epic

Break a hypothesis into an epic with child tasks for testing.

## Purpose

Bridges strategy → tactics. Takes a hypothesis from HYPOTHESES.md and creates:
- One epic task (the hypothesis itself)
- Multiple child tasks (work needed to test it)

## When to Use

- After creating/updating HYPOTHESES.md
- When ready to start testing a hypothesis
- When current epic is complete and you need the next one

## Prerequisites

- HYPOTHESES.md must exist with at least one untested (🟡) hypothesis

## Instructions

### 1. Select Hypothesis

1. Read HYPOTHESES.md
2. Filter to 🟡 Untested hypotheses
3. Score by priority:
   - **Impact:** How much does this serve the transformation?
   - **Uncertainty:** How unsure are we? (higher = more valuable to test)
   - **Effort:** How hard to test?
4. Present top choice to user with reasoning
5. Confirm before proceeding (or let user pick different one)

### 2. Brainstorm Work Items

Invoke `superpowers:brainstorming` skill to identify work needed to test the hypothesis.

Consider:
- What do we need to **prepare** before testing?
- What do we need to **build or change** to run the test?
- How will we **run** the test?
- How will we **measure** results?
- How will we **analyze** and document learnings?

Aim for 3-7 concrete work items. Each should be:
- Small enough to complete in a focused session
- Clear about what "done" looks like
- Necessary to test the hypothesis (no nice-to-haves)

### 3. Create Epic Task

```bash
hte tasks create --title "Epic: Test [H#] - [Hypothesis Name]" --status brainstorm --data '{"body":"## Hypothesis\n\n**Belief:** [From HYPOTHESES.md]\n\n**Test:** [From HYPOTHESES.md]\n\n**Investment Area:** [From HYPOTHESES.md]\n\n---\n\n## Child Tasks\n\n(Child tasks will be linked here after creation)\n\n---\n\n*Created via /product-epic*"}'
```

Note the epic task ID for the next step.

### 4. Create Child Tasks

For each work item identified in step 2:

```bash
hte tasks create --title "[Task title]" --status brainstorm --data '{"body":"## Context\n\nPart of epic task [EPIC_ID]: Test [H#] - [Hypothesis Name]\n\n## Goal\n\n[What this task accomplishes toward testing the hypothesis]\n\n## Done When\n\n[Clear completion criteria]\n\n---\n\n*Created via /product-epic*"}'
```

Collect all child task IDs.

### 5. Update Epic with Tasklist

Update the epic task body to include the child task links:

```
## Hypothesis

**Belief:** [From HYPOTHESES.md]

**Test:** [From HYPOTHESES.md]

**Investment Area:** [From HYPOTHESES.md]

---

## Child Tasks

- [ ] [CHILD_1_ID] - [Task 1 title]
- [ ] [CHILD_2_ID] - [Task 2 title]
- [ ] [CHILD_3_ID] - [Task 3 title]
...

---

*Created via /product-epic*
```

### 6. Update HYPOTHESES.md

Change the hypothesis status from 🟡 Untested to 🔵 Testing.

Add to Evidence section:
```markdown
**Evidence:**
- [Date]: Epic created (task [EPIC_ID]) with [N] child tasks
```

### 7. Commit Changes

```bash
git add HYPOTHESES.md
git commit -m "docs: start testing [H#] - [Hypothesis Name]"
```

## Pipeline Integration

After `/product-epic` completes, child tasks enter the normal pipeline:

```
/product-epic
    │
    ▼
brainstorm ──► plan ──► ready
     │            │        │
/brainstorm  /plan-issue  /pick-issue
```

When all child tasks are complete, run `/product-iteration` to:
- Analyze results
- Update hypothesis status (Validated/Invalidated)
- Document learnings
- Generate follow-up hypotheses if needed

## Example Output

**Epic:** `Epic: Test H2 - Interview Completion` (task 01KFJ4YM...)

**Children:**
- `Recruit 5 test users from target audience` (01KFJ4YN...) - brainstorm
- `Set up session recording for user tests` (01KFJ4YP...) - brainstorm
- `Create post-interview feedback questions` (01KFJ4YQ...) - brainstorm
- `Run test sessions and document observations` (01KFJ4YR...) - brainstorm
- `Analyze results and update hypothesis` (01KFJ4YS...) - brainstorm

**HYPOTHESES.md updated:** H2 status changed to 🔵 Testing
