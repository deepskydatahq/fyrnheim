---
description: Pick a task needing brainstorming and run a design session
allowed-tools: Bash(hte tasks:*), Skill, Read, Write, Glob, Grep
---

# Brainstorm

Pick a task from the `brainstorm` queue and run a brainstorming session.

## Arguments

- No argument: Pick from queue
- Task ID (e.g., `/brainstorm 01KFJ4YM...`): Brainstorm specific task regardless of status

## Current Tasks Needing Brainstorming

!`hte tasks list --status brainstorm --json`

## Instructions

### 1. Select Task

**If argument provided:**
- Use that task ID directly
- Fetch details: `hte tasks get <id>`

**If no argument:**
- If no tasks with `brainstorm` status: Report "No tasks need brainstorming. Run `/new-feature` to create some." and stop.
- Otherwise, pick the best task based on:
  - Skip tasks with `in_progress` status
  - Age: older tasks first

### 2. Claim the Task

```bash
hte tasks update <id> --status in_progress
```

### 3. Run Brainstorming Session

Invoke the `superpowers:brainstorming` skill with the issue context:

- Explore relevant code and patterns
- Ask questions one-by-one to understand requirements
- Propose 2-3 approaches with trade-offs
- Lead with your recommended approach
- Present design in sections, validating each

### 4. Assess Output

After brainstorming completes, determine:

**Single coherent piece of work:**
- Update the original task with design decisions
- Assess next status (see criteria below)
- Move to that status

**Multiple independent pieces:**
- Create child tasks for each piece
- Each child gets its own status assessment
- Mark original as done with links to children

### 5. Status Assessment Criteria

For each piece of work (original or child), assess:

```
brainstorm if ANY of:
  - Still has unresolved design questions
  - Needs further user input on approach
  - Affects architecture and needs more thought

plan if:
  - Design decisions are made
  - Solution is known but involves multiple files/steps
  - Ready for detailed implementation planning

ready if ALL of:
  - Trivial, mechanical change
  - Specific file and location known
  - No risk of unintended consequences
  (This should be RARE - prefer plan)
```

### 6. Save Design Document (if substantial)

For non-trivial design work, save to: `docs/plans/YYYY-MM-DD-<feature-slug>-design.md`

Document structure:
```markdown
# <Feature Name> Design

## Overview
<2-3 sentence summary>

## Problem Statement
<What problem does this solve?>

## Proposed Solution
<High-level approach>

## Design Details
<Architecture, components, data flow>

## Alternatives Considered
<Other approaches and why not chosen>

## Open Questions
<Any unresolved decisions>

## Success Criteria
<How do we know it works?>
```

### 6a. Validate Before Status Advancement

Before advancing to any status beyond brainstorm, verify brainstorming work is documented:

1. **Check for brainstorm output** in task body:
   - Look for "Brainstorming Complete" or "Auto-Brainstorming Complete" marker
   - OR design document saved to `docs/plans/YYYY-MM-DD-*-design.md`

2. **If validation fails:**
   - Report: "Cannot advance task <id>: no brainstorm output documented. Complete the brainstorming session first."
   - Move back to brainstorm status and stop
   - Do NOT proceed with status transition

3. **If validation passes:** Continue to next section

### 7. Create Child Tasks (if breaking down)

For each child task:

```bash
hte tasks create --title "<Child title>" --status <status> --data '{"body":"## Summary\n<what this piece does>\n\n## Context\nBroken out from task <parent-id> during brainstorming.\n\n## Design Decisions\n- <relevant decisions from brainstorm session>\n\n## Scope\n- <specific files/components involved>\n\n---\n*Created via /brainstorm from task <parent-id>*"}'
```

### 8. Mark Original Done (if broken down)

```bash
hte tasks update <id> --status done
```

Update the task body to document the breakdown:
```
Broken down into:
- <child1-id> - <title> (status: <status>)
- <child2-id> - <title> (status: <status>)
- <child3-id> - <title> (status: <status>)

Design exploration complete.
```

### 9. Update Original (if not broken down)

```bash
hte tasks update <id> --status <next-status>
```

Update the task body with brainstorming results:
```
## Brainstorming Complete

### Design Decisions
- <key decision 1>
- <key decision 2>

### Approach
<summary of chosen approach>

### Next Steps
<what the implementation plan should cover>

---
*Updated via /brainstorm*
```

## Output Format

```
Selected: <id> - <title>

[Brainstorming session...]

Outcome: <Single piece | Broken into N pieces>

<If single:>
Moving to: <next-status>
Task updated with design decisions.

<If broken down:>
Created:
- <child1-id> - <title> (status: <status>)
- <child2-id> - <title> (status: <status>)
Original task <id> marked done.
```
