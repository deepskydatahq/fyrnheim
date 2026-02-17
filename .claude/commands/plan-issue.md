---
description: Pick a task needing planning and write an implementation plan
allowed-tools: Bash(hte tasks:*), Skill, Read, Write, Glob, Grep
---

# Plan Issue

Pick a task from the `plan` queue and write a detailed implementation plan.

## Arguments

- No argument: Pick from queue
- Task ID (e.g., `/plan-issue 01KFJ4YM...`): Plan specific task regardless of status

## Current Tasks Needing Planning

!`hte tasks list --status plan --json`

## Instructions

### 1. Select Task

**If argument provided:**
- Use that task ID directly
- Fetch details: `hte tasks get <id>`

**If no argument:**
- If no tasks with `plan` status: Report "No tasks need planning. Run `/brainstorm` to process brainstorming queue, or `/new-feature` to create tasks." and stop.
- Otherwise, pick the best task based on:
  - Skip tasks with `in_progress` status
  - Age: older tasks first

### 2. Claim the Task

```bash
hte tasks update <id> --status in_progress
```

### 3. Fetch Full Context

```bash
hte tasks get <id>
```

Read these files to understand the project (if they exist):
- README.md - Project overview and setup
- VISION.md - Product direction and goals

Review:
- Task description and requirements
- Any design decisions from brainstorming
- Linked design documents in `docs/plans/`
- Scope and constraints

### 4. Write Implementation Plan

You are planning implementation for this task. Create a plan with:

**1. Summary** - What this issue accomplishes (2-3 sentences)

**2. Acceptance Criteria** - Agent-verifiable conditions for completion

**Write acceptance criteria that are verifiable:**

❌ Bad: "All tests pass"
✅ Good: "`npm test` exits with code 0 and outputs 'X passing'"

❌ Bad: "Config file is updated"
✅ Good: "`src/config/settings.json` contains `{maxConnections: 20}`"

❌ Bad: "Error handling works correctly"
✅ Good: "Clicking 'Submit' with empty form shows 'Email required' error message"

**3. Implementation Tasks** - Ordered steps to complete the work
- Identify all files that need changes
- Break work into specific, ordered steps
- Include file paths and what changes in each

**4. Test Plan** - How to verify each AC
- What tests to write or run
- What commands to execute
- Expected outcomes

### 5. Add Plan to Task

Update the task body to include the implementation plan:

```
## Implementation Plan

### Summary
<2-3 sentence summary of what this accomplishes>

### Acceptance Criteria
- [ ] <AC 1 - testable/file checkpoint/command output/behavioral spec>
- [ ] <AC 2>
- [ ] <AC 3>

### Implementation Tasks

1. **<Step title>**
   - File: `<path/to/file>`
   - Change: <what to do>

2. **<Step title>**
   - File: `<path/to/file>`
   - Change: <what to do>

### Test Plan
- [ ] <How to verify AC 1>
- [ ] <How to verify AC 2>
- [ ] <How to verify AC 3>

---
*Plan created via /plan-issue*
```

### 5.5 Validate Plan

**Before advancing to `ready` status, validate the plan quality.**

**Validation Rules:**

1. **Required Sections** - Plan must include all of:
   - Summary (2-3 sentences)
   - Acceptance Criteria (at least 1 AC)
   - Implementation Tasks (at least 1 task)
   - Test Plan (at least 1 test)

2. **AC Quality** - Each acceptance criterion must contain at least one of:
   - File path (e.g., `src/config/settings.json`)
   - Command with expected output (e.g., "`npm test` exits with code 0")
   - Specific behavioral description (e.g., "clicking X shows Y")

3. **Task Specificity** - Each implementation task must reference:
   - A file path (e.g., `File: src/utils/helper.ts`)

**Run Validation:**

Check your plan against these rules. For each rule, verify compliance.

**If Validation Fails:**

Update the task body with the validation failure details:

```
## Plan Validation Failed

The plan did not meet quality requirements:

- [ ] Required sections: <PASS/FAIL - list missing sections>
- [ ] AC quality: <PASS/FAIL - list ACs missing specificity>
- [ ] Task specificity: <PASS/FAIL - list tasks missing file paths>

Please update the plan to address these issues before moving to `ready` status.

---
*Validation performed via /plan-issue*
```

Move back to plan status:
```bash
hte tasks update <id> --status plan
```

Report: "Plan validation failed for task <id>. See task details." and stop.

**If Validation Passes:**

Proceed to Section 6 to move the task to `ready` status.

### 6. Move to Ready

```bash
hte tasks update <id> --status ready
```

## Output Format

```
Selected: <id> - <title>

[Planning session...]

Plan added to task with N steps.
Moved to: ready

Task <id> is now ready for implementation.
Run /pick-issue to start working on it.
```
