# Skill: product-epic-breakdown

## Trigger

Command: `/product-epic-breakdown {epic_id}`

## Instructions

You are breaking down a product epic into implementable stories. Your goal is to create stories small enough to complete in a single implementation session, with specific testable acceptance criteria.

### Step 1: Read the Epic

Read the epic file at `product/epics/{epic_id}-*.toml`

Also read the parent mission for broader context.

Extract:
- The job story
- The outcome description
- The testing criteria
- The relevant paths

### Step 2: Analyze Implementation Scope

Review the files in `relevant_paths`:
- What code exists?
- What patterns are used?
- What needs to be created vs modified?

### Step 3: Identify Stories

Break the epic into implementation units. Each story should:
- Take roughly one implementation session
- Have a single clear purpose
- Be testable in isolation
- Result in working code (not just scaffolding)

Common story patterns:
- "Create [function/component] that [does X]"
- "Add [feature] to [existing code]"
- "Handle [edge case] in [function]"
- "Connect [A] to [B]"

### Step 4: Determine Dependencies

Map out the story sequence:
- What must be built first?
- What can be parallelized?
- What are the integration points?

### Step 5: Write Acceptance Criteria

For each story, write specific acceptance criteria.

**Make them executable when possible:**

Good:
```toml
[[acceptance_criteria.criteria]]
test = "unit"
description = "calculateMRR([]) returns 0"
```

Bad:
```toml
[[acceptance_criteria.criteria]]
test = "manual"
description = "MRR calculation works correctly"
```

**Cover the key cases:**
- Happy path (main functionality)
- Edge cases (empty inputs, boundaries)
- Error cases (invalid inputs)

### Step 6: Create Story Files

For each story, create a file at `product/stories/{epic_id}-S{NNN}-{slug}.toml`

Use the story template at `product/templates/story.template.toml`. Ensure:
- `id` follows the pattern `{epic_id}-S{NNN}`
- `parent` references the epic ID
- `acceptance_criteria.executable` is `true` when tests can be written
- `depends_on` lists any blocking stories

### Step 7: Verify Quality

Before finishing, verify:
- [ ] Stories collectively deliver the epic outcome
- [ ] Each story is implementable in one session
- [ ] Acceptance criteria are specific (not vague)
- [ ] Test types are appropriate (unit vs integration vs e2e)
- [ ] Dependencies form a valid DAG (no cycles)

### Output

Create the story TOML files and report:
- Number of stories created
- Suggested implementation order
- Any stories that need human refinement
- Questions or concerns

### Example

Input: `/product-epic-breakdown M001-E001`

Output:
```
Created 5 stories for epic M001-E001 (Core MRR Calculation Engine):

1. M001-E001-S001: Calculate base MRR from active subscriptions
   - 7 acceptance criteria (all unit tests)
   - No dependencies
   - Status: ready

2. M001-E001-S002: Categorize revenue by change type
   - 6 acceptance criteria (unit + integration)
   - Depends on: S001
   - Status: draft (needs fixture data defined)

3. M001-E001-S003: Handle mid-month subscription changes
   - 5 acceptance criteria (unit tests)
   - Depends on: S001
   - Status: ready

4. M001-E001-S004: Process annual subscription conversions
   - 4 acceptance criteria (unit tests)
   - Depends on: S001
   - Status: ready

5. M001-E001-S005: Integration test suite for MRR engine
   - 3 acceptance criteria (integration)
   - Depends on: S001, S002, S003, S004
   - Status: draft (waiting for all unit stories)

Recommended order: S001 first, then S002/S003/S004 in parallel, then S005.

S002 needs review: fixture data for revenue categorization scenarios should be defined before implementation.
```

## Sizing Guide

**Too Big -> Split:**
- Multiple unrelated behaviors
- Would need multiple test files
- "Implement the entire [feature]"

**Just Right:**
- One function or component
- One logical behavior
- Can describe outcome in one sentence

**Too Small -> Combine:**
- Just adds a type definition
- Only renames something
- Pure formatting/cleanup
