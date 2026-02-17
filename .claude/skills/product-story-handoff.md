# Skill: product-story-handoff

## Trigger

Command: `/product-story-handoff {story_id}`

Options:
- `/product-story-handoff M001-E001-S001` - Hand off specific story
- `/product-story-handoff --epic M001-E001` - Hand off all ready stories in epic
- `/product-story-handoff --ready` - Hand off all ready stories

## Instructions

You transform product stories into Beads tasks, bridging the product layer to the implementation layer.

### Step 1: Validate Readiness

Check the story file at `product/stories/{story_id}-*.toml`

Verify:
- Status is `ready`
- All required fields are populated
- Dependencies are satisfied (dependent stories are `complete`)

If not ready, report why and stop.

### Step 2: Gather Context

Read the story and assemble context from the hierarchy:

```
Mission -> Epic -> Story
```

From Mission:
- Overall outcome description
- User progress statement
- Why this work matters

From Epic:
- Job story
- Feature context
- Related files

From Story:
- Specific outcome
- Acceptance criteria
- Implementation hints

### Step 3: Create Beads Task

Create the task using:

```bash
bd create "Story title" --labels brainstorm -d "rich context description" --silent
```

The task description should contain:

**Context (why this exists):**
- Mission outcome summary
- Epic job story
- How this story fits

**Specification (what to build):**
- Story outcome description
- All acceptance criteria with test types
- Expected behavior

**Guidance (how to approach):**
- Implementation hints from story
- Reference files
- Relevant paths
- Input fixtures

### Step 4: Update Story Status

Set story status to `in-progress` in the TOML file.

### Step 5: Register Dependencies in Beads

After creating all tasks, link them based on story dependencies.

For each story with `depends_on`:
1. Map story IDs to Beads task IDs (use the mapping from Step 3)
2. Register the dependency:

```bash
# If M003-E001-S002 depends on M003-E001-S001
# And S001 → basesignal-6ab, S002 → basesignal-dns
bd dep add basesignal-dns basesignal-6ab
```

This ensures parallel workers will skip blocked tasks until dependencies complete.

### Output Example

```
Handed off: M001-E001-S001 "Calculate base MRR from active subscriptions"

Context:
  Mission: Growth Intelligence Dashboard MVP
  Epic: Core MRR Calculation Engine

Acceptance Criteria: 7 (6 unit, 1 integration)

Files to create/modify:
  - src/analytics/mrr/calculator.ts

References provided:
  - src/types/subscription.ts
  - docs/metrics-definitions.md
  - tests/fixtures/basic-subscriptions.json

Beads task created: basesignal-abc
Story status updated: ready -> in-progress
Dependencies registered: 0 (no dependencies)

Ready for brainstorming phase.
```

### Error Cases

**Story not ready:**
```
Cannot hand off M001-E001-S003: status is 'draft'
Action: Set status to 'ready' after review
```

**Dependencies not met:**
```
Cannot hand off M001-E001-S003: depends on M001-E001-S002 (status: in-progress)
Action: Complete S002 first
```

**Missing acceptance criteria:**
```
Cannot hand off M001-E001-S003: no acceptance criteria defined
Action: Add specific criteria before handoff
```

### Batch Handoff

For `--epic` or `--ready` options:

1. Find all matching stories
2. Filter to status = `ready`
3. Verify dependencies for each
4. Hand off in dependency order
5. Build story ID → Beads task ID mapping
6. Register all dependencies using `bd dep add`
7. Report summary

```
Batch handoff for epic M001-E001:

Created tasks:
- M001-E001-S001 → basesignal-abc - handed off
- M001-E001-S003 → basesignal-def - handed off
- M001-E001-S004 → basesignal-ghi - handed off
x M001-E001-S002 - skipped (missing fixture data)
x M001-E001-S005 - skipped (depends on S002)

Registered dependencies:
- basesignal-def depends on basesignal-abc (S003 → S001)
- basesignal-ghi depends on basesignal-def (S004 → S003)

Handed off: 3
Skipped: 2
Dependencies registered: 2
```
