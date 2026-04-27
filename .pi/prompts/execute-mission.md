---
description: Execute a Fyrnheim mission using product TOML stories as canonical tasks
argument-hint: "<mission-id>"
---
# Execute Mission

Execute mission `$1` using Fyrnheim's product TOML workflow. Do not use Beads.

## Phase 1: Read context

1. Read `HOW_WE_WORK.md` and `CLAUDE.md`.
2. Read the mission file: `product/missions/$1-*.toml`.
3. Extract outcome, scope, testing criteria, relevant paths, and dependencies.

## Phase 2: Ensure epics and stories exist

1. Check for existing epics: `product/epics/$1-E*.toml`.
2. If missing, use the `product-mission-breakdown` skill to create epics.
3. For each epic, check for existing stories: `product/stories/{epic_id}-S*.toml`.
4. If missing, use the `product-epic-breakdown` skill to create stories.
5. Assign each story a `triage = "ready" | "plan" | "brainstorm"` field if missing.

## Phase 3: Build execution order

1. Build the dependency graph from epic `depends_on` and story `[context].depends_on`.
2. Identify ready stories whose dependencies are complete.
3. Prefer completing stories in epic order unless independent stories can safely be handled in parallel by separate Pi sessions/worktrees.

## Phase 4: Implement stories

For each selected story:

1. Use the `product-story-execution` skill.
2. Set story `status = "in_progress"`.
3. Implement production code and tests.
4. Run focused tests, then:
   - `uv run pytest`
   - `uv run ruff check src/ tests/`
   - `uv run mypy src/`
5. Commit after each completed story.
6. Set story `status = "complete"` and record commit metadata if an `[execution]` table exists.

If blocked, set `status = "blocked"` and document the reason in `[execution].failure_reason`.

## Phase 5: Validate and ship

1. Use the `product-judgment` skill to validate completed stories and parent epics.
2. Run all quality gates again.
3. Push the branch and create a PR if this is feature work.
4. Update mission/epic/story statuses based on validation.

## Report

Return:

```text
## Mission Execution Report: $1

### Completed Stories
- ...

### Blocked or Failed Stories
- ...

### Quality Gates
- pytest: pass/fail
- ruff: pass/fail
- mypy: pass/fail

### Commits / PR
- ...

### Remaining Work
- ...
```
