---
description: Execute a Fyrnheim mission using product TOML as the canonical workflow
argument-hint: "<mission-id>"
---
# Execute Mission

Execute mission `$1` using Fyrnheim's product TOML workflow. Do not use Beads.

## Phase 1: Read context and mode

1. Read `AGENTS.md`, `HOW_WE_WORK.md`, and `CLAUDE.md`.
2. If docs disagree, follow `AGENTS.md` and product TOML.
3. Read the mission file: `product/missions/$1-*.toml`.
4. Extract outcome, scope, testing criteria, relevant paths, dependencies, and `[execution]` if present.
5. Determine execution mode:
   - Use `[execution].mode` when present.
   - Otherwise infer mode from the mission outcome/scope.

## Execution modes

| Mode | Behavior |
|------|----------|
| `implementation` | Break into epics/stories, implement code/tests, validate, PR |
| `audit` / `planning` | Produce audit notes, decisions, and follow-up mission TOMLs; do not force implementation stories |
| `docs` | Update docs/examples and validate them |
| `release` | Run version/changelog/package-release workflow |

If the mission is audit/planning-shaped, do not create implementation stories just to satisfy the default hierarchy. Create only the artifacts the mission asks for, usually an audit doc and follow-up draft mission TOMLs.

## Implementation mode

### Phase 2: Ensure epics and stories exist

1. Check for existing epics: `product/epics/$1-E*.toml`.
2. If missing, use the `product-mission-breakdown` skill to create epics.
3. For each epic, check for existing stories: `product/stories/{epic_id}-S*.toml`.
4. If missing, use the `product-epic-breakdown` skill to create stories.
5. Assign each story a `triage = "ready" | "plan" | "brainstorm"` field if missing.

### Phase 3: Build execution order

1. Build the dependency graph from epic `depends_on` and story `[context].depends_on`.
2. Identify ready stories whose dependencies are complete.
3. Prefer completing stories in epic order unless independent stories can safely be handled in parallel by separate Pi sessions/worktrees.

### Phase 4: Implement stories

For each selected story:

1. Use the `product-story-execution` skill.
2. Set story `status = "in_progress"`.
3. Implement production code and tests.
4. Run focused tests, then `scripts/quality-gates.sh`.
5. Commit after each completed story.
6. Set story `status = "complete"` and record commit metadata if an `[execution]` table exists.

If blocked, set `status = "blocked"` and document the reason in `[execution].failure_reason`.

## Audit/planning mode

1. Create or update a documentation artifact under `docs/` when useful.
2. Classify findings by impact, risk, and disposition.
3. Write findings into new draft mission TOMLs when they are coherent future work.
4. Do not create low-level implementation stories unless the mission explicitly asks for them or a follow-up mission already exists.
5. Mark the audit/planning mission complete only when its requested artifacts exist and validate.

## Docs mode

1. Update the requested docs/examples.
2. Run relevant docs checks or `scripts/quality-gates.sh` when code examples are affected.
3. Commit and push.

## Release mode

1. Follow the mission's release criteria exactly.
2. Run `scripts/quality-gates.sh`.
3. Prepare changelog/version/package artifacts.
4. Do not tag, publish, or merge unless explicitly instructed.

## Validate and ship

1. Use the `product-judgment` skill to validate completed stories/epics/missions or mode-specific artifacts.
2. Run `scripts/quality-gates.sh` when code changed.
3. Push the branch and create/update a PR if this is feature work.
4. Use `/pr-status` to inspect checks and review comments.
5. Use `/pr-merge-if-ready` only when the user explicitly asks to merge.

## Report

Return:

```text
## Mission Execution Report: $1

### Mode
- ...

### Completed Work
- ...

### Blocked or Failed Work
- ...

### Quality Gates
- pytest: pass/fail/skipped
- ruff: pass/fail/skipped
- mypy: pass/fail/skipped

### Commits / PR
- ...

### Remaining Work
- ...
```
