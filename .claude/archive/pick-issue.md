---
description: Pick the next HTE task ready for development and implement it
allowed-tools: Bash(hte tasks:*), Bash(gh pr create:*), Bash(gh pr view:*), Bash(gh pr merge:*), Bash(npm run:*), Bash(git checkout:*), Bash(git worktree:*), Bash(git push:*), Bash(git add:*), Bash(git commit:*), Bash(git log:*), Bash(git branch:*), Bash(mkdir:*), Skill, Read, Write, Edit, Glob, Grep
---

# Pick Issue

Pick a task from the `ready` queue and implement it.

## Arguments

- No argument: Pick from queue
- Task ID (e.g., `/pick-issue 01KFJ4YM...`): Work on specific task regardless of status

## Current Tasks Ready for Development

!`hte tasks list --status ready --json`

## Instructions

### 1. Select Task

**If argument provided:**
- Use that task ID directly
- Fetch details: `hte tasks get <id>`

**If no argument:**
- If no tasks with `ready` status: Report "No tasks ready for development. Run `/plan-issue` to process planning queue, or `/brainstorm` for brainstorming queue." and stop.
- Otherwise, use smart task selection:

**Smart Selection Algorithm:**

1. **Fetch all candidates:**
   ```bash
   hte tasks list --status ready --json
   ```

2. **Filter out blocked tasks:**
   - Scan task body for patterns: "blocked by", "depends on", "waiting on"
   - If referenced task is still open, skip this task
   - List skipped blocked tasks in output

3. **Filter out in-progress tasks:**
   - Skip tasks with `in_progress` status (already claimed)

4. **Sort by impact and scope:**
   - Consider: impact, scope, risk
   - Prefer smaller scope when impact is similar

5. **Present top 3 candidates to yourself:**
   - Show: ID, title
   - Consider: impact, scope, risk
   - Prefer smaller scope when impact is similar

6. **Select and document reasoning:**
   - Pick the best task from candidates
   - Document why you chose it in the output (1-2 sentences)

### 2. Claim the Task

```bash
hte tasks update <id> --status in_progress
```

### 3. Fetch Full Context

```bash
hte tasks get <id>
```

Review:
- Task description
- Implementation plan (should be in task body)
- Design decisions
- Test requirements

### 3a. CRITICAL: Headless Mode - NO QUESTIONS

**This is a HEADLESS session. You CANNOT ask questions or request feedback.**

- Do NOT ask "Should I proceed?" or "Ready for feedback?"
- Do NOT wait for user confirmation
- Make autonomous decisions based on the plan
- If unsure, choose the simpler/safer option and proceed
- Complete the work fully, then mark the task done

### 3b. Load Project Context

Read these files to understand the project (if they exist):

- **README.md** - Project setup, tech stack, and conventions
- **VISION.md** - What we're building and why
- **PROGRESS.md** - Recent learnings and patterns to follow (including Failure Patterns section)

Skip any files that don't exist. Use this context to inform your implementation decisions.

**Failure Patterns:** If PROGRESS.md has a "Failure Patterns" section, review it before implementation. These are lessons from previous failed attempts that may apply to this work.

### 3c. Check for Checkpoint (Resume Mode)

**Look for checkpoint data in task body:**

The task body may contain checkpoint metadata:
```
<!--checkpoint:{"task":"01KFJ4YM...","completed":2,"total":5,"branch":"feat/task-slug","worktree":"../worktrees/feat/task-slug"}-->
```

**If checkpoint exists:**

1. **Parse checkpoint data** from the hidden JSON in the task body

2. **Extract progress:**
   - Which steps are already completed (marked with `[x]`)
   - Which step is current (marked with `**CURRENT**`)
   - The branch name and worktree path

3. **Resume in existing worktree:**
   ```bash
   # Check if worktree exists
   WORKTREE_PATH="<worktree-from-checkpoint>"
   if [ -d "$WORKTREE_PATH" ]; then
       cd "$WORKTREE_PATH"
   else
       # Worktree was cleaned up, recreate it
       git worktree add "$WORKTREE_PATH" "<branch-from-checkpoint>"
       cd "$WORKTREE_PATH"
   fi
   ```

4. **Skip completed steps** - When creating your TodoWrite, mark checkpoint-completed steps as `completed` and start with the `**CURRENT**` step as `in_progress`.

5. **Report resume status:**
   ```
   Resuming from checkpoint: 2/5 steps complete
   Branch: feat/task-slug
   Worktree: ../worktrees/feat/task-slug
   Current step: Step 3 - <description>
   ```

**If no checkpoint:** Proceed normally to Section 4.

### 4. Extract ALL Tasks into TodoWrite

**CRITICAL: You MUST complete EVERY step in the implementation plan before marking this task done.**

Before writing any code, read the implementation plan and extract EVERY step into TodoWrite.

Look for:
- Numbered steps (Step 1, Step 2, etc.)
- Batches of tasks
- Steps within sections
- Any work items mentioned

Create a TodoWrite entry for EACH step. This is your checklist - do not skip any.

### 5. Implement

The task should have a detailed implementation plan. Follow it:

1. **Create feature branch and worktree**
   ```bash
   # Create branch name from task slug
   BRANCH="feat/<task-slug>"

   # Get the repo root and create worktree directory
   REPO_ROOT=$(git rev-parse --show-toplevel)
   WORKTREE_BASE=$(dirname "$REPO_ROOT")/worktrees
   mkdir -p "$WORKTREE_BASE"

   # Create the worktree with a new branch
   git worktree add "$WORKTREE_BASE/$BRANCH" -b "$BRANCH"

   # Change to the worktree directory
   cd "$WORKTREE_BASE/$BRANCH"
   ```
   Where `<task-slug>` is a short kebab-case version of the task title (e.g., `feat/coderabbit-integration`)

   **All subsequent work happens in the worktree directory.**

2. **For each step in your todo list:**
   - Mark it as in_progress
   - Implement the step fully
   - Run relevant tests
   - Commit with message describing the change
   - Mark it as completed
   - Move to the next step

**Do NOT mark the task done until ALL todos are completed.**

If the plan is missing or unclear:
- The task may have been incorrectly staged
- Route back: `hte tasks update <id> --status plan`
- Report: "Task <id> lacks implementation plan. Moved back to plan status."

### 5a. Save Checkpoint After Each Step

**After completing each step and committing, update the task with checkpoint info:**

The checkpoint should be appended to the task body or stored in task metadata. Format:

```
## Checkpoint - <TIMESTAMP>

**Progress:** <completed>/<total> steps complete
- [x] Step 1: <description> (commit: <sha>)
- [x] Step 2: <description> (commit: <sha>)
- [ ] Step 3: <description> **CURRENT**
- [ ] Step 4: <description>
- [ ] Step 5: <description>

**Branch:** <branch-name>
**Worktree:** ../worktrees/<branch-name>
**Last commit:** <sha>

<!--checkpoint:{"task":"<id>","completed":<N>,"total":<M>,"branch":"<branch-name>","worktree":"../worktrees/<branch-name>"}-->
```

**Checkpoint Rules:**
- Save checkpoint after EVERY step completion (not just at the end)
- Include the hidden JSON metadata for machine parsing
- Mark completed steps with `[x]` and commit SHA
- Mark the next step with `**CURRENT**`
- Use ISO 8601 timestamp (e.g., `2026-01-18T10:30:00Z`)

**This enables resume if the session fails mid-implementation.**

### 6. Verify Before Completing

**REQUIRED: You MUST run tests and build before closing this issue.**

Run verification commands and **include the output in your session** as proof:

```bash
# Detect test command from package.json scripts, or use project convention
# Common commands by project type:
# - Node.js: npm test
# - Python: pytest
# - dbt: dbt test
# - Go: go test ./...

# Run tests (required)
npm test  # or your project's test command

# Run build (required for compiled projects)
npm run build
```

**Requirements:**
- All tests must pass before closing
- Build must succeed with no errors
- Test output MUST be included in session (proof that tests ran)
- Do NOT close the issue if tests or build fail

**If tests or build fail:**
1. Read the error output carefully
2. Fix the issue in your code
3. Re-run tests until they pass
4. Only then proceed to close the issue

**Configuring test command:**
- The test command should be auto-detected from `package.json` scripts
- If `package.json` has a `test` script, use `npm test`
- For other project types, look for common conventions (pytest, dbt test, etc.)
- If no obvious test command exists, skip tests and note it in the completion comment

### 6a. After Tests Pass

If all verification checks pass, the task is ready for code review. Proceed to section 7.

### 6b. Update PROGRESS.md

After verification passes, update the progress log:

1. **Check if PROGRESS.md exists** - if not, create from template:

```markdown
# Progress Log

## Reusable Patterns

<!-- Patterns promoted from session logs that apply broadly -->

*No patterns yet - they'll be added as we learn from implementations.*

---

## Session Log

<!-- New entries are added below this line -->
```

2. **Append session entry** to Session Log (after the "New entries" comment line):

Format:
```markdown
### YYYY-MM-DD - Issue #<number>: <issue title>

**Files Changed:**
- `path/to/file.ts` - What changed

**Learnings:**
- What you learned during implementation

**Patterns Discovered:**
- Any reusable patterns found (or "None")

**Gotchas:**
- Pitfalls to avoid next time (or "None")
```

3. **Stage for commit** - PROGRESS.md will be committed with implementation changes.

### 7. Create Pull Request

After tests pass, create a PR:

**Push and create PR:**
```bash
git push -u origin HEAD
gh pr create --title "<type>: <description>" --body "$(cat <<'EOF'
## Summary
<what this PR does>

HTE Task: <task-id>
EOF
)"
```

Use conventional commit types: `feat`, `fix`, `refactor`, `docs`, `test`, `chore`

**Note the PR URL** - report it in your output.

### 8. Mark Task Done

After PR is created, mark the task as done:

```bash
hte tasks update <id> --status done
```

**The task is complete once the PR is created.** The PR will be reviewed and merged separately.

### 9. Cleanup Worktree (Optional)

After marking the task done, you can optionally remove the worktree:

```bash
# Return to main repo
cd <original-repo-path>

# Remove the worktree (keeps the branch for the PR)
git worktree remove ../worktrees/feat/<task-slug>
```

**Note:** Only remove the worktree if you don't need to make further changes. The branch remains for the PR.

### 10. Handle CodeRabbit Feedback (If Installed)

If CodeRabbit is installed on the repo, it will review the PR automatically.

**Check for review:**
```bash
gh pr view --json reviews,comments --jq '.comments[] | select(.author.login == "coderabbitai[bot]") | .body' | head -100
```

**Address feedback using judgment:**

| Fix immediately | Create new task |
|-----------------|-----------------|
| Style/formatting | Architecture concerns |
| Naming suggestions | Security issues needing investigation |
| Missing error handling | Significant scope changes |
| Simple refactors | Performance concerns needing research |
| Documentation tweaks | |

For fixes: commit to the branch, push, CodeRabbit re-reviews automatically.

For new tasks:
```bash
hte tasks create --title "CodeRabbit: <summary>" --status brainstorm --data '{"body":"From review of task <id>:\n\n<feedback details>"}'
```

### 11. Optional Retrospective

Run `/retro` automatically if ANY of these apply:
- You noticed related code that could be improved
- You saw similar patterns elsewhere that need the same fix
- You found TODOs or tech debt while working
- The change touched multiple files that may have adjacent issues

Otherwise, skip retro and exit. **Do NOT ask - just decide and act.**

### 12. On Failure

**If you encounter a blocker that prevents completion, capture context before stopping.**

**Failure Categories:**
- `plan-quality` - Vague plan, missing files, incorrect assumptions
- `test-failures` - Implementation broke existing tests
- `blockers` - External dependency, unclear requirements
- `scope-creep` - Implementation grew beyond plan
- `abandoned` - Session terminated without completion

**When blocked, update the task with failure info and move back to plan status:**

```bash
hte tasks update <id> --status plan
```

Document the failure in the task body with:
- Category
- Progress (N/M steps complete)
- What happened
- What was attempted
- Blocker details
- Suggested fix
- Branch name
- Last checkpoint

**Then report:** "Implementation failed for task <id>: <brief reason>. Moved back to plan status."

## Output Format

```
## Task Selection

Blocked (skipped):
- <id>: <title> (blocked by <blocker-id>)

Candidates:
1. <id>: <title>
2. <id>: <title>
3. <id>: <title>

Selection reasoning: <1-2 sentences explaining why you chose this task>

Selected: <id> - <title>
Branch: feat/<slug>
Worktree: ../worktrees/feat/<slug>

[Implementation...]

Verification:
- Tests: passed (output included above)
- Build: succeeded (if applicable)
- PROGRESS.md: updated

PR: <pr-url>
Task marked done: <id>
```
