#!/bin/bash
# Run ready tasks in headless Claude Code with worktree isolation
# Usage: ./run-issue.sh [TASK_ID] [--loop] [--max N] [--continue-on-error]
#
# Arguments:
#   TASK_ID             Run a specific task by ID (skips queue, ignores status)
#
# Options:
#   --loop              Process multiple tasks sequentially
#   --max N             Maximum number of tasks to process (default: all)
#   --continue-on-error Continue to next task if one fails
#
# Workflow:
#   1. Select task from ready queue (or use specified ID)
#   2. Create feature branch and worktree in ../worktrees/<branch>
#   3. Run claude in the worktree directory
#   4. Claude implements, creates PR, marks task done
#   5. Optionally cleanup worktree

set -e

source "$(dirname "$0")/lib/log.sh"

# Get the repo root for worktree calculations
REPO_ROOT=$(git rev-parse --show-toplevel)
WORKTREE_BASE=$(dirname "$REPO_ROOT")/worktrees

# Parse args
SPECIFIC_TASK=""
LOOP_MODE=false
MAX_TASKS=0
CONTINUE_ON_ERROR=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --loop)
            LOOP_MODE=true
            shift
            ;;
        --max)
            MAX_TASKS="$2"
            shift 2
            ;;
        --continue-on-error)
            CONTINUE_ON_ERROR=true
            shift
            ;;
        *)
            # Assume it's a task ID if not a flag
            if [[ "$1" != --* ]]; then
                SPECIFIC_TASK="$1"
            else
                echo "Unknown option: $1"
                echo "Usage: ./run-issue.sh [TASK_ID] [--loop] [--max N] [--continue-on-error]"
                exit 1
            fi
            shift
            ;;
    esac
done

# Function to fetch ready tasks with full details for AI selection
fetch_ready_tasks_detailed() {
    bd list --label ready --json
}

# Function to format tasks for the prompt
format_tasks_for_prompt() {
    local tasks="$1"
    echo "$tasks" | jq -r '.[] | "### Task \(.id): \(.title)\n**Status:** \(.status)\n**Description preview:** \(.body | split("\n")[0:5] | join("\n"))\n"'
}

# Function to create a slug from task title
create_slug() {
    local title="$1"
    echo "$title" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]/-/g' | sed 's/--*/-/g' | sed 's/^-//' | sed 's/-$//' | cut -c1-50
}

# Function to process a single task with worktree
process_task_with_worktree() {
    local TASK_ID="$1"
    local TASK_TITLE="$2"

    echo "Processing task: $TASK_ID - $TASK_TITLE"

    # Create branch name from task title
    local SLUG=$(create_slug "$TASK_TITLE")
    local BRANCH="feat/$SLUG"
    local WORKTREE_PATH="$WORKTREE_BASE/$BRANCH"

    echo "Branch: $BRANCH"
    echo "Worktree: $WORKTREE_PATH"

    # Claim the task
    echo "Claiming task..."
    bd update "$TASK_ID" --status in_progress
    log_activity "run-issue" "CLAIM" "$TASK_ID" "$TASK_TITLE"

    # Create worktree directory
    mkdir -p "$WORKTREE_BASE"

    # Check if worktree already exists
    if [ -d "$WORKTREE_PATH" ]; then
        echo "Worktree already exists, using it..."
    else
        echo "Creating worktree..."
        git worktree add "$WORKTREE_PATH" -b "$BRANCH" 2>/dev/null || \
            git worktree add "$WORKTREE_PATH" "$BRANCH"
    fi

    # Get task body for the prompt
    local TASK_DATA=$(bd show "$TASK_ID" --json | jq '.[0]')
    local TASK_BODY=$(echo "$TASK_DATA" | jq -r '.body')

    # Build the prompt
    local PROMPT="# Implement Task

**Task ID:** $TASK_ID
**Title:** $TASK_TITLE
**Branch:** $BRANCH
**Worktree:** $WORKTREE_PATH

## Task Description

$TASK_BODY

---

## CRITICAL: Headless Mode - NO QUESTIONS

**This is a HEADLESS session. You CANNOT ask questions or request feedback.**

- Do NOT ask \"Should I proceed?\" or \"Ready for feedback?\"
- Do NOT wait for user confirmation
- Make autonomous decisions based on the plan
- If unsure, choose the simpler/safer option and proceed

## CRITICAL: Complete ALL Steps

**You MUST complete EVERY step in the implementation plan.**

Partial implementation is NOT acceptable. If the plan has 7 steps, you must complete all 7.

## Instructions

### Step 1: Load Project Context

Read these files to understand the project (if they exist):
- **README.md** - Project setup, tech stack, and conventions
- **PROGRESS.md** - Recent learnings and patterns to follow

### Step 2: Extract ALL Steps from the Plan

Read the implementation plan and extract EVERY step into TodoWrite.

### Step 3: Implement Each Step

For each step:
1. Mark it as in_progress
2. Implement fully
3. Run relevant tests
4. Commit with descriptive message
5. Mark as completed
6. Move to next step

### Step 4: Verify

Before creating PR:
- [ ] All steps completed
- [ ] All tests pass
- [ ] All commits made

### Step 5: Update PROGRESS.md

Add a session entry to PROGRESS.md with:
- Files changed
- Learnings
- Patterns discovered
- Gotchas

### Step 6: Create Pull Request

\`\`\`bash
git push -u origin HEAD
gh pr create --title \"<type>: <description>\" --body \"\$(cat <<'EOF'
## Summary
<what this PR does>

Task: $TASK_ID
EOF
)\"
\`\`\`

Use conventional commit types: feat, fix, refactor, docs, test, chore

**Report the PR URL in your output.**

### Step 7: Mark Task Done

\`\`\`bash
bd close $TASK_ID
\`\`\`

## Start

You are already in the worktree at: $WORKTREE_PATH
Branch: $BRANCH

Begin by reading the project context, then extract all steps and implement them."

    # Run Claude Code in the worktree directory
    echo ""
    echo "=========================================="
    echo "Starting Claude Code in worktree"
    echo "Worktree: $WORKTREE_PATH"
    echo "=========================================="
    echo ""

    # Change to worktree and run claude
    TASK_START=$SECONDS
    (cd "$WORKTREE_PATH" && claude --dangerously-skip-permissions -p "$PROMPT")
    DURATION=$((SECONDS - TASK_START))
}

# Function to process tasks (either specific or AI-selected)
process_tasks() {
    local SPECIFIC="$1"

    if [[ -n "$SPECIFIC" ]]; then
        # Specific task mode - fetch just that task
        echo "Fetching task $SPECIFIC..."
        TASK_DATA=$(bd show "$SPECIFIC" --json 2>/dev/null)

        if [[ -z "$TASK_DATA" ]]; then
            echo "Error: Task $SPECIFIC not found"
            return 1
        fi

        TASK_ID=$(echo "$TASK_DATA" | jq -r '.id')
        TASK_TITLE=$(echo "$TASK_DATA" | jq -r '.title')

        process_task_with_worktree "$TASK_ID" "$TASK_TITLE"
    else
        # AI selection mode - pick first available task
        echo "Fetching all ready tasks..."
        TASKS=$(fetch_ready_tasks_detailed)

        COUNT=$(echo "$TASKS" | jq 'length')
        if [[ "$COUNT" -eq 0 ]]; then
            echo "No tasks with ready status available."
            echo "Run /plan-issue to process the planning queue, or /brainstorm for brainstorming queue."
            return 1
        fi

        echo "Found $COUNT ready task(s)"

        # Pick the first task (could add smarter selection later)
        TASK_ID=$(echo "$TASKS" | jq -r '.[0].id')
        TASK_TITLE=$(echo "$TASKS" | jq -r '.[0].title')

        process_task_with_worktree "$TASK_ID" "$TASK_TITLE"
    fi
}

# Handle specific task mode
if [[ -n "$SPECIFIC_TASK" ]]; then
    process_tasks "$SPECIFIC_TASK"
    exit 0
fi

# Track stats for loop mode
PROCESSED=0
FAILED=0

# Main loop
while true; do
    # Check if we've hit max
    if [[ "$MAX_TASKS" -gt 0 && "$PROCESSED" -ge "$MAX_TASKS" ]]; then
        echo ""
        echo "=========================================="
        echo "Reached maximum tasks ($MAX_TASKS)"
        echo "  Completed: $PROCESSED"
        echo "  Failed: $FAILED"
        echo "=========================================="
        exit 0
    fi

    # Process tasks
    set +e
    process_tasks ""
    EXIT_CODE=$?
    set -e

    if [[ "$EXIT_CODE" -ne 0 ]]; then
        echo "Session exited with code $EXIT_CODE"
        log_activity "run-issue" "FAIL" "-" "" "exit=$EXIT_CODE"
        if [[ "$CONTINUE_ON_ERROR" == true ]]; then
            FAILED=$((FAILED + 1))
        else
            echo "Stopping loop (use --continue-on-error to keep going)"
            exit "$EXIT_CODE"
        fi
    else
        log_activity "run-issue" "SUCCESS" "-" ""
        PROCESSED=$((PROCESSED + 1))
    fi

    # Exit if not in loop mode
    if [[ "$LOOP_MODE" != true ]]; then
        break
    fi

    echo ""
    echo "=========================================="
    echo "Session complete. Moving to next..."
    echo "  Progress: $PROCESSED processed, $FAILED failed"
    echo "=========================================="
    echo ""

    # Small delay between tasks to prevent rate limiting
    sleep 2
done

# Note: Worktrees are left in place for potential follow-up work
# Clean up with: git worktree remove ../worktrees/feat/<slug>
