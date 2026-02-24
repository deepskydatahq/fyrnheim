#!/bin/bash
# Parallel implementation workers with dependency awareness
# Usage: ./run-parallel.sh [--workers N] [--max N] [--continue-on-error]
#
# Spawns N parallel workers that each claim and process ready tasks.
# Workers skip tasks that have unresolved dependencies.
# Uses file locking to prevent double-claiming.

set -e

source "$(dirname "$0")/lib/log.sh"

WORKERS=3
MAX_TASKS=0
CONTINUE_ON_ERROR=false
LOCK_DIR="/tmp/run-locks"

# Get the repo root for worktree calculations
REPO_ROOT=$(git rev-parse --show-toplevel)
WORKTREE_BASE=$(dirname "$REPO_ROOT")/worktrees

# Parse args
while [[ $# -gt 0 ]]; do
    case "$1" in
        --workers|-w)
            WORKERS="$2"
            shift 2
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
            echo "Unknown option: $1"
            echo "Usage: ./run-parallel.sh [--workers N] [--max N] [--continue-on-error]"
            exit 1
            ;;
    esac
done

# Create lock directory
mkdir -p "$LOCK_DIR"

# Cleanup old locks on start
find "$LOCK_DIR" -name "*.lock" -mmin +60 -delete 2>/dev/null || true

echo "=========================================="
echo "Parallel Implementation"
echo "  Workers: $WORKERS"
echo "  Max tasks: ${MAX_TASKS:-unlimited}"
echo "  Lock dir: $LOCK_DIR"
echo "  Worktree base: $WORKTREE_BASE"
echo "=========================================="
echo ""

log_activity "run-parallel" "START" "-" "Parallel session" "workers=$WORKERS"

# Function to check if a task has unresolved dependencies
has_unresolved_deps() {
    local TASK_ID="$1"

    # Get dependencies (what this task depends on)
    local DEPS=$(bd dep list "$TASK_ID" --json 2>/dev/null || echo "[]")

    if [[ "$DEPS" == "[]" || -z "$DEPS" ]]; then
        return 1  # No dependencies, can proceed
    fi

    # Check each dependency - if any are not closed, we're blocked
    local DEP_IDS=$(echo "$DEPS" | jq -r '.[].id' 2>/dev/null)

    for DEP_ID in $DEP_IDS; do
        local DEP_STATUS=$(bd show "$DEP_ID" --json 2>/dev/null | jq -r '.[0].status' 2>/dev/null)
        if [[ "$DEP_STATUS" != "closed" ]]; then
            return 0  # Has unresolved dependency
        fi
    done

    return 1  # All dependencies resolved
}

# Function to create a slug from task title
create_slug() {
    local title="$1"
    echo "$title" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]/-/g' | sed 's/--*/-/g' | sed 's/^-//' | sed 's/-$//' | cut -c1-50
}

# Worker function
worker() {
    local WORKER_ID="$1"
    local PROCESSED=0
    local FAILED=0

    echo "[Run Worker $WORKER_ID] Started"

    while true; do
        # Fetch available tasks (ready label, open status only)
        TASKS=$(bd list --label ready --json | jq '[.[] | select(.status == "open")]')
        COUNT=$(echo "$TASKS" | jq 'length')

        if [[ "$COUNT" -eq 0 ]]; then
            echo "[Run Worker $WORKER_ID] No more open ready tasks. Exiting."
            break
        fi

        # Try to claim a task (try each until one succeeds)
        CLAIMED=false
        for i in $(seq 0 $((COUNT - 1))); do
            TASK_ID=$(echo "$TASKS" | jq -r ".[$i].id")
            TASK_TITLE=$(echo "$TASKS" | jq -r ".[$i].title")

            # Check dependencies first
            if has_unresolved_deps "$TASK_ID"; then
                echo "[Run Worker $WORKER_ID] Skipping $TASK_ID (has unresolved dependencies)"
                log_activity "run-parallel:w$WORKER_ID" "SKIP" "$TASK_ID" "$TASK_TITLE" "blocked by deps"
                continue
            fi

            # Try to acquire lock
            LOCK_FILE="$LOCK_DIR/$TASK_ID.lock"
            if (set -o noclobber; echo $$ > "$LOCK_FILE") 2>/dev/null; then
                # Double-check task is still open
                CURRENT_STATUS=$(bd show "$TASK_ID" --json | jq -r '.[0].status')
                if [[ "$CURRENT_STATUS" != "open" ]]; then
                    rm -f "$LOCK_FILE"
                    continue
                fi

                # Claim the task
                bd update "$TASK_ID" --status in_progress 2>/dev/null || true
                CLAIMED=true
                echo "[Run Worker $WORKER_ID] Claimed: $TASK_ID - $TASK_TITLE"
                log_activity "run-parallel:w$WORKER_ID" "CLAIM" "$TASK_ID" "$TASK_TITLE"
                break
            fi
        done

        if [[ "$CLAIMED" != true ]]; then
            # Check if all remaining tasks are blocked
            BLOCKED_COUNT=0
            for i in $(seq 0 $((COUNT - 1))); do
                TID=$(echo "$TASKS" | jq -r ".[$i].id")
                if has_unresolved_deps "$TID"; then
                    BLOCKED_COUNT=$((BLOCKED_COUNT + 1))
                fi
            done

            if [[ "$BLOCKED_COUNT" -eq "$COUNT" ]]; then
                echo "[Run Worker $WORKER_ID] All remaining tasks are blocked. Waiting for dependencies..."
                sleep 30
            else
                echo "[Run Worker $WORKER_ID] Could not claim any task. Waiting..."
                sleep 5
            fi
            continue
        fi

        # Create worktree and process the task
        echo "[Run Worker $WORKER_ID] Processing $TASK_ID..."

        SLUG=$(create_slug "$TASK_TITLE")
        BRANCH="feat/$SLUG"
        WORKTREE_PATH="$WORKTREE_BASE/$BRANCH"

        # Create worktree directory
        mkdir -p "$WORKTREE_BASE"

        # Check if worktree already exists
        if [ -d "$WORKTREE_PATH" ]; then
            echo "[Run Worker $WORKER_ID] Worktree exists, using it..."
        else
            echo "[Run Worker $WORKER_ID] Creating worktree..."
            git worktree add "$WORKTREE_PATH" -b "$BRANCH" 2>/dev/null || \
                git worktree add "$WORKTREE_PATH" "$BRANCH" 2>/dev/null || \
                git worktree add "$WORKTREE_PATH" HEAD 2>/dev/null
        fi

        # Get task description for the prompt
        TASK_DATA=$(bd show "$TASK_ID" --json | jq '.[0]')
        TASK_BODY=$(echo "$TASK_DATA" | jq -r '.description')

        # Build the prompt
        PROMPT="# Implement Task

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

        if [[ "$CONTINUE_ON_ERROR" == true ]]; then
            set +e
        fi

        TASK_START=$SECONDS
        # Run Claude Code in the worktree directory
        (cd "$WORKTREE_PATH" && claude --dangerously-skip-permissions -p "$PROMPT")
        EXIT_CODE=$?
        DURATION=$((SECONDS - TASK_START))

        set -e

        # Release lock
        rm -f "$LOCK_FILE"

        if [[ "$EXIT_CODE" -ne 0 ]]; then
            echo "[Run Worker $WORKER_ID] Task $TASK_ID failed (exit $EXIT_CODE)"
            log_activity "run-parallel:w$WORKER_ID" "FAIL" "$TASK_ID" "$TASK_TITLE" "exit=$EXIT_CODE duration=${DURATION}s"
            FAILED=$((FAILED + 1))
            # Reset status on failure
            bd update "$TASK_ID" --status open 2>/dev/null || true

            if [[ "$CONTINUE_ON_ERROR" != true ]]; then
                break
            fi
        else
            echo "[Run Worker $WORKER_ID] Task $TASK_ID completed"
            # Safety net: ensure task was closed
            CURRENT_STATUS=$(bd show "$TASK_ID" --json 2>/dev/null | jq -r '.[0].status' 2>/dev/null)
            if [[ "$CURRENT_STATUS" != "closed" ]]; then
                echo "[Run Worker $WORKER_ID] Task not closed, fixing..."
                bd close "$TASK_ID" 2>/dev/null || true
            fi
            log_activity "run-parallel:w$WORKER_ID" "SUCCESS" "$TASK_ID" "$TASK_TITLE" "duration=${DURATION}s"
            PROCESSED=$((PROCESSED + 1))
        fi

        # Check max
        if [[ "$MAX_TASKS" -gt 0 && "$PROCESSED" -ge "$MAX_TASKS" ]]; then
            echo "[Run Worker $WORKER_ID] Reached max tasks"
            break
        fi

        sleep 1
    done

    echo "[Run Worker $WORKER_ID] Finished. Processed: $PROCESSED, Failed: $FAILED"
}

# Export for subshells
export CONTINUE_ON_ERROR MAX_TASKS LOCK_DIR WORKTREE_BASE
export -f worker has_unresolved_deps create_slug

# Spawn workers
PIDS=()
for i in $(seq 1 $WORKERS); do
    worker "$i" &
    PIDS+=($!)
    sleep 2  # Stagger starts
done

echo ""
echo "Spawned $WORKERS implementation workers: ${PIDS[*]}"
echo ""

# Wait for all workers
TOTAL_EXIT=0
for PID in "${PIDS[@]}"; do
    wait $PID || TOTAL_EXIT=$?
done

# Cleanup locks
rm -f "$LOCK_DIR"/*.lock 2>/dev/null || true

echo ""
echo "=========================================="
echo "All implementation workers finished"
echo "=========================================="

# Show final status
echo ""
echo "Final task status:"
echo "  Ready: $(bd list --label ready --json | jq '[.[] | select(.status == "open")] | length')"
echo "  In Progress: $(bd list --label ready --json | jq '[.[] | select(.status == "in_progress")] | length')"
echo "  Closed: $(bd list --label ready --json | jq '[.[] | select(.status == "closed")] | length')"

exit $TOTAL_EXIT
