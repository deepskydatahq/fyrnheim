#!/bin/bash
# Plan tasks in headless Claude Code
# Usage: ./plan-issues.sh [--random] [--loop] [--max N] [--continue-on-error]
#
# Options:
#   --random            Pick tasks randomly instead of in order
#   --loop              Process multiple tasks sequentially
#   --max N             Maximum number of tasks to process (default: all)
#   --continue-on-error Continue to next task if one fails

set -e

source "$(dirname "$0")/lib/log.sh"

# Parse args
PICK_RANDOM=false
LOOP_MODE=false
MAX_TASKS=0
CONTINUE_ON_ERROR=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --random)
            PICK_RANDOM=true
            shift
            ;;
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
            echo "Unknown option: $1"
            echo "Usage: ./plan-issues.sh [--random] [--loop] [--max N] [--continue-on-error]"
            exit 1
            ;;
    esac
done

# Function to fetch plan tasks
fetch_plan_tasks() {
    bd list --label plan --json
}

# Function to process a single task
process_task() {
    local TASK_ID="$1"
    local TASK_TITLE="$2"

    echo "Selected: $TASK_ID - $TASK_TITLE"

    # Claim the task
    echo "Claiming task (setting in_progress status)..."
    bd update "$TASK_ID" --status in_progress
    log_activity "plan-issues" "CLAIM" "$TASK_ID" "$TASK_TITLE"

    # Get full task content
    echo "Fetching task details..."
    TASK_DATA=$(bd show "$TASK_ID" --json | jq '.[0]')
    TASK_BODY=$(echo "$TASK_DATA" | jq -r '"# Task: \(.title)\n\n## Description\n\(.body)"')

    # Build the prompt
    PROMPT="Plan task $TASK_ID: $TASK_TITLE

$TASK_BODY

---

## Instructions

You are creating an implementation plan for this task. Follow these steps:

### 1. Invoke the Writing Plans Skill

Use the \`superpowers:writing-plans\` skill with this argument:
\`\`\`
Task $TASK_ID: $TASK_TITLE
\`\`\`

The skill will guide you through:
- Exploring the codebase to understand current state
- Identifying all files that need changes
- Breaking work into specific, ordered TDD steps
- Including test strategy

### 2. Testing Strategy

Include appropriate testing in the plan:
- Identify what needs to be tested
- Determine the testing approach based on the project's existing patterns
- Specify test commands if the project has a test suite

### 3. Save the Plan

Save the detailed plan to: \`docs/plans/$(date +%Y-%m-%d)-<feature-name>.md\`

### 4. Update Task with Plan Summary

After creating the full plan document, update the task body with:

\`\`\`
## Implementation Plan

**Plan document:** \`docs/plans/<filename>.md\`

### Overview
<1-2 sentence summary of approach>

### Tasks
<N> TDD tasks covering:
- <brief task list>

### Testing
- [ ] Run project test suite to verify changes

---
*Plan created via headless session*
\`\`\`

### 5. Move to Ready

\`\`\`bash
bd update $TASK_ID --remove-label plan --add-label ready
\`\`\`

## Output Format

When complete, report:
- Plan document location
- Number of tasks
- Task moved to ready status

## Start

Begin planning now."

    # Run Claude Code in headless mode
    echo ""
    echo "=========================================="
    echo "Starting Claude Code for task $TASK_ID"
    echo "=========================================="
    echo ""

    TASK_START=$SECONDS
    claude --dangerously-skip-permissions -p "$PROMPT"
    DURATION=$((SECONDS - TASK_START))
}

# Track stats for loop mode
PROCESSED=0
FAILED=0

# Main loop
while true; do
    # Fetch tasks fresh each iteration (to see newly completed ones)
    echo "Fetching plan tasks..."
    TASKS=$(fetch_plan_tasks)

    # Check if any tasks available
    COUNT=$(echo "$TASKS" | jq 'length')
    if [[ "$COUNT" -eq 0 ]]; then
        if [[ "$PROCESSED" -gt 0 ]]; then
            echo ""
            echo "=========================================="
            echo "All tasks planned!"
            echo "  Completed: $PROCESSED"
            echo "  Failed: $FAILED"
            echo "=========================================="
        else
            echo "No tasks with plan status available (all may be in_progress)."
            echo "Run /brainstorm to process the brainstorming queue, or /new-feature to create tasks."
        fi
        exit 0
    fi

    echo "Found $COUNT task(s) needing planning"

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

    # Pick a task
    if [[ "$PICK_RANDOM" == true ]]; then
        INDEX=$((RANDOM % COUNT))
        echo "Picking random task (index $INDEX)..."
    else
        INDEX=0
        echo "Picking first task..."
    fi

    TASK_ID=$(echo "$TASKS" | jq -r ".[$INDEX].id")
    TASK_TITLE=$(echo "$TASKS" | jq -r ".[$INDEX].title")

    # Process the task
    if [[ "$CONTINUE_ON_ERROR" == true ]]; then
        set +e
        process_task "$TASK_ID" "$TASK_TITLE"
        EXIT_CODE=$?
        set -e

        if [[ "$EXIT_CODE" -ne 0 ]]; then
            echo "Task $TASK_ID failed with exit code $EXIT_CODE"
            log_activity "plan-issues" "FAIL" "$TASK_ID" "$TASK_TITLE" "exit=$EXIT_CODE"
            FAILED=$((FAILED + 1))
            # Reset status on failure so it can be retried
            bd update "$TASK_ID" --status open 2>/dev/null || true
            log_activity "plan-issues" "RESET" "$TASK_ID" "$TASK_TITLE"
        else
            ensure_plan_body "$TASK_ID" "$TASK_TITLE" "plan-issues"
            log_activity "plan-issues" "SUCCESS" "$TASK_ID" "$TASK_TITLE"
            PROCESSED=$((PROCESSED + 1))
        fi
    else
        process_task "$TASK_ID" "$TASK_TITLE"
        ensure_plan_body "$TASK_ID" "$TASK_TITLE" "plan-issues"
        log_activity "plan-issues" "SUCCESS" "$TASK_ID" "$TASK_TITLE"
        PROCESSED=$((PROCESSED + 1))
    fi

    # Exit if not in loop mode
    if [[ "$LOOP_MODE" != true ]]; then
        break
    fi

    echo ""
    echo "=========================================="
    echo "Task $TASK_ID planned. Moving to next..."
    echo "  Progress: $PROCESSED planned, $FAILED failed"
    echo "=========================================="
    echo ""

    # Small delay between tasks to prevent rate limiting
    sleep 2
done

# Note: If claude exits, the in_progress status remains
# This is intentional - manual cleanup needed if abandoned
