#!/bin/bash
# Brainstorm tasks in headless Claude Code
# Usage: ./brainstorm-issues.sh [TASK_ID] [--random] [--loop] [--max N] [--continue-on-error]
#
# Arguments:
#   TASK_ID             Specific task to brainstorm (skips claiming)
#
# Options:
#   --random            Pick tasks randomly instead of in order
#   --loop              Process multiple tasks sequentially
#   --max N             Maximum number of tasks to process (default: all)
#   --continue-on-error Continue to next task if one fails

set -e

source "$(dirname "$0")/lib/log.sh"

# Parse args
SPECIFIC_TASK=""
PICK_RANDOM=false
LOOP_MODE=false
MAX_TASKS=0
CONTINUE_ON_ERROR=false

# Check if first argument is a task ID (not a flag)
if [[ $# -gt 0 && "$1" != --* ]]; then
    SPECIFIC_TASK="$1"
    shift
fi

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
            echo "Usage: ./brainstorm-issues.sh [TASK_ID] [--random] [--loop] [--max N] [--continue-on-error]"
            exit 1
            ;;
    esac
done

# Function to fetch brainstorm tasks
fetch_brainstorm_tasks() {
    bd list --label brainstorm --json
}

# Function to fetch specific task
fetch_specific_task() {
    local TASK_ID="$1"
    bd show "$TASK_ID" --json | jq '.[0]'
}

# Function to process a single task
process_task() {
    local TASK_ID="$1"
    local TASK_TITLE="$2"
    local SKIP_CLAIM="$3"

    echo "Selected: $TASK_ID - $TASK_TITLE"

    # Claim the task (unless skipping)
    if [[ "$SKIP_CLAIM" != "true" ]]; then
        echo "Claiming task (setting in_progress status)..."
        bd update "$TASK_ID" --status in_progress
    else
        echo "Skipping claim (specific task mode)"
    fi

    log_activity "brainstorm-issues" "CLAIM" "$TASK_ID" "$TASK_TITLE"

    # Build the prompt - delegate to /brainstorm-auto
    PROMPT="Run /brainstorm-auto $TASK_ID"

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

# Specific task mode
if [[ -n "$SPECIFIC_TASK" ]]; then
    echo "Fetching task $SPECIFIC_TASK..."
    TASK_DATA=$(fetch_specific_task "$SPECIFIC_TASK")

    if [[ -z "$TASK_DATA" ]]; then
        echo "Error: Task $SPECIFIC_TASK not found"
        exit 1
    fi

    TASK_ID=$(echo "$TASK_DATA" | jq -r '.id')
    TASK_TITLE=$(echo "$TASK_DATA" | jq -r '.title')

    process_task "$TASK_ID" "$TASK_TITLE" "true"
    exit 0
fi

# Queue mode - main loop
while true; do
    # Fetch tasks fresh each iteration (to see newly completed ones)
    echo "Fetching brainstorm tasks..."
    TASKS=$(fetch_brainstorm_tasks)

    # Check if any tasks available
    COUNT=$(echo "$TASKS" | jq 'length')
    if [[ "$COUNT" -eq 0 ]]; then
        if [[ "$PROCESSED" -gt 0 ]]; then
            echo ""
            echo "=========================================="
            echo "All tasks brainstormed!"
            echo "  Completed: $PROCESSED"
            echo "  Failed: $FAILED"
            echo "=========================================="
        else
            echo "No tasks with brainstorm status available (all may be in_progress)."
            echo "Run /new-feature to create tasks, or /brainstorm-epics to generate epics."
        fi
        exit 0
    fi

    echo "Found $COUNT task(s) needing brainstorming"

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
        process_task "$TASK_ID" "$TASK_TITLE" "false"
        EXIT_CODE=$?
        set -e

        if [[ "$EXIT_CODE" -ne 0 ]]; then
            echo "Task $TASK_ID failed with exit code $EXIT_CODE"
            log_activity "brainstorm-issues" "FAIL" "$TASK_ID" "$TASK_TITLE" "exit=$EXIT_CODE"
            FAILED=$((FAILED + 1))
            # Reset status on failure so it can be retried
            bd update "$TASK_ID" --status open 2>/dev/null || true
            log_activity "brainstorm-issues" "RESET" "$TASK_ID" "$TASK_TITLE"
        else
            ensure_brainstorm_body "$TASK_ID" "$TASK_TITLE" "brainstorm-issues"
            log_activity "brainstorm-issues" "SUCCESS" "$TASK_ID" "$TASK_TITLE"
            PROCESSED=$((PROCESSED + 1))
        fi
    else
        process_task "$TASK_ID" "$TASK_TITLE" "false"
        ensure_brainstorm_body "$TASK_ID" "$TASK_TITLE" "brainstorm-issues"
        log_activity "brainstorm-issues" "SUCCESS" "$TASK_ID" "$TASK_TITLE"
        PROCESSED=$((PROCESSED + 1))
    fi

    # Exit if not in loop mode
    if [[ "$LOOP_MODE" != true ]]; then
        break
    fi

    echo ""
    echo "=========================================="
    echo "Task $TASK_ID brainstormed. Moving to next..."
    echo "  Progress: $PROCESSED brainstormed, $FAILED failed"
    echo "=========================================="
    echo ""

    # Small delay between tasks to prevent rate limiting
    sleep 2
done

# Note: If claude exits, the in_progress status remains
# This is intentional - manual cleanup needed if abandoned
