#!/bin/bash
# Parallel plan workers
# Usage: ./plan-parallel.sh [--workers N] [--max N] [--continue-on-error] [--max-retries N]
#
# Spawns N parallel workers that each claim and process plan tasks.
# Uses file locking to prevent double-claiming.
# Tracks retries per task — skips tasks that have failed too many times.

set -euo pipefail

source "$(dirname "$0")/lib/log.sh"

# Disable set -e for worker subshells — workers handle errors via trap + explicit checks
set +e

WORKERS=3
MAX_TASKS=0
MAX_RETRIES=2
CONTINUE_ON_ERROR=false
LOCK_DIR="/tmp/plan-locks"
RETRY_DIR="/tmp/plan-retries"

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
        --max-retries)
            MAX_RETRIES="$2"
            shift 2
            ;;
        --continue-on-error)
            CONTINUE_ON_ERROR=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: ./plan-parallel.sh [--workers N] [--max N] [--max-retries N] [--continue-on-error]"
            exit 1
            ;;
    esac
done

# Create directories
mkdir -p "$LOCK_DIR" "$RETRY_DIR"

# Cleanup old locks on start (but preserve retry counts)
find "$LOCK_DIR" -name "*.lock" -mmin +60 -delete 2>/dev/null || true

echo "=========================================="
echo "Parallel Planning"
echo "  Workers: $WORKERS"
echo "  Max tasks: ${MAX_TASKS:-unlimited}"
echo "  Max retries per task: $MAX_RETRIES"
echo "  Lock dir: $LOCK_DIR"
echo "=========================================="
echo ""

log_activity "plan-parallel" "START" "-" "Parallel session" "workers=$WORKERS"

# Get retry count for a task (0 if never tried)
get_retries() {
    local TASK_ID="$1"
    local RETRY_FILE="$RETRY_DIR/$TASK_ID.retries"
    if [[ -f "$RETRY_FILE" ]]; then
        cat "$RETRY_FILE" 2>/dev/null || echo "0"
    else
        echo "0"
    fi
}

# Increment retry count for a task
inc_retries() {
    local TASK_ID="$1"
    local RETRY_FILE="$RETRY_DIR/$TASK_ID.retries"
    local COUNT
    COUNT=$(get_retries "$TASK_ID")
    echo $((COUNT + 1)) > "$RETRY_FILE"
}

# Worker function — runs in background subshell, must not use set -e
worker() {
    local WORKER_ID="$1"
    local PROCESSED=0
    local FAILED=0
    local SKIPPED=0
    local CURRENT_TASK_ID=""
    local CURRENT_LOCK_FILE=""

    # Trap: on unexpected exit, release lock and reset task
    cleanup_worker() {
        if [[ -n "$CURRENT_TASK_ID" ]]; then
            echo "[Plan Worker $WORKER_ID] Unexpected exit — resetting $CURRENT_TASK_ID"
            bd update "$CURRENT_TASK_ID" --status open 2>/dev/null || true
        fi
        if [[ -n "$CURRENT_LOCK_FILE" && -f "$CURRENT_LOCK_FILE" ]]; then
            rm -f "$CURRENT_LOCK_FILE"
        fi
    }
    trap cleanup_worker EXIT

    echo "[Plan Worker $WORKER_ID] Started"

    while true; do
        CURRENT_TASK_ID=""
        CURRENT_LOCK_FILE=""

        # Fetch available tasks (open status only)
        TASKS=$(bd list --label plan --json 2>/dev/null | jq '[.[] | select(.status == "open")]' 2>/dev/null) || TASKS="[]"
        COUNT=$(echo "$TASKS" | jq 'length' 2>/dev/null) || COUNT=0

        if [[ "$COUNT" -eq 0 ]]; then
            echo "[Plan Worker $WORKER_ID] No more open tasks. Exiting."
            break
        fi

        # Try to claim a task (try each until one succeeds)
        CLAIMED=false
        ALL_MAXED=true
        for i in $(seq 0 $((COUNT - 1))); do
            TASK_ID=$(echo "$TASKS" | jq -r ".[$i].id" 2>/dev/null) || continue
            TASK_TITLE=$(echo "$TASKS" | jq -r ".[$i].title" 2>/dev/null) || continue

            # Check retry count
            RETRIES=$(get_retries "$TASK_ID")
            if [[ "$RETRIES" -ge "$MAX_RETRIES" ]]; then
                continue  # Skip — already tried too many times
            fi
            ALL_MAXED=false

            # Try to acquire lock
            LOCK_FILE="$LOCK_DIR/$TASK_ID.lock"
            if (set -o noclobber; echo $$ > "$LOCK_FILE") 2>/dev/null; then
                # Double-check task is still open
                CURRENT_STATUS=$(bd show "$TASK_ID" --json 2>/dev/null | jq -r '.[0].status' 2>/dev/null) || CURRENT_STATUS="unknown"
                if [[ "$CURRENT_STATUS" != "open" ]]; then
                    rm -f "$LOCK_FILE"
                    continue
                fi

                # Claim the task
                bd update "$TASK_ID" --status in_progress 2>/dev/null || true
                CURRENT_TASK_ID="$TASK_ID"
                CURRENT_LOCK_FILE="$LOCK_FILE"
                CLAIMED=true
                echo "[Plan Worker $WORKER_ID] Claimed: $TASK_ID - $TASK_TITLE (attempt $((RETRIES + 1))/$MAX_RETRIES)"
                log_activity "plan-parallel:w$WORKER_ID" "CLAIM" "$TASK_ID" "$TASK_TITLE" "attempt=$((RETRIES + 1))"
                break
            fi
        done

        if [[ "$CLAIMED" != true ]]; then
            if [[ "$ALL_MAXED" == true ]]; then
                echo "[Plan Worker $WORKER_ID] All remaining tasks hit max retries ($MAX_RETRIES). Exiting."
                break
            fi
            echo "[Plan Worker $WORKER_ID] Could not claim any task. Waiting..."
            sleep 5
            continue
        fi

        # Process the task
        echo "[Plan Worker $WORKER_ID] Processing $TASK_ID..."

        TASK_START=$SECONDS
        claude --dangerously-skip-permissions -p "Run /plan-issue $TASK_ID"
        EXIT_CODE=$?
        DURATION=$((SECONDS - TASK_START))

        # Release lock
        rm -f "$CURRENT_LOCK_FILE"

        if [[ "$EXIT_CODE" -ne 0 ]]; then
            echo "[Plan Worker $WORKER_ID] Task $TASK_ID failed (exit $EXIT_CODE)"
            log_activity "plan-parallel:w$WORKER_ID" "FAIL" "$TASK_ID" "$TASK_TITLE" "exit=$EXIT_CODE duration=${DURATION}s"
            FAILED=$((FAILED + 1))
            inc_retries "$TASK_ID"
            # Reset status on failure
            bd update "$TASK_ID" --status open 2>/dev/null || true
            CURRENT_TASK_ID=""
            CURRENT_LOCK_FILE=""

            if [[ "$CONTINUE_ON_ERROR" != true ]]; then
                break
            fi
        else
            echo "[Plan Worker $WORKER_ID] Task $TASK_ID completed"
            ensure_plan_body "$TASK_ID" "$TASK_TITLE" "Plan Worker $WORKER_ID"

            # Check if ensure_plan_body reverted the task (no plan content)
            CURRENT_LABEL=$(bd show "$TASK_ID" --json 2>/dev/null | jq -r '.[0].labels[0]' 2>/dev/null) || CURRENT_LABEL="unknown"
            if [[ "$CURRENT_LABEL" == "plan" ]]; then
                echo "[Plan Worker $WORKER_ID] Task $TASK_ID: no plan produced, counting as failed attempt"
                log_activity "plan-parallel:w$WORKER_ID" "NO_PLAN" "$TASK_ID" "$TASK_TITLE" "duration=${DURATION}s"
                inc_retries "$TASK_ID"
                FAILED=$((FAILED + 1))
            else
                log_activity "plan-parallel:w$WORKER_ID" "SUCCESS" "$TASK_ID" "$TASK_TITLE" "duration=${DURATION}s"
                PROCESSED=$((PROCESSED + 1))
            fi
            CURRENT_TASK_ID=""
            CURRENT_LOCK_FILE=""
        fi

        # Check max
        if [[ "$MAX_TASKS" -gt 0 && "$PROCESSED" -ge "$MAX_TASKS" ]]; then
            echo "[Plan Worker $WORKER_ID] Reached max tasks"
            break
        fi

        sleep 1
    done

    # Clear trap state so cleanup doesn't double-reset
    CURRENT_TASK_ID=""
    CURRENT_LOCK_FILE=""
    trap - EXIT

    echo "[Plan Worker $WORKER_ID] Finished. Processed: $PROCESSED, Failed: $FAILED, Skipped (max retries): $SKIPPED"
}

# Export for subshells
export CONTINUE_ON_ERROR MAX_TASKS MAX_RETRIES LOCK_DIR RETRY_DIR
export -f worker get_retries inc_retries

# Spawn workers
PIDS=()
for i in $(seq 1 $WORKERS); do
    worker "$i" &
    PIDS+=($!)
    sleep 1  # Stagger starts slightly
done

echo ""
echo "Spawned $WORKERS plan workers: ${PIDS[*]}"
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
echo "All plan workers finished"
echo "=========================================="

# Show final status and any maxed-out tasks
echo ""
echo "Final task status:"
echo "  Plan: $(bd list --label plan --json 2>/dev/null | jq 'length')"
echo "  Ready: $(bd list --label ready --json 2>/dev/null | jq 'length')"

if ls "$RETRY_DIR"/*.retries &>/dev/null; then
    echo ""
    echo "Retry counts:"
    for f in "$RETRY_DIR"/*.retries; do
        TASK_ID=$(basename "$f" .retries)
        COUNT=$(cat "$f")
        if [[ "$COUNT" -ge "$MAX_RETRIES" ]]; then
            echo "  $TASK_ID: $COUNT attempts (MAX REACHED)"
        else
            echo "  $TASK_ID: $COUNT attempts"
        fi
    done
fi

exit $TOTAL_EXIT
