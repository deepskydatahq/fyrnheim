#!/bin/bash
# Parallel brainstorm workers
# Usage: ./brainstorm-parallel.sh [--workers N] [--max N] [--continue-on-error]
#
# Spawns N parallel workers that each claim and process brainstorm tasks.
# Uses file locking to prevent double-claiming.

set -euo pipefail

source "$(dirname "$0")/lib/log.sh"

# Disable set -e for worker subshells — workers handle errors via trap + explicit checks
set +e

WORKERS=3
MAX_TASKS=0
CONTINUE_ON_ERROR=false
LOCK_DIR="/tmp/brainstorm-locks"

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
            echo "Usage: ./brainstorm-parallel.sh [--workers N] [--max N] [--continue-on-error]"
            exit 1
            ;;
    esac
done

# Create lock directory
mkdir -p "$LOCK_DIR"

# Cleanup old locks on start
find "$LOCK_DIR" -name "*.lock" -mmin +60 -delete 2>/dev/null || true

echo "=========================================="
echo "Parallel Brainstorm"
echo "  Workers: $WORKERS"
echo "  Max tasks: ${MAX_TASKS:-unlimited}"
echo "  Lock dir: $LOCK_DIR"
echo "=========================================="
echo ""

log_activity "brainstorm-parallel" "START" "-" "Parallel session" "workers=$WORKERS"

# Worker function — runs in background subshell, must not use set -e
worker() {
    local WORKER_ID="$1"
    local PROCESSED=0
    local FAILED=0
    local CURRENT_TASK_ID=""
    local CURRENT_LOCK_FILE=""

    # Trap: on unexpected exit, release lock and reset task
    cleanup_worker() {
        if [[ -n "$CURRENT_TASK_ID" ]]; then
            echo "[Worker $WORKER_ID] Unexpected exit — resetting $CURRENT_TASK_ID"
            bd update "$CURRENT_TASK_ID" --status open 2>/dev/null || true
        fi
        if [[ -n "$CURRENT_LOCK_FILE" && -f "$CURRENT_LOCK_FILE" ]]; then
            rm -f "$CURRENT_LOCK_FILE"
        fi
    }
    trap cleanup_worker EXIT

    echo "[Worker $WORKER_ID] Started"

    while true; do
        CURRENT_TASK_ID=""
        CURRENT_LOCK_FILE=""

        # Fetch available tasks (open status only)
        TASKS=$(bd list --label brainstorm --json 2>/dev/null | jq '[.[] | select(.status == "open")]' 2>/dev/null) || TASKS="[]"
        COUNT=$(echo "$TASKS" | jq 'length' 2>/dev/null) || COUNT=0

        if [[ "$COUNT" -eq 0 ]]; then
            echo "[Worker $WORKER_ID] No more open tasks. Exiting."
            break
        fi

        # Try to claim a task (try each until one succeeds)
        CLAIMED=false
        for i in $(seq 0 $((COUNT - 1))); do
            TASK_ID=$(echo "$TASKS" | jq -r ".[$i].id" 2>/dev/null) || continue
            TASK_TITLE=$(echo "$TASKS" | jq -r ".[$i].title" 2>/dev/null) || continue

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
                echo "[Worker $WORKER_ID] Claimed: $TASK_ID - $TASK_TITLE"
                log_activity "brainstorm-parallel:w$WORKER_ID" "CLAIM" "$TASK_ID" "$TASK_TITLE"
                break
            fi
        done

        if [[ "$CLAIMED" != true ]]; then
            echo "[Worker $WORKER_ID] Could not claim any task. Waiting..."
            sleep 5
            continue
        fi

        # Process the task
        echo "[Worker $WORKER_ID] Processing $TASK_ID..."

        TASK_START=$SECONDS
        claude --dangerously-skip-permissions -p "Run /brainstorm-auto $TASK_ID"
        EXIT_CODE=$?
        DURATION=$((SECONDS - TASK_START))

        # Release lock
        rm -f "$CURRENT_LOCK_FILE"

        if [[ "$EXIT_CODE" -ne 0 ]]; then
            echo "[Worker $WORKER_ID] Task $TASK_ID failed (exit $EXIT_CODE)"
            log_activity "brainstorm-parallel:w$WORKER_ID" "FAIL" "$TASK_ID" "$TASK_TITLE" "exit=$EXIT_CODE duration=${DURATION}s"
            FAILED=$((FAILED + 1))
            # Reset status on failure
            bd update "$TASK_ID" --status open 2>/dev/null || true
            CURRENT_TASK_ID=""
            CURRENT_LOCK_FILE=""

            if [[ "$CONTINUE_ON_ERROR" != true ]]; then
                break
            fi
        else
            echo "[Worker $WORKER_ID] Task $TASK_ID completed"
            ensure_brainstorm_body "$TASK_ID" "$TASK_TITLE" "Worker $WORKER_ID"
            log_activity "brainstorm-parallel:w$WORKER_ID" "SUCCESS" "$TASK_ID" "$TASK_TITLE" "duration=${DURATION}s"
            PROCESSED=$((PROCESSED + 1))
            CURRENT_TASK_ID=""
            CURRENT_LOCK_FILE=""
        fi

        # Check max
        if [[ "$MAX_TASKS" -gt 0 && "$PROCESSED" -ge "$MAX_TASKS" ]]; then
            echo "[Worker $WORKER_ID] Reached max tasks"
            break
        fi

        sleep 1
    done

    # Clear trap state so cleanup doesn't double-reset
    CURRENT_TASK_ID=""
    CURRENT_LOCK_FILE=""
    trap - EXIT

    echo "[Worker $WORKER_ID] Finished. Processed: $PROCESSED, Failed: $FAILED"
}

# Export for subshells
export CONTINUE_ON_ERROR MAX_TASKS LOCK_DIR

# Spawn workers
PIDS=()
for i in $(seq 1 $WORKERS); do
    worker "$i" &
    PIDS+=($!)
    sleep 1  # Stagger starts slightly
done

echo ""
echo "Spawned $WORKERS workers: ${PIDS[*]}"
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
echo "All workers finished"
echo "=========================================="

# Show final status
echo ""
echo "Final task status:"
echo "  Brainstorm: $(bd list --label brainstorm --json | jq 'length')"
echo "  Plan: $(bd list --label plan --json | jq 'length')"
echo "  Ready: $(bd list --label ready --json | jq 'length')"

exit $TOTAL_EXIT
