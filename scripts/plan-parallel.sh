#!/bin/bash
# Parallel plan workers
# Usage: ./plan-parallel.sh [--workers N] [--max N] [--continue-on-error]
#
# Spawns N parallel workers that each claim and process plan tasks.
# Uses file locking to prevent double-claiming.

set -e

source "$(dirname "$0")/lib/log.sh"

WORKERS=3
MAX_TASKS=0
CONTINUE_ON_ERROR=false
LOCK_DIR="/tmp/plan-locks"

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
            echo "Usage: ./plan-parallel.sh [--workers N] [--max N] [--continue-on-error]"
            exit 1
            ;;
    esac
done

# Create lock directory
mkdir -p "$LOCK_DIR"

# Cleanup old locks on start
find "$LOCK_DIR" -name "*.lock" -mmin +60 -delete 2>/dev/null || true

echo "=========================================="
echo "Parallel Planning"
echo "  Workers: $WORKERS"
echo "  Max tasks: ${MAX_TASKS:-unlimited}"
echo "  Lock dir: $LOCK_DIR"
echo "=========================================="
echo ""

log_activity "plan-parallel" "START" "-" "Parallel session" "workers=$WORKERS"

# Worker function
worker() {
    local WORKER_ID="$1"
    local PROCESSED=0
    local FAILED=0

    echo "[Plan Worker $WORKER_ID] Started"

    while true; do
        # Fetch available tasks (open status only)
        TASKS=$(bd list --label plan --json | jq '[.[] | select(.status == "open")]')
        COUNT=$(echo "$TASKS" | jq 'length')

        if [[ "$COUNT" -eq 0 ]]; then
            echo "[Plan Worker $WORKER_ID] No more open tasks. Exiting."
            break
        fi

        # Try to claim a task (try each until one succeeds)
        CLAIMED=false
        for i in $(seq 0 $((COUNT - 1))); do
            TASK_ID=$(echo "$TASKS" | jq -r ".[$i].id")
            TASK_TITLE=$(echo "$TASKS" | jq -r ".[$i].title")

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
                echo "[Plan Worker $WORKER_ID] Claimed: $TASK_ID - $TASK_TITLE"
                log_activity "plan-parallel:w$WORKER_ID" "CLAIM" "$TASK_ID" "$TASK_TITLE"
                break
            fi
        done

        if [[ "$CLAIMED" != true ]]; then
            echo "[Plan Worker $WORKER_ID] Could not claim any task. Waiting..."
            sleep 5
            continue
        fi

        # Process the task
        echo "[Plan Worker $WORKER_ID] Processing $TASK_ID..."

        if [[ "$CONTINUE_ON_ERROR" == true ]]; then
            set +e
        fi

        TASK_START=$SECONDS
        claude --dangerously-skip-permissions -p "Run /plan-issue $TASK_ID"
        EXIT_CODE=$?
        DURATION=$((SECONDS - TASK_START))

        set -e

        # Release lock
        rm -f "$LOCK_FILE"

        if [[ "$EXIT_CODE" -ne 0 ]]; then
            echo "[Plan Worker $WORKER_ID] Task $TASK_ID failed (exit $EXIT_CODE)"
            log_activity "plan-parallel:w$WORKER_ID" "FAIL" "$TASK_ID" "$TASK_TITLE" "exit=$EXIT_CODE duration=${DURATION}s"
            FAILED=$((FAILED + 1))
            # Reset status on failure
            bd update "$TASK_ID" --status open 2>/dev/null || true

            if [[ "$CONTINUE_ON_ERROR" != true ]]; then
                break
            fi
        else
            echo "[Plan Worker $WORKER_ID] Task $TASK_ID completed"
            ensure_plan_body "$TASK_ID" "$TASK_TITLE" "Plan Worker $WORKER_ID"
            log_activity "plan-parallel:w$WORKER_ID" "SUCCESS" "$TASK_ID" "$TASK_TITLE" "duration=${DURATION}s"
            PROCESSED=$((PROCESSED + 1))
        fi

        # Check max
        if [[ "$MAX_TASKS" -gt 0 && "$PROCESSED" -ge "$MAX_TASKS" ]]; then
            echo "[Plan Worker $WORKER_ID] Reached max tasks"
            break
        fi

        sleep 1
    done

    echo "[Plan Worker $WORKER_ID] Finished. Processed: $PROCESSED, Failed: $FAILED"
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

# Show final status
echo ""
echo "Final task status:"
echo "  Plan: $(bd list --label plan --json | jq 'length')"
echo "  Ready: $(bd list --label ready --json | jq 'length')"

exit $TOTAL_EXIT
