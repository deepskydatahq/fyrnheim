# CLAUDE.md - Fyrnheim

Best practices and conventions for working on Fyrnheim.

---

## Overview

Fyrnheim is a Python-native dbt alternative that lets data teams define typed entities in Python and run transformations on any backend via Ibis.

**Target users:** Data engineers and analytics engineers on Python-first teams.

---

## Tech Stack

- **Language:** Python >=3.11
- **Core:** Pydantic, Ibis Framework, Click
- **Testing:** pytest, pytest-cov
- **Linting:** ruff, mypy
- **Package manager:** uv

---

## Development

```bash
# Install dependencies
uv sync --all-extras

# Run tests
uv run pytest

# Lint
uv run ruff check src/ tests/
uv run mypy src/

# CLI
uv run fyr --help
```

---

## Development Workflow

This project uses Beads (`bd`) as the task engine with a label-based pipeline and a structured product layer (Missions → Epics → Stories).

```
┌─────────────────────────────────────────────────────────────────────────┐
│  Product Layer (TOML files)                                             │
│                                                                         │
│   /brainstorm-epics ──► Mission TOML                                    │
│   /product-epic ──────► /product-mission-breakdown ──► Epic TOMLs       │
│                         /product-epic-breakdown ──► Story TOMLs          │
│                         /product-story-handoff ──► Beads tasks           │
│                                                                         │
├─────────────────────────────────────────────────────────────────────────┤
│  Task Pipeline (Beads labels)                                           │
│                                                                         │
│   /new-feature ──┐                                                      │
│   /retro ────────┼──► brainstorm ──► plan ──► ready                     │
│                  │       │             │         │                      │
│                  │  /brainstorm    /plan-issue  /pick-issue             │
│                  │  /brainstorm-auto           run-issue.sh             │
│                  │  brainstorm-issues.sh  plan-issues.sh               │
│                  │                                                      │
│                  └──────────────────────────────────────────┘           │
│                              (retro discovers more tasks)               │
│                                                                         │
├─────────────────────────────────────────────────────────────────────────┤
│  Validation                                                             │
│                                                                         │
│   /product-judgment ──► validates story → epic → mission                │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Task Statuses (Beads)

Tasks use Beads status + labels:

| Beads State | Label | Description |
|-------------|-------|-------------|
| `open` | `brainstorm` | Needs design exploration |
| `open` | `plan` | Needs implementation plan |
| `open` | `ready` | Has plan, ready to code |
| `in_progress` | — | Currently being worked on |
| `closed` | — | Completed |

### Beads CLI Reference

```bash
# List tasks by label
bd list --label brainstorm --json

# Show task details
bd show <id> --json

# Create task with label
bd create "Title" --labels brainstorm -d "description" --silent

# Update status/labels
bd update <id> --status in_progress
bd update <id> --remove-label brainstorm --add-label plan

# Close task
bd close <id>

# Check status
bd status
```

### Commands

| Command | Description |
|---------|-------------|
| `/new-feature` | Brainstorm idea → design doc → Beads task |
| `/plan-mission` | Plan a mission with codebase exploration and scope mapping |
| `/brainstorm-epics` | Generate mission candidates from vision + value ladder |
| `/brainstorm` | Interactive brainstorm for `brainstorm` queue |
| `/brainstorm-auto` | Autonomous brainstorm with expert personas (from `.claude/experts/`) |
| `/plan-issue` | Process `plan` queue |
| `/pick-issue` | Process `ready` queue |
| `/retro` | Post-implementation analysis |
| `/product-mission-breakdown` | Break mission into epics |
| `/product-epic-breakdown` | Break epic into stories |
| `/product-story-handoff` | Create Beads tasks from ready stories |
| `/product-judgment` | Validate story/epic/mission completion |

### Headless Automation

```bash
# Brainstorm tasks (brainstorm → plan)
./scripts/brainstorm-issues.sh              # Single task
./scripts/brainstorm-issues.sh --loop       # Process all brainstorm tasks
./scripts/brainstorm-issues.sh <task-id>    # Brainstorm specific task

# Plan tasks (plan → ready)
./scripts/plan-issues.sh                    # Single task
./scripts/plan-issues.sh --loop             # Process all plan tasks
./scripts/plan-issues.sh --max 5            # Limit to 5 tasks

# Implement tasks (ready → done)
./scripts/run-issue.sh                      # Single task
./scripts/run-issue.sh --loop               # Process all ready tasks
./scripts/run-issue.sh --max 5              # Limit to 5 tasks
```

All scripts support `--loop`, `--max N`, and `--continue-on-error` flags.

### Parallel Automation

For faster processing, use parallel workers with file-based locking:

```bash
# Parallel brainstorming (3 workers default)
./scripts/brainstorm-parallel.sh            # 3 workers
./scripts/brainstorm-parallel.sh -w 5       # 5 workers

# Parallel planning (3 workers default)
./scripts/plan-parallel.sh                  # 3 workers
./scripts/plan-parallel.sh -w 5             # 5 workers

# Parallel implementation with dependency awareness
./scripts/run-parallel.sh                   # 3 workers, respects task dependencies
./scripts/run-parallel.sh -w 5              # 5 workers
```

Workers automatically skip tasks with unresolved dependencies (registered via `bd dep add`).

---

## Product Thinking

The full development workflow (vision → value ladder → missions → epics → stories → tasks → implementation) is documented in [HOW_WE_WORK.md](./HOW_WE_WORK.md). That document is the primary reference for how ideas become shipped features.

### Quick Reference

```
VISION.md              ← "What transformation?" (rarely)
    ↓
VALUES.md              ← "What value?" (when levels change)
    ↓
product/missions/      ← Outcome-oriented work packages
    ↓
product/epics/         ← /product-mission-breakdown creates epic TOMLs
    ↓
product/stories/       ← /product-epic-breakdown creates story TOMLs
    ↓
Beads tasks            ← /product-story-handoff creates bd tasks
    ↓
Task Pipeline → /retro → /product-judgment validates up the hierarchy
```

### Product Commands

| Command | Artifact | Updates |
|---------|----------|---------|
| `/product-vision` | VISION.md | Rarely (pivots only) |
| `/product-values` | VALUES.md | When levels change |
| `/brainstorm-epics` | Mission candidates | From value levels + ideas |
| `/product-mission-breakdown` | Creates epic TOMLs from mission | Per mission |
| `/product-epic-breakdown` | Creates story TOMLs from epic | Per epic |
| `/product-story-handoff` | Creates Beads tasks from stories | When stories are ready |
| `/product-judgment` | Validates completion up hierarchy | After implementation |
| `/product-iteration` | Updates value ladder with learnings | After features |

### The Flow

1. **Direction:** `/product-vision` → `/product-values`
2. **Planning:** `/brainstorm-epics` → mission TOML → `/product-mission-breakdown` → `/product-epic-breakdown`
3. **Handoff:** `/product-story-handoff` → Beads tasks with `brainstorm` label
4. **Implementation:** Task pipeline (brainstorm → plan → ready → implement → close)
5. **Validation:** `/product-judgment` → validates story → epic → mission
6. **Learning:** `/product-iteration` → update value ladder → next cycle

---

## Testing

**Commands:**
```bash
uv run pytest                           # All tests
uv run pytest tests/test_cli.py         # Specific test file
uv run pytest -x                        # Stop on first failure
uv run pytest --cov=fyrnheim            # With coverage
```

**Key patterns:**
- Tests live in `tests/` directory
- Use `pytest` fixtures for setup
- Write tests for all new functions and modules

---

## Critical Rules

**Every feature must have tests**
- Write tests for all new functions and modules
- Never mark work complete until tests pass

**Work is not complete until pushed**
- Always push to remote before ending a session
- Create PRs for all feature branches

---

## Huginn Memory (Project-Scoped)

This project is registered in Huginn Memory as `project="fyrnheim"`.

When using MCP tools (`mcp__huginn-memory__*`), **always pass `project="fyrnheim"`** on these scoped tools:
- **Memory:** remember, recall, decide, forget, summarize
- **Tasks:** create_task, update_task, list_tasks, get_task, dismiss_task, surface_daily_candidates
- **Agents:** get_agent_profile, update_agent_profile, log_agent_execution, get_agent_metrics, log_feedback, get_feedback_summary, get_content_calendar, update_content_calendar

Global tools (daily entries, cashflow, time tracking, brainstorm, web distillation) do **not** take a project parameter.
