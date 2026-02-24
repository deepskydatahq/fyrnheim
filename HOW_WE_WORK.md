# How We Work

This project uses a structured product development workflow powered by Claude Code and Beads task management. This document is the single source of truth for how ideas become shipped features.

For technical conventions, code patterns, and testing requirements, see [CLAUDE.md](./CLAUDE.md).

---

## The Hierarchy

```
VISION.md           "What transformation are we enabling?"
    ↓
VALUES.md           "What value do we deliver?"
    ↓
product/missions/   "What do we need to prove?"
    ↓
product/epics/      "What chunks of work deliver this?"
    ↓
product/stories/    "What specific things do we build?"
    ↓
Beads tasks         "What's the implementation work?"
    ↓
brainstorm → plan → implement → validate
```

Each level feeds the next. Validation flows back up — a completed story validates its epic, a completed epic validates its mission.

---

## Levels

### Vision (VISION.md)

The north star. What transformation does this project enable? Updates rarely — only on fundamental pivots.

- **Command:** `/product-vision`
- **Updates:** Rarely (pivots only)
- **Question:** "What transformation are we enabling?"

### Value Ladder (VALUES.md)

The ordered progression of value delivered. Each level independently valuable, each compounding on the last.

- **Command:** `/product-values`
- **Updates:** When missions complete or level statuses change
- **Question:** "What value do we deliver?"

### Missions (product/missions/*.toml)

Outcome-oriented work packages. Each mission proves something about the product — it has a clear outcome, testing criteria, and a definition of done.

- **Command:** `/brainstorm-epics` to generate mission candidates
- **Naming:** `M001-short-description.toml`
- **Key fields:** `id`, `title`, `status`, `outcome.description`, `testing.criteria`

### Epics (product/epics/*.toml)

Breakdown of missions into implementable chunks. Each epic delivers a working, testable piece.

- **Command:** `/product-mission-breakdown`
- **Naming:** `M001-E001-short-description.toml`
- **Key fields:** `id`, `parent`, `title`, `status`, `outcome.description`

### Stories (product/stories/*.toml)

Concrete, implementable work items with acceptance criteria. Small enough to complete in one session.

- **Command:** `/product-epic-breakdown`
- **Naming:** `M001-E001-S001-short-description.toml`
- **Key fields:** `id`, `parent`, `title`, `status`, `acceptance_criteria`

### Beads Tasks

Implementation tasks tracked with the `bd` CLI. Stories become Beads tasks via handoff.

- **Command:** `/product-story-handoff`
- **Tool:** `bd` (Beads CLI)

---

## Task Pipeline

Once stories become Beads tasks, they flow through the pipeline:

```
brainstorm ──► plan ──► ready ──► implement ──► close
```

### Stages

| Stage | What Happens | Trigger |
|-------|-------------|---------|
| **brainstorm** | Design exploration, consider approaches | `/brainstorm` or `/brainstorm-auto` |
| **plan** | Write implementation plan, identify files | `/plan-issue` |
| **ready** | Has plan, ready to code | Automatic after planning |
| **implement** | Write code, write tests, verify | `/pick-issue` or `run-issue.sh` |
| **close** | Done, tests pass | `bd close <id>` |

### Beads CLI Quick Reference

```bash
bd list --label brainstorm          # Tasks needing design
bd list --label plan                # Tasks needing plans
bd list --label ready               # Tasks ready to implement
bd show <id> --json                 # Full task details
bd update <id> --status in_progress # Start working
bd close <id>                       # Mark complete
```

---

## Automation

### Sequential (one at a time)

```bash
./scripts/brainstorm-issues.sh              # Brainstorm next task
./scripts/plan-issues.sh                    # Plan next task
./scripts/run-issue.sh                      # Implement next task
```

All support `--loop` (process all), `--max N` (limit count), and specific task IDs.

### Parallel (multiple workers)

```bash
./scripts/brainstorm-parallel.sh -w 5       # 5 brainstorm workers
./scripts/plan-parallel.sh -w 5             # 5 planning workers
./scripts/run-parallel.sh -w 5              # 5 implementation workers
```

Workers use file-based locking to avoid conflicts. Implementation workers respect task dependencies registered via `bd dep add`.

### Activity Log

All automation scripts write to a daily JSONL activity log in `logs/`. Each entry records what happened, when, and which worker did it.

```bash
# Today's log
cat logs/activity-$(date -u +%Y-%m-%d).jsonl | jq .

# Find failures
grep FAIL logs/activity-*.jsonl

# Filter by script
grep brainstorm-parallel logs/activity-*.jsonl | jq .
```

Entry format:
```json
{"ts":"2026-02-22T14:32:15Z","src":"brainstorm-parallel:w3","act":"CLAIM","task":"task-123","title":"Design lifecycle types","detail":""}
```

| Field | Description |
|-------|-------------|
| `ts` | ISO 8601 UTC timestamp |
| `src` | Script name + worker ID (e.g., `plan-parallel:w2`, `run-issue`) |
| `act` | Action: `START`, `CLAIM`, `SUCCESS`, `FAIL`, `SKIP`, `RESET` |
| `task` | Beads task ID (`-` for session-level events) |
| `title` | Task title |
| `detail` | Duration, exit code, skip reason, etc. |

Log files are gitignored. The shared logging function lives in `scripts/lib/log.sh`.

---

## Validation

`/product-judgment` validates completion up the hierarchy:

```
story complete? ──► epic complete? ──► mission complete?
```

Each level checks its acceptance criteria / testing criteria against what was actually built.

---

## The Full Flow

1. **Direction:** `/product-vision` → `/product-values`
2. **Planning:** `/plan-mission` (or `/brainstorm-epics`) → mission TOML → `/product-mission-breakdown` → `/product-epic-breakdown`
3. **Handoff:** `/product-story-handoff` → Beads tasks with `brainstorm` label
4. **Implementation:** brainstorm → plan → ready → implement → close
5. **Validation:** `/product-judgment` → validates story → epic → mission
6. **Learning:** `/product-iteration` → update value ladder → next cycle

---

## Slash Commands Reference

### Product Layer

| Command | Creates | From |
|---------|---------|------|
| `/product-vision` | VISION.md | Strategy discussion |
| `/product-values` | VALUES.md | Vision + learnings |
| `/plan-mission` | Mission TOML (with codebase exploration) | User's mission idea |
| `/brainstorm-epics` | Mission candidates | Value levels + ideas |
| `/product-mission-breakdown` | Epic TOMLs | Mission TOML |
| `/product-epic-breakdown` | Story TOMLs | Epic TOML |
| `/product-story-handoff` | Beads tasks | Story TOMLs |
| `/product-judgment` | Validation result | Completed work |
| `/product-iteration` | Updated value ladder | Completed features |

### Task Pipeline

| Command | Does | Stage Transition |
|---------|------|-----------------|
| `/brainstorm` | Interactive design exploration | brainstorm → plan |
| `/brainstorm-auto` | Autonomous brainstorm with expert personas | brainstorm → plan |
| `/plan-issue` | Write implementation plan | plan → ready |
| `/pick-issue` | Implement next ready task | ready → done |
| `/new-feature` | Create task from idea | → brainstorm |
| `/retro` | Post-implementation retrospective | → discovers new tasks |

---

## Key Files

| File | Purpose |
|------|---------|
| `VISION.md` | North star transformation |
| `VALUES.md` | Value ladder — ordered progression of value levels |
| `CLAUDE.md` | Technical conventions, code patterns, testing rules |
| `HOW_WE_WORK.md` | This document — the development workflow |
| `product/missions/` | Mission TOML files |
| `product/epics/` | Epic TOML files |
| `product/stories/` | Story TOML files |
| `scripts/lib/log.sh` | Shared activity logging function for automation scripts |
