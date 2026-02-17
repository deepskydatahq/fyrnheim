# CLAUDE.md - Fyrnheim

Best practices and conventions for working on Fyrnheim.

---

## Overview

Fyrnheim — vision and tech stack to be defined via `/product-vision`.

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
| `/brainstorm-epics` | Generate mission candidates from vision + roadmap |
| `/brainstorm` | Interactive brainstorm for `brainstorm` queue |
| `/brainstorm-auto` | Autonomous brainstorm with expert personas (from `.claude/experts/`) |
| `/plan-issue` | Process `plan` queue |
| `/pick-issue` | Process `ready` queue |
| `/retro` | Post-implementation analysis |
| `/product-mission-breakdown` | Break mission into epics |
| `/product-epic-breakdown` | Break epic into stories |
| `/product-story-handoff` | Create Beads tasks from ready stories |
| `/product-judgment` | Validate story/epic/mission completion |

---

## Product Thinking

Strategic product commands that sit above the development workflow, using a structured hierarchy of TOML files.

```
VISION.md              ← "What transformation?" (rarely)
    ↓
ROADMAP.md             ← "Where investing?" (periodic)
    ↓
HYPOTHESES.md          ← "What bets?" (living)
    ↓
product/missions/      ← /product-epic creates mission TOML (per hypothesis)
    ↓
product/epics/         ← /product-mission-breakdown creates epic TOMLs
    ↓
product/stories/       ← /product-epic-breakdown creates story TOMLs
    ↓
Beads tasks            ← /product-story-handoff creates bd tasks
    ↓
Task Pipeline → /retro → /product-judgment validates up the hierarchy
    ↓
product iteration      ← "What did we learn?"
```

### Product Commands

| Command | Artifact | Updates |
|---------|----------|---------|
| `/product-vision` | VISION.md | Rarely (pivots only) |
| `/product-roadmap` | ROADMAP.md | Periodic (monthly/quarterly) |
| `/product-hypotheses` | HYPOTHESES.md | Constantly (living) |
| `/product-epic` | Creates mission TOML + breakdowns | Per hypothesis |
| `/product-mission-breakdown` | Creates epic TOMLs from mission | Per mission |
| `/product-epic-breakdown` | Creates story TOMLs from epic | Per epic |
| `/product-story-handoff` | Creates Beads tasks from stories | When stories are ready |
| `/product-judgment` | Validates completion up hierarchy | After implementation |
| `/product-iteration` | Updates HYPOTHESES.md | After features |

### The Flow

1. **Starting:** `/product-vision` → `/product-roadmap` → `/product-hypotheses`
2. **Planning:** `/product-epic` → mission TOML → `/product-mission-breakdown` → `/product-epic-breakdown`
3. **Handoff:** `/product-story-handoff` → Beads tasks with `brainstorm` label
4. **Implementation:** Task pipeline (brainstorm → plan → ready → implement → close)
5. **Validation:** `/product-judgment` → validates story → epic → mission
6. **Learning:** `/product-iteration` → update hypotheses → next cycle
