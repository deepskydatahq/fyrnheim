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

This project uses Beads (`bd`) as the task engine with a structured product layer (Missions → Epics → Stories) and autonomous execution via Claude Code sub-agents.

```
┌─────────────────────────────────────────────────────────────────────────┐
│  Product Layer (TOML files)                                             │
│                                                                         │
│   product/missions/ → product/epics/ → product/stories/                 │
│                                                                         │
├─────────────────────────────────────────────────────────────────────────┤
│  Execution (Claude Code sub-agents)                                     │
│                                                                         │
│   /execute-mission M010                                                 │
│     Phase 1: Breakdown + LLM triage → Beads tasks                       │
│     Phase 2: Epic sub-agents (parallel, worktree) → PRs                 │
│     Phase 3: /fix-pr-feedback → address review comments                 │
│     Phase 4: Report                                                     │
│                                                                         │
├─────────────────────────────────────────────────────────────────────────┤
│  Post-Implementation                                                    │
│                                                                         │
│   /retro → discover follow-up issues → Beads tasks                      │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Commands

| Command | Description |
|---------|-------------|
| `/execute-mission` | Full autopilot: breakdown → triage → implement → PR feedback → report |
| `/fix-pr-feedback` | Read PR review comments, fix actionable feedback (max 2 rounds) |
| `/retro` | Post-implementation retrospective, discover follow-up issues |
| `/plan-mission` | Plan a mission with codebase exploration and scope mapping |
| `/brainstorm-epics` | Generate mission candidates from vision + value ladder |
| `/product-judgment` | Validate story/epic/mission completion |

### Beads CLI Reference

```bash
bd list --label ready               # Tasks ready to implement
bd show <id> --json                 # Full task details
bd update <id> --status in_progress # Start working
bd close <id>                       # Mark complete
bd status                           # Overview
```

---

## Product Thinking

The full development workflow is documented in [HOW_WE_WORK.md](./HOW_WE_WORK.md). That document is the primary reference for how ideas become shipped features.

### The Flow

1. **Direction:** Refine vision and value ladder as needed
2. **Mission:** Define the mission TOML (via `/plan-mission` or `/brainstorm-epics`)
3. **Execute:** `/execute-mission M010` — breakdown, triage, implement, PR feedback — all autonomous
4. **Review:** Review and merge epic PRs
5. **Retro:** `/retro` — discover follow-up issues
6. **Learn:** Update value ladder with learnings

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
