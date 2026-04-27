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

# Lint/type check
uv run ruff check src/ tests/
uv run mypy src/

# CLI
uv run fyr --help
```

---

## Development Workflow

Fyrnheim uses a structured product layer as the canonical task system. Product TOML files are the source of truth; do not create Beads/`bd` tasks unless explicitly asked to work with the legacy workflow.

```text
VISION.md
  -> VALUES.md
  -> product/missions/*.toml
  -> product/epics/*.toml
  -> product/stories/*.toml
  -> commits / PRs
```

Stories are implementation tasks. Story status and triage live in the story TOML.

### Story Status Values

- `draft` — not ready for implementation
- `ready` — ready and unclaimed
- `in_progress` — currently being implemented
- `blocked` — cannot proceed without a decision or prerequisite
- `complete` — implemented, tested, and committed
- `failed` — attempted but not completed; include a failure reason

### Story Triage Values

- `ready` — implement directly
- `plan` — inspect relevant files and make a short plan first
- `brainstorm` — compare approaches before planning/implementing

### Pi Commands

| Command | Description |
|---------|-------------|
| `/plan-mission` | Plan a mission with codebase exploration and scope mapping |
| `/execute-mission` | Break down and execute a mission using product TOML stories |
| `/fix-pr-feedback` | Read PR review comments and fix actionable feedback, max 2 rounds |
| `/retro` | Post-implementation retrospective; discover follow-up stories |
| `/mission-status` | Summarize a mission's epics and stories |
| `/story-list` | List stories, optionally filtered by status |
| `/story-set-status` | Update story status in TOML |
| `/fyrnheim-status` | Show git status and story counts |

---

## Product Thinking

The full development workflow is documented in [HOW_WE_WORK.md](./HOW_WE_WORK.md). That document is the primary reference for how ideas become shipped features.

### The Flow

1. **Direction:** Refine vision and value ladder as needed.
2. **Mission:** Define the mission TOML via `/plan-mission` or product planning.
3. **Breakdown:** Mission → epics → stories with explicit acceptance criteria.
4. **Execute:** Implement stories directly from `product/stories/*.toml`.
5. **Review:** Open and review PRs for completed epic/mission branches.
6. **Retro:** `/retro` discovers follow-up stories.
7. **Learn:** Update value ladder and product docs with learnings.

---

## Testing

**Commands:**

```bash
uv run pytest                           # All tests
uv run pytest tests/test_cli.py         # Specific test file
uv run pytest -x                        # Stop on first failure
uv run pytest --cov=fyrnheim            # With coverage
uv run ruff check src/ tests/           # Lint
uv run mypy src/                        # Type check
```

**Key patterns:**

- Tests live in `tests/`.
- Use `pytest` fixtures for setup.
- Write tests for all new functions and modules.
- Prefer behavior-focused tests tied to story acceptance criteria.

---

## Critical Rules

**Every feature must have tests**

- Write tests for all new functions and modules.
- Never mark a story complete until relevant tests pass or a documented blocker exists.

**Product TOML is the task source of truth**

- Update story status as work progresses.
- Add follow-up stories for remaining work.
- Do not duplicate story state into Beads.

**Work is not complete until pushed**

- Commit completed work.
- Push to remote before ending a session unless explicitly instructed not to.
- Create PRs for feature branches when appropriate.
