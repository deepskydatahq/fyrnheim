# Agent Instructions

Fyrnheim uses product TOML files as the canonical planning and implementation task system.

## Product Workflow

```text
VISION.md
  -> VALUES.md
  -> product/missions/*.toml
  -> product/epics/*.toml
  -> product/stories/*.toml
  -> commits / PRs
```

Stories are implementation tasks. Do **not** use Beads/`bd` unless the user explicitly asks for legacy Beads operations.

## Quick Reference

```bash
# In Pi
/mission-status M001          # Summarize mission, epics, and stories
/story-list ready             # List ready stories
/story-set-status <id> ready  # Update story status
/execute-mission M001         # Execute mission from TOML workflow
/retro                        # Discover follow-up stories

# Shell
uv run pytest
uv run ruff check src/ tests/
uv run mypy src/
git status --short --branch
```

## Story Statuses

Use these status values in `product/stories/*.toml`:

- `draft` — not ready for implementation
- `ready` — ready and unclaimed
- `in_progress` — currently being implemented
- `blocked` — cannot proceed without a decision or prerequisite
- `complete` — implemented, tested, and committed
- `failed` — attempted but not completed; include a failure reason

Use these triage values:

- `ready` — implement directly
- `plan` — inspect relevant files and make a short plan first
- `brainstorm` — compare approaches before planning/implementing

## Landing the Plane (Session Completion)

**When ending a work session**, complete all steps below. Work is not complete until `git push` succeeds.

1. **File remaining work** — Create or update product stories for follow-up work.
2. **Run quality gates** if code changed:
   ```bash
   uv run pytest
   uv run ruff check src/ tests/
   uv run mypy src/
   ```
3. **Update product status** — Mark completed stories/epics/missions in TOML; document blocked/failed work with reasons.
4. **Commit changes** — Use clear, scoped commit messages.
5. **Push to remote** — mandatory:
   ```bash
   git pull --rebase
   git push
   git status --short --branch
   ```
6. **Verify** — All intended changes are committed and pushed; working tree only contains intentional untracked local files.
7. **Hand off** — Summarize completed work, quality gates, remaining stories, and any blockers.

## Critical Rules

- Work is not complete until `git push` succeeds.
- Never stop before pushing committed work unless the user explicitly asks you not to push.
- Never say "ready to push when you are" — push it yourself when completing work.
- If push fails, resolve and retry until it succeeds or report the exact blocker.
- Product TOML files are the source of truth for task state.
