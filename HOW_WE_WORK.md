# How We Work

Fyrnheim uses a structured product development workflow powered by Pi, product TOML files, git branches, and PRs. This document is the single source of truth for how ideas become shipped features.

For technical conventions, code patterns, and testing requirements, see [CLAUDE.md](./CLAUDE.md).

---

## The Hierarchy

```text
VISION.md             "What transformation are we enabling?"
    ↓
VALUES.md             "What value do we deliver?"
    ↓
product/missions/     "What do we need to prove?"
    ↓
product/epics/        "What chunks of work deliver this?"
    ↓
product/stories/      "What specific things do we build?"
    ↓
commits / PRs         "How does the implementation ship?"
```

Each level feeds the next. Validation flows back up: a completed story validates its epic, a completed epic validates its mission.

Product TOML files are the canonical task system. Do not create Beads/`bd` tasks unless explicitly working with the legacy workflow.

---

## Levels

### Vision (`VISION.md`)

The north star. What transformation does Fyrnheim enable? Updates rarely — only on fundamental pivots.

- **Updates:** Rarely, for pivots only
- **Question:** "What transformation are we enabling?"

### Value Ladder (`VALUES.md`)

The ordered progression of value Fyrnheim delivers. Each level is independently valuable and compounds on the previous levels.

- **Updates:** When missions complete or level statuses change
- **Question:** "What value do we deliver?"

### Missions (`product/missions/*.toml`)

Outcome-oriented work packages. Each mission proves something about the product and has a clear outcome, testing criteria, scope, and definition of done.

- **Naming:** `M001-short-description.toml`
- **Key fields:** `id`, `title`, `status`, `outcome.description`, `testing.criteria`, `scope`

### Epics (`product/epics/*.toml`)

Breakdown of missions into implementable chunks. Each epic delivers a working, testable piece of the mission.

- **Naming:** `M001-E001-short-description.toml`
- **Key fields:** `id`, `parent`, `title`, `status`, `depends_on`, `outcome.description`

### Stories (`product/stories/*.toml`)

Concrete implementation tasks with acceptance criteria. Stories should be small enough to complete in one focused implementation session.

- **Naming:** `M001-E001-S001-short-description.toml`
- **Key fields:** `id`, `parent`, `title`, `status`, `triage`, `acceptance_criteria`, `context.depends_on`

Stories replace the old Beads task layer. Story TOML files hold implementation state.

---

## Story State

### Status

| Status | Meaning |
|--------|---------|
| `draft` | Not ready for implementation |
| `ready` | Ready and unclaimed |
| `in_progress` | Currently being implemented |
| `blocked` | Cannot proceed without a decision or prerequisite |
| `complete` | Implemented, tested, and committed |
| `failed` | Attempted but not completed; failure reason documented |

### Triage

| Triage | Criteria | What Happens |
|--------|----------|--------------|
| `ready` | Specific paths, clear verifiable ACs, small/mechanical scope | Implement directly |
| `plan` | Clear goal but needs codebase exploration to identify files/steps | Explore, make a short plan, then implement |
| `brainstorm` | Multiple approaches, architectural decisions, ambiguity, cross-cutting scope | Compare approaches, choose simplest, plan, implement |

### Optional Execution Metadata

Stories may include an `[execution]` table:

```toml
[execution]
branch = ""
worktree = ""
pr = ""
commit = ""
started = ""
completed = ""
failure_reason = ""
```

---

## Pi Configuration

Project-local Pi resources live under `.pi/`:

```text
.pi/prompts/      Slash-command prompt templates
.pi/skills/       Product and testing skills
.pi/extensions/   Fyrnheim workflow commands and guardrails
```

Useful commands:

| Command | Purpose |
|---------|---------|
| `/plan-mission <idea>` | Explore code and create a mission TOML |
| `/execute-mission <Mxxx>` | Break down and execute a mission from TOML |
| `/fix-pr-feedback <pr>` | Fix actionable PR review feedback, max 2 rounds |
| `/retro` | Discover follow-up product stories after implementation |
| `/mission-status <Mxxx>` | Show a mission's epics and stories |
| `/story-list [status]` | List stories, optionally by status |
| `/story-set-status <story-id> <status>` | Update story status |
| `/fyrnheim-status` | Show git status and story counts |

---

## Execution Model

```text
/execute-mission M010
         │
         ▼
┌──────────────────────────────────┐
│ Phase 1: Breakdown + Triage       │
│                                   │
│ Mission → Epics → Stories         │
│ Triage stories:                   │
│   ready / plan / brainstorm       │
│ Write state into story TOML       │
└──────────┬───────────────────────┘
           ▼
┌──────────────────────────────────┐
│ Phase 2: Story Implementation     │
│                                   │
│ For each ready story:             │
│   claim in TOML                   │
│   plan/brainstorm if needed       │
│   implement + test                │
│   commit                          │
│   mark complete                   │
└──────────┬───────────────────────┘
           ▼
┌──────────────────────────────────┐
│ Phase 3: Validation + PR          │
│                                   │
│ Run quality gates                 │
│ Validate stories/epics/mission    │
│ Push branch and create PR         │
└──────────┬───────────────────────┘
           ▼
┌──────────────────────────────────┐
│ Phase 4: PR Feedback + Report     │
│                                   │
│ /fix-pr-feedback if needed        │
│ Report completed/blocked work     │
└──────────────────────────────────┘
```

For large missions, separate Pi sessions or git worktrees may execute independent epics in parallel, but the product TOML files remain the shared source of truth.

---

## Phase Details

### Phase 1: Breakdown + Triage

The agent reads the mission TOML, creates missing epics and stories, and assigns each story a triage label.

A good story has:

- one clear outcome
- specific acceptance criteria
- relevant paths
- dependencies listed in `context.depends_on`
- a triage label

### Phase 2: Story Implementation

For each story:

1. Verify dependencies are complete.
2. Set `status = "in_progress"`.
3. Follow the triage path:
   - `ready`: implement directly
   - `plan`: inspect code, write a short plan, implement
   - `brainstorm`: compare 2-3 approaches, choose simplest, implement
4. Add or update tests for acceptance criteria.
5. Run focused tests and relevant gates.
6. Commit the change.
7. Set `status = "complete"` and record metadata where useful.

If blocked, set `status = "blocked"` or `failed` and document the reason.

### Phase 3: Validation + PR

Use product judgment to validate whether stories/epics/missions achieved their stated outcomes. Run quality gates:

```bash
uv run pytest
uv run ruff check src/ tests/
uv run mypy src/
```

Push the branch and create a PR for feature work.

### Phase 4: PR Feedback + Report

Use `/fix-pr-feedback <pr-number>` to read automated review comments, classify feedback, fix actionable issues, and push updates. Maximum two rounds.

Final report includes:

- completed stories
- blocked/failed stories and reasons
- quality gate results
- commits and PRs
- remaining work

---

## Standalone Commands

### `/plan-mission <idea>`

Explores the codebase, presents scope options, and creates a mission TOML after scope is chosen.

### `/execute-mission <mission-id>`

Runs the mission workflow from breakdown through implementation, validation, and PR preparation using product TOML story state.

### `/fix-pr-feedback <pr-number>`

Reads PR review comments, filters actionable feedback from noise, fixes issues, pushes updates, and stops after at most two rounds.

### `/retro`

Post-implementation retrospective. Analyzes recent work, discovers follow-up issues, and creates product stories rather than Beads tasks.

---

## The Full Flow

1. **Direction:** Refine vision and value ladder as needed.
2. **Mission:** Define the mission TOML via `/plan-mission` or product planning.
3. **Breakdown:** Create epics and stories with triage labels.
4. **Execute:** Implement stories directly from `product/stories/*.toml`.
5. **Review:** Review and merge PRs.
6. **Retro:** Run `/retro` to discover follow-up stories.
7. **Learn:** Update value ladder and docs with learnings.

---

## Key Files

| File | Purpose |
|------|---------|
| `VISION.md` | North star transformation |
| `VALUES.md` | Value ladder — ordered progression of value levels |
| `CLAUDE.md` | Technical conventions, code patterns, testing rules |
| `HOW_WE_WORK.md` | Product development workflow |
| `AGENTS.md` | Agent operating instructions |
| `.pi/prompts/` | Pi slash-command prompt templates |
| `.pi/skills/` | Pi skills for product/testing workflows |
| `.pi/extensions/` | Pi extension commands and guardrails |
| `product/missions/` | Mission TOML files |
| `product/epics/` | Epic TOML files |
| `product/stories/` | Story TOML files, canonical implementation tasks |
| `.claude/archive/` | Archived commands and skills from previous workflow |
