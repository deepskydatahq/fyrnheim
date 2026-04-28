---
name: product-mission-breakdown
description: Breaks Fyrnheim mission TOML files into product epics. Use when creating product/epics files from a product/missions TOML, or when checking mission-to-epic coverage.
---

# Product Mission Breakdown

Use this skill to turn a mission into 3-6 scoped epics.

## Steps

1. Read `HOW_WE_WORK.md`, `CLAUDE.md`, and the mission file at `product/missions/{mission_id}-*.toml`.
2. Extract the mission outcome, user progress statement, testing criteria, scope, relevant paths, and notes.
3. Review the relevant paths to understand current code and constraints.
4. Identify epic boundaries around independently shippable work:
   - core logic vs interfaces
   - data model changes vs execution changes
   - tests/infrastructure vs production behavior
   - dependency ordering
5. Create `product/epics/{mission_id}-E{NNN}-{slug}.toml` for each epic.
6. Verify coverage:
   - every mission outcome is addressed
   - no major gaps or overlaps
   - dependencies are explicit

## Epic requirements

Each epic should include:

- `id`, `parent`, `title`, `status`, `created`, `depends_on`
- `[outcome].description`
- `[job_story].description`
- `[testing].approach`, `criteria`, and `validator_context`
- `[context].relevant_paths` and `dependencies`
- `[notes].considerations`
- `estimated_stories`

Set new epics to `status = "ready"` unless they need human refinement, in which case use `status = "draft"` and explain why.

## Output

Report:

- epics created or updated
- how each epic maps to the mission outcome
- dependencies and suggested sequencing
- any gaps requiring human review
