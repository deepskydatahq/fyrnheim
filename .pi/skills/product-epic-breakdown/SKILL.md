---
name: product-epic-breakdown
description: Breaks Fyrnheim epic TOML files into implementable story TOMLs. Use when creating product/stories files from a product/epics TOML or checking story quality.
---

# Product Epic Breakdown

Use this skill to turn an epic into small implementation stories.

## Steps

1. Read `HOW_WE_WORK.md`, `CLAUDE.md`, the epic file at `product/epics/{epic_id}-*.toml`, and its parent mission.
2. Extract the epic outcome, job story, testing criteria, relevant paths, and dependencies.
3. Review the relevant paths to understand existing code patterns.
4. Break the epic into stories that:
   - are doable in one implementation session
   - have one clear purpose
   - are testable in isolation
   - result in working code, not only scaffolding
5. Write executable acceptance criteria whenever possible.
6. Create `product/stories/{epic_id}-S{NNN}-{slug}.toml` files.
7. Assign each story:
   - `status = "ready"` when it can be implemented
   - `triage = "ready" | "plan" | "brainstorm"`
   - dependencies in `[context].depends_on`

## Triage labels

Use `triage = "ready"` when:
- file paths are specific
- changes are mechanical or obvious
- scope is small
- no design decision is needed

Use `triage = "plan"` when:
- acceptance criteria are clear
- implementation approach is likely obvious
- codebase exploration is needed to identify exact files or steps

Use `triage = "brainstorm"` when:
- there are multiple plausible approaches
- architecture or API decisions are needed
- scope is unclear or cross-cutting

## Story requirements

Each story should include:

- `id`, `parent`, `title`, `status`, `triage`, `created`
- `[outcome].description`
- `[acceptance_criteria].executable`
- one or more `[[acceptance_criteria.criteria]]` entries
- `[context].relevant_paths`, `input_fixtures`, and `depends_on`
- `[handoff].implementation_hints` and `reference_files`
- optional `[execution]` metadata for branch, PR, commit, failure reason

## Output

Report:

- stories created or updated
- implementation order
- parallelizable groups
- stories needing human refinement
