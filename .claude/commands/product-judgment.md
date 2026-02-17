---
description: Validate story/epic/mission completion against acceptance criteria
allowed-tools: Bash(uv run pytest*), Bash(uv run ruff*), Read, Write, Glob, Grep, Skill
---

# Product Judgment

Validate whether a product artifact (story, epic, or mission) has met its defined outcomes.

## Arguments

- Story: `/product-judgment M001-E001-S001` - Validate specific story
- Epic: `/product-judgment M001-E001` - Validate epic and all its stories
- Mission: `/product-judgment M001` - Validate mission and all its epics/stories
- In-progress: `/product-judgment --in-progress` - Validate all in-progress stories

## Instructions

Invoke the `product-judgment` skill with the provided artifact ID.

The skill will:
1. Read the artifact TOML and check each acceptance criterion
2. Examine actual code, tests, and test results as evidence
3. Document pass/fail for each criterion with specific evidence
4. Update TOML status to `complete` if all criteria pass
5. Cascade: story pass -> check epic -> check mission

## After Running

- Failed artifacts show actionable feedback (file paths, missing tests)
- Passed artifacts cascade validation up the hierarchy
- Run `/product-iteration` after a mission completes for retrospective
