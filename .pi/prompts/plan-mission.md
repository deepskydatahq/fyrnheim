---
description: Plan a Fyrnheim mission with codebase exploration, scope options, and structured TOML creation
argument-hint: "<mission idea or draft mission>"
---
# Plan Mission

Plan a new Fyrnheim mission through codebase exploration and scope definition. Produce a mission TOML grounded in actual code.

User request: $ARGUMENTS

## Instructions

1. Read `HOW_WE_WORK.md`, `CLAUDE.md`, `VISION.md`, and `VALUES.md`.
2. Clarify the intended outcome, why now, constraints, and non-goals. Ask before proceeding if the request is ambiguous.
3. Explore the relevant codebase areas:
   - blast radius
   - current state
   - dependencies
   - risks
   - test coverage
4. Present scope options before writing the mission:
   - full scope
   - reduced scope
   - phased approach
5. After the user chooses scope, define:
   - in scope
   - out of scope
   - acceptable breakage
   - success criteria
   - dependencies
6. Determine the next mission ID from `product/missions/`.
7. Create `product/missions/M{NNN}-{slug}.toml` using the project's mission structure.
8. Report the created file and recommended next steps.

## Key principles

- Explore before planning.
- Scope is a choice, not an accident.
- Success criteria must be specific and verifiable.
- Product TOML files are the source of truth; do not create Beads tasks.
