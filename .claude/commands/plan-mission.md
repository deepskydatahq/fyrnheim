---
description: Plan a mission with deep codebase exploration, scope mapping, and structured TOML creation
allowed-tools: Bash(git:*), Bash(bd:*), Skill, Task, Read, Write, Glob, Grep
---

# Plan Mission

Plan a new mission through structured codebase exploration and scope definition. Produces a mission TOML grounded in actual code, not guesswork.

## When to Use

- When you know *what* you want to do but need to understand the *scope*
- Before any significant refactoring, new feature, or architectural change
- When a mission idea needs codebase reality to become a concrete plan

## Why This Exists

Mission planning without codebase exploration produces vague TOMLs that lead to surprise scope during implementation. This command front-loads the exploration so the mission TOML reflects reality.

## Arguments

- Freeform description of the mission idea (e.g., `/plan-mission add real-time collaboration to the editor`)
- Or a reference to an existing draft mission TOML

## Instructions

### Phase 1: Understand Intent

Confirm the mission idea with the user. Clarify:
- **What** they want to accomplish (the outcome, not the tasks)
- **Why now** (what makes this timely)
- **Constraints** (what's acceptable to break, what must be preserved)
- **Non-goals** (what this mission explicitly does NOT include)

If anything is ambiguous, ask before proceeding. Don't assume.

### Phase 2: Codebase Exploration

This is the most important phase. Use Task agents (Explore type) to map the actual codebase state relevant to the mission. Run multiple explorations in parallel.

**For every mission, explore:**

1. **Blast radius** — What files/packages/modules are affected?
   - Count files, lines of code, dependencies
   - Map which packages touch which

2. **Current state** — What exists today that's relevant?
   - Key interfaces, types, patterns in use
   - Test coverage of affected areas
   - Any existing abstractions or extension points

3. **Dependencies** — What depends on what we're changing?
   - Internal dependencies (other packages importing from affected code)
   - External dependencies (npm packages, config files, CI/CD)
   - Data dependencies (schemas, migrations, stored data)

4. **Risk map** — What could go wrong?
   - What breaks if we change X?
   - What's hard to test?
   - What's hard to reverse?

Present findings as a structured summary before proceeding. The user should see the scope *before* committing to the mission.

### Phase 3: Scope Decision

Based on exploration findings, present the user with:

```
## Exploration Summary

### Blast Radius
- N files across M packages
- [specific breakdown]

### Key Findings
- [important discoveries from exploration]

### Risk Areas
- [things that could go wrong]

### Scope Options

**Option A: Full scope**
[what this includes, estimated effort]

**Option B: Reduced scope**
[what to defer, what to include]

**Option C: Phased approach**
[phase 1 = X, phase 2 = Y]
```

Let the user decide scope before creating the mission TOML. This is the key decision point.

### Phase 4: Define Mission Boundaries

Based on the scope decision, define explicitly:

- **In scope:** [specific list of what this mission covers]
- **Out of scope:** [what's explicitly deferred]
- **Acceptable breakage:** [what's OK to break, with labels/docs]
- **Success criteria:** [how we know it's done — specific, verifiable]
- **Dependencies:** [what must exist before this mission starts]

### Phase 5: Create Mission TOML

Determine the next mission ID from existing files in `product/missions/`.

Create `product/missions/M{NNN}-{slug}.toml` with this structure:

```toml
# Mission: {Title}
# {One-line description of what this accomplishes}

id = "M{NNN}"
title = "{Title}"
status = "draft"
created = {today's date}
depends_on = [{list of prerequisite mission IDs if any}]

[outcome]
description = """
{2-4 sentences describing the desired end state.
Be specific about what changes and what stays the same.
Reference the codebase exploration findings.}
"""

user_progress = "From '{current state}' to '{desired state}'"

[testing]
approach = "agent-judgment"
criteria = [
    {list of specific, verifiable criteria from Phase 4}
]

validator_context = [
    {list of key paths an agent would check to validate completion}
]

[context]
related_docs = [
    "VISION.md",
    "VALUES.md",
]
relevant_paths = [
    {list of directories/files affected, from Phase 2 exploration}
]

[scope]
in_scope = [
    {explicit list from Phase 4}
]
out_of_scope = [
    {explicit list from Phase 4}
]
acceptable_breakage = [
    {what's OK to break, from Phase 4}
]

[notes]
considerations = """
{Key findings from codebase exploration.
Risk areas and mitigation strategies.
Sequencing recommendations for epic breakdown.
Any decisions made during planning and their rationale.}
"""
```

### Phase 6: Offer Next Steps

After creating the mission TOML:

```
Created Mission M{NNN}: {Title}
  File: product/missions/M{NNN}-{slug}.toml
  Status: draft

Next steps:
1. Review the mission TOML and adjust if needed
2. Run /product-mission-breakdown M{NNN} to create epics
3. Or commit as-is and break down later

Want me to run the breakdown now, or commit the mission as draft?
```

## Key Principles

**Explore before you plan.** The codebase is the source of truth, not assumptions. A 5-minute exploration saves hours of wrong-direction implementation.

**Scope is a choice, not a discovery.** Exploration reveals what *could* be in scope. The user decides what *is* in scope. Always present options.

**Boundaries prevent scope creep.** The `[scope]` section in the TOML is the contract. If it's not in `in_scope`, it's not part of this mission. Period.

**Acceptable breakage is honest.** If a refactoring will break the frontend, say so in the TOML. Don't pretend everything will be seamless. Document what breaks and what the plan is to fix it later.

## Anti-Patterns

- **Skipping exploration:** Creating a mission TOML from vibes without reading the code
- **Boiling the ocean:** Putting everything in scope because "while we're at it..."
- **Vague criteria:** "Code is cleaner" is not a testing criterion. "No files import from legacy/" is.
- **Missing non-goals:** If you don't say what's out of scope, everything is in scope
- **No acceptable breakage:** Refusing to acknowledge trade-offs leads to paralysis

## Example

Input: `/plan-mission replace legacy auth system with new provider`

Phase 2 explores: files importing legacy auth, config files, auth flows, session patterns, which modules depend on what.

Phase 3 presents: "87 files import from legacy-auth/, 15 use the old session API. The core packages have zero legacy imports. Option A: replace everything at once. Option B: replace in core only (already isolated). Option C: phase 1 = add new provider alongside old, phase 2 = migrate consumers, phase 3 = remove old."

Phase 4 defines: in scope = replace legacy auth imports in all active code, out of scope = rebuilding the admin panel, acceptable breakage = admin panel auth needs separate migration.

Phase 5 creates: `product/missions/M003-replace-legacy-auth.toml` with full context.
