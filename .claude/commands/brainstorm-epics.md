---
description: Generate mission candidates from vision, value ladder, and ideas (self-directed or expert-driven via .claude/experts/ catalog)
allowed-tools: Bash(git:*), Bash(bd:*), Skill, Task, Read, Write, Glob, Grep
---

# Brainstorm Epics

Generate mission candidates by combining strategic context with user ideas. Choose between:
- **Self-directed**: Guided exploration of your own ideas
- **Product Expert**: Let an expert panel (from `.claude/experts/` catalog) generate and review 3 candidates

Present structured candidates, let user select, then create mission TOMLs and optionally run breakdown.

## When to Use

- Periodic planning sessions ("what should we build next?")
- When you have ideas to explore
- When stuck on what to work on

## Prerequisites

- VISION.md should exist (run `/product-vision` if not)
- VALUES.md should exist (run `/product-values` if not)

If either file is missing, the command can still run but will have less strategic context. Consider creating them first for better results.

## Position in Workflow

```
                         brainstorm-epics
                              |
                    +---------+---------+
                    v                   v
             Self-Directed        Product Expert
                    |                   |
                    v                   v
         User explores ideas    Expert Panel generates:
         with guidance          - Product Expert (from catalog)
                    |           - Technical Expert (from catalog)
                    |           - Design Reviewer (from catalog)
                    |                   |
                    +---------+---------+
                              v
                    3 structured candidates
                              |
                              v
                       user selects
                              |
                              v
              Create mission TOML + optional breakdown
```

## Instructions

### 1. Ask Brainstorming Mode

First, ask the user how they want to brainstorm:

> "How would you like to brainstorm?
> 1. **Self-directed** - I'll guide you through exploring ideas
> 2. **Product Expert** - Let an expert panel generate candidates"

**If Self-directed:** Continue to step 2
**If Product Expert:** Skip to step 2b (Expert-Driven Mode)

### 2. Gather Context (Self-Directed)

Run these in parallel:

1. Read VISION.md - extract transformation, beliefs, target users, current phase
2. Read VALUES.md - extract value levels and statuses
3. Check existing missions: `ls product/missions/` - avoid duplicates
4. Run `git log --oneline -30` - understand recent momentum

### 2a. Ask User for Input

> "Do you have specific ideas you want to explore, or should we brainstorm from scratch based on vision and roadmap?"

**If user has ideas:**
- Capture them
- Use vision/roadmap to enrich and validate alignment

**If brainstorming from scratch:**
- May ask: "Any areas feeling particularly painful right now?"
- May ask: "Anything you've been thinking about but haven't written down?"

### 2b. Expert-Driven Mode

If user selected Product Expert mode, launch an expert brainstorming session:

#### 2b.1 Gather Context
Run these in parallel:
1. Read VISION.md
2. Read VALUES.md
3. Check existing missions: `ls product/missions/`
4. Run `git log --oneline -30`

#### 2b.2 Launch Expert Panel

Read `.claude/experts/product-strategist.md` and use its philosophy + principles to build the agent prompt.

Use the Task tool to launch a Product Expert agent (haiku model) with this prompt:

```
You are a product expert generating mission candidates for a software project.

## Your Role
{product-strategist.role}

## Philosophy
{product-strategist.philosophy}

## Principles
{product-strategist.principles}

## Context

### Vision
{VISION.md content}

### Value Ladder
{VALUES.md content}

### Existing Missions (avoid duplicates)
{ls product/missions/ output}

### Recent Momentum
{git log output}

## Your Task

Generate exactly 3 mission candidates that would create the most value.

For each candidate, provide:
1. **Title**: Clear, actionable mission name
2. **Problem**: What pain point or opportunity (2-3 sentences)
3. **User Progress**: From [current state] to [desired state]
4. **Magic Moment**: What would make someone say "wow"?
5. **Scope**: S / M / L
6. **Value Level**: Which value level(s) this advances
7. **Why Now**: What makes this timely
8. **Potential Epics**: 3-5 rough epic ideas
```

#### 2b.3 Review with Technical Expert

Read `.claude/experts/technical-architect.md` and use its philosophy + principles to build the agent prompt.

After Product Expert generates candidates, launch a Technical Expert agent (haiku model):

```
Review these mission candidates from a technical perspective.

## Your Role
{technical-architect.role}

## Philosophy
{technical-architect.philosophy}

## Principles
{technical-architect.principles}

## Candidates
{Product Expert output}

## Your Task
For each candidate:
1. Flag any technical complexity or risks
2. Suggest simpler alternatives if over-engineered
3. Note implementation considerations
4. Rate feasibility: Easy / Medium / Hard

Keep it brief - 2-3 sentences per candidate.
```

#### 2b.4 Simplification Review

Read `.claude/experts/simplification-reviewer.md` and use its philosophy + principles to build the agent prompt.

Launch Design Reviewer agent (haiku model) for final check:

```
Review these mission candidates for ruthless simplification.

## Your Role
{simplification-reviewer.role}

## Philosophy
{simplification-reviewer.philosophy}

## Principles
{simplification-reviewer.principles}

## Candidates with Technical Notes
{Combined output}

## Your Task
For each candidate:
1. What would you cut or simplify?
2. Is this truly essential or "nice to have"?
3. Rate: STRONG / GOOD / WEAK

Output a final ranking with brief justification.
```

Then proceed to step 4 (Present Candidates) with the expert-generated candidates.

### 3. Brainstorm Candidates (Self-Directed)

Invoke `superpowers:brainstorming` skill to generate 3-5 mission candidates.

Each candidate should have:
- **Title:** Clear, actionable mission name
- **Problem:** What pain point or opportunity this addresses (2-3 sentences)
- **User Progress:** From [current state] to [desired state]
- **Scope:** S / M / L
- **Value Level:** Which value level(s) this advances
- **Why now:** What makes this timely given current state
- **Potential epics:** 3-5 rough epic ideas

### 4. Present Candidates

Present all candidates with comparison:

```
Here are [N] mission candidates based on [inputs used]:

### Candidate 1: [Title]
**Problem:** ...
**User Progress:** From ... to ...
**Scope:** M
**Value Level:** [Level(s)]
**Why now:** ...
**Potential epics:**
- Epic idea 1
- Epic idea 2
- Epic idea 3

### Candidate 2: [Title]
...

---

**Comparison:**
- Candidate 1 is highest impact but largest scope
- Candidate 3 addresses a gap in current missions
- Candidate 5 builds on recent momentum in [area]

Which would you like to pursue? (Enter number, or "none" to refine)
```

### 5. Handle Selection

**If user selects a candidate:**
- Create mission TOML from the candidate
- Proceed to step 6

**If user says "none" or asks to refine:**
- Ask clarifying questions
- Generate new/modified candidates
- Present again

### 6. Create Mission and Run Breakdown

1. Determine next mission ID from existing files in `product/missions/`
2. Create `product/missions/M{NNN}-{slug}.toml` with candidate details
3. Optionally invoke `/product-mission-breakdown M{NNN}` to create epics
4. Optionally invoke `/product-epic-breakdown` for each epic to create stories

### 7. Report Results

```
Created Mission M{NNN}: [Title]
  File: product/missions/M{NNN}-{slug}.toml

Epics created: N (if breakdown was run)
  - M{NNN}-E001: [Epic 1]
  - M{NNN}-E002: [Epic 2]
  - M{NNN}-E003: [Epic 3]

Next steps:
- Run /product-epic-breakdown M{NNN}-E{NNN} to create stories for each epic
- Run /product-story-handoff to create Beads tasks from ready stories
- Or run /brainstorm to start working on existing tasks
```
