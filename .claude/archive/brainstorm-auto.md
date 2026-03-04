---
description: Auto-brainstorm a task using expert personas from .claude/experts/
allowed-tools: Bash(bd:*), Task, Read, Write, Glob, Grep
---

# Brainstorm Auto

Pick a task from the `brainstorm` queue and run an autonomous brainstorming session. Routes questions to expert personas during design, then runs a final simplification review.

## Arguments

- No argument: Pick from queue
- Task ID (e.g., `/brainstorm-auto basesignal-a3f2dd`): Brainstorm specific task

## Current Tasks Needing Brainstorming

!`bd list --label brainstorm --json`

## Expert Catalog

Expert personas are loaded from `.claude/experts/*.md` files. Each file defines an expert with domain, role, type (advisor/reviewer), philosophy, principles, and response format.

The Brainstormer agent receives a manifest of available experts and selects which to consult based on the task. It can bring in additional experts mid-session and picks its own reviewer.

**Current catalog:**

!`ls .claude/experts/`

## Instructions

### 1. Select Task

**If argument provided:**
- Use that task ID directly
- Fetch details: `bd show <id> --json`

**If no argument:**
- If no tasks with `brainstorm` label: Report "No tasks need brainstorming. Run `/new-feature` to create some." and stop.
- Pick the best task:
  - Skip tasks with `in_progress` status
  - Age: older tasks first

### 2. Claim the Task

```bash
bd update <id> --status in_progress
```

### 3. Launch Brainstormer Agent

Before launching, glob `.claude/experts/*.md` and read the frontmatter (lines before first `**Principles:**`) of each file to build the manifest table:

```
| Expert | Domain | Role | Type |
|--------|--------|------|------|
| product-strategist | Product strategy, user value, ... | Answers product design questions... | advisor |
| technical-architect | Architecture, patterns, ... | Answers technical design questions... | advisor |
| ... | ... | ... | ... |
```

Use the Task tool to launch the Brainstormer agent:

```
Subagent type: general-purpose
Prompt: [See Brainstormer Agent Prompt below, with manifest table injected]
```

The Brainstormer will explore the codebase and output tagged questions.

### 4. Run the Question/Answer Loop

When the Brainstormer outputs a question, parse the expert name(s) and route:

**Question format from Brainstormer:**
```xml
<question expert="product-strategist">What problem are we solving?</question>
<question expert="technical-architect">Should we use X or Y pattern?</question>
<question expert="product-strategist,technical-architect">How do we balance UX with implementation complexity?</question>
```

**Routing logic:**

```
Parse expert name(s) from <question expert="name1,name2">
For each expert name:
  1. Read .claude/experts/{name}.md (cache for session)
  2. Build agent prompt from Generic Expert Agent Prompt Template + file content
  3. Launch expert agent (haiku model)
If multiple experts: launch in parallel, synthesize answers
```

**For multi-expert questions, synthesize the answers:**
- Present both perspectives
- Find the common ground or complementary insights
- Produce a unified recommendation

**Continue the Brainstormer** with the answer (start a new agent with full context - do not use resume) until it outputs:
```xml
<design-complete reviewer="expert-name">
[Final design summary]
</design-complete>
```

### 5. Validate Design Against Requirements

**Before accepting the design, verify:**

1. Extract all "Done When" items from the task
2. Check that the design addresses each item
3. Check that the design doesn't introduce scope creep (custom solutions when simple ones exist)

**Common divergences to catch:**
- Custom file browser when native dialog suffices
- localStorage when electron-store is the pattern
- Complex state management when simple props work
- New abstractions when existing patterns apply

If the design diverges from requirements, ask the relevant expert for a correction before proceeding.

### 6. Run the Review

Parse the `reviewer="name"` attribute from the `<design-complete>` tag. Load that expert's file from `.claude/experts/{name}.md`. Use its principles for the review prompt.

Launch the reviewer agent (use haiku model) with the complete design using the Generic Reviewer Agent Prompt Template below.

**The reviewer will output:**
```xml
<review>
## Verdict: <APPROVED | SIMPLIFY>

## What to Remove
- <component or feature to cut>
- <unnecessary abstraction>

## What to Simplify
- <area that's over-engineered>

## Integration Check
<Do perspectives feel unified?>

## Final Assessment
<1-2 sentences on whether this is truly minimal>
</review>
```

**If verdict is SIMPLIFY:**
- Apply the suggested cuts to the design
- Remove the identified components/abstractions
- Do NOT re-run the full brainstorming - just simplify

**If verdict is APPROVED:**
- Proceed to save the design

### 7. Assess Output

After brainstorming and review complete, determine:

**Single coherent piece of work:**
- Update the original task with design decisions
- Assess next status (see criteria below)
- Move to that status

**Multiple independent pieces:**
- Create child tasks for each piece
- Each child gets its own status assessment
- Mark original as done with links to children

### 8. Status Assessment Criteria

```
brainstorm if ANY of:
  - Still has unresolved design questions
  - Needs further user input on approach
  - Affects architecture and needs more thought

plan if:
  - Design decisions are made
  - Solution is known but involves multiple files/steps
  - Ready for detailed implementation planning

ready if ALL of:
  - Trivial, mechanical change
  - Specific file and location known
  - No risk of unintended consequences
  (This should be RARE - prefer plan)
```

### 9. Save Design Document

Save to: `docs/plans/YYYY-MM-DD-<feature-slug>-design.md`

```markdown
# <Feature Name> Design

## Overview
<2-3 sentence summary>

## Problem Statement
<What problem does this solve?>

## Expert Perspectives

### Product
<Key product insights from the session>

### Technical
<Key technical insights from the session>

### Simplification Review
<What was cut or simplified during review>

## Proposed Solution
<High-level approach synthesizing all perspectives>

## Design Details
<Architecture, components, data flow>

## Alternatives Considered
<Other approaches and why not chosen>

## Success Criteria
<How do we know it works?>
```

### 10. Update Task

```bash
bd update <id> --remove-label brainstorm --add-label <next-label>
```

Where `<next-label>` is `plan` or `ready` based on assessment.

Update the task body with brainstorming results:
```
## Auto-Brainstorming Complete

### Product Perspective
- <key insight 1>
- <key insight 2>

### Technical Perspective
- <key insight 1>
- <key insight 2>

### Simplification Review
- <what was cut or simplified>

### Final Design
<summary of minimal approach>

### Next Steps
<what the implementation plan should cover>

---
*Updated via /brainstorm-auto*
```

---

## Agent Prompts

### Brainstormer Agent Prompt

```
You are a brainstorming agent designing a solution for a Beads task.

## Task Context
<task title and body>

## Available Experts

<manifest table built from .claude/experts/ files>

Select experts from the Available Experts table based on what the task needs. You can bring in additional experts at any point if new topics arise. When your design is complete, pick a reviewer from the catalog.

## Your Task
1. Explore the relevant codebase to understand context
2. Ask questions to refine the design - but you don't ask the user
3. Output questions in this format for the expert personas to answer:

<question expert="expert-name">Question for a single expert</question>
<question expert="name1,name2">Question that needs multiple perspectives</question>

4. Wait for answers before continuing
5. After sufficient exploration, output the final design with a reviewer

## CRITICAL RULES

### ONE Question Rule
Output exactly ONE <question> tag, then STOP and wait for the answer.
Multiple questions in one response will be REJECTED.
You will receive the answer before you can ask another question.

### Requirements Checkpoint
Before outputting <design-complete>, you MUST verify:
1. List each "Done When" item from the task
2. Confirm your design addresses each one
3. Flag any divergences from stated requirements

Prefer simple solutions:
- Native OS dialogs over custom file browsers
- Existing patterns over new abstractions
- Built-in APIs over custom implementations

## Output Format

For questions (ONE at a time):
<question expert="expert-name">Your single question here</question>

For final design (pick a reviewer from Available Experts):
<design-complete reviewer="reviewer-name">
## Requirements Check
- [x] Requirement 1 - addressed by X
- [x] Requirement 2 - addressed by Y

## Summary
<what we're building>

## Key Decisions
- <decision 1>
- <decision 2>

## Approach
<high-level solution>

## Components
<what pieces are needed>
</design-complete>

## Guidelines
- Ask ONE question at a time - this is enforced
- Select experts by name from the Available Experts table
- You can bring in new experts as topics arise
- Consider 2-3 approaches before settling
- Apply YAGNI - remove unnecessary features
- Focus on the simplest solution that works
- Verify requirements before completing
```

### Generic Expert Agent Prompt Template

```
Answer this question using the principles below.

<question>{question}</question>

## Context
{task context and prior discussion}

## Your Role
{expert.role}

## Philosophy
{expert.philosophy}

## Principles
{expert.principles}

## Response Format
{expert.response_format}
```

### Generic Reviewer Agent Prompt Template

```
Review this design using the principles below.

## Design to Review
{complete design from brainstormer}

## Task Context
{original task title and requirements}

## Your Role
{reviewer.role}

## Philosophy
{reviewer.philosophy}

## Principles
{reviewer.principles}

## Your Task
1. Identify anything that can be REMOVED entirely
2. Identify anything that's over-engineered
3. Check if perspectives integrated well
4. Determine if this is truly minimal

## Output Format
<review>
## Verdict: <APPROVED | SIMPLIFY>

## What to Remove
- <component, feature, or abstraction that isn't essential>
(If nothing to remove, write "Nothing - design is minimal")

## What to Simplify
- <area that could be simpler>
(If nothing to simplify, write "Nothing - already simple")

## Integration Check
<Does the product vision and technical approach feel unified? Any friction?>

## Final Assessment
<1-2 sentences: Is this design inevitable and minimal, or is there hidden bloat?>
</review>
```

---

## Output Format

```
Selected: <id> - <title>

Claimed task with in_progress status.

[Brainstorming session with expert Q&A...]

[Review]
Verdict: <APPROVED | SIMPLIFY>
<cuts/simplifications if any>

---

Outcome: <Single piece | Broken into N pieces>

<If single:>
Moving to: <next-status>
Design saved to: docs/plans/YYYY-MM-DD-<slug>-design.md
Task updated with expert insights.

<If broken down:>
Created:
- <child1-id> - <title> (status: <status>)
- <child2-id> - <title> (status: <status>)
Original task <id> marked done.
```
