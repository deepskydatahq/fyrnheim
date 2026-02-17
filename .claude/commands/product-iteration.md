---
description: Product retrospective after completing features - learn and plan next steps
allowed-tools: Bash(git:*), Bash(hte tasks:*), Skill, Read, Write, Glob, Grep
---

# Product Iteration

Run a product retrospective on completed work to generate next-step recommendations.

## Purpose

Product iteration answers: "What did we learn, and what should we build next?" This closes the loop between implementation and strategy.

## Instructions

### 1. Invoke Brainstorming

Use the `superpowers:brainstorming` skill to guide the retrospective.

### 2. Understand What Was Built

Examine recent work:
```bash
git log --oneline -10
git diff --name-only $(git merge-base HEAD main)..HEAD
```

Identify:
- The user problem it addresses
- The current solution shape
- What transformation it enables for users

### 3. Apply Product Lenses

Evaluate through these frameworks:

**Utility Curve Position** (Butterfield): Where is this feature on the S-curve?
- Pre-threshold: Needs more investment before users see value
- Crossing threshold: At the "aha" moment, users getting real value
- Diminishing returns: Further polish has low impact

**Transformation Clarity** (Butterfield): Are we selling saddles or horseback riding?
- What's the "horseback riding" for this feature?
- Is the transformation obvious to users?

**Simplicity Depth** (Jobs): Have we understood the essence?
- What could be removed without losing the core?
- Is there complexity masquerading as features?

**Experience-First** (Jobs): Starting from experience, what's missing?
- What does the user want to feel when using this?
- What friction exists between intent and outcome?

### 4. Update Hypotheses

Read HYPOTHESES.md and update:
- Which hypothesis did this work test?
- Status change: 🟢 Validated / 🔴 Invalidated / 🟡 Inconclusive
- Add evidence with date
- Note any new hypotheses generated

### 5. Generate Next Iterations

Based on the lenses, propose 2-4 concrete next steps. Each should include:
- **What**: Clear description of the iteration
- **Why**: Which product lens suggests this
- **Impact**: Expected user transformation improvement

Rank by utility curve logic:
1. Features crossing the value threshold (biggest leverage)
2. Pre-threshold features needing investment to become useful
3. Post-threshold polish (only if others are addressed)

### 6. Offer Task Creation

After presenting recommendations, ask:

"Create tasks for these recommendations? (y/n)"

If yes, for each recommendation:

```bash
hte tasks create --title "[Iteration Name]" --status brainstorm --data '{"body":"## Recommended Iteration\n\n**From:** Product iteration retrospective\n\n**What:** [Description]\n\n**Why:** [Product lens rationale]\n\n**Impact:** [Expected transformation improvement]\n\n**Generated Hypothesis:**\n- Belief: [What we believe]\n- Test: [How we'"'"'ll know]\n\n---\n*Created via /product-iteration*"}'
```

## Output Format

Present findings as:

```
## Product Retrospective: [Feature/Work Name]

### What We Built
[Brief summary of the feature and the problem it solves]

### Transformation Assessment
[What "horseback riding" are we selling? Is it clear?]

### Utility Curve Position
[Where is this feature on the S-curve? Evidence for position]

### Simplicity Check
[What's essential? What could be removed?]

### Hypothesis Update
- Tested: [H#]
- Status: [Validated/Invalidated/Inconclusive]
- Evidence: [What we learned]
- New hypotheses: [If any]

### Recommended Next Iterations

1. **[Iteration Name]** (Priority: High/Medium/Low)
   - What: [Specific change]
   - Why: [Product lens rationale]
   - Impact: [Expected transformation improvement]

2. [Additional iterations...]

### Anti-Recommendations
[What NOT to build and why - features in diminishing returns zone]
```

## After Running This Command

1. HYPOTHESES.md should be updated with learnings
2. If major insight, consider running `/product-roadmap` to review investment areas
3. Created tasks enter the dev workflow: `/brainstorm` → `/plan-issue` → `/pick-issue`

## Connection to Dev Workflow

```
/pick-issue completes
    ↓
/retro → technical follow-ups → HTE tasks
    ↓
/product-iteration → product insights → HYPOTHESES.md + optional tasks
    ↓
/product-hypotheses select → next bet → brainstorm task
```
