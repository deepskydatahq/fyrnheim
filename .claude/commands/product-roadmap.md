---
description: Create or update product roadmap - strategic investment areas that serve the vision
allowed-tools: Bash(git:*), Skill, Read, Write, Glob, Grep
---

# Product Roadmap

Create or update strategic investment areas that serve the vision.

## Purpose

The roadmap answers: "Where are we investing to deliver the transformation?" Not a feature list—strategic bets on problem areas worth solving.

## Key Principle

A roadmap is NOT:
- A list of features to build
- A timeline with deadlines
- A backlog in disguise

A roadmap IS:
- Strategic investment areas
- Bets on what will move us toward the vision
- A communication tool for focus

## Instructions

### 1. Invoke Brainstorming

Use the `superpowers:brainstorming` skill to guide roadmap creation/refinement.

### 2. Load Context

Read VISION.md first. Every roadmap item must trace back to the transformation.

If VISION.md doesn't exist, suggest running `/product-vision` first.

### 3. Identify Investment Areas

For each potential area, explore:

**From vision:** How does this serve the transformation?

**From utility curves (Butterfield):**
- Is this area pre-threshold (needs investment to become valuable)?
- Is it crossing threshold (high leverage right now)?
- Is it past threshold (diminishing returns)?

**From simplicity (Jobs):**
- Is this essential to the transformation or adjacent?
- What would we be saying "no" to by investing here?

### 4. Structure Investment Areas

Each area should have:
- **Name:** Clear, memorable label
- **Why:** Connection to vision/transformation
- **Current state:** Where we are
- **Desired state:** Where we want to be
- **Key questions:** What we need to learn

### 5. Prioritize

Use utility curve logic:
1. Areas crossing the value threshold (biggest leverage now)
2. Areas needing investment to become valuable (foundational)
3. Polish on working areas (only if 1-2 are addressed)

### 6. Save Document

Store as `ROADMAP.md` in project root.

```markdown
# [Product Name] Roadmap

**Vision:** [One-line transformation from VISION.md]

**Last Updated:** [Date]

**Planning Horizon:** [e.g., "Next 3 months" or "Q1 2025"]

---

## Current Focus Areas

### 1. [Investment Area Name]
**Why:** [How this serves the transformation]
**Current State:** [Where we are]
**Desired State:** [Where we want to be]
**Key Questions:**
- [What we need to learn]
- [Hypotheses to test]

**Utility Curve Position:** [Pre-threshold / Crossing / Post-threshold]

### 2. [Investment Area Name]
[Same structure...]

---

## Parked Areas

[Areas we've consciously decided NOT to invest in right now, and why]

### [Area Name]
**Why Not Now:** [Reasoning - often "past threshold" or "doesn't serve current transformation"]

---

## Completed Areas

[Areas where we've achieved desired state - moved to maintenance]

### [Area Name]
**Outcome:** [What we achieved]
**Moved to Maintenance:** [Date]
```

Commit the roadmap:
```bash
git add ROADMAP.md
git commit -m "docs: add/update product roadmap"
```

## When to Update

Roadmap should update periodically (monthly/quarterly), not constantly. Update when:
- Completing an investment area
- Major learning changes priorities
- New opportunities emerge that serve the vision
- Quarterly planning cycles

## Connection to Other Documents

- **Vision** → Roadmap traces back to transformation
- **Hypotheses** → Each investment area generates hypotheses to test
- **Features** → Built to test hypotheses within investment areas
