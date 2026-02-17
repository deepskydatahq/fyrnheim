---
description: Create or refine a product vision document - the north star transformation we enable
allowed-tools: Bash(git:*), Skill, Read, Write, Glob, Grep
---

# Product Vision

Create or refine the north star document for a product.

## Purpose

The vision answers: "What transformation are we enabling?" Not features, not technology—the horseback riding, not the saddle.

## Instructions

### 1. Invoke Brainstorming

Use the `superpowers:brainstorming` skill to guide the vision creation/refinement.

### 2. Understand Current State

If VISION.md exists, read it first. If not, gather context:
- What problem space are we in?
- Who are our users?
- What do they struggle with today?

### 3. Articulate the Transformation

Ask these questions (from Butterfield's "We Don't Sell Saddles Here"):

**Who do we want users to become?**
Not what they'll do with the product—who they'll be after using it.

**What's the "before and after"?**
- Before: [Current painful state]
- After: [Transformed state we enable]

**What market are we creating or owning?**
Selling saddles = competing on features. Selling horseback riding = defining the category.

### 4. Apply Jobs' Simplicity Test

- Can you explain the transformation in one sentence?
- Does it pass the "would this excite a non-expert?" test?
- What's NOT in scope? (Innovation = saying no)

### 5. Define Success Signals

How would we know users are experiencing the transformation? Not metrics—observable behavior changes.

### 6. Save Document

Store as `VISION.md` in project root.

```markdown
# [Product Name] Vision

## The Transformation

**We help [users] become [transformed state] by [core mechanism].**

### Before
[What life/work looks like without us - the pain, friction, limitation]

### After
[What life/work looks like with us - the transformed state]

## Who We Serve

[Primary user, their context, why they care]

## What We Don't Do

[Explicit boundaries - the "no" that enables focus]

## Success Looks Like

[Observable behavior changes that indicate transformation is happening]

## Last Updated
[Date]
```

Commit the vision:
```bash
git add VISION.md
git commit -m "docs: add/update product vision"
```

## When to Update

Vision should be stable. Update only when:
- Fundamental pivot in direction
- Major learning invalidates core assumptions
- Expanding to genuinely new transformation

If updating frequently, the vision isn't a vision—it's tactics.

## Connection to Other Documents

- **Roadmap** should trace back to vision (every investment area serves the transformation)
- **Hypotheses** should test beliefs about how to deliver the transformation
- **Features** should be evaluated against "does this serve the transformation?"
