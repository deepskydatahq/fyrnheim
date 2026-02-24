---
description: Review and update value ladder - the ordered progression of value your product delivers
allowed-tools: Bash(git:*), Skill, Read, Write, Glob, Grep
---

# Product Values

Review and update the value ladder — the ordered progression of value levels your product delivers.

## Purpose

The value ladder answers: "What value do we deliver, in what order?" Each level is independently valuable, builds on previous levels, and compounds toward the end-state.

## Key Principle

The value ladder is NOT:
- A feature list or backlog
- A timeline with deadlines
- Strategic investment areas

The value ladder IS:
- An ordered progression of value levels
- Each level independently useful, each compounding on the last
- A clear prioritization heuristic: build the lowest unshipped level first

## Instructions

### 1. Load Context

Read VALUES.md first. Understand the current status of all levels.

If VALUES.md doesn't exist, suggest creating it or running this command to bootstrap it.

### 2. Check Mission Status

Read existing missions in `product/missions/` to verify level statuses are accurate:
- Which missions map to which levels?
- Have any levels been advanced by recent work?
- Are any statuses stale?

### 3. Review Level Statuses

For each level, assess its current status:

**shipped -> shipped:** Confirm it still works. No change needed.

**planned -> designed:** Has the architecture been defined? Update if so.

**designed -> building:** Has active development started? Update if so.

**building -> shipped:** Has the implementation landed and been validated? Update if so.

**future -> planned:** Has scope been understood through design work? Update if so.

### 4. Identify the Next Level

The default prioritization heuristic: **build the lowest unshipped level first.**

Look at the status summary table. The first non-shipped level is the highest-leverage next step because:
- It builds directly on what's already shipped
- It has the fewest dependencies
- It extends the value story incrementally

If multiple levels are close to shipping, note which would complete a tier.

### 5. Update VALUES.md

If any statuses changed:
1. Update the level entry's `**Status:**` line
2. Update the status summary table
3. Add any new mission references
4. Refine descriptions if understanding has deepened

### 6. Save and Commit

```bash
git add VALUES.md
git commit -m "docs: update value ladder statuses"
```

## When to Update

Update the value ladder when:
- A mission completes that advances a level
- Design work clarifies a planned level
- Active development begins on a new level
- Understanding of a level's scope changes materially

Do NOT update just because time has passed. Status changes should reflect real work.

## Connection to Other Documents

- **Vision** -> The value ladder delivers the transformation described in VISION.md
- **Missions** -> Each mission advances one or more value levels
- **Task Pipeline** -> `/brainstorm-epics` uses value levels to prioritize candidates
- **Judgment** -> `/product-judgment` validates work against level requirements
