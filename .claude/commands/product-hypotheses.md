---
description: Manage the hypotheses catalog - testable beliefs about what will work
allowed-tools: Bash(git:*), Bash(hte tasks:*), Skill, Read, Write, Glob, Grep
---

# Product Hypotheses

Manage the catalog of testable beliefs about what will move us toward the vision.

## Purpose

The hypotheses catalog answers: "What do we believe will work, and how will we know?" This is the living document that connects strategy to implementation.

## Key Principle

A hypothesis is NOT:
- A feature request
- A user story
- A task to complete

A hypothesis IS:
- A testable belief
- Connected to the transformation
- Something that can be validated or invalidated

## Modes

Infer the mode from context:
- No HYPOTHESES.md exists → **Generate** mode
- HYPOTHESES.md exists → Ask what to do (Review, Update, or Select)

### Generate New Hypotheses

When to use: After roadmap update, after learning something new, when stuck on what to build next.

1. Invoke `superpowers:brainstorming` skill
2. Read VISION.md and ROADMAP.md
3. For each investment area, explore:
   - What do we believe will move users toward the transformation?
   - What's uncertain that we could test?
   - What would change our approach if we learned it?

4. Format each hypothesis:
   - **Belief:** We believe that [action/feature] will [outcome]
   - **Because:** [Reasoning/evidence]
   - **Test:** We'll know this is true when [observable signal]
   - **Investment Area:** [Link to roadmap area]

### Review & Refine Hypotheses

When to use: Periodically, or when hypotheses feel stale.

1. Invoke `superpowers:brainstorming` skill
2. Read existing HYPOTHESES.md
3. For each hypothesis:
   - Is this still relevant to current roadmap?
   - Has evidence changed?
   - Is the test clear and achievable?
   - Should this be split, merged, or archived?

### Update After Feature Work

When to use: After running `/product-iteration` on completed feature.

1. Which hypothesis did this feature test?
2. What did we learn?
3. Update status: Validated / Invalidated / Inconclusive
4. Add evidence notes
5. Generate follow-up hypotheses if needed

### Select Next Hypothesis to Test

When to use: When deciding what to build next.

1. Filter to untested hypotheses
2. Score by:
   - **Impact:** How much does this serve the transformation?
   - **Confidence:** How sure are we this will work? (lower = more valuable to test)
   - **Effort:** How hard to test?
   - **Utility curve:** Is the area pre/crossing/post threshold?

3. Prioritize:
   - High impact + low confidence + reasonable effort = test this
   - High impact + high confidence = just build it
   - Low impact = park it

4. **Create HTE Task:**

   After selecting a hypothesis, create a task:

   ```bash
   hte tasks create --title "Test [H#]: [Hypothesis Name]" --status brainstorm --data '{"body":"## Hypothesis Test\n\n**From:** HYPOTHESES.md - [H#]\n\n**Belief:** We believe that [X] will [Y]\n\n**Because:** [reasoning]\n\n**Test:** We'"'"'ll know this works when [signal]\n\n**Investment Area:** [from roadmap]\n\n---\n*Created via /product-hypotheses*"}'
   ```

   If this is a large hypothesis requiring multiple child tasks, use `/product-epic` after creating the task.

5. Update HYPOTHESES.md status to "🔵 Testing"

## Document Format

Store as `HYPOTHESES.md` in project root.

```markdown
# [Product Name] Hypotheses Catalog

**Vision:** [One-line transformation]
**Last Updated:** [Date]

---

## Active Hypotheses

### H1: [Short Name]
**Status:** 🟡 Untested | 🔵 Testing | 🟢 Validated | 🔴 Invalidated

**Belief:** We believe that [action/feature] will [outcome for users]

**Because:** [Reasoning - why we think this]

**Test:** We'll know this works when [specific observable signal]

**Investment Area:** [Link to roadmap area]

**Evidence:**
- [Date]: [What we learned]

**Next Steps:** [If validated, what follows? If invalidated, what changes?]

---

## Validated Hypotheses

### H[X]: [Name]
**Validated:** [Date]
**Key Learning:** [What we now know]
**Resulted In:** [What we built/changed]

---

## Invalidated Hypotheses

### H[X]: [Name]
**Invalidated:** [Date]
**Key Learning:** [What we now know]
**Pivot:** [How this changed our approach]

---

## Parked Hypotheses

### H[X]: [Name]
**Parked Because:** [Reasoning]
**Revisit When:** [Conditions that would make this relevant]
```

## Hypothesis Quality Checklist

Good hypotheses are:
- [ ] **Specific:** Clear what we're testing
- [ ] **Testable:** Observable signal for success/failure
- [ ] **Connected:** Traces to investment area and vision
- [ ] **Falsifiable:** We can be wrong
- [ ] **Actionable:** We know what to build to test it

## Anti-Patterns

- **Feature-as-hypothesis:** "We believe users want dark mode" — too specific, not connected to transformation
- **Unfalsifiable:** "We believe good UX matters" — can't be invalidated
- **Too big:** "We believe our product will succeed" — not testable
- **Disconnected:** Hypothesis doesn't trace to roadmap/vision
