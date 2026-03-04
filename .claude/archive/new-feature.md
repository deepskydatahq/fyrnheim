---
description: Brainstorm a new feature, create design doc, and add to HTE tasks
allowed-tools: Bash(git:*), Bash(hte tasks:*), Read, Write, Glob, Grep, Skill
---

# New Feature

Guide a new feature from idea to design document and HTE task.

## Instructions

1. **Capture the idea:**

   If argument provided (e.g., `/new-feature add dark mode support`):
   - Use as the starting point for brainstorming

   If no argument:
   - Ask: "What feature would you like to design?"

2. **Invoke full brainstorming:**

   Use the `superpowers:brainstorming` skill with the feature idea:

   - Explore project context (check relevant files, docs, recent commits)
   - Ask questions one-by-one to understand requirements
   - Propose 2-3 different approaches with trade-offs
   - Lead with your recommended approach
   - Present design in sections (200-300 words each)
   - Validate each section before continuing

3. **Save design document:**

   Save to: `docs/plans/YYYY-MM-DD-<feature-slug>-design.md`

   Document structure:
   ```markdown
   # <Feature Name> Design

   ## Overview
   <2-3 sentence summary>

   ## Problem Statement
   <What problem does this solve?>

   ## Proposed Solution
   <High-level approach>

   ## Design Details
   <Architecture, components, data flow>

   ## Alternatives Considered
   <Other approaches and why not chosen>

   ## Open Questions
   <Any unresolved decisions>

   ## Success Criteria
   <How do we know it works?>
   ```

   Commit the design:
   ```bash
   git add docs/plans/YYYY-MM-DD-<feature-slug>-design.md
   git commit -m "docs: add <feature> design"
   ```

4. **Assess status for the task:**

   After brainstorming, determine the appropriate status:

   | Condition | Status |
   |-----------|--------|
   | Open questions remain, needs more design exploration | `brainstorm` |
   | Design is complete, ready for implementation planning (DEFAULT) | `plan` |
   | Trivial to implement, no planning needed | `ready` (RARE) |

   **Default to `plan`** - the brainstorming session just completed, so the design should be ready for planning.

5. **Create HTE task:**

   ```bash
   hte tasks create --title "<Feature Name>" --status <status> --data '{"body":"## Summary\n<2-3 sentences from design overview>\n\n## Design Document\nSee: docs/plans/YYYY-MM-DD-<feature>-design.md\n\n## Key Decisions\n- <Main architectural choice>\n- <Key trade-off made>\n- <Important constraint>\n\n## Next Steps\n- [ ] Review design\n- [ ] Create implementation plan\n- [ ] Implement\n\n---\n*Created via /new-feature command*"}'
   ```

6. **Offer continuation:**

   ```
   Feature documented and task <id> created (status: <status>).

   Ready to create implementation plan now? (y/n)
   ```

   If yes:
   - Invoke `superpowers:writing-plans` skill
   - Pass the design document as context
   - After plan is written, move task to `ready` status

   If no:
   - Report: "Design saved. Run `/plan-issue` when ready to create implementation plan."
