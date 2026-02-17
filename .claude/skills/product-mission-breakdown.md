# Skill: product-mission-breakdown

## Trigger

Command: `/product-mission-breakdown {mission_id}`

## Instructions

You are breaking down a product mission into epics. Your goal is to create a set of epics that collectively achieve the mission outcome.

### Step 1: Read the Mission

Read the mission file at `product/missions/{mission_id}-*.toml`

Extract:
- The outcome description
- The user progress statement (from → to)
- The testing criteria
- The relevant paths and context

### Step 2: Understand the Scope

Review the files in `relevant_paths` to understand:
- What exists already
- What needs to be built
- Technical constraints

### Step 3: Identify Epic Boundaries

Think about logical groupings:
- Backend vs frontend concerns
- Data layer vs presentation layer
- Core functionality vs supporting features
- Independent workstreams

Aim for 3-6 epics per mission. Each epic should:
- Deliver standalone value
- Be roughly similar in scope
- Have clear boundaries

### Step 4: Create Epic Files

For each epic, create a file at `product/epics/{mission_id}-E{NNN}-{slug}.toml`

Use the epic template at `product/templates/epic.template.toml`. Ensure:
- `id` follows the pattern `{mission_id}-E{NNN}`
- `parent` references the mission ID
- `job_story` follows "When... I want... so that..." format
- `testing.criteria` are specific and verifiable
- `relevant_paths` are scoped to this epic

### Step 5: Verify Coverage

Before finishing, verify:
- [ ] All mission outcomes are addressed by at least one epic
- [ ] No significant gaps between epics
- [ ] No major overlaps
- [ ] Dependencies between epics are noted

### Output

Create the epic TOML files and report:
- Number of epics created
- Brief description of each
- Any dependencies or sequencing notes
- Questions or concerns for human review

### Example

Input: `/product-mission-breakdown M001`

Output:
```
Created 4 epics for mission M001 (Growth Intelligence Dashboard MVP):

1. M001-E001: Core MRR Calculation Engine
   - Foundation calculations all other features depend on
   - No dependencies

2. M001-E002: Dashboard Visualization Components
   - Charts, graphs, and UI for displaying metrics
   - Depends on: M001-E001

3. M001-E003: Data Connection Pipeline
   - Import subscription data from sources
   - No dependencies (can parallel with E001)

4. M001-E004: Time Range Selection
   - Date pickers and filtering logic
   - Depends on: M001-E001, M001-E002

Recommended order: E001 and E003 in parallel, then E002, then E004.

Ready for review. Set epic status to 'active' to begin story breakdown.
```
