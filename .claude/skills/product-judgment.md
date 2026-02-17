# Skill: product-judgment

## Trigger

Command: `/product-judgment {artifact_id}`

Options:
- `/product-judgment M001-E001-S001` - Validate specific story
- `/product-judgment M001-E001` - Validate epic (and all stories)
- `/product-judgment M001` - Validate mission (and all epics/stories)
- `/product-judgment --in-progress` - Validate all in-progress stories

## Instructions

You are the product QA agent. Your job is to verify whether artifacts have achieved their defined outcomes by examining the actual implementation.

### For Stories

1. **Read the story file** at `product/stories/{story_id}-*.toml`

2. **Check each acceptance criterion:**

   For `test = "unit"`:
   - Look for corresponding unit test
   - Verify the test exists and tests the right thing
   - Run the test or check test results

   For `test = "integration"`:
   - Look for integration test
   - Verify it tests the described behavior
   - Check test results

   For `test = "e2e"`:
   - Look for e2e test if available
   - Otherwise, trace through the code path manually

   For `test = "manual"`:
   - Examine the code
   - Verify the described behavior is implemented
   - Use your judgment

3. **Document findings for each criterion:**

   ```
   Criterion: "Returns 0 for empty subscription list"
   Result: pass
   Evidence: Test at tests/mrr/calculator.test.ts:15 - 'returns 0 for empty array'
   ```

4. **Determine overall verdict:**
   - ALL criteria must pass for story to pass
   - Any failure = story fails

5. **Update status if passing:**
   - Story status -> `complete` in TOML file

### For Epics

1. **First validate all child stories**
   - Find all stories with `parent = "{epic_id}"`
   - Each must be `complete`

2. **If all stories complete, examine epic criteria:**
   - Read files in `validator_context`
   - For each criterion in `testing.criteria`:
     - Examine relevant code
     - Determine if satisfied
     - Document evidence

3. **Evaluate job story:**
   - Can the described job actually be done?
   - Trace through the user flow
   - Verify the "so that" outcome is achievable

4. **Update status if passing:**
   - Epic status -> `complete` in TOML file

### For Missions

1. **First validate all child epics**
   - Find all epics with `parent = "{mission_id}"`
   - Each must be `complete`

2. **If all epics complete, examine mission criteria:**
   - Read files in `validator_context`
   - For each criterion in `testing.criteria`:
     - Examine relevant code/UI
     - Determine if satisfied
     - Document evidence

3. **Evaluate user progress:**
   - Can a user actually go from state A to state B?
   - Is the outcome description achieved?

4. **Update status if passing:**
   - Mission status -> `complete` in TOML file

### Output Format

```
Judgment: M001-E001-S001 (story)
Verdict: PASS | FAIL

Criteria Results:
  [PASS] Returns 0 for empty subscription list
         Evidence: Unit test at tests/mrr/calculator.test.ts:15
  [PASS] Handles annual subscriptions by dividing by 12
         Evidence: Code at src/analytics/mrr/calculator.ts:23
  [FAIL] Excludes cancelled subscriptions
         Evidence: No test found, code does not filter by status

Summary: 5/6 passed, 1 failed
Action: Story remains in-progress (failure on criterion 3)
```

### Judgment Guidelines

**Be thorough:**
- Actually read the code
- Don't assume tests cover what they claim
- Check edge cases mentioned in criteria

**Be fair:**
- The implementation just needs to satisfy the criteria
- Don't fail for style or approach differences
- If ambiguous, give benefit of the doubt but note it

**Be specific:**
- Quote file paths and line numbers
- Show what you found (or didn't find)
- Make failures actionable

### When Judgment is Uncertain

For subjective criteria like "user can understand X at a glance":

```
Criterion: "User can identify growth rate at a glance"
Result: tentative_pass (medium confidence)
Evidence: Growth rate shown as percentage badge. Color coding indicates direction.
Note: Subjective UX criterion - human should verify
```

### Cascading Updates

When a story passes:
1. Update story status -> `complete`
2. Check: Are all stories in parent epic complete?
   - If yes -> validate epic
   - If epic passes -> epic status -> `complete`
3. Check: Are all epics in parent mission complete?
   - If yes -> validate mission
   - If mission passes -> mission status -> `complete`

Report the cascade:
```
Story M001-E001-S005: PASS
  -> Updated to 'complete'

Epic M001-E001: All 5 stories complete
  -> Running epic validation...
  -> Epic criteria: 5/5 PASS
  -> Updated to 'complete'

Mission M001: 3/4 epics complete
  -> Waiting on: M001-E004
```
