---
name: product-judgment
description: Validates whether Fyrnheim stories, epics, or missions achieved their stated outcomes by checking code, tests, and TOML acceptance criteria.
---

# Product Judgment

Use this skill to verify product artifacts against actual implementation.

## Story judgment

1. Read `product/stories/{story_id}-*.toml`.
2. Read parent epic and mission.
3. For each acceptance criterion:
   - find the relevant code and tests
   - run focused tests when practical
   - determine pass/fail with evidence
4. A story passes only when all criteria pass.
5. If passing, update `status = "complete"` if not already complete.
6. If failing, leave or set `status = "in_progress"`, `blocked`, or `failed` and explain why.

## Epic judgment

1. Find stories with `parent = "{epic_id}"`.
2. Verify all required stories are complete.
3. Check epic `[testing].criteria` against code/tests in `validator_context`.
4. Evaluate whether the job story is actually satisfied.
5. If passing, update epic `status = "complete"`.

## Mission judgment

1. Find epics with `parent = "{mission_id}"`.
2. Verify all required epics are complete.
3. Check mission `[testing].criteria` against validator context.
4. Evaluate whether the user progress and outcome are achieved.
5. If passing, update mission `status = "complete"`.

## Output format

```text
Judgment: <artifact-id> (<story|epic|mission>)
Verdict: PASS | FAIL

Criteria Results:
  [PASS] <criterion>
         Evidence: <file:line/test output>
  [FAIL] <criterion>
         Evidence: <missing behavior or failing test>

Summary: <passed>/<total> passed
Action: <status updates made or recommended>
```

Be strict about criteria but do not fail for unrelated style preferences.
