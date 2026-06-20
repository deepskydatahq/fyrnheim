# Parallel multi-model coding workflow

Fyrnheim can evaluate multiple coding implementations for the same product story by running each candidate in an isolated git worktree and recording an evaluator judgement before anything is promoted back to the main workflow.

This is a local-first, provider-neutral workflow. Pi, Claude, another coding agent, or a human can execute a candidate prompt. The Fyrnheim product TOML files remain the source of truth throughout.

## Concepts

- **Run** — one comparison session for a single story or coding task.
- **Candidate** — one model/build variant attempting the run in its own branch/worktree.
- **Evaluator** — a separate review pass that compares candidates against the story acceptance criteria, diffs, tests, simplicity, maintainability, and risks.
- **Synthesis candidate** — an additional candidate created when no single candidate wins but multiple candidates contain useful parts.

Default run artifacts live under `.pi/coding-runs/<run-id>/`. Candidate worktrees default to a sibling worktree area under `../worktrees/coding-runs/<run-id>/<candidate>/`.

## Prepare candidates

Use `scripts/multi_model_coding.py prepare` to create run metadata, candidate metadata, prompts, artifact folders, and optionally git worktrees.

Dry-run example for planning or tests:

```bash
python scripts/multi_model_coding.py prepare \
  --story product/stories/M110-E001-S001-add-candidate-worktree-orchestrator.toml \
  --run-id m110-orchestrator \
  --variant sonnet=claude:sonnet-4.5 \
  --variant gpt=openai:gpt-5 \
  --dry-run
```

Real worktree setup omits `--dry-run`:

```bash
python scripts/multi_model_coding.py prepare \
  --story product/stories/<story>.toml \
  --run-id <run-id> \
  --variant sonnet=claude:sonnet-4.5 \
  --variant gpt=openai:gpt-5
```

Each candidate receives:

```text
.pi/coding-runs/<run-id>/
  run.toml
  candidates/<candidate>/
    candidate.toml
    prompt.md
    artifacts/
      status.toml
      notes.md                 # written by candidate
      quality-gates.txt        # written by candidate
```

`candidate.toml` records the variant name, model/build metadata, branch, worktree, artifact directory, status, and dry-run flag.

## Execute candidates

Open each candidate worktree and run the generated `prompt.md` with the intended coding model/tool. Candidate agents must:

1. Work only inside their own worktree.
2. Avoid mutating sibling worktrees or the source checkout.
3. Keep candidate commits on the candidate branch.
4. Write implementation notes and quality output under the candidate artifact directory.
5. Push the candidate branch when complete.

Candidate runs should not mark the shared story complete unless that candidate has later been selected and promoted.

## Evaluate candidates

The evaluator compares each candidate using at least these inputs:

- story TOML outcome and acceptance criteria
- candidate diff and commits
- `artifacts/notes.md`
- `artifacts/quality-gates.txt`
- `artifacts/status.toml`
- code simplicity and maintainability
- risks and follow-up work

Record the evaluator decision with `judge`:

```bash
python scripts/multi_model_coding.py judge \
  --run-dir .pi/coding-runs/<run-id> \
  --decision accept_candidate \
  --winner sonnet \
  --reason "Smallest complete diff with passing tests" \
  --risk "Needs normal PR review" \
  --follow-up "Document any evaluator learnings"
```

Supported decisions:

- `accept_candidate` — choose one candidate for promotion. Requires `--winner <candidate-slug>`.
- `synthesize` — request another run that combines useful parts. Requires `--synthesis-guidance`.
- `all_failed` — no candidate is acceptable; document blockers and follow-up work.

The judgement is written to `.pi/coding-runs/<run-id>/judgement.toml` and includes candidate dispositions, criteria placeholders, risks, follow-up work, reason, winner, and synthesis guidance when applicable.

## Request synthesis

When the decision is `synthesize`, first log the judgement:

```bash
python scripts/multi_model_coding.py judge \
  --run-dir .pi/coding-runs/<run-id> \
  --decision synthesize \
  --reason "Candidate A has stronger tests; candidate B has simpler API" \
  --synthesis-guidance "Use A's tests and B's smaller public surface. Avoid A's extra abstraction."
```

Then create a new synthesis candidate:

```bash
python scripts/multi_model_coding.py synthesize \
  --run-dir .pi/coding-runs/<run-id> \
  --variant synthesis=claude:sonnet-4.5 \
  --from candidate-a \
  --from candidate-b \
  --guidance "Use A's tests and B's smaller public surface. Avoid A's extra abstraction."
```

The synthesis candidate gets a fresh prompt and, unless `--dry-run` is used, a fresh worktree. It should re-implement the simplest coherent solution rather than blindly merge patches.

## Promote a selected candidate

Promotion is explicit and happens only after judgement:

1. Check `judgement.toml` and identify the accepted or synthesized candidate.
2. Inspect the selected candidate's branch, commits, diff, notes, and quality output.
3. Bring the selected changes into the normal feature branch using the team's preferred git flow.
4. Run `scripts/quality-gates.sh` from the promoted branch.
5. Update the product story/epic/mission TOML statuses.
6. Commit, push, open/update the PR, and use the normal review workflow.

Do not automatically merge unreviewed candidate code into `main`. Product TOML stories remain canonical; candidate artifacts are provenance for how a decision was made.

## Artifact schema summary

`run.toml`:

- `schema_version = "fyrnheim.coding_run.v1"`
- `run_id`
- `story_id`
- `story_path`
- `base_ref`
- `status`
- `candidate_count`

`candidate.toml`:

- `schema_version = "fyrnheim.coding_candidate.v1"`
- `variant_name`
- `model`
- `build`
- `branch`
- `worktree`
- `artifact_dir`
- `status`

`judgement.toml`:

- `schema_version = "fyrnheim.coding_judgement.v1"`
- `decision`
- `winner`
- `reason`
- `synthesis_guidance`
- `risks`
- `follow_up`
- `[[candidates.summary]]`
- `[[evaluation.criteria]]`

## Current limits

- The script prepares and records workflow artifacts; it does not launch provider-specific coding agents.
- The first version is local-first. Parallelism can be achieved by running candidate agents in separate shells/sessions against the prepared worktrees.
- Quality-gate collection is currently a candidate responsibility recorded in artifacts; evaluator automation can be added later.
