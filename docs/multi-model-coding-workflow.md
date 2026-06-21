# Parallel multi-model coding workflow

Fyrnheim can evaluate multiple coding implementations for the same product story by running each candidate in an isolated git worktree and recording an evaluator judgement before anything is promoted back to the main workflow.

This is a local-first, provider-neutral workflow. Pi, Claude, another coding agent, or a human can execute a candidate prompt. The Fyrnheim product TOML files remain the source of truth throughout.

## Concepts

- **Execution strategy** — an optional product TOML convention that tells mission execution whether to use the normal single-agent path or the multi-model candidate workflow.
- **Run** — one comparison session for a single story or coding task.
- **Candidate** — one model/build variant attempting the run in its own branch/worktree.
- **Evaluator** — a separate review pass that compares candidates against the story acceptance criteria, diffs, tests, simplicity, maintainability, and risks.
- **Synthesis candidate** — an additional candidate created when no single candidate wins but multiple candidates contain useful parts.

Default run artifacts live under `.pi/coding-runs/<run-id>/`. Candidate worktrees default to a sibling worktree area under `../worktrees/coding-runs/<run-id>/<candidate>/`.

## Product TOML integration

Single-agent execution remains the default for missions and stories. Add `[execution_strategy]` only when multi-model comparison is useful or required:

```toml
[execution_strategy]
mode = "recommend_multi_model"  # single_agent | recommend_multi_model | multi_model
rationale = "Multiple plausible approaches; compare candidates before promotion."
dry_run_allowed = false
candidate_variants = [
  "qwen=openrouter-capped/qwen/qwen3-coder",
  "fallback=openai-codex:gpt-5.5-high"
]
required_artifacts = [
  "run.toml",
  "candidate artifacts",
  "judgement.toml",
  "cleanup evidence"
]
```

Use `single_agent` for small, mechanical stories. Use `recommend_multi_model` when candidate comparison may reduce risk, but the executor can downgrade to single-agent with a documented reason. Use `multi_model` when the story explicitly requires candidate runs, evaluator judgement, or workflow dogfooding.

Stories completed through this workflow should also record the result:

```toml
[multi_model]
run_dir = ".pi/coding-runs/<run-id>"
decision = "accept_candidate"
winner = "fallback"
cleanup = ".pi/coding-runs/<run-id>/cleanup.out"
provider_blockers = []
```

Completion requires the run directory, candidate artifacts, `judgement.toml`, selected/synthesized outcome or `all_failed` rationale, and cleanup evidence. Missing provider credentials must be recorded as provider blockers or candidate failures; do not silently skip them.

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

Fyrnheim includes a project-local OpenRouter provider for the low-cost Qwen code candidate used during dogfood runs: `openrouter-capped/qwen/qwen3-coder`. See `docs/openrouter-provider-setup.md` for credential guardrails, token cap configuration, and smoke tests.

You can run candidates manually by opening each worktree and feeding `prompt.md` to the intended coding model/tool. For provider-backed local execution, use `run-candidates` with command mappings:

```bash
python scripts/multi_model_coding.py run-candidates \
  --run-dir "$(pwd)/.pi/coding-runs/<run-id>" \
  --command 'sonnet=claude -p "$(cat {prompt})"' \
  --command 'gpt=codex exec {prompt}'
```

Use `default=...` to provide one command for every candidate without a specific mapping:

```bash
python scripts/multi_model_coding.py run-candidates \
  --run-dir "$(pwd)/.pi/coding-runs/<run-id>" \
  --command 'default=python -c "print(open(\"{prompt}\").read())"' \
  --dry-run
```

Command templates support:

- `{prompt}` — candidate prompt path
- `{artifact_dir}` — candidate artifact directory
- `{worktree}` — candidate worktree path
- `{candidate}` — candidate slug

The runner executes each command from that candidate's worktree, so avoid hard-coded repo-relative paths in command mappings. Prefer an absolute `--run-dir` (for example `--run-dir "$(pwd)/.pi/coding-runs/<run-id>"`) and the `{prompt}`/`{artifact_dir}` placeholders so provider commands can still read prompts and write artifacts after the working directory changes.

The runner writes:

- `artifacts/execution.toml` — command, worktree, exit code, duration, timestamps
- `artifacts/stdout.txt` — provider stdout
- `artifacts/stderr.txt` — provider stderr

Candidate agents must:

1. Work only inside their own worktree.
2. Avoid mutating sibling worktrees or the source checkout.
3. Keep candidate commits scoped to the candidate branch.
4. Write implementation notes and quality output under the candidate artifact directory.
5. Push the candidate branch when complete.

Candidate runs should not mark the shared story complete unless that candidate has later been selected and promoted.

## Evaluate candidates

Before judging, check artifact readiness:

```bash
python scripts/multi_model_coding.py check-artifacts \
  --run-dir .pi/coding-runs/<run-id>
```

The check reports missing `notes.md`, `quality-gates.txt`, `status.toml`, and `diff.patch` for each candidate. It exits successfully only when candidates have all expected artifacts, or when a candidate explicitly reports an incomplete terminal status such as `failed`, `blocked`, `incomplete`, or `skipped` in `status.toml`.

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

## Clean up candidate worktrees

After promotion and review, remove candidate worktrees safely:

```bash
python scripts/multi_model_coding.py cleanup \
  --run-dir .pi/coding-runs/<run-id> \
  --dry-run
```

When the dry run looks right, remove clean worktrees:

```bash
python scripts/multi_model_coding.py cleanup \
  --run-dir .pi/coding-runs/<run-id>
```

Dirty worktrees are protected by default. Inspect them with `git -C <worktree> status`; only use `--force` when you intentionally want to discard candidate scratch work.

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

`execution.toml`:

- `candidate`
- `status`
- `command`
- `worktree`
- `exit_code`
- `duration_seconds`
- `started_at`
- `completed_at`
- `stdout`
- `stderr`

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

## Troubleshooting

Use these recovery steps when a candidate run gets messy:

| Problem | Recovery |
| --- | --- |
| `gh` reports bad credentials when creating a PR | Keep the pushed branch, re-authenticate with `gh auth login`, then create or update the PR without rerunning candidates. |
| A candidate did not write artifacts | Treat the candidate as incomplete until `notes.md`, `quality-gates.txt`, and `status.toml` exist. Record missing artifacts as evaluator risks or rejection reasons. |
| Candidate worktree has scratch changes | Inspect with `git -C <worktree> status`, save useful notes, then commit or discard before removal. |
| Worktree cleanup fails or a directory was removed manually | Check `git worktree list`, remove clean worktrees with `git worktree remove <path>`, then run `git worktree prune` if needed. |
| No single candidate wins | Choose `synthesize`, write guidance naming the useful pieces from each candidate, and create a fresh synthesis candidate instead of merging patches blindly. |

## Current limits

- Provider-backed execution is command-based. The workflow does not import provider SDKs or manage provider credentials.
- The first version is local-first. Parallelism can be achieved by running candidate commands from separate shells/sessions, or by launching one run at a time through `run-candidates`.
- Quality-gate collection is currently a candidate responsibility recorded in artifacts; evaluator automation can be added later.
