# M113 OpenRouter provider dogfood retro

Date: 2026-06-20
Run: `.pi/coding-runs/m113-openrouter-dogfood`
Story: `M113-E001-S001`

## Candidate selection

Selection came from Propel Explorer's Pi setup:

- `qwen=openrouter-capped/qwen/qwen3-coder`
  - Source: `/home/tmo/roadtothebeach/propel/propel-explorer/.pi/extensions/openrouter-capped.ts`
  - Rationale: Propel Explorer's low-cost/high-context capped code execution path.
  - Max output cap: `PI_EXPLORER_OPENROUTER_CODE_MAX_TOKENS`, default 4096.
- `fallback=openai-codex:gpt-5.5-high`
  - Source: Propel Explorer's `.pi/scripts/multi-model-mission.sh` fallback/evaluator defaults.
  - Rationale: stronger quality baseline and known available model in this Fyrnheim checkout.

## What happened

- `prepare` created qwen and fallback worktrees and metadata.
- Initial provider run exposed a bug in `scripts/multi_model_coding.py`: `{prompt}` was formatted as a repo-relative path, but commands execute from candidate worktrees.
- The runner was fixed to format `{prompt}` as an absolute path.
- Final `run-candidates` results:
  - `fallback` executed successfully with `openai-codex/gpt-5.5:high` and produced usable guidance.
  - `qwen` failed before making a provider request because `openrouter-capped/qwen/qwen3-coder` is not registered in Fyrnheim's current Pi provider configuration.
- `check-artifacts` passed after qwen was marked explicitly failed and all expected artifacts were present.
- `judgement.toml` accepted `fallback` and recorded qwen as unavailable.
- `cleanup` removed both candidate worktrees cleanly.

## Cost/token observations

- Qwen/OpenRouter produced no token/cost data because no OpenRouter request was made; Pi rejected the model name locally.
- Fallback command output did not expose token/cost data.
- Propel Explorer's local provider metadata declares qwen cost as zero, but real accounting still needs OpenRouter request metadata or dashboard data.

## What worked

- Provider-backed command execution captured stdout, stderr, exit code, duration, and command metadata.
- The artifact readiness check correctly allowed an explicitly failed qwen candidate while requiring artifacts.
- Cleanup dry-run and real cleanup were useful and safe.
- The stronger fallback candidate discovered and documented a real command-path issue.

## Friction / follow-up

- Fyrnheim needs its own OpenRouter provider setup or a documented way to load Propel Explorer's provider before qwen can be tested for real.
- Provider command examples should prefer absolute `--run-dir` until all placeholders are guaranteed absolute.
- Token/cost accounting is not yet captured by the workflow.

Follow-up filed:

- `M114-fyrnheim-openrouter-provider-configuration.toml`
