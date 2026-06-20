# OpenRouter capped provider setup

Fyrnheim registers a project-local Pi provider for low-cost code candidate runs:

- Provider/model: `openrouter-capped/qwen/qwen3-coder`
- Extension: `.pi/extensions/openrouter-capped.ts`
- Base URL: `https://openrouter.ai/api/v1`
- Default max output tokens: `4096`
- Override max output tokens with: `FYRNHEIM_OPENROUTER_CODE_MAX_TOKENS`

## Credentials

Set `OPENROUTER_API_KEY` in your shell environment before running the model:

```bash
export OPENROUTER_API_KEY=...
```

Do not commit the key to this repository, `.env` files, prompts, logs, or candidate artifacts.

The provider intentionally registers with a non-secret sentinel when `OPENROUTER_API_KEY` is absent so `pi --list-models` can still prove that the model is configured. Actual model execution fails locally before making a network request with this message:

```text
OPENROUTER_API_KEY is required for openrouter-capped/qwen/qwen3-coder. Set it in the environment; do not commit it to the repository.
```

## Smoke tests

Verify model registration:

```bash
pi --list-models | grep 'openrouter-capped.*qwen/qwen3-coder'
```

Verify missing-credential behavior without making an OpenRouter request:

```bash
unset OPENROUTER_API_KEY
pi --no-session --model openrouter-capped/qwen/qwen3-coder --print "say ok"
```

With credentials present, run a minimal request:

```bash
pi --no-session --model openrouter-capped/qwen/qwen3-coder --print "Reply with ok."
```

## Candidate workflow usage

Use the model as a provider-backed candidate in `scripts/multi_model_coding.py` command mappings. Prefer an absolute `--run-dir` and placeholders because commands execute from each candidate worktree:

```bash
python scripts/multi_model_coding.py run-candidates \
  --run-dir "$(pwd)/.pi/coding-runs/<run-id>" \
  --command 'qwen=pi --no-session --model openrouter-capped/qwen/qwen3-coder --print "$(cat {prompt})"'
```
