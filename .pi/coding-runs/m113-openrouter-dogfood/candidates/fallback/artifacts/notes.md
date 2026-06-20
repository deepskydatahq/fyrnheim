# Fallback candidate notes

## Summary

- Proposed a small documentation clarification in `docs/multi-model-coding-workflow.md` for provider-backed candidate execution.
- The clarification uses the observed dogfood failure mode: `run-candidates` launches commands from each candidate worktree, so repo-relative prompt paths can break. The docs now recommend an absolute `--run-dir` and `{prompt}`/`{artifact_dir}` placeholders.
- Preserved the candidate diff in `diff.patch` and quality output in `quality-gates.txt`.

## Provider observations

- Candidate selection came from Propel Explorer:
  - `qwen=openrouter-capped/qwen/qwen3-coder` for capped, high-context OpenRouter code execution.
  - `fallback=openai-codex/gpt-5.5:high` for stronger fallback/evaluator-style execution.
- The qwen provider command failed because this Fyrnheim checkout does not have the `openrouter-capped` provider registered: `Model "openrouter-capped/qwen/qwen3-coder" not found`.
- The fallback provider command exited 0, but stderr shows the prompt was not read because the command used a repo-relative `.pi/.../prompt.md` path from inside the candidate worktree. Stdout was empty, so this candidate implemented the tiny docs change manually using the captured failure as evidence.

## Tradeoffs

- Kept the change documentation-only instead of changing runner behavior, because the story asks for a tiny docs improvement and the observed command-path issue can be avoided by invoking `run-candidates` with an absolute run directory.
- Did not mark the shared product story complete from the candidate branch.

## Risks and follow-ups

- A code hardening follow-up may still be useful: normalize `--run-dir` to an absolute path inside `scripts/multi_model_coding.py` before formatting command placeholders.
- Re-run qwen only after loading/registering the Propel Explorer `openrouter-capped` provider or adding an equivalent Fyrnheim extension.
