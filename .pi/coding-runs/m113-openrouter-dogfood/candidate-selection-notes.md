# M113 candidate selection notes

Source inspected:

- `/home/tmo/roadtothebeach/propel/propel-explorer/.pi/extensions/openrouter-capped.ts`
- `/home/tmo/roadtothebeach/propel/propel-explorer/.pi/scripts/multi-model-mission.sh`
- `/home/tmo/roadtothebeach/propel/propel-explorer/.pi/prompts/multi-model-mission.md`

## Selected candidates

1. `qwen=openrouter-capped/qwen/qwen3-coder`
   - Chosen because Propel Explorer registers this model through `openrouter-capped` for code execution.
   - It has a large declared context window and capped max output tokens via `PI_EXPLORER_OPENROUTER_CODE_MAX_TOKENS` (default 4096).
   - Propel Explorer records zero cost in the local provider metadata, so observed cost must come from OpenRouter/provider logs if available.

2. `fallback=openai-codex:gpt-5.5-high`
   - Chosen as the stronger fallback/evaluator-style comparison because Propel Explorer uses `openai-codex/gpt-5.5:high` for mission planning/evaluation and as the code fallback when Qwen tool use fails.

## Expected risks

- The `openrouter-capped` provider is registered in Propel Explorer's `.pi/extensions`, not Fyrnheim's current `.pi/extensions`, so the Qwen command may fail unless this repo has equivalent provider registration loaded.
- No secrets should be written to artifacts. Command logs were inspected before commit.
