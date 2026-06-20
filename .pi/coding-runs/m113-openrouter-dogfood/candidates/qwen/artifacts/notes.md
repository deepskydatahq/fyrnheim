# Qwen candidate notes

## Summary

The Qwen/OpenRouter candidate was selected from Propel Explorer's `openrouter-capped` configuration:

- Model: `openrouter-capped/qwen/qwen3-coder`
- Rationale: low-cost/high-context code execution path used by Propel Explorer.

## Result

The provider command failed before candidate work could run:

```text
Error: Model "openrouter-capped/qwen/qwen3-coder" not found. Use --list-models to see available models.
```

This indicates Fyrnheim's current `.pi` configuration does not register the Propel Explorer `openrouter-capped` provider. No OpenRouter token/cost data was available because no OpenRouter request was made.

## Disposition

Rejected as unavailable in this checkout. A future mission should add or configure a Fyrnheim-local OpenRouter provider before re-testing this candidate.
