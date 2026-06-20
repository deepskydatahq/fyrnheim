import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { streamSimpleOpenAICompletions } from "@earendil-works/pi-ai";

const DEFAULT_MAX_TOKENS = Number(process.env.FYRNHEIM_OPENROUTER_CODE_MAX_TOKENS ?? "4096");
const MISSING_API_KEY_SENTINEL = "__missing_openrouter_api_key__";

export default function (pi: ExtensionAPI) {
  pi.registerProvider("openrouter-capped", {
    name: "OpenRouter capped for code execution",
    baseUrl: "https://openrouter.ai/api/v1",
    // Use a non-secret sentinel instead of "$OPENROUTER_API_KEY" so `pi --list-models`
    // can still show the provider when credentials are absent. The stream guard below
    // fails closed with a clear local error before any network request is attempted.
    apiKey: process.env.OPENROUTER_API_KEY ?? MISSING_API_KEY_SENTINEL,
    api: "openai-completions",
    streamSimple: (model, context, options) => {
      const apiKey = process.env.OPENROUTER_API_KEY;
      if (!apiKey) {
        throw new Error(
          "OPENROUTER_API_KEY is required for openrouter-capped/qwen/qwen3-coder. " +
            "Set it in the environment; do not commit it to the repository.",
        );
      }

      return streamSimpleOpenAICompletions(model as any, context, {
        ...options,
        apiKey,
        maxTokens: options?.maxTokens ?? DEFAULT_MAX_TOKENS,
      } as any);
    },
    models: [
      {
        id: "qwen/qwen3-coder",
        name: "Qwen3 Coder via OpenRouter (capped)",
        reasoning: false,
        input: ["text"],
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
        contextWindow: 1_000_000,
        maxTokens: DEFAULT_MAX_TOKENS,
        compat: {
          supportsDeveloperRole: false,
          supportsReasoningEffort: false,
          maxTokensField: "max_tokens",
        },
      },
    ],
  });
}
