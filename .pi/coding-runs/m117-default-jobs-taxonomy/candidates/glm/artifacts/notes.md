# Candidate Notes — glm (M117-E001-S001: Default Jobs Taxonomy)

## Summary

Implemented an opinionated default jobs taxonomy for dbt model classification that
the M116 classifier loads and applies without a custom rules file, with model-level
evidence and layer/job mismatch detection.

### Files

- `src/fyrnheim/default_jobs_taxonomy.py` (new) — single source of truth. The
  taxonomy ships as an inline YAML constant (`DEFAULT_TAXONOMY_YAML`) parsed on
  demand, so docs and rules stay aligned. Exposes `load_default_taxonomy()`,
  `default_classification_rules()`, `load_default_classification_rules()`,
  `describe_default_job()`, and the `DEFAULT_JOB_IDS` tuple.
- `src/fyrnheim/dbt_classification.py` (modified) — added `layer_mapping` support
  to `classify_inventory`, plus per-model `layer` assessment with mismatch
  evidence when `flag_layer_job_mismatch` is true. Layer detection reuses the
  existing rule matcher (`_match_rule`) so layers use the same operators as
  rules. Summary now reports `layer_job_mismatches`.
- `src/fyrnheim/cli.py` (modified) — `fyr dbt classify` uses the default taxonomy
  automatically when `--rules` is omitted; `--rules` help text updated.
- `docs/dbt-default-jobs-taxonomy.md` (new) — human docs: why jobs not only
  layers, the seven default jobs each with description/signals/examples/
  anti-examples/suggested principles/related layers, layer mapping table,
  mismatch detection example, override guidance, relationship to dbt layers.
- `docs/dbt-model-classification.md` (modified) — cross-link to default taxonomy,
  clarify default behavior when `--rules` is omitted.
- `tests/test_default_jobs_taxonomy.py` (new) — 10 tests covering taxonomy
  shape, per-job describe, classification without a rules file, mismatch
  detection (on/off), layer mapping in output, summary text, and CLI flows.

### The seven default jobs

source_mapping, format_alignment, data_contract_enforcement,
entity_definition, business_rule_application, analytical_output_shaping,
quality_validation — mirroring the layers-and-jobs blog framing.

Each job rule accepts `meta.job` as an explicit override so project intent wins
over heuristics. Layers cover staging/intermediate/marts and medallion
bronze/silver/gold.

## How acceptance criteria are met

1. **Load/apply defaults without a custom rules file** — `load_default_classification_rules()`
   produces a config compatible with `classify_inventory`; CLI uses it when
   `--rules` omitted. Verified by `test_default_classification_rules_can_classify_without_custom_file`
   and the two CLI tests.
2. **Model-level evidence + layer/job mismatch evidence** — every model record
   carries `evidence` (matched rule + condition evidence); the `layer` assessment
   carries detection `evidence` plus `mismatch_evidence` when a matched job is
   not implied by the detected layer. Verified by
   `test_default_classification_flags_layer_job_mismatch` and
   `test_default_classification_layer_mapping_included_in_output`.
3. **Docs describe each default job** with description, signals, examples,
   anti-examples, suggested principles, and related layers — see
   `docs/dbt-default-jobs-taxonomy.md`.

## Tradeoffs

- **Single inline YAML source.** The taxonomy lives as a Python string constant
  rather than a separate `.yaml` data file. This keeps docs and rules literally
  the same object and avoids a packaging/import-resources step, at the cost of
  edits happening inside a Python module. Round-trip YAML validity is asserted
  by `test_default_taxonomy_round_trips_as_yaml_for_docs`.
- **Defaults are opinionated but humble.** `allow_multiple_labels` is true and
  mismatch detection is informational (not an error), preserving the blog's
  nuance that layers are useful but not enough.
- **Layer detection reuses the rule matcher.** Layers are expressed with the
  same `any`/`all` condition syntax as rules, so there is one matcher to
  maintain. A layer is detected on the first match (priority by list order).
- **M118 enforcement is out of scope.** Suggested principles are documented but
  not enforced, per the mission's `out_of_scope`.

## Risks

- **Heuristic rules can over- or under-label.** The defaults are deliberately
  broad (e.g., `name` regex for `clean|cast|...`). Projects can override with a
  custom rules file or `meta.job`. This is acceptable for an opinionated default
  but worth calling out.
- **Layer detection is first-match by list order.** A model matching multiple
  layer detectors (e.g., tagged `silver` but path under `/marts/`) gets only the
  first. Order in the YAML currently lists staging→gold, which is reasonable but
  not order-independent.
- **Pre-existing test failures unrelated to this change** (clickhouse/bigquery
  missing optional deps, a pandas `NaN` vs `None` in `test_multi_model`, and one
  arrow-projection test) are present on the base commit and not caused by this
  candidate. See `quality-gates.txt`.

## Quality gates

- `uv run ruff check src/ tests/` — clean.
- `uv run mypy src/` — clean (70 files).
- `uv run pytest` — 750 passed, 9 pre-existing failures (verified by stashing
  this candidate's changes and re-running; failures persist on the base
  commit). The candidate's own tests (`test_default_jobs_taxonomy.py`,
  `test_dbt_classification.py`) all pass.

## Status

`complete` — all acceptance criteria implemented, tested, documented, and
committed on the candidate branch. Not marking the shared story complete; that
is the evaluator's job after promoting a candidate.
