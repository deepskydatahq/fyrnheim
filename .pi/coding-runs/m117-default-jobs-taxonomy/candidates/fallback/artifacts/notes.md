# Candidate notes — m117-default-jobs-taxonomy / fallback

## Implementation summary

- Added a built-in dbt jobs taxonomy in `src/fyrnheim/dbt_classification.py` with seven default labels: `source_mapping`, `format_alignment`, `data_contract_enforcement`, `entity_definition`, `business_rule_application`, `analytical_output_shaping`, and `quality_validation`.
- `classify_inventory(inventory)` now works without a custom rules config by loading the default taxonomy. Custom YAML/JSON rules still work as before.
- `fyr dbt classify --inventory ...` now defaults to the built-in taxonomy; `--rules` remains available for overrides.
- Classification output now includes `layer_job_mismatch_evidence` per model and `summary.layer_job_mismatches` when configured mismatch checks find layer/job tension.
- Added docs in `docs/default-dbt-jobs-taxonomy.md` covering each default job's description, signals, examples, anti-examples, suggested principles, and relationship to common layer labels.
- Updated `docs/dbt-model-classification.md` for the default workflow and mismatch evidence.
- Added tests for default loading, CLI operation without `--rules`, evidence emission, mismatch evidence, and isolated default rule copies.

## Tradeoffs

- The taxonomy is embedded as a Python dictionary rather than a packaged YAML resource. This avoids packaging changes and keeps loading simple, but editing the taxonomy is less friendly than editing data-only YAML.
- Default rules rely on inventory metadata (paths, names, tags, lineage, tests, descriptions) rather than SQL parsing. This keeps the classifier transparent and compatible with M116, but complex business logic that is only visible inside SQL may not be detected.
- Default taxonomy sets `allow_multiple_labels: true`, because jobs are often cross-cutting. `primary_label` remains available for precedence, but consumers should inspect `matched_labels` and evidence.

## Risks / follow-up ideas

- Some naming conventions may produce false positives; teams should override defaults with project-specific rules when needed.
- Data contract detection uses attached tests, column documentation, tags, and metadata as signals; it does not infer contracts from compiled SQL.
- A future refinement could move the default taxonomy to a data file and expose a CLI command to print/export the defaults.
