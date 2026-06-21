# dbt Data Model Principles

M118 adds a small principles engine for checking dbt model quality from Fyrnheim inventory and classification artifacts. It is not a SQL linter and it does not replace dbt tests. The goal is to encode data-model review guidance as configurable, evidence-backed checks.

## Quick start

```bash
uv run fyr dbt scan --project-path path/to/dbt_project --output inventory.json
uv run fyr dbt classify --inventory inventory.json --output classification.json
uv run fyr dbt principles --inventory inventory.json --classification classification.json
```

For one model:

```bash
uv run fyr dbt principles \
  --inventory inventory.json \
  --classification classification.json \
  --model fct_orders
```

Write JSON for agents or CI prototypes:

```bash
uv run fyr dbt principles \
  --inventory inventory.json \
  --classification classification.json \
  --output principles.json \
  --format json
```

## Result artifact

The command emits `fyrnheim.dbt_principles.v1` JSON with:

- project metadata
- principle config schema/source
- optional classification schema
- summary counts for pass, fail, warning, not applicable, and config errors
- one finding per model/principle pair

Each finding includes:

- `model_unique_id` and `model_name`
- `principle_id` and title
- `status`: `pass`, `fail`, `warning`, `not_applicable`, or `config_error`
- `severity`: usually `error` or `warning`
- evidence showing the fields and values used
- remediation text

## Built-in starter principles

The default starter set is intentionally small and opinionated:

| Principle | Applies when | Check | Why |
| --- | --- | --- | --- |
| `owner-presence` | every model | `owner` or `meta.owner` exists | someone should be accountable for model semantics |
| `description-presence` | every model | `description` is non-empty | agents and reviewers need model purpose |
| `analytical-output-grain` | analytical output labels or mart/report paths | `meta.grain`, `meta.primary_key`, or grain language in the description | consumers need to know what one row means |
| `source-contract-tests` | source mapping labels or source dependencies | at least one attached test | raw-source assumptions should be protected |
| `high-risk-model-tests` | tables, incremental models, widely consumed models, or business-rule models | at least two attached tests | high-impact models need safer changes |
| `complexity-warning` | six or more upstream dependencies | warn when upstream fan-in is high | complex models deserve decomposition or documentation review |

These checks are starter guidance, not a universal modeling standard. Teams should copy and adapt them as they learn what is high-signal for their dbt project.

## Config format

A principle config is YAML or JSON:

```yaml
schema_version: fyrnheim.dbt_principle_config.v1
principles_source: analytics-team-principles.v1
principles:
  - id: finance-mart-owner
    title: Finance marts have owners
    severity: error
    description: Finance-facing marts need explicit ownership.
    applies_to:
      all:
        - field: path
          operator: contains
          value: marts
        - field: materialized
          operator: equals
          value: table
        - field: classification.labels
          operator: contains
          value: analytical_output_shaping
    check:
      operator: present
      field: owner
    remediation: Add owner or meta.owner in the model YAML.
```

### `applies_to` conditions

Use `all` and/or `any` condition lists. A principle is not applicable when conditions do not match.

Supported condition operators:

- `equals`
- `contains`
- `contains_any`
- `regex`
- `exists`
- `count_gte`

Conditions can inspect model inventory fields and attached classification fields, for example:

- `path`
- `materialized`
- `meta.domain`
- `owner`
- `depends_on`
- `downstream`
- `attached_tests`
- `classification.labels`
- `classification.primary_label`

This lets principles depend on metadata, path, materialization, lineage, and model classification.

### Check operators

Supported check operators:

- `present`: one field must be present and non-empty.
- `any_present`: at least one field in `fields` must be present.
- `any_present_or_regex`: at least one field is present or one regex field matches `value`.
- `count_gte`: a list-like field must have at least `value` items.
- `max_count`: a list-like field must have no more than `value` items.

Invalid config is reported as `config_error` findings so users can see configuration problems without losing the rest of the report.

## Relationship to classification

Principles become more useful when paired with `fyr dbt classify`. For example, grain clarity should apply to analytical outputs, while source contract tests should apply to source mapping models. If no classification artifact is provided, path, materialization, metadata, and lineage conditions still work.

## Modeling philosophy

Use principles for review guidance such as:

- Is the model's purpose documented?
- Is the row grain clear where consumers depend on it?
- Are source assumptions and high-risk transformations protected by tests?
- Is ownership explicit?
- Is the model becoming too complex to reason about safely?

Avoid using principles for generic formatting or SQL style rules. Those belong in tools like SQLFluff. Fyrnheim principles should explain why a model is or is not a good model for its job.
