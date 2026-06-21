# dbt Model Classification

Fyrnheim model classification labels dbt models from an M115 inventory artifact using transparent, configurable rules. The goal is not to guess with an LLM; it is to make the team's model taxonomy inspectable and adjustable.

## Command

First create an inventory:

```bash
fyr dbt scan --output .fyrnheim/dbt-inventory.json
```

Then classify it:

```bash
fyr dbt classify \
  --inventory .fyrnheim/dbt-inventory.json \
  --rules fyrnheim-classification.yml \
  --output .fyrnheim/dbt-classification.json
```

The command prints a concise summary and writes a versioned JSON artifact. To print the artifact to stdout:

```bash
fyr dbt classify --inventory .fyrnheim/dbt-inventory.json --rules fyrnheim-classification.yml --format json
```

## Rule file

Rules can be YAML or JSON. A minimal YAML file:

```yaml
schema_version: fyrnheim.dbt_classification_rules.v1
allow_multiple_labels: false
rules:
  - id: staging-path
    label: source_mapping
    priority: 10
    any:
      - field: path
        operator: contains
        value: /staging/
      - field: tags
        operator: contains
        value: staging

  - id: marts-path
    label: analytical_output
    priority: 20
    all:
      - field: path
        operator: contains
        value: /marts/
      - field: downstream
        operator: count_gte
        value: 1
```

Each rule has:

- `id` — stable rule identifier for evidence.
- `label` — classification label to apply when the rule matches.
- `priority` — lower numbers win precedence for `primary_label`.
- `all` — every listed condition must match.
- `any` — at least one listed condition must match.

If both `all` and `any` are present, all `all` conditions and at least one `any` condition must match. `conditions` is accepted as a synonym for `all` for simple rules.

## Fields

Conditions can read model fields from the inventory:

- `name`
- `path`
- `original_file_path`
- `materialized`
- `tags`
- `description`
- `package_name`
- `meta.<key>`
- `depends_on`
- `downstream`
- `attached_tests`
- `columns`

They can also read inventory-level fields with the `inventory.` prefix:

```yaml
- field: inventory.generation_source
  operator: equals
  value: manifest
```

This is useful when rules should behave differently for manifest-backed inventories versus fallback project-file scans.

## Operators

Supported operators:

- `equals`
- `not_equals`
- `contains`
- `contains_any`
- `startswith`
- `endswith`
- `regex`
- `in`
- `exists` / `not_empty`
- `missing`
- `count_gte`
- `count_lte`

List fields such as `tags`, `depends_on`, and `downstream` work naturally with `contains`, `contains_any`, and count operators.

## Output

The classification artifact has schema version:

```json
{
  "schema_version": "fyrnheim.dbt_classification.v1",
  "project": {"name": "jaffle_shop"},
  "summary": {
    "total": 3,
    "classified": 2,
    "unclassified": 1,
    "ambiguous": 0,
    "labels": {"source_mapping": 1, "analytical_output": 1}
  },
  "models": []
}
```

Each model result includes:

- `unique_id`
- `name`
- `path`
- `status` — `classified`, `unclassified`, or `ambiguous`
- `primary_label`
- `labels`
- `matched_labels`
- `evidence`

Evidence records the matching rule, condition field, operator, expected value, and actual value. This is what makes the classifier inspectable: users should be able to understand why a label was applied and adjust the rule if needed.

## Ambiguity and multiple labels

By default, `allow_multiple_labels: false`. If more than one rule matches a model, Fyrnheim marks the model `ambiguous`, sets `primary_label` by rule priority, and preserves all matches in `matched_labels` and `evidence`.

Set `allow_multiple_labels: true` when a model should intentionally receive multiple labels, such as a model being both an analytical output and a finance-domain model.

## Relationship to default jobs

This mission provides the configurable rule engine. The opinionated default jobs taxonomy — source mapping, format alignment, data contract enforcement, entity definition, business rule application, analytical output shaping, and quality validation — is planned separately in M117.
