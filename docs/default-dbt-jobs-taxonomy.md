# Default dbt Jobs Taxonomy

Fyrnheim's default dbt taxonomy classifies models by the job they appear to perform. Layers such as staging, intermediate, marts, bronze, silver, and gold are still useful, but they are treated as signals rather than the taxonomy itself.

The defaults are intentionally humble: they provide inspectable evidence and a starting point for discussion. Override them with `fyr dbt classify --rules <file>` when your team uses different conventions.

## Jobs

### `source_mapping`

**Description:** Map raw dbt sources or bronze inputs into project-owned model names with minimal semantic change.

**Typical signals:**

- Path segments such as `models/staging/`, `models/bronze/`, `models/source/`, or `models/sources/`.
- Names such as `stg_*`, `src_*`, or `base_*`.
- Tags such as `staging`, `bronze`, or `source_mapping`.
- Direct dependencies on dbt `source.*` nodes.

**Examples:**

- `models/staging/stg_stripe__payments.sql`
- `models/bronze/base_salesforce__accounts.sql`

**Anti-examples:**

- `fct_revenue` that aggregates orders for reporting.
- `dim_customer` that defines a durable customer entity with business identity rules.

**Suggested principles:** Keep transformations thin, preserve source grain, name columns consistently, document source assumptions, and avoid business-specific calculations.

**Common layer relation:** Strongly associated with staging and bronze. It may appear in silver when teams combine source mapping and normalization, but it is surprising in marts or gold.

### `format_alignment`

**Description:** Align types, naming, grain, units, and formatting so downstream models receive consistent inputs.

**Typical signals:**

- Path segments such as `silver`, `standardized`, `normalized`, or `cleaned`.
- Names containing `clean`, `cast`, `dedupe`, `format`, `normalize`, `rename`, or `standardize`.
- Tags such as `silver`, `format_alignment`, `standardized`, or `normalized`.
- Descriptions mentioning cleaning, casting, deduplication, normalization, standardization, or renaming.

**Examples:**

- `models/silver/standardized_payments.sql`
- `models/intermediate/int_orders_deduped.sql`

**Anti-examples:**

- A raw `stg_*` model that only exposes source columns.
- A `report_revenue_dashboard` model that chooses dashboard-specific measures.

**Suggested principles:** Make conversions explicit, use stable canonical names, isolate deduplication choices, and avoid hiding domain policy in formatting models.

**Common layer relation:** Common in silver and intermediate layers. It can sit after staging/bronze and before entity, business-rule, or mart models.

### `data_contract_enforcement`

**Description:** Make assumptions explicit with tests, documented columns, or contract metadata.

**Typical signals:**

- Attached dbt tests.
- Several documented columns in model metadata.
- Tags such as `contract`, `data_contract`, or `tested`.
- `meta.contract` metadata.

**Examples:**

- `stg_customers` with `not_null` and `unique` tests on `customer_id`.
- `dim_product` with documented columns and contract metadata.

**Anti-examples:**

- A scratch model with no tests or column metadata.
- A model that only performs reconciliation checks without defining an input/output contract; that is closer to `quality_validation`.

**Suggested principles:** Put tests close to the assumptions they protect, document grain and required keys, distinguish source constraints from business expectations, and keep evidence inspectable.

**Common layer relation:** Cross-cutting. It often appears in staging, silver, entities, and marts because contracts can be enforced at many boundaries.

### `entity_definition`

**Description:** Define durable business entities and dimensions such as customers, accounts, products, or orders.

**Typical signals:**

- Path segments such as `dimensions`, `entities`, `entity`, or `dim`.
- Names such as `dim_customer`, `entity_account`, or models ending in `_entity`, `_customer`, `_account`, `_user`, `_product`, `_order`, or `_subscription`.
- Tags such as `dimension`, `entity`, or `entity_definition`.
- Descriptions mentioning entities, dimensions, or common business objects.

**Examples:**

- `models/marts/core/dim_customer.sql`
- `models/entities/account_entity.sql`

**Anti-examples:**

- `stg_stripe__customers`, which maps a source but does not define the durable customer concept.
- `fct_orders_daily`, which shapes analytical measures rather than defining the order entity.

**Suggested principles:** State the entity grain, choose durable identifiers, keep identity resolution explicit, and separate entity definition from report-specific metrics.

**Common layer relation:** Often found in dimensions, entity, marts, or gold layers. It can be fed by staging/silver models but is usually not itself a raw staging job.

### `business_rule_application`

**Description:** Apply domain-specific calculations, eligibility rules, categorization, enrichment, or aggregation.

**Typical signals:**

- Path segments such as `intermediate`, `business`, `rules`, or `calculations`.
- Names such as `int_*`, `calc_*`, `agg_*`, or models ending in `_enriched`, `_rollup`, `_rules`, `_eligibility`, `_scored`, `_daily`, or `_monthly`.
- Tags such as `intermediate`, `business_rules`, or `business_rule_application`.
- Descriptions mentioning rules, calculations, derivations, aggregations, eligibility, or enrichment.

**Examples:**

- `models/intermediate/int_orders_enriched.sql`
- `models/business/calc_customer_lifetime_value.sql`

**Anti-examples:**

- `base_shopify__orders`, which should mainly map source fields.
- `rpt_sales_dashboard`, which mainly shapes a consumption-ready output.

**Suggested principles:** Name the policy being applied, isolate reusable business logic from dashboard-specific shaping, document edge cases, and keep source-format cleanup elsewhere when practical.

**Common layer relation:** Common in intermediate and silver layers. It may feed marts/gold outputs, but if it is stored in staging/bronze the layer/job mismatch evidence should prompt review.

### `analytical_output_shaping`

**Description:** Shape facts, marts, reports, metrics, or dashboard-facing outputs for consumption.

**Typical signals:**

- Path segments such as `marts`, `mart`, `gold`, `reports`, `reporting`, or `metrics`.
- Names such as `fct_*`, `fact_*`, `mart_*`, `metric_*`, `rpt_*`, `report_*`, or models ending in `_dashboard`.
- Tags such as `mart`, `gold`, `report`, `dashboard`, or `analytical_output`.
- Downstream dbt exposures or metrics.

**Examples:**

- `models/marts/finance/fct_orders.sql`
- `models/gold/report_revenue_dashboard.sql`

**Anti-examples:**

- `stg_payments`, which should expose raw payment fields without shaping a dashboard.
- `audit_order_totals`, which validates model quality rather than serving analysis.

**Suggested principles:** Optimize for consumer clarity, make grain and measures obvious, keep presentation-specific choices at the edge, and avoid duplicating reusable business rules across reports.

**Common layer relation:** Strongly associated with marts and gold. It is surprising in staging/bronze and may indicate a layer/job mismatch.

### `quality_validation`

**Description:** Create audit, reconciliation, assertion, or data-quality models that validate other models.

**Typical signals:**

- Path segments such as `audit`, `audits`, `quality`, `validation`, or `assertions`.
- Names such as `audit_*`, `assert_*`, `dq_*`, `quality_*`, `reconcile_*`, `validation_*`, or models ending in `_audit`.
- Tags such as `audit`, `data_quality`, `quality_validation`, or `validation`.
- Descriptions mentioning audits, assertions, quality checks, reconciliation, or validation.

**Examples:**

- `models/audits/audit_order_payment_totals.sql`
- `models/quality/dq_customer_email_validity.sql`

**Anti-examples:**

- A normal model with attached tests; that evidence is usually `data_contract_enforcement`.
- `fct_revenue`, which is consumed by analysts rather than used to validate another model.

**Suggested principles:** Keep validation intent explicit, make failures actionable, compare against clear sources of truth, and avoid mixing audit outputs with business-facing marts.

**Common layer relation:** Often lives in audit, quality, or validation folders. It is usually separate from marts/gold consumption layers, although marts can have adjacent contract tests.

## Layer/job mismatch checks

The built-in taxonomy includes mismatch checks that add `layer_job_mismatch_evidence` to model output when configured layer signals conflict with matched jobs:

- `staging-output-job-mismatch` flags staging/bronze models that also look like `analytical_output_shaping` or `business_rule_application`.
- `mart-source-job-mismatch` flags marts/gold models that also look like `source_mapping`, `format_alignment`, or `quality_validation`.

These are evidence prompts, not failures. They are meant to start a conversation such as "is this model in the right layer?" or "is this model doing more than one job?"
