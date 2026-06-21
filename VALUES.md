# Fyrnheim Value Ladder

**Vision:** We help data teams turn an existing dbt project into a living data model context layer that humans and coding agents can understand, critique, and improve.

**Last Updated:** 2026-06-20

---

## Status Summary

| Level | Value | Status |
|-------|-------|--------|
| 1 | dbt Project Inventory | shipped |
| 2 | Model Classification | shipped |
| 3 | Data Model Principles | shipped |
| 4 | Agent Context CLI | planned |
| 5 | Guided dbt Development | future |
| 6 | Team Workflow Integration | future |

**Next level to build:** Level 4 — Agent Context CLI

---

## Pivot Note

Fyrnheim is no longer pursuing the compiler path of defining Pydantic entities and generating Ibis transformations. That direction produced useful learning, but for complex data setups it became too cumbersome to justify the promised time and resource savings.

The new value ladder starts from the reality that many teams already have dbt projects. Fyrnheim should not replace those projects first. It should understand them, add context around them, classify their models, enforce data modeling principles, and make AI-assisted development safer and more useful.

---

## Level 1: dbt Project Inventory

**Status:** shipped

Run Fyrnheim inside a dbt project and generate a structured inventory of what exists.

**What the user gets:**

- dbt resource discovery for models, sources, tests, exposures, metrics, macros, and project metadata.
- Lineage extraction from dbt manifests or project files.
- Model summaries with name, path, materialization, package, tags, owner, description, dependencies, downstream consumers, and test coverage.
- Machine-readable context artifacts that can be used by the CLI and coding agents.
- A quick way to answer: “What models exist, how are they connected, and what metadata is missing?”

**Why this comes first:**

Fyrnheim cannot classify, critique, or guide changes until it can reliably understand the dbt project structure.

---

## Level 2: Model Classification

**Status:** shipped

Classify dbt models using configurable taxonomies, with a strong default based on data model layers and jobs.

**What the user gets:**

- Rule-based model classification from paths, naming conventions, tags, config, SQL patterns, lineage position, and metadata.
- Default categories inspired by a jobs-oriented model of transformation work: source mapping, format alignment, data contract enforcement, entity definition, business rule application, analytical output shaping, and quality validation.
- Compatibility with familiar layer labels such as staging, intermediate, marts, bronze, silver, and gold — without treating those labels as sufficient explanations.
- Team-defined classification schemes for domains, entities, ownership, maturity, risk, or output type.
- Reports showing unclassified models, ambiguous models, and models whose declared layer does not match their apparent job.

**Why this matters:**

Layer names are useful folders, but they often hide the real purpose of a transformation. Classification gives teams and agents a richer map of what each model is meant to do.

---

## Level 3: Data Model Principles

**Status:** shipped

Define and check data modeling principles against the dbt project.

**What the user gets:**

- A configurable principle set for model naming, grain clarity, ownership, documentation, source contracts, test coverage, lineage shape, materialization choices, and complexity limits.
- Principle checks that produce actionable findings rather than generic lint output.
- Model-level explanations: which principles apply, which pass, which fail, and what evidence supports the result.
- A path to encode a team’s modeling philosophy without forcing a rewrite of the whole project.
- A way to review not just whether SQL runs, but whether a model is a good model.

**Why after classification:**

Principles often depend on what kind of model something is. A source mapping model, entity model, and analytical output should not all be judged by the same rules.

---

## Level 4: Agent Context CLI

**Status:** planned

Expose Fyrnheim’s inventory, classifications, and principles through a CLI that Pi, Claude Code, and humans can use during dbt development.

**What the user gets:**

- Commands to ask for model context, lineage context, classification context, and applicable principles.
- Concise outputs designed for coding agents: “before editing this model, here is what you need to know.”
- Context bundles for a model, folder, domain, or proposed change.
- Guardrails for AI-assisted work: expected grain, upstream/downstream impact, tests to update, docs to preserve, and principles to satisfy.
- A foundation for agent workflows such as plan, edit, review, and explain.

**Why this is the product wedge:**

The fastest way to make Fyrnheim useful is to make AI-assisted dbt development more context-aware. The CLI is the bridge between dbt projects and coding agents.

---

## Level 5: Guided dbt Development

**Status:** future

Use the context layer to actively guide model changes, refactors, and reviews.

**What the user gets:**

- Change planning for dbt models based on lineage and principles.
- Suggested refactors when model jobs are mixed, duplicated, or unclear.
- Pull request checks that explain principle violations and missing context.
- Model documentation improvements generated from SQL, lineage, and classification.
- Safer agent-driven edits that update SQL, YAML, tests, and docs together.

**Why after the CLI:**

Guided development only works if Fyrnheim can first provide reliable context on demand.

---

## Level 6: Team Workflow Integration

**Status:** future

Integrate Fyrnheim into the everyday workflows of data teams.

**What the user gets:**

- CI checks for classification coverage and principle compliance.
- PR annotations for risky model changes.
- Baseline reports that track model quality and documentation improvements over time.
- Shared team configuration for taxonomies and principles.
- Optional publishing of model context artifacts for documentation portals or internal tools.

**Why last:**

Workflow integration should amplify a proven context layer, not compensate for one that does not yet understand the project.
