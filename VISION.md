# Fyrnheim Vision

## The Transformation

**We help data teams turn an existing dbt project into a living data model context layer that humans and coding agents can understand, critique, and improve.**

Fyrnheim is pivoting away from the previous compiler approach — Pydantic entity definitions compiled to Ibis transformations. In practice, that approach made complex setups too cumbersome, and the hoped-for time and resource savings did not materialize. The new direction keeps the useful ambition — better data model development — but changes the mechanism: Fyrnheim should understand the transformation system teams already use, starting with dbt, and provide structured context around it.

## Before

Data teams working in mature dbt projects often have:

- **Hundreds of models with unclear intent** — staging, intermediate, marts, and custom folders exist, but the actual job of each model is often implicit.
- **Layer labels that hide variation** — two teams can both say “staging” while doing very different work there.
- **Business logic scattered across SQL, YAML, macros, and naming conventions** — hard for humans to review and harder for AI agents to reason about.
- **Weak model-level context** — model descriptions, tests, owners, exposures, and lineage are incomplete or inconsistent.
- **Agent-unfriendly projects** — Pi, Claude Code, or other coding agents can edit files, but they lack a reliable map of what models mean, which principles apply, and where changes belong.
- **No shared decision frame for data modeling** — teams debate where a transformation belongs instead of asking what progress the transformation is meant to deliver.

## After

Data teams run Fyrnheim inside a dbt project and get:

- **A structured model inventory** — dbt models, sources, tests, metrics, exposures, macros, lineage, materializations, tags, and ownership metadata summarized into machine-readable context.
- **Model classification** — models can be classified by layer, job, business domain, entity, maturity, ownership, risk, or any team-defined taxonomy.
- **A jobs-oriented modeling lens** — instead of only asking “which layer does this live in?”, Fyrnheim helps ask “what job does this model perform?”
- **Principle enforcement** — teams define data model principles, and Fyrnheim checks whether models follow them.
- **AI-ready context for development** — Pi, Claude Code, and similar agents can use Fyrnheim’s context layer to plan changes, review SQL, propose refactors, and explain model impact.
- **Better dbt development without replacing dbt** — dbt remains the transformation engine; Fyrnheim becomes the context, classification, and guidance layer around it.

## Who We Serve

**Analytics engineers, data engineers, and data modelers** working in dbt projects who:

- Need to understand and improve an existing transformation codebase.
- Want stronger conventions without rewriting everything from scratch.
- Want coding agents to make safer, more context-aware dbt changes.
- Need a shared vocabulary for model purpose, lineage, ownership, and quality.
- Care about data model principles, not just whether SQL compiles.

**Secondary:** consultants and solo operators who inherit messy dbt projects and need to quickly build a map of what exists, what matters, and what should change.

## Core Mechanism

**Data model context layer**: Fyrnheim scans a dbt project, extracts model metadata and lineage, classifies models using configurable rules, and produces structured context that humans and AI agents can use during development.

```text
dbt project → model inventory → classifications + principles → CLI + AI context
```

### Key Concepts

- **Model inventory** — a generated map of dbt resources: models, sources, tests, exposures, metrics, macros, lineage, materializations, tags, descriptions, and owners.
- **Classifications** — configurable labels that explain what a model is: layer, job, domain, entity, output type, risk, maturity, or team-specific categories.
- **Jobs** — a purpose-first framing for transformations. A model can perform jobs such as source mapping, format alignment, data contract enforcement, entity definition, business rule application, analytical output shaping, or quality validation.
- **Principles** — team-defined rules for good data modeling: naming conventions, ownership, grain clarity, source contract discipline, test expectations, documentation requirements, and model complexity limits.
- **Context artifacts** — machine-readable summaries that can be consumed by the CLI, Pi, Claude Code, reviews, or future integrations.
- **Agent guidance** — generated context that helps coding agents decide where changes belong, what constraints apply, and how to validate them.

## What We Don't Do

- **Not a transformation compiler** — we no longer aim to replace dbt SQL with Pydantic-to-Ibis compilation.
- **Not an orchestrator** — we do not schedule or monitor pipelines.
- **Not an extraction tool** — we do not ingest data from APIs.
- **Not a BI tool** — we do not render dashboards.
- **Not a warehouse runtime** — dbt and the warehouse still execute transformations.
- **Not a universal semantic layer** — we provide development context for data models, not a runtime metric query API.

## Success Looks Like

- A data team runs `fyr scan` in a dbt project and gets a useful model inventory in minutes.
- Fyrnheim can explain what a model does, where it sits in lineage, what job it performs, who owns it, and which principles apply.
- A team can classify models using its own taxonomy, while Fyrnheim ships a strong default based on jobs such as source mapping, entities, and analytics.
- Pi or Claude Code can ask Fyrnheim for context before editing a dbt model and receives concise, accurate guidance.
- A pull request changing a dbt model can be checked against data model principles before review.
- Teams spend less time rediscovering intent and more time making deliberate, principled data model improvements.

## Last Updated

2026-06-20
