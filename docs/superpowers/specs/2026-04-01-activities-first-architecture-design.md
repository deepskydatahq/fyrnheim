# Activities-First Architecture

**Date:** 2026-04-01
**Status:** Proposed

## Summary

Fyrnheim shifts from an entity-centric model (define a business object, optionally detect events) to an event-centric model (all data becomes events, entities are derived views). The core insight: a snapshot-diff engine that compares daily state snapshots produces more reliable events than source systems emit natively (e.g., CRM webhooks). This makes eventification a strength, not a workaround.

## Motivation

The current architecture treats entities as the primary asset and activities as an optional branch off the dimension layer. This creates several tensions:

1. **Activities feel second-class** — nested inside entity layer config, hard to compose across entities
2. **Identity resolution is buried** — DerivedSource with IdentityGraphConfig mixes identity concerns with source concerns
3. **The real value is often the event stream** — attribution, funnels, and sequences operate on events, not entity state
4. **Source systems hide change history** — CRM exports show current state, not what changed. Webhooks are unreliable. A proper diff engine extracts the truth the source system doesn't expose.

## Architecture

### Pipeline Overview

```
State Source → Snapshot Store → Diff Engine → Raw Events ─┐
                                                           ├→ Activity Definitions → Named Events ─┐
Event Source → Schema Normalization ──────────────────────┘                                        │
                                                                                                    ↓
                                                                              Identity Resolution → Enriched Activity Stream
                                                                                                    │
                                                                                        ┌───────────┼───────────┐
                                                                                        ↓           ↓           ↓
                                                                                  Entity Models  Analytics   Raw Stream
                                                                                (current state)  Models     (sequences,
                                                                                                            attribution)
```

### Core Assets

| Asset | Role | User-defined? |
|---|---|---|
| Source | Declares where data comes from and whether it is state or event shaped | Yes |
| Snapshot-Diff Engine | Produces raw field-level changes from state source snapshots | No (infrastructure) |
| Activity Definition | Interprets raw changes into named business events | Yes |
| Identity Graph | Maps raw IDs across sources to canonical IDs | Yes |
| Enriched Activity Stream | The single source of truth — all events with canonical IDs | No (derived) |
| Entity Model | Current-state projection from events | Yes |
| Analytics Model | Time-grain metric aggregation from events | Yes |

### Data Flow Invariants

- **One direction**: sources → events → models (no circular dependencies)
- **Single source of truth**: the enriched activity stream; everything else is a materialized view
- **Infrastructure vs. business logic**: the diff engine is mechanical; activity definitions are semantic
- **Identity is explicit**: not buried inside source definitions

## Component Design

### 1. Sources

Sources declare raw data inputs. Two types based on the shape of the data.

#### StateSource

For data that represents current state (CRM exports, dimension tables, configuration tables).

```python
StateSource(
    name: str                    # e.g., "crm_contacts"
    project: str
    dataset: str
    table: str
    id_field: str                # row identity for diffing
    fields: list[Field]          # schema declaration
    transforms: SourceTransforms # renames, casts (same as today)
)
```

#### EventSource

For data that is already event-shaped (page views, transactions, webhook logs).

```python
EventSource(
    name: str                    # e.g., "page_views"
    project: str
    dataset: str
    table: str
    entity_id_field: str         # who did the action
    timestamp_field: str         # when it happened
    event_type: str | None       # static type, or...
    event_type_field: str | None # ...column containing the type
    fields: list[Field]
    transforms: SourceTransforms
)
```

### 2. Snapshot-Diff Engine

Operates automatically on all StateSources. Not user-configured — it is infrastructure.

**Behavior:**
- Maintains a snapshot store with daily copies of each state source, keyed by `id_field`
- On each run, compares current snapshot to previous snapshot
- Produces three raw event types:
  - `row_appeared` — new ID detected
  - `field_changed` — field value differs from previous snapshot (one event per changed field, includes `old_value` and `new_value`)
  - `row_disappeared` — ID no longer present in current snapshot

**Output schema (universal for all events, both from diff engine and event sources):**

```
source: str              # source name
entity_id: str           # raw ID from this source
ts: timestamp            # snapshot date for state sources, native timestamp for event sources
event_type: str          # "row_appeared", "field_changed", "page_view", etc.
payload: struct/json     # event-specific data
```

**Properties:**
- Daily granularity for state sources (events are as fresh as the snapshot frequency)
- First snapshot produces only `row_appeared` events (no "before" state to diff against)
- All field changes are detected mechanically — business interpretation happens in Activity Definitions

### 3. Activity Definitions

User-authored business logic that interprets raw changes into named, meaningful events. The diff engine detects all changes; activity definitions filter and name the ones that matter.

```python
ActivityDefinition(
    name: str                    # e.g., "subscription_changed"
    source: str                  # which source's events to watch
    trigger: Trigger             # what pattern to match
    include_fields: list[str]    # additional fields to carry in payload
)
```

**Trigger types:**

```python
# Fire when a new row appears
RowAppeared()

# Fire when a specific field changes to/from specific values
FieldChanged(
    field: str
    from_values: list[str] | None    # optional: only from these values
    to_values: list[str] | None      # optional: only to these values
)

# Fire when a row disappears
RowDisappeared()

# Passthrough for event sources — match on event_type
EventOccurred(
    event_type: str | None           # filter by type, or None for all
)
```

**Example:**
```python
ActivityDefinition(
    name="became_paying",
    source="crm_contacts",
    trigger=FieldChanged(
        field="subscription_status",
        from_values=["trial", "free"],
        to_values=["pro", "starter", "enterprise"],
    ),
    include_fields=["plan_amount", "currency"],
)
```

### 4. Identity Graph

A top-level asset that resolves raw source IDs to canonical entity IDs by observing match keys across sources.

```python
IdentityGraph(
    name: str                          # e.g., "customer_identity"
    canonical_id: str                  # output field name
    sources: list[IdentitySource]
    resolution_strategy: "match_key"   # future: probabilistic, etc.
)

IdentitySource(
    source: str                        # reference to a Source name
    id_field: str                      # raw ID in this source
    match_key_field: str               # field to match across sources
)
```

**How it works:**
1. As events flow from all sources, the identity graph observes match keys
2. When the same match key appears in multiple sources, it links those raw IDs to a single canonical ID
3. Events get enriched with `canonical_id` alongside their original `entity_id`

**Example:**
```python
IdentityGraph(
    name="customer_identity",
    canonical_id="customer_id",
    sources=[
        IdentitySource(source="crm_contacts", id_field="contact_id", match_key_field="email_hash"),
        IdentitySource(source="billing_transactions", id_field="billing_id", match_key_field="email_hash"),
        IdentitySource(source="page_views", id_field="cookie_id", match_key_field="email_hash"),
    ],
)
```

**Key change from today:** The current DerivedSource does FULL OUTER JOINs on state data and coalesces fields with priority. In the new model, the identity graph is a lighter concept — just an ID mapping. Field coalescing happens downstream in Entity Models.

### 5. Entity Models (Derived State)

Declared projections over the enriched activity stream. "Given all events for a canonical ID, what is the current state?"

```python
EntityModel(
    name: str                          # e.g., "customers"
    identity_graph: str                # which graph provides the canonical ID
    state_fields: list[StateField]
    computed_fields: list[ComputedColumn]
)

StateField(
    name: str                          # output field name
    source: str                        # which source owns this field
    field: str                         # field name in that source
    strategy: "latest" | "first" | "coalesce"
    priority: list[str] | None         # for coalesce: source priority order
)
```

**Resolution strategies:**
- `latest` — most recent value from a specific source
- `first` — earliest observed value (e.g., `first_seen_date`)
- `coalesce` — latest non-null across multiple sources, ordered by priority

**Example:**
```python
EntityModel(
    name="customers",
    identity_graph="customer_identity",
    state_fields=[
        StateField(name="email", source="crm_contacts", field="email", strategy="latest"),
        StateField(name="name", source="crm_contacts", field="full_name", strategy="latest"),
        StateField(name="plan", source="billing_transactions", field="plan_name", strategy="latest"),
        StateField(name="first_seen", source="page_views", field="ts", strategy="first"),
    ],
    computed_fields=[
        ComputedColumn(name="is_paying", expression="plan != 'free'"),
    ],
)
```

**Materialization:** Entity models are materialized as tables for fast reads. They are recomputed from the activity stream, not incrementally mutated.

### 6. Analytics Models

Time-grain metric aggregations over the enriched activity stream. Top-level assets, not nested inside entities.

```python
AnalyticsModel(
    name: str                          # e.g., "customer_metrics_daily"
    identity_graph: str
    date_grain: "daily" | "weekly" | "monthly"
    dimensions: list[str]
    metrics: list[Metric]
)

Metric(
    name: str
    expression: str
    event_filter: str | None
    metric_type: "count" | "sum" | "snapshot"
)
```

**Example:**
```python
AnalyticsModel(
    name="customer_metrics_daily",
    identity_graph="customer_identity",
    date_grain="daily",
    dimensions=["country"],
    metrics=[
        Metric(name="new_signups", expression="count()", event_filter="event_type == 'row_appeared'", metric_type="count"),
        Metric(name="upgrades", expression="count()", event_filter="event_type == 'subscription_changed'", metric_type="count"),
        Metric(name="total_customers", expression="count_distinct(canonical_id)", metric_type="snapshot"),
    ],
)
```

## Migration from Current Architecture

| Current concept | Replacement |
|---|---|
| Entity (with full pipeline) | EntityModel (projection only) |
| HelperEntity | No longer needed — intermediate state lives in the event stream |
| PrepLayer | Absorbed into Source transforms |
| DimensionLayer | Absorbed into EntityModel computed_fields |
| SnapshotLayer | Replaced by Snapshot-Diff Engine (infrastructure) |
| ActivityConfig (nested in entity) | Top-level ActivityDefinition |
| AnalyticsLayer (nested in entity) | Top-level AnalyticsModel |
| DerivedSource / IdentityGraphConfig | Top-level IdentityGraph |
| AggregationSource | AnalyticsModel or EntityModel with aggregation strategy |
| UnionSource | Multiple Sources feeding the same IdentityGraph |
| LayersConfig (prep, dim, snapshot, activity, analytics) | No equivalent — pipeline is fixed infrastructure, not per-entity config |

## Risks and Open Questions

### Storage
The snapshot store maintains daily copies of every state source to enable diffing. This has storage cost implications that scale with the number of state sources and their size.

### Schema Evolution
The diff engine needs to handle schema changes gracefully — new columns appearing, columns being removed, type changes. Needs a clear strategy for how these map to events.

### Meaningful vs. Noise Changes
Not all field changes are meaningful (e.g., a `last_synced_at` timestamp updating daily). Activity Definitions handle this for named events, but the raw event stream could be noisy. May need a way to exclude fields from diffing at the source level.

### Multi-Match-Key Identity
The current design assumes a single match key per identity source. Real-world identity resolution often involves multiple match keys (email, phone, device_id) with different confidence levels. The `resolution_strategy` field is a placeholder for this.

### Performance of State Derivation
Deriving current entity state from the full event history could be expensive. Materialized entity projections help, but incremental update strategies may be needed for large-scale deployments.

### Cold Start
First snapshot has no "before" state. Initial load produces only `row_appeared` events. Entity models will work correctly (latest values), but analytics models will show an artificial spike on day one.

### Backward Compatibility
Current Fyrnheim users have entities defined in the existing format. Need a migration path — possibly a compatibility layer that translates old Entity definitions to the new Source + ActivityDefinition + EntityModel format.
