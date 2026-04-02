---
title: Identity
description: Resolve raw source IDs to canonical entity IDs with IdentityGraph.
---

Real-world entities appear in multiple systems with different IDs -- a CRM contact ID, a billing customer ID, a website cookie ID. The identity graph links these together into a single canonical ID.

## IdentityGraph

An `IdentityGraph` is a top-level asset that observes match keys across sources and maps raw IDs to a canonical ID.

```python
from fyrnheim.core import IdentityGraph, IdentitySource

customer_identity = IdentityGraph(
    name="customer_identity",
    canonical_id="customer_id",
    sources=[
        IdentitySource(
            source="crm_contacts",
            id_field="contact_id",
            match_key_field="email_hash",
        ),
        IdentitySource(
            source="billing_events",
            id_field="customer_id",
            match_key_field="email_hash",
        ),
    ],
)
```

## How it works

1. As events flow from all sources, the identity graph observes match keys (like `email_hash`).
2. When the same match key appears in multiple sources, those raw IDs are linked to a single canonical ID.
3. Every event in the activity stream gets enriched with `canonical_id` alongside its original `entity_id`.

## IdentitySource

Each `IdentitySource` declares:

| Parameter | Description |
|-----------|-------------|
| `source` | Name of the source to resolve |
| `id_field` | The raw ID column in that source |
| `match_key_field` | The field used to match across sources |

Multiple sources can share the same match key field (e.g., `email_hash`) to link records that belong to the same real-world entity.

## How identity connects to the pipeline

The identity graph sits between [activities](/concepts/activities/) and downstream models. Once events have canonical IDs, [entity models](/concepts/entity-models/) can project current state and [analytics models](/concepts/analytics/) can aggregate metrics across all sources for each entity.
