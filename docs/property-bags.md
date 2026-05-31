# Property bags and dynamic JSON properties

Fyrnheim can model semi-structured JSON-ish fields as declared property bags. A property bag is useful when upstream exports contain user-defined keys that are valuable to inspect, but too messy or unstable to promote into permanent semantic columns immediately.

```python
from fyrnheim import AnalyticsEntity, PropertyBag

workshop_people = AnalyticsEntity(
    name="workshop_people",
    property_bags=[
        PropertyBag(
            name="workshop_custom_properties",
            source="workshop_attendees",
            field="custom_properties",
            backend_type="json",
            discoverable=True,
        )
    ],
)
```

Property bags are catalog metadata, not expanded declared dimensions. Agents should first discover and sample keys, then query selected keys explicitly.

## MCP workflow

1. `list_property_bags(model="workshop_people")` — find declared bags.
2. `discover_property_keys(model="workshop_people", property_bag="custom_properties", limit=100)` — list bounded keys with `row_count` and `distinct_value_count`.
3. `sample_property_values(model="workshop_people", property_bag="custom_properties", key="company", limit=50)` — inspect examples and rough type inference.
4. `query_analytics_model(...)` — query a validated property through explicit syntax such as `custom_properties.company` or `custom_properties['job_title']`.

Example query:

```json
{
  "model": "workshop_people",
  "metrics": ["attendee_count"],
  "dimensions": ["custom_properties.company"],
  "filters": {"custom_properties['job_title']": {"contains": "analyst"}},
  "order_by": [{"field": "attendee_count", "direction": "desc"}],
  "limit": 20
}
```

## ClickHouse SQL shape

Fyrnheim generates backend-specific JSON access rather than accepting arbitrary SQL from agents. For ClickHouse, property extraction uses helpers like:

```sql
JSONExtractString(toString(custom_properties), 'What company do you work for?')
JSONExtractString(toString(custom_properties), 'What is your job title?')
JSONExtractBool(toString(custom_properties), 'has_joined_event')
JSONExtractRaw(toString(custom_properties), '<key>')
```

Key discovery uses a bounded generated query:

```sql
SELECT
  kv.1 AS key,
  count() AS row_count,
  uniqExact(kv.2) AS distinct_value_count
FROM workshop_people
ARRAY JOIN JSONExtractKeysAndValuesRaw(toString(custom_properties)) AS kv
GROUP BY key
ORDER BY row_count DESC, key ASC
LIMIT 100
```

## Safety and promotion

Dynamic properties remain opt-in and bounded:

- property bags must be declared in Fyrnheim metadata;
- undiscoverable bags cannot be sampled through MCP;
- keys are validated before SQL generation;
- limits are capped;
- arbitrary SQL is not accepted.

When a key becomes stable and semantically important, promote it later into a declared `StateField`, computed field, or other durable semantic field. For workshop exports, keep `company` and `job_title` dynamic until real usage confirms the best names and types.

Propel-specific wiring, such as keeping raw `custom_properties` on `workshop_attendees` and exposing it on person/account models, should happen after the Fyrnheim property-bag support is available in the project consuming it.
