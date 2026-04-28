# M088 — Column pushdown for warehouse sources

Fyrnheim now derives a conservative set of required columns for each declared source and applies a projection immediately after the backend table read, before source-stage transforms, joins, JSON extraction, computed columns, and filters.

## What contributes required columns

The collector keeps columns referenced by:

- source identity/timestamp fields (`id_field`, `entity_id_field`, `timestamp_field`, dynamic `event_type_field`)
- declarative join keys
- source transforms, including renamed raw columns when downstream assets reference the renamed output
- JSON-path extraction source columns
- computed-column and filter expressions when they use simple `t.column` or `t["column"]` references
- activity triggers and `include_fields`
- identity graph `id_field` and `match_key_field`
- analytics entity state fields and activity measure payload fields
- metrics model state fields and `count_distinct` payload fields

## Conservative fallbacks

Column pruning favors correctness over minimality. Fyrnheim may keep extra columns when analysis is uncertain.

Known conservative cases:

- Sources used as declarative join targets are not pruned yet, because joined-column lineage can flow into the dependent source payload with backend-generated suffixes.
- Opaque SQL staging views are not parsed. If a source reads from a staging view, Fyrnheim can project the source read from that view, but it does not infer or rewrite the staging SQL itself.
- Arbitrary Python/Ibis expression strings are analyzed only for straightforward `t.column` and `t["column"]` references. Dynamic references or helper functions may require retaining extra columns in future refinements.
- EventSource payload packing still includes every post-stage selected column not explicitly excluded by `payload_exclude`; the collector decides which payload columns need to survive based on downstream assets.

If analysis cannot prove a column is unnecessary, it should be retained. Dropping a required column is considered incorrect; keeping an extra column is acceptable.
