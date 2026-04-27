---
description: Break a Fyrnheim mission TOML into scoped epics
argument-hint: "<mission-id>"
---
# Product Mission Breakdown

Use the `product-mission-breakdown` skill for mission `$1`.

Create or update `product/epics/$1-E*.toml` files. Product TOML is the source of truth; do not create Beads tasks.

Report created epics, dependency order, coverage of mission outcomes, and any gaps needing review.
