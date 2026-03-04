---
description: Break a mission into epics using the product-mission-breakdown skill
allowed-tools: Bash(git:*), Read, Write, Glob, Grep, Skill
---

# Product Mission Breakdown

Break a product mission into 3-6 epics.

## Arguments

- Mission ID (e.g., `/product-mission-breakdown M001`)

## Instructions

Invoke the `product-mission-breakdown` skill with the provided mission ID.

The skill will:
1. Read the mission TOML file from `product/missions/`
2. Analyze the scope by reviewing relevant code paths
3. Create 3-6 epic TOML files in `product/epics/`
4. Report what was created with dependency notes

## After Running

- Review the generated epics for accuracy
- Set epic status to `active` when ready to begin story breakdown
- Run `/product-epic-breakdown {epic_id}` to create stories for each epic
