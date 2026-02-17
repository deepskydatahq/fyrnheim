# Quick-Start Tutorial Design (M002-E004-S002)

## Overview
Add a "Quick Start" section to README.md (after S001's install section) that walks a user from install to a working pipeline in 5 steps with real CLI output. README must stay under 200 lines total.

## Problem Statement
A data engineer lands on the repo and needs to go from zero to a running pipeline in under 5 minutes. The current README is a one-liner. After S001 adds the overview/concepts sections, this story adds the hands-on tutorial that proves the tool works.

## Dependencies
- **M002-E004-S001**: README must already have install section, overview, and core concepts in place. The quick-start slots in between install and core concepts.
- **M002-E002-S003**: `fyr run` must be implemented so we can capture real output. Also implies `fyr init` (E001-S003) and `fyr generate` (E002-S002) are working.

## Section Placement in README

```
# Fyrnheim                        (from S001)
## What is Fyrnheim?              (from S001)
## Install                        (from S001)
## Quick Start        <-- THIS STORY
## Core Concepts                  (from S001)
## Why Fyrnheim?                  (from S001)
## Status                         (from S001)
## License                        (from S001)
```

## Tutorial Structure: 5 Steps

### Step 1: Install
```bash
pip install fyrnheim[duckdb]
```
No output block needed -- install output is noisy and unhelpful. One-liner is enough.

### Step 2: Initialize a project
```bash
fyr init myproject && cd myproject
```
Show the `fyr init` output, which per E001-S003 prints:
```
Created myproject/
  fyrnheim.yaml
  entities/customers.py
  data/customers.parquet
  generated/

Next steps:
  cd myproject
  fyr generate
  fyr run
```

### Step 3: Inspect the sample entity (code block, not a command)
Show a trimmed version of `entities/customers.py` with 2-3 inline annotations as comments. This is the "aha moment" -- the reader sees a typed Python entity for the first time.

Keep only the essential structure: Entity with source, one PrepLayer column, one DimensionLayer column, and one quality check. Trim the full customers.py (84 lines) down to ~20-25 lines. Use `# ...` to indicate elided sections.

### Step 4: Generate transforms
```bash
fyr generate
```
Show the output per E002-S002:
```
  customers   generated/customers_transforms.py   written

Generated: 1 written, 0 unchanged
```

### Step 5: Run the pipeline
```bash
fyr run
```
Show the output per E002-S003:
```
Discovering entities... 1 found
Running on duckdb

  customers    prep -> dim       12 rows    0.3s  ok

Done: 1 success, 0 errors (0.3s)
```

### Next Steps (2-3 lines)
- Point to `examples/` for more entity definitions
- Point to Core Concepts section (anchor link) for understanding layers, primitives, components
- Mention `fyr list` to see discovered entities

## Line Budget

S001 estimated sections (tagline + what + install + concepts + why + status + license) will use roughly 100-120 lines. The quick-start section needs to fit in 60-80 lines to stay under the 200-line cap.

| Sub-section | Estimated lines |
|---|---|
| `## Quick Start` heading + intro sentence | 3 |
| Step 1 (install) | 5 |
| Step 2 (init + output) | 14 |
| Step 3 (entity code) | 28 |
| Step 4 (generate + output) | 10 |
| Step 5 (run + output) | 14 |
| Next steps | 6 |
| **Total** | **~80** |

If over budget, trim Step 3 (entity code) first -- it's the most compressible. Could go as low as 15 lines with aggressive elision.

## Implementation Workflow

1. Confirm all CLI dependencies are implemented and working (init, generate, run)
2. Run the full tutorial sequence in a temp directory:
   ```bash
   pip install fyrnheim[duckdb]
   fyr init myproject
   cd myproject
   fyr generate
   fyr run
   ```
3. Capture actual terminal output from each command
4. Trim entity code to the essential structure with annotations
5. Insert the Quick Start section into README.md after the Install section
6. Verify total README line count is under 200
7. Verify all code blocks are copy-pasteable and correct

## Key Decisions

- **Real output only**: Every terminal output block must be captured from an actual `fyr` command run. No fabricated output. This is an explicit acceptance criterion.
- **Trimmed entity, not full**: Show a condensed version of customers.py to keep the tutorial scannable. The full version lives in `examples/`.
- **No flags or options**: The tutorial uses only bare commands (`fyr generate`, not `fyr generate --output-dir`). Advanced usage is for docs, not the README.
- **No explanation of internals**: Brief annotations ("# define source table", "# add computed columns") but no paragraphs explaining what Ibis does or how layers work. That's what Core Concepts is for.

## Success Criteria
- Quick-start section appears between Install and Core Concepts
- 5 clearly numbered steps with command + output
- Entity code shown with brief annotations
- All output captured from real CLI runs
- Ends with next-steps pointers
- README total under 200 lines
