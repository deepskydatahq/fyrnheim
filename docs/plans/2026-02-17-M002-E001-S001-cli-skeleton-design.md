# CLI Skeleton Design (M002-E001-S001)

## Overview
Create a single `src/fyrnheim/cli.py` with a Click group and five stub subcommands. Add `click>=8.1.0` to pyproject.toml. The entry point already exists.

## Problem Statement
Fyrnheim has no CLI — users must use the Python API directly. This story adds the `fyr` command skeleton so all subsequent CLI stories have a foundation to build on.

## Expert Perspectives

### Technical
- Use `from fyrnheim import __version__` (simple, unified; no premature optimization with importlib.metadata)
- Use `click.echo()` for all output (consistent Click idiom)
- Use Click's default version format
- Single cli.py file, no subpackage (YAGNI)

### Simplification Review
- Verdict: APPROVED — design is inevitable and minimal
- Nothing to remove, nothing to simplify
- Five commands map directly to the core domain without abstraction bloat

## Proposed Solution

**pyproject.toml**: Add `"click>=8.1.0"` to dependencies.

**src/fyrnheim/cli.py** (~40 lines):
- `main` Click group with version option
- 5 stub commands: `init`, `generate`, `run`, `check`, `list`
- Each prints "Not implemented yet." via `click.echo()`
- `list` uses `name="list"` with function `list_cmd` to avoid shadowing builtin

**tests/test_cli.py** (~50 lines):
- CliRunner tests for `--help`, `--version`, all 5 stubs

## Success Criteria
- `fyr --help` lists all 5 commands
- `fyr --version` prints version
- All stubs exit 0
- Existing 452 tests pass
