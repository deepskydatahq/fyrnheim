#!/usr/bin/env python3
"""Local orchestration for parallel multi-model coding candidates.

The script is intentionally provider-neutral: it prepares isolated worktrees,
metadata, and prompts; candidate agents can then be launched by Pi, Claude, or
another coding tool. Evaluator decisions are recorded as durable TOML artifacts.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import time
import tomllib
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

Decision = Literal["accept_candidate", "synthesize", "all_failed"]
VALID_DECISIONS = {"accept_candidate", "synthesize", "all_failed"}
QUALITY_GATES = [
    "uv run pytest",
    "uv run ruff check src/ tests/",
    "uv run mypy src/",
]
REQUIRED_CANDIDATE_ARTIFACTS = ["notes.md", "quality-gates.txt", "status.toml", "diff.patch"]
INCOMPLETE_STATUSES = {"blocked", "failed", "incomplete", "skipped"}


@dataclass(frozen=True)
class Variant:
    """A candidate model/build variant."""

    name: str
    model: str
    build: str | None = None

    @property
    def slug(self) -> str:
        return slugify(self.name)


def slugify(value: str) -> str:
    """Convert arbitrary text into a branch/worktree-safe slug."""

    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower()).strip("-")
    return slug or "candidate"


def parse_variant(raw: str) -> Variant:
    """Parse ``name=model`` or ``name=model:build`` variant syntax."""

    if "=" not in raw:
        name = raw.strip()
        if not name:
            raise ValueError("variant must not be empty")
        return Variant(name=name, model=name)
    name, model_spec = raw.split("=", 1)
    name = name.strip()
    model_spec = model_spec.strip()
    if not name or not model_spec:
        raise ValueError(f"invalid variant {raw!r}; expected name=model[:build]")
    model, sep, build = model_spec.partition(":")
    return Variant(name=name, model=model, build=build if sep else None)


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_toml(path: Path) -> dict[str, Any]:
    with path.open("rb") as handle:
        return tomllib.load(handle)


def toml_value(value: Any) -> str:
    if value is None:
        return '""'
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return str(value)
    if isinstance(value, list):
        return "[" + ", ".join(toml_value(item) for item in value) + "]"
    escaped = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def write_toml(path: Path, data: dict[str, Any]) -> None:
    """Write a small deterministic TOML document."""

    lines: list[str] = []
    for key, value in data.items():
        if isinstance(value, dict):
            continue
        lines.append(f"{key} = {toml_value(value)}")
    for key, value in data.items():
        if not isinstance(value, dict):
            continue
        lines.append("")
        lines.append(f"[{key}]")
        for nested_key, nested_value in value.items():
            if isinstance(nested_value, list) and nested_value and all(
                isinstance(item, dict) for item in nested_value
            ):
                for item in nested_value:
                    lines.append("")
                    lines.append(f"[[{key}.{nested_key}]]")
                    for item_key, item_value in item.items():
                        lines.append(f"{item_key} = {toml_value(item_value)}")
            else:
                lines.append(f"{nested_key} = {toml_value(nested_value)}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")


def git(*args: str, cwd: Path | None = None) -> str:
    return subprocess.check_output(["git", *args], cwd=cwd, text=True).strip()


def story_summary(story_path: Path) -> dict[str, Any]:
    story = read_toml(story_path)
    criteria = [
        item.get("description", "")
        for item in story.get("acceptance_criteria", {}).get("criteria", [])
    ]
    return {
        "id": story.get("id", story_path.stem),
        "title": story.get("title", story_path.stem),
        "status": story.get("status", ""),
        "triage": story.get("triage", ""),
        "outcome": story.get("outcome", {}).get("description", "").strip(),
        "criteria": [criterion for criterion in criteria if criterion],
    }


def candidate_prompt(
    *,
    story_path: Path,
    story: dict[str, Any],
    run_id: str,
    variant: Variant,
    artifact_dir: Path,
) -> str:
    criteria = "\n".join(f"- {criterion}" for criterion in story["criteria"]) or "- None listed"
    return f"""# Candidate Coding Run: {run_id} / {variant.name}

You are implementing one candidate solution for Fyrnheim story `{story['id']}`.

## Variant

- Name: {variant.name}
- Model: {variant.model}
- Build: {variant.build or 'default'}

## Story

- Path: `{story_path}`
- Title: {story['title']}
- Triage: {story['triage']}

### Outcome

{story['outcome'] or '(No outcome description found.)'}

### Acceptance Criteria

{criteria}

## Isolation Rules

- Work only in this candidate worktree.
- Do not mutate sibling worktrees or the source checkout.
- Keep commits scoped to this candidate branch.
- Leave notes and command output under `{artifact_dir}`.

## Required Artifacts

Write or update these files before handing off to the evaluator:

- `{artifact_dir / 'notes.md'}` — implementation summary, tradeoffs, risks.
- `{artifact_dir / 'quality-gates.txt'}` — focused tests and quality gate output.
- `{artifact_dir / 'status.toml'}` — final status such as `complete`, `failed`, or `blocked`.

## Quality Gates

Run the normal gates before declaring the candidate complete:

```bash
{chr(10).join(QUALITY_GATES)}
```

## Product Workflow

Product TOML remains the source of truth. Do not mark the shared story complete from
inside this candidate unless this candidate has been selected and promoted by the
evaluator. Commit and push the candidate branch when finished.
"""


def prepare_run(
    *,
    story_path: Path,
    variants: list[Variant],
    run_id: str | None = None,
    repo_root: Path | None = None,
    run_root: Path | None = None,
    worktree_root: Path | None = None,
    base_ref: str = "HEAD",
    dry_run: bool = False,
) -> Path:
    if not variants:
        raise ValueError("at least one variant is required")
    repo_root = repo_root or Path(git("rev-parse", "--show-toplevel"))
    story_path = story_path if story_path.is_absolute() else repo_root / story_path
    if not story_path.exists():
        raise FileNotFoundError(story_path)
    run_id = run_id or f"{story_path.stem}-{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}"
    run_root = run_root or repo_root / ".pi" / "coding-runs"
    worktree_root = worktree_root or repo_root.parent / "worktrees" / "coding-runs"
    run_dir = run_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    story = story_summary(story_path)
    run_data: dict[str, Any] = {
        "schema_version": "fyrnheim.coding_run.v1",
        "run_id": run_id,
        "story_id": story["id"],
        "story_path": str(story_path.relative_to(repo_root)),
        "repo_root": str(repo_root),
        "base_ref": base_ref,
        "status": "prepared",
        "created_at": utc_now(),
        "dry_run": dry_run,
        "candidate_count": len(variants),
    }
    write_toml(run_dir / "run.toml", run_data)

    for variant in variants:
        candidate_dir = run_dir / "candidates" / variant.slug
        artifacts_dir = candidate_dir / "artifacts"
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        branch = f"coding/{story['id'].lower()}-{run_id}-{variant.slug}"
        worktree = worktree_root / run_id / variant.slug
        candidate_data = {
            "schema_version": "fyrnheim.coding_candidate.v1",
            "run_id": run_id,
            "story_id": story["id"],
            "variant_name": variant.name,
            "model": variant.model,
            "build": variant.build or "",
            "branch": branch,
            "worktree": str(worktree),
            "artifact_dir": str(artifacts_dir),
            "status": "created",
            "created_at": utc_now(),
            "dry_run": dry_run,
        }
        write_toml(candidate_dir / "candidate.toml", candidate_data)
        (candidate_dir / "prompt.md").write_text(
            candidate_prompt(
                story_path=story_path.relative_to(repo_root),
                story=story,
                run_id=run_id,
                variant=variant,
                artifact_dir=artifacts_dir,
            )
        )
        write_toml(artifacts_dir / "status.toml", {"status": "created", "updated_at": utc_now()})
        if not dry_run:
            worktree.parent.mkdir(parents=True, exist_ok=True)
            if not worktree.exists():
                git("worktree", "add", "-b", branch, str(worktree), base_ref, cwd=repo_root)
    return run_dir


def candidate_dirs(run_dir: Path) -> list[Path]:
    candidates = run_dir / "candidates"
    if not candidates.exists():
        return []
    return sorted(path for path in candidates.iterdir() if (path / "candidate.toml").exists())


def write_judgement(
    *,
    run_dir: Path,
    decision: str,
    reason: str,
    winner: str | None = None,
    synthesis_guidance: str | None = None,
    risks: list[str] | None = None,
    follow_up: list[str] | None = None,
) -> Path:
    if decision not in VALID_DECISIONS:
        raise ValueError(f"decision must be one of: {', '.join(sorted(VALID_DECISIONS))}")
    candidates = [read_toml(path / "candidate.toml") for path in candidate_dirs(run_dir)]
    candidate_slugs = [slugify(candidate["variant_name"]) for candidate in candidates]
    if decision == "accept_candidate" and not winner:
        raise ValueError("accept_candidate requires --winner")
    if decision == "accept_candidate" and winner not in candidate_slugs:
        raise ValueError(f"winner {winner!r} does not match candidates: {', '.join(candidate_slugs)}")
    if decision == "synthesize" and not synthesis_guidance:
        raise ValueError("synthesize requires --synthesis-guidance")

    criteria_results = []
    run_data = read_toml(run_dir / "run.toml")
    repo_root = Path(run_data.get("repo_root", run_dir.parents[2] if len(run_dir.parents) > 2 else "."))
    story_path = repo_root / run_data["story_path"]
    if story_path.exists():
        story = story_summary(story_path)
        criteria_results = [
            {"criterion": criterion, "status": "needs_evaluator_review", "evidence": ""}
            for criterion in story["criteria"]
        ]

    judgement = {
        "schema_version": "fyrnheim.coding_judgement.v1",
        "run_id": run_data.get("run_id", run_dir.name),
        "decision": decision,
        "winner": winner or "",
        "reason": reason,
        "synthesis_guidance": synthesis_guidance or "",
        "created_at": utc_now(),
        "risks": risks or [],
        "follow_up": follow_up or [],
        "candidates": {
            "summary": [
                {
                    "variant": candidate["variant_name"],
                    "slug": slugify(candidate["variant_name"]),
                    "branch": candidate["branch"],
                    "worktree": candidate["worktree"],
                    "status": candidate.get("status", "created"),
                    "disposition": "selected" if slugify(candidate["variant_name"]) == winner else "not_selected",
                }
                for candidate in candidates
            ]
        },
        "evaluation": {"criteria": criteria_results},
    }
    path = run_dir / "judgement.toml"
    write_toml(path, judgement)
    return path


def parse_command_mapping(raw: str) -> tuple[str, str]:
    """Parse ``candidate=command`` mappings for provider-neutral execution."""

    if "=" not in raw:
        raise ValueError(f"invalid command mapping {raw!r}; expected candidate=command")
    candidate, command = raw.split("=", 1)
    candidate = candidate.strip()
    command = command.strip()
    if not candidate or not command:
        raise ValueError(f"invalid command mapping {raw!r}; expected candidate=command")
    return slugify(candidate), command


def parse_command_mappings(raw_commands: list[str]) -> dict[str, str]:
    """Parse command mappings keyed by candidate slug or ``default``."""

    commands: dict[str, str] = {}
    for raw in raw_commands:
        key, command = parse_command_mapping(raw)
        commands[key] = command
    return commands


def _candidate_slug(candidate: dict[str, Any]) -> str:
    return slugify(str(candidate.get("variant_name", "candidate")))


def _candidate_command(candidate: dict[str, Any], commands: dict[str, str]) -> str:
    slug = _candidate_slug(candidate)
    command = commands.get(slug) or commands.get("default")
    if command is None:
        raise ValueError(f"missing command for candidate {slug!r}")
    return command


def _format_command(command: str, candidate_dir: Path, candidate: dict[str, Any]) -> str:
    artifact_dir = Path(str(candidate["artifact_dir"]))
    return command.format(
        prompt=str((candidate_dir / "prompt.md").resolve()),
        artifact_dir=str(artifact_dir),
        worktree=str(candidate["worktree"]),
        candidate=_candidate_slug(candidate),
    )


def run_candidates(
    *,
    run_dir: Path,
    commands: dict[str, str],
    dry_run: bool = False,
    fail_fast: bool = False,
) -> list[dict[str, Any]]:
    """Run configured provider commands from each candidate worktree."""

    results: list[dict[str, Any]] = []
    for candidate_dir in candidate_dirs(run_dir):
        candidate_path = candidate_dir / "candidate.toml"
        candidate = read_toml(candidate_path)
        command = _format_command(_candidate_command(candidate, commands), candidate_dir, candidate)
        artifact_dir = Path(str(candidate["artifact_dir"]))
        worktree = Path(str(candidate["worktree"]))
        artifact_dir.mkdir(parents=True, exist_ok=True)
        started_at = utc_now()
        if dry_run:
            result = {
                "candidate": _candidate_slug(candidate),
                "status": "dry_run",
                "command": command,
                "worktree": str(worktree),
                "exit_code": 0,
                "duration_seconds": 0.0,
                "started_at": started_at,
                "completed_at": utc_now(),
            }
            write_toml(artifact_dir / "execution.toml", result)
            results.append(result)
            continue
        if not worktree.exists():
            raise FileNotFoundError(f"worktree does not exist for candidate {_candidate_slug(candidate)!r}: {worktree}")
        started = time.monotonic()
        completed = subprocess.run(command, cwd=worktree, shell=True, capture_output=True, text=True)
        duration = time.monotonic() - started
        (artifact_dir / "stdout.txt").write_text(completed.stdout)
        (artifact_dir / "stderr.txt").write_text(completed.stderr)
        result = {
            "candidate": _candidate_slug(candidate),
            "status": "complete" if completed.returncode == 0 else "failed",
            "command": command,
            "worktree": str(worktree),
            "exit_code": completed.returncode,
            "duration_seconds": round(duration, 3),
            "started_at": started_at,
            "completed_at": utc_now(),
            "stdout": str(artifact_dir / "stdout.txt"),
            "stderr": str(artifact_dir / "stderr.txt"),
        }
        write_toml(artifact_dir / "execution.toml", result)
        candidate.update(
            {
                "status": "run_complete" if completed.returncode == 0 else "run_failed",
                "execution_command": command,
                "last_exit_code": completed.returncode,
                "updated_at": utc_now(),
            }
        )
        write_toml(candidate_path, candidate)
        results.append(result)
        if fail_fast and completed.returncode != 0:
            break
    return results


def artifact_status(path: Path) -> str | None:
    if not path.exists():
        return None
    try:
        status = read_toml(path).get("status")
    except tomllib.TOMLDecodeError:
        return None
    return str(status) if status is not None else None


def check_artifacts(run_dir: Path) -> tuple[bool, list[dict[str, Any]]]:
    """Return artifact readiness for every candidate in a run."""

    reports: list[dict[str, Any]] = []
    all_ready = True
    for candidate_dir in candidate_dirs(run_dir):
        candidate = read_toml(candidate_dir / "candidate.toml")
        artifact_dir = Path(str(candidate["artifact_dir"]))
        missing = [name for name in REQUIRED_CANDIDATE_ARTIFACTS if not (artifact_dir / name).exists()]
        status = artifact_status(artifact_dir / "status.toml")
        explicitly_incomplete = status in INCOMPLETE_STATUSES
        ready = not missing or explicitly_incomplete
        all_ready = all_ready and ready
        reports.append(
            {
                "candidate": _candidate_slug(candidate),
                "artifact_dir": str(artifact_dir),
                "status": status or "missing_status",
                "missing": missing,
                "ready": ready,
                "explicitly_incomplete": explicitly_incomplete,
            }
        )
    return all_ready, reports


def worktree_is_clean(worktree: Path) -> bool:
    output = git("-C", str(worktree), "status", "--porcelain")
    return output == ""


def cleanup_worktrees(*, run_dir: Path, dry_run: bool = False, force: bool = False) -> list[dict[str, Any]]:
    """Remove candidate worktrees when safe, or when explicitly forced."""

    run_data = read_toml(run_dir / "run.toml")
    repo_root = Path(str(run_data.get("repo_root", git("rev-parse", "--show-toplevel"))))
    results: list[dict[str, Any]] = []
    for candidate_dir in candidate_dirs(run_dir):
        candidate = read_toml(candidate_dir / "candidate.toml")
        worktree = Path(str(candidate["worktree"]))
        slug = _candidate_slug(candidate)
        if not worktree.exists():
            results.append({"candidate": slug, "worktree": str(worktree), "status": "missing"})
            continue
        clean = worktree_is_clean(worktree)
        if not clean and not force:
            results.append({"candidate": slug, "worktree": str(worktree), "status": "dirty", "clean": False})
            continue
        if dry_run:
            results.append({"candidate": slug, "worktree": str(worktree), "status": "would_remove", "clean": clean})
            continue
        args = ["worktree", "remove"]
        if force:
            args.append("--force")
        args.append(str(worktree))
        git(*args, cwd=repo_root)
        results.append({"candidate": slug, "worktree": str(worktree), "status": "removed", "clean": clean})
    if not dry_run:
        git("worktree", "prune", cwd=repo_root)
    return results


def add_synthesis_candidate(
    *,
    run_dir: Path,
    variant: Variant,
    guidance: str,
    from_candidates: list[str],
    repo_root: Path | None = None,
    worktree_root: Path | None = None,
    base_ref: str = "HEAD",
    dry_run: bool = False,
) -> Path:
    run_data = read_toml(run_dir / "run.toml")
    repo_root = repo_root or Path(git("rev-parse", "--show-toplevel"))
    story_path = repo_root / run_data["story_path"]
    story = story_summary(story_path)
    worktree_root = worktree_root or repo_root.parent / "worktrees" / "coding-runs"
    candidate_dir = run_dir / "candidates" / variant.slug
    artifacts_dir = candidate_dir / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    branch = f"coding/{story['id'].lower()}-{run_data['run_id']}-{variant.slug}"
    worktree = worktree_root / run_data["run_id"] / variant.slug
    candidate_data = {
        "schema_version": "fyrnheim.coding_candidate.v1",
        "run_id": run_data["run_id"],
        "story_id": story["id"],
        "variant_name": variant.name,
        "model": variant.model,
        "build": variant.build or "",
        "branch": branch,
        "worktree": str(worktree),
        "artifact_dir": str(artifacts_dir),
        "status": "synthesis_created",
        "created_at": utc_now(),
        "dry_run": dry_run,
        "synthesis_from": from_candidates,
    }
    write_toml(candidate_dir / "candidate.toml", candidate_data)
    base_prompt = candidate_prompt(
        story_path=Path(run_data["story_path"]),
        story=story,
        run_id=run_data["run_id"],
        variant=variant,
        artifact_dir=artifacts_dir,
    )
    synthesis_section = f"""

## Synthesis Guidance

Use the evaluator judgement to combine useful parts from: {', '.join(from_candidates)}.

{guidance}

Do not blindly merge candidates. Re-implement the simplest coherent solution and rerun quality gates.
"""
    (candidate_dir / "prompt.md").write_text(base_prompt + synthesis_section)
    write_toml(artifacts_dir / "status.toml", {"status": "synthesis_created", "updated_at": utc_now()})
    if not dry_run:
        worktree.parent.mkdir(parents=True, exist_ok=True)
        if not worktree.exists():
            git("worktree", "add", "-b", branch, str(worktree), base_ref, cwd=repo_root)
    return candidate_dir


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare = subparsers.add_parser("prepare", help="prepare candidate run metadata/prompts")
    prepare.add_argument("--story", required=True, type=Path)
    prepare.add_argument("--variant", action="append", required=True, help="name=model[:build]")
    prepare.add_argument("--run-id")
    prepare.add_argument("--run-root", type=Path)
    prepare.add_argument("--worktree-root", type=Path)
    prepare.add_argument("--base-ref", default="HEAD")
    prepare.add_argument("--dry-run", action="store_true")

    judge = subparsers.add_parser("judge", help="write evaluator judgement")
    judge.add_argument("--run-dir", required=True, type=Path)
    judge.add_argument("--decision", required=True, choices=sorted(VALID_DECISIONS))
    judge.add_argument("--winner")
    judge.add_argument("--reason", required=True)
    judge.add_argument("--synthesis-guidance")
    judge.add_argument("--risk", action="append", default=[])
    judge.add_argument("--follow-up", action="append", default=[])

    synthesize = subparsers.add_parser("synthesize", help="add a synthesis candidate prompt")
    synthesize.add_argument("--run-dir", required=True, type=Path)
    synthesize.add_argument("--variant", required=True, help="name=model[:build]")
    synthesize.add_argument("--guidance", required=True)
    synthesize.add_argument("--from", dest="from_candidates", action="append", required=True)
    synthesize.add_argument("--worktree-root", type=Path)
    synthesize.add_argument("--base-ref", default="HEAD")
    synthesize.add_argument("--dry-run", action="store_true")

    run = subparsers.add_parser("run-candidates", help="run provider commands for prepared candidates")
    run.add_argument("--run-dir", required=True, type=Path)
    run.add_argument(
        "--command",
        dest="command_mappings",
        action="append",
        required=True,
        help="candidate=command mapping; use default=command for unmatched candidates. Supports {prompt}, {artifact_dir}, {worktree}, {candidate}.",
    )
    run.add_argument("--dry-run", action="store_true")
    run.add_argument("--fail-fast", action="store_true")

    check = subparsers.add_parser("check-artifacts", help="check candidate artifact readiness")
    check.add_argument("--run-dir", required=True, type=Path)

    cleanup = subparsers.add_parser("cleanup", help="remove candidate worktrees safely")
    cleanup.add_argument("--run-dir", required=True, type=Path)
    cleanup.add_argument("--dry-run", action="store_true")
    cleanup.add_argument("--force", action="store_true")

    return parser


def _print_records(records: list[dict[str, Any]]) -> None:
    for record in records:
        print(" ".join(f"{key}={value}" for key, value in record.items()))


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "prepare":
            run_dir = prepare_run(
                story_path=args.story,
                variants=[parse_variant(raw) for raw in args.variant],
                run_id=args.run_id,
                run_root=args.run_root,
                worktree_root=args.worktree_root,
                base_ref=args.base_ref,
                dry_run=args.dry_run,
            )
            print(run_dir)
            return 0
        if args.command == "judge":
            path = write_judgement(
                run_dir=args.run_dir,
                decision=args.decision,
                winner=args.winner,
                reason=args.reason,
                synthesis_guidance=args.synthesis_guidance,
                risks=args.risk,
                follow_up=args.follow_up,
            )
            print(path)
            return 0
        if args.command == "synthesize":
            path = add_synthesis_candidate(
                run_dir=args.run_dir,
                variant=parse_variant(args.variant),
                guidance=args.guidance,
                from_candidates=args.from_candidates,
                worktree_root=args.worktree_root,
                base_ref=args.base_ref,
                dry_run=args.dry_run,
            )
            print(path)
            return 0
        if args.command == "run-candidates":
            results = run_candidates(
                run_dir=args.run_dir,
                commands=parse_command_mappings(args.command_mappings),
                dry_run=args.dry_run,
                fail_fast=args.fail_fast,
            )
            _print_records(results)
            return 0 if all(result["exit_code"] == 0 for result in results) else 1
        if args.command == "check-artifacts":
            ready, reports = check_artifacts(args.run_dir)
            _print_records(reports)
            return 0 if ready else 1
        if args.command == "cleanup":
            results = cleanup_worktrees(run_dir=args.run_dir, dry_run=args.dry_run, force=args.force)
            _print_records(results)
            return 0 if all(result["status"] != "dirty" for result in results) else 1
    except Exception as exc:  # pragma: no cover - CLI guardrail
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
