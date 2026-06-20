from pathlib import Path

import pytest

import scripts.multi_model_coding as mmc
from scripts.multi_model_coding import (
    Variant,
    add_synthesis_candidate,
    check_artifacts,
    cleanup_worktrees,
    parse_command_mappings,
    parse_variant,
    prepare_run,
    read_toml,
    run_candidates,
    slugify,
    write_judgement,
)

STORY = '''
id = "M110-E001-S999"
parent = "M110-E001"
title = "Test story"
status = "ready"
triage = "ready"

[outcome]
description = "Implement the test story."

[acceptance_criteria]
executable = true

[[acceptance_criteria.criteria]]
test = "unit"
description = "The implementation passes a unit criterion."
'''


def write_story(repo: Path) -> Path:
    story = repo / "product" / "stories" / "M110-E001-S999-test-story.toml"
    story.parent.mkdir(parents=True)
    story.write_text(STORY)
    return story


def test_parse_variant_and_slugify() -> None:
    assert slugify("Claude Sonnet 4.5!") == "claude-sonnet-4-5"
    assert parse_variant("sonnet=claude:sonnet-4.5") == Variant(
        name="sonnet",
        model="claude",
        build="sonnet-4.5",
    )
    assert parse_variant("gpt-5").model == "gpt-5"


@pytest.mark.parametrize("raw", ["", "name=", "=:model"])
def test_parse_variant_rejects_invalid(raw: str) -> None:
    with pytest.raises(ValueError):
        parse_variant(raw)


def test_prepare_run_dry_run_writes_metadata_and_prompts(tmp_path: Path) -> None:
    story = write_story(tmp_path)

    run_dir = prepare_run(
        story_path=story,
        variants=[Variant("sonnet", "claude", "sonnet"), Variant("gpt", "openai", "gpt-5")],
        run_id="run-1",
        repo_root=tmp_path,
        run_root=tmp_path / ".pi" / "coding-runs",
        worktree_root=tmp_path / "worktrees",
        dry_run=True,
    )

    run = read_toml(run_dir / "run.toml")
    sonnet = read_toml(run_dir / "candidates" / "sonnet" / "candidate.toml")
    prompt = (run_dir / "candidates" / "sonnet" / "prompt.md").read_text()

    assert run["schema_version"] == "fyrnheim.coding_run.v1"
    assert run["story_id"] == "M110-E001-S999"
    assert run["candidate_count"] == 2
    assert sonnet["branch"] == "coding/m110-e001-s999-run-1-sonnet"
    assert sonnet["status"] == "created"
    assert sonnet["dry_run"] is True
    assert "Work only in this candidate worktree" in prompt
    assert "uv run pytest" in prompt
    assert "Product TOML remains the source of truth" in prompt
    assert not (tmp_path / "worktrees" / "run-1" / "sonnet").exists()


def test_write_judgement_validates_decisions_and_winner(tmp_path: Path) -> None:
    story = write_story(tmp_path)
    run_dir = prepare_run(
        story_path=story,
        variants=[Variant("sonnet", "claude"), Variant("gpt", "openai")],
        run_id="run-2",
        repo_root=tmp_path,
        run_root=tmp_path / ".pi" / "coding-runs",
        dry_run=True,
    )

    with pytest.raises(ValueError, match="requires --winner"):
        write_judgement(run_dir=run_dir, decision="accept_candidate", reason="best")
    with pytest.raises(ValueError, match="does not match candidates"):
        write_judgement(run_dir=run_dir, decision="accept_candidate", winner="missing", reason="best")

    judgement_path = write_judgement(
        run_dir=run_dir,
        decision="accept_candidate",
        winner="sonnet",
        reason="smallest passing diff",
        risks=["needs PR review"],
        follow_up=["document learnings"],
    )
    judgement = read_toml(judgement_path)

    assert judgement["decision"] == "accept_candidate"
    assert judgement["winner"] == "sonnet"
    assert judgement["risks"] == ["needs PR review"]
    assert judgement["follow_up"] == ["document learnings"]
    selected = [
        candidate
        for candidate in judgement["candidates"]["summary"]
        if candidate["disposition"] == "selected"
    ]
    assert selected[0]["slug"] == "sonnet"


def test_run_candidates_dry_run_records_execution_plan(tmp_path: Path) -> None:
    story = write_story(tmp_path)
    run_dir = prepare_run(
        story_path=story,
        variants=[Variant("sonnet", "claude")],
        run_id="run-dry",
        repo_root=tmp_path,
        run_root=tmp_path / ".pi" / "coding-runs",
        worktree_root=tmp_path / "worktrees",
        dry_run=True,
    )

    results = run_candidates(
        run_dir=run_dir,
        commands=parse_command_mappings(["sonnet=echo {candidate} {prompt}"]),
        dry_run=True,
    )
    execution = read_toml(run_dir / "candidates" / "sonnet" / "artifacts" / "execution.toml")

    assert results[0]["status"] == "dry_run"
    assert execution["command"].startswith("echo sonnet")
    assert execution["worktree"] == str(tmp_path / "worktrees" / "run-dry" / "sonnet")


def test_run_candidates_executes_command_and_captures_logs(tmp_path: Path) -> None:
    story = write_story(tmp_path)
    run_dir = prepare_run(
        story_path=story,
        variants=[Variant("sonnet", "claude")],
        run_id="run-real",
        repo_root=tmp_path,
        run_root=tmp_path / ".pi" / "coding-runs",
        worktree_root=tmp_path / "worktrees",
        dry_run=True,
    )
    worktree = tmp_path / "worktrees" / "run-real" / "sonnet"
    worktree.mkdir(parents=True)

    results = run_candidates(
        run_dir=run_dir,
        commands=parse_command_mappings([
            "sonnet=python -c \"import sys; print('out'); print('err', file=sys.stderr)\""
        ]),
    )
    artifact_dir = run_dir / "candidates" / "sonnet" / "artifacts"
    execution = read_toml(artifact_dir / "execution.toml")
    candidate = read_toml(run_dir / "candidates" / "sonnet" / "candidate.toml")

    assert results[0]["exit_code"] == 0
    assert execution["status"] == "complete"
    assert (artifact_dir / "stdout.txt").read_text().strip() == "out"
    assert (artifact_dir / "stderr.txt").read_text().strip() == "err"
    assert candidate["status"] == "run_complete"


def test_check_artifacts_reports_missing_and_ready_candidates(tmp_path: Path) -> None:
    story = write_story(tmp_path)
    run_dir = prepare_run(
        story_path=story,
        variants=[Variant("sonnet", "claude"), Variant("blocked", "manual")],
        run_id="run-check",
        repo_root=tmp_path,
        run_root=tmp_path / ".pi" / "coding-runs",
        dry_run=True,
    )
    artifact_dir = run_dir / "candidates" / "sonnet" / "artifacts"
    for name in ["notes.md", "quality-gates.txt", "diff.patch"]:
        (artifact_dir / name).write_text("ok")
    blocked_artifacts = run_dir / "candidates" / "blocked" / "artifacts"
    (blocked_artifacts / "status.toml").write_text('status = "blocked"\n')

    ready, reports = check_artifacts(run_dir)

    assert ready is True
    report_by_candidate = {report["candidate"]: report for report in reports}
    assert report_by_candidate["sonnet"]["missing"] == []
    assert report_by_candidate["blocked"]["explicitly_incomplete"] is True


def test_check_artifacts_fails_when_required_files_are_missing(tmp_path: Path) -> None:
    story = write_story(tmp_path)
    run_dir = prepare_run(
        story_path=story,
        variants=[Variant("sonnet", "claude")],
        run_id="run-missing",
        repo_root=tmp_path,
        run_root=tmp_path / ".pi" / "coding-runs",
        dry_run=True,
    )

    ready, reports = check_artifacts(run_dir)

    assert ready is False
    assert "notes.md" in reports[0]["missing"]
    assert "diff.patch" in reports[0]["missing"]


def test_cleanup_worktrees_dry_run_and_dirty_protection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    story = write_story(tmp_path)
    run_dir = prepare_run(
        story_path=story,
        variants=[Variant("sonnet", "claude")],
        run_id="run-cleanup",
        repo_root=tmp_path,
        run_root=tmp_path / ".pi" / "coding-runs",
        worktree_root=tmp_path / "worktrees",
        dry_run=True,
    )
    worktree = tmp_path / "worktrees" / "run-cleanup" / "sonnet"
    worktree.mkdir(parents=True)
    calls: list[tuple[str, ...]] = []

    def fake_git(*args: str, cwd: Path | None = None) -> str:
        calls.append(args)
        if args[:3] == ("-C", str(worktree), "status"):
            return " M dirty.txt"
        if args == ("rev-parse", "--show-toplevel"):
            return str(tmp_path)
        return ""

    monkeypatch.setattr(mmc, "git", fake_git)

    dirty = cleanup_worktrees(run_dir=run_dir)
    forced_dry_run = cleanup_worktrees(run_dir=run_dir, dry_run=True, force=True)

    assert dirty[0]["status"] == "dirty"
    assert forced_dry_run[0]["status"] == "would_remove"
    assert not any(call[:2] == ("worktree", "remove") for call in calls)


def test_synthesize_decision_and_candidate_prompt(tmp_path: Path) -> None:
    story = write_story(tmp_path)
    run_dir = prepare_run(
        story_path=story,
        variants=[Variant("sonnet", "claude"), Variant("gpt", "openai")],
        run_id="run-3",
        repo_root=tmp_path,
        run_root=tmp_path / ".pi" / "coding-runs",
        worktree_root=tmp_path / "worktrees",
        dry_run=True,
    )

    judgement_path = write_judgement(
        run_dir=run_dir,
        decision="synthesize",
        reason="both candidates have useful pieces",
        synthesis_guidance="Use sonnet's tests and gpt's simpler API.",
    )
    candidate_dir = add_synthesis_candidate(
        run_dir=run_dir,
        variant=Variant("synthesis", "claude", "sonnet"),
        guidance="Use sonnet's tests and gpt's simpler API.",
        from_candidates=["sonnet", "gpt"],
        repo_root=tmp_path,
        worktree_root=tmp_path / "worktrees",
        dry_run=True,
    )

    judgement = read_toml(judgement_path)
    candidate = read_toml(candidate_dir / "candidate.toml")
    prompt = (candidate_dir / "prompt.md").read_text()

    assert judgement["decision"] == "synthesize"
    assert candidate["status"] == "synthesis_created"
    assert candidate["synthesis_from"] == ["sonnet", "gpt"]
    assert "Synthesis Guidance" in prompt
    assert "Use sonnet's tests and gpt's simpler API." in prompt
