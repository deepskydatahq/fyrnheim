from pathlib import Path

import pytest

from scripts.multi_model_coding import (
    Variant,
    add_synthesis_candidate,
    parse_variant,
    prepare_run,
    read_toml,
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
