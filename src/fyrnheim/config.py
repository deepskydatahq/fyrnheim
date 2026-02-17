"""Project configuration loading from fyrnheim.yaml."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

CONFIG_FILENAME = "fyrnheim.yaml"


class ConfigError(Exception):
    """Raised when fyrnheim.yaml exists but is malformed."""


@dataclass(frozen=True)
class ProjectConfig:
    """Fyrnheim project configuration."""

    project_root: Path
    entities_dir: Path
    data_dir: Path
    output_dir: Path
    backend: str


def find_config(start_dir: Path) -> Path | None:
    """Walk up from start_dir looking for fyrnheim.yaml. Returns path or None."""
    current = start_dir.resolve()
    while True:
        candidate = current / CONFIG_FILENAME
        if candidate.is_file():
            return candidate
        parent = current.parent
        if parent == current:
            return None
        current = parent


def load_config(start_dir: Path) -> ProjectConfig | None:
    """Find and parse fyrnheim.yaml. Returns None if not found, raises ConfigError if malformed."""
    config_path = find_config(start_dir)
    if config_path is None:
        return None

    project_root = config_path.parent

    try:
        raw = yaml.safe_load(config_path.read_text())
    except yaml.YAMLError as exc:
        raise ConfigError(f"Malformed YAML in {config_path}: {exc}") from exc

    if raw is None:
        raw = {}

    if not isinstance(raw, dict):
        raise ConfigError(f"Expected a mapping in {config_path}, got {type(raw).__name__}")

    return ProjectConfig(
        project_root=project_root,
        entities_dir=project_root / raw.get("entities_dir", "entities"),
        data_dir=project_root / raw.get("data_dir", "data"),
        output_dir=project_root / raw.get("output_dir", "generated"),
        backend=raw.get("backend", "duckdb"),
    )
