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
    backend_config: dict[str, str] | None = None
    output_backend: str | None = None
    output_config: dict[str, str] | None = None


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


def _resolve_dir(project_root: Path, raw_path: str) -> Path:
    """Resolve a directory: absolute paths kept as-is, relative paths joined to project_root."""
    p = Path(raw_path)
    if p.is_absolute():
        return p
    return project_root / p


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

    raw_backend_config = raw.get("backend_config")
    backend_config = dict(raw_backend_config) if isinstance(raw_backend_config, dict) else None

    raw_output_config = raw.get("output_config")
    output_config = dict(raw_output_config) if isinstance(raw_output_config, dict) else None

    return ProjectConfig(
        project_root=project_root,
        entities_dir=_resolve_dir(project_root, raw.get("entities_dir", "entities")),
        data_dir=_resolve_dir(project_root, raw.get("data_dir", "data")),
        output_dir=_resolve_dir(project_root, raw.get("output_dir", "generated")),
        backend=raw.get("backend", "duckdb"),
        backend_config=backend_config,
        output_backend=raw.get("output_backend"),
        output_config=output_config,
    )


@dataclass(frozen=True)
class ResolvedConfig:
    """Effective configuration after merging config file with CLI overrides."""

    entities_dir: Path
    data_dir: Path
    output_dir: Path
    backend: str
    project_root: Path
    backend_config: dict[str, str] | None = None
    output_backend: str | None = None
    output_config: dict[str, str] | None = None


def resolve_config(
    *,
    entities_dir: str | None = None,
    data_dir: str | None = None,
    output_dir: str | None = None,
    backend: str | None = None,
    backend_config: dict[str, str] | None = None,
    output_backend: str | None = None,
    output_config: dict[str, str] | None = None,
) -> ResolvedConfig:
    """Load project config and merge CLI overrides.

    CLI args take precedence over fyrnheim.yaml values,
    which take precedence over built-in defaults.
    """
    config = load_config(Path.cwd())

    return ResolvedConfig(
        entities_dir=Path(entities_dir) if entities_dir else (config.entities_dir if config else Path("entities")),
        data_dir=Path(data_dir) if data_dir else (config.data_dir if config else Path("data")),
        output_dir=Path(output_dir) if output_dir else (config.output_dir if config else Path("generated")),
        backend=backend if backend else (config.backend if config else "duckdb"),
        project_root=config.project_root if config else Path("."),
        backend_config=backend_config if backend_config is not None else (config.backend_config if config else None),
        output_backend=output_backend if output_backend is not None else (config.output_backend if config else None),
        output_config=output_config if output_config is not None else (config.output_config if config else None),
    )
