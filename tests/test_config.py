"""Unit tests for ``ResolvedConfig.max_parallel_io`` precedence.

Covers the three-way resolution (CLI override > fyrnheim.yaml > default) in
:func:`fyrnheim.config.resolve_config`, plus the ``ConfigError`` path for
invalid yaml values. Complements M059's end-to-end fan-out coverage in
``test_parallel_io.py`` by locking in the config-layer contract on its own.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from fyrnheim.config import ConfigError, resolve_config


def test_resolve_config_max_parallel_io_default(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """With no ``fyrnheim.yaml`` and no CLI override, the default is 4."""
    monkeypatch.chdir(tmp_path)
    cfg = resolve_config()
    assert cfg.max_parallel_io == 4


def test_resolve_config_max_parallel_io_yaml_override(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A ``fyrnheim.yaml`` value overrides the default when no CLI arg
    is supplied."""
    (tmp_path / "fyrnheim.yaml").write_text("max_parallel_io: 8\n")
    monkeypatch.chdir(tmp_path)
    cfg = resolve_config()
    assert cfg.max_parallel_io == 8


def test_resolve_config_max_parallel_io_cli_beats_yaml(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The CLI override wins over ``fyrnheim.yaml``."""
    (tmp_path / "fyrnheim.yaml").write_text("max_parallel_io: 8\n")
    monkeypatch.chdir(tmp_path)
    cfg = resolve_config(max_parallel_io=2)
    assert cfg.max_parallel_io == 2


def test_resolve_config_max_parallel_io_invalid_yaml_rejected(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """``max_parallel_io: 0`` in yaml raises :class:`ConfigError`."""
    (tmp_path / "fyrnheim.yaml").write_text("max_parallel_io: 0\n")
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ConfigError):
        resolve_config()
