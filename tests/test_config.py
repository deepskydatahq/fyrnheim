"""Tests for fyrnheim.config — project configuration loading."""

from pathlib import Path

import pytest
import yaml

from fyrnheim.config import ConfigError, ProjectConfig, find_config, load_config


class TestFindConfig:
    def test_finds_in_cwd(self, tmp_path):
        (tmp_path / "fyrnheim.yaml").write_text("backend: duckdb\n")
        assert find_config(tmp_path) == tmp_path / "fyrnheim.yaml"

    def test_walks_up_parents(self, tmp_path):
        (tmp_path / "fyrnheim.yaml").write_text("backend: duckdb\n")
        child = tmp_path / "sub" / "deep"
        child.mkdir(parents=True)
        assert find_config(child) == tmp_path / "fyrnheim.yaml"

    def test_returns_none_when_missing(self, tmp_path):
        assert find_config(tmp_path) is None


class TestLoadConfig:
    def test_returns_none_when_no_config(self, tmp_path):
        assert load_config(tmp_path) is None

    def test_all_defaults(self, tmp_path):
        (tmp_path / "fyrnheim.yaml").write_text("{}\n")
        cfg = load_config(tmp_path)
        assert cfg is not None
        assert cfg.project_root == tmp_path
        assert cfg.entities_dir == tmp_path / "entities"
        assert cfg.data_dir == tmp_path / "data"
        assert cfg.output_dir == tmp_path / "generated"
        assert cfg.backend == "duckdb"

    def test_all_keys_specified(self, tmp_path):
        (tmp_path / "fyrnheim.yaml").write_text(
            yaml.dump({
                "entities_dir": "my_entities",
                "data_dir": "my_data",
                "output_dir": "my_output",
                "backend": "bigquery",
            })
        )
        cfg = load_config(tmp_path)
        assert cfg is not None
        assert cfg.entities_dir == tmp_path / "my_entities"
        assert cfg.data_dir == tmp_path / "my_data"
        assert cfg.output_dir == tmp_path / "my_output"
        assert cfg.backend == "bigquery"

    def test_paths_relative_to_config_location(self, tmp_path):
        (tmp_path / "fyrnheim.yaml").write_text("entities_dir: src/entities\n")
        child = tmp_path / "sub"
        child.mkdir()
        cfg = load_config(child)
        assert cfg is not None
        assert cfg.entities_dir == tmp_path / "src" / "entities"

    def test_project_root_is_config_parent(self, tmp_path):
        (tmp_path / "fyrnheim.yaml").write_text("{}\n")
        cfg = load_config(tmp_path)
        assert cfg.project_root == tmp_path

    def test_empty_file_uses_defaults(self, tmp_path):
        (tmp_path / "fyrnheim.yaml").write_text("")
        cfg = load_config(tmp_path)
        assert cfg is not None
        assert cfg.backend == "duckdb"
        assert cfg.entities_dir == tmp_path / "entities"

    def test_malformed_yaml_raises_config_error(self, tmp_path):
        (tmp_path / "fyrnheim.yaml").write_text(": :\n  - ][")
        with pytest.raises(ConfigError, match="Malformed YAML"):
            load_config(tmp_path)

    def test_non_dict_yaml_raises_config_error(self, tmp_path):
        (tmp_path / "fyrnheim.yaml").write_text("- item1\n- item2\n")
        with pytest.raises(ConfigError, match="Expected a mapping"):
            load_config(tmp_path)

    def test_unknown_keys_ignored(self, tmp_path):
        (tmp_path / "fyrnheim.yaml").write_text("unknown_key: value\nbackend: duckdb\n")
        cfg = load_config(tmp_path)
        assert cfg is not None
        assert cfg.backend == "duckdb"


class TestProjectConfig:
    def test_frozen(self, tmp_path):
        (tmp_path / "fyrnheim.yaml").write_text("{}\n")
        cfg = load_config(tmp_path)
        with pytest.raises(AttributeError):
            cfg.backend = "bigquery"
