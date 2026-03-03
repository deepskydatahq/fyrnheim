"""Tests for fyrnheim.config — project configuration loading."""


from pathlib import Path

import pytest
import yaml

from fyrnheim.config import ConfigError, find_config, load_config, resolve_config


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

    def test_absolute_path_preserved(self, tmp_path):
        abs_ent = tmp_path / "elsewhere" / "my_entities"
        (tmp_path / "fyrnheim.yaml").write_text(f"entities_dir: {abs_ent}\n")
        cfg = load_config(tmp_path)
        assert cfg.entities_dir == abs_ent

    def test_mixed_absolute_and_relative(self, tmp_path):
        abs_data = tmp_path / "shared_data"
        (tmp_path / "fyrnheim.yaml").write_text(
            yaml.dump({
                "entities_dir": "src/entities",
                "data_dir": str(abs_data),
                "output_dir": "build/gen",
            })
        )
        cfg = load_config(tmp_path)
        assert cfg.entities_dir == tmp_path / "src" / "entities"
        assert cfg.data_dir == abs_data
        assert cfg.output_dir == tmp_path / "build" / "gen"

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


class TestResolveConfig:
    def test_no_config_no_overrides(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        cfg = resolve_config()
        assert cfg.entities_dir == Path("entities")
        assert cfg.data_dir == Path("data")
        assert cfg.output_dir == Path("generated")
        assert cfg.backend == "duckdb"

    def test_config_values_used(self, tmp_path, monkeypatch):
        (tmp_path / "fyrnheim.yaml").write_text("entities_dir: my_ents\nbackend: bigquery\n")
        monkeypatch.chdir(tmp_path)
        cfg = resolve_config()
        assert cfg.entities_dir == tmp_path / "my_ents"
        assert cfg.backend == "bigquery"

    def test_cli_overrides_config(self, tmp_path, monkeypatch):
        (tmp_path / "fyrnheim.yaml").write_text("entities_dir: config_ents\n")
        monkeypatch.chdir(tmp_path)
        cfg = resolve_config(entities_dir="/cli/path")
        assert cfg.entities_dir == Path("/cli/path")

    def test_partial_overrides(self, tmp_path, monkeypatch):
        (tmp_path / "fyrnheim.yaml").write_text("entities_dir: cfg_ent\ndata_dir: cfg_dat\n")
        monkeypatch.chdir(tmp_path)
        cfg = resolve_config(entities_dir="cli_ent")
        assert cfg.entities_dir == Path("cli_ent")
        assert cfg.data_dir == tmp_path / "cfg_dat"

    def test_project_root_from_config(self, tmp_path, monkeypatch):
        (tmp_path / "fyrnheim.yaml").write_text("{}\n")
        monkeypatch.chdir(tmp_path)
        cfg = resolve_config()
        assert cfg.project_root == tmp_path

    def test_backend_override(self, tmp_path, monkeypatch):
        (tmp_path / "fyrnheim.yaml").write_text("backend: duckdb\n")
        monkeypatch.chdir(tmp_path)
        cfg = resolve_config(backend="bigquery")
        assert cfg.backend == "bigquery"

    def test_backend_default_without_override(self, tmp_path, monkeypatch):
        (tmp_path / "fyrnheim.yaml").write_text("backend: bigquery\n")
        monkeypatch.chdir(tmp_path)
        cfg = resolve_config()
        assert cfg.backend == "bigquery"


class TestBackendConfig:
    def test_backend_config_parsed(self, tmp_path):
        (tmp_path / "fyrnheim.yaml").write_text(
            "backend: bigquery\n"
            "backend_config:\n"
            "  project_id: my-project\n"
            "  dataset_id: my_dataset\n"
        )
        cfg = load_config(tmp_path)
        assert cfg.backend_config == {"project_id": "my-project", "dataset_id": "my_dataset"}

    def test_backend_config_none_when_absent(self, tmp_path):
        (tmp_path / "fyrnheim.yaml").write_text("backend: duckdb\n")
        cfg = load_config(tmp_path)
        assert cfg.backend_config is None

    def test_backend_config_flows_to_resolved(self, tmp_path, monkeypatch):
        (tmp_path / "fyrnheim.yaml").write_text(
            "backend: bigquery\n"
            "backend_config:\n"
            "  project_id: test-proj\n"
        )
        monkeypatch.chdir(tmp_path)
        cfg = resolve_config()
        assert cfg.backend_config == {"project_id": "test-proj"}

    def test_backend_config_cli_override(self, tmp_path, monkeypatch):
        (tmp_path / "fyrnheim.yaml").write_text(
            "backend: bigquery\n"
            "backend_config:\n"
            "  project_id: yaml-proj\n"
            "  dataset_id: yaml-ds\n"
        )
        monkeypatch.chdir(tmp_path)
        cfg = resolve_config(backend_config={"project_id": "cli-proj", "dataset_id": "cli-ds"})
        assert cfg.backend_config == {"project_id": "cli-proj", "dataset_id": "cli-ds"}

    def test_backend_config_cli_none_uses_yaml(self, tmp_path, monkeypatch):
        (tmp_path / "fyrnheim.yaml").write_text(
            "backend: bigquery\n"
            "backend_config:\n"
            "  project_id: yaml-proj\n"
        )
        monkeypatch.chdir(tmp_path)
        cfg = resolve_config(backend_config=None)
        assert cfg.backend_config == {"project_id": "yaml-proj"}


class TestOutputConfig:
    def test_output_backend_parsed(self, tmp_path):
        (tmp_path / "fyrnheim.yaml").write_text(
            "backend: duckdb\n"
            "output_backend: clickhouse\n"
        )
        cfg = load_config(tmp_path)
        assert cfg.output_backend == "clickhouse"

    def test_output_config_parsed(self, tmp_path):
        (tmp_path / "fyrnheim.yaml").write_text(
            "backend: duckdb\n"
            "output_backend: clickhouse\n"
            "output_config:\n"
            "  host: ch.example.com\n"
            "  port: '8123'\n"
            "  database: analytics\n"
        )
        cfg = load_config(tmp_path)
        assert cfg.output_config == {"host": "ch.example.com", "port": "8123", "database": "analytics"}

    def test_output_backend_none_when_absent(self, tmp_path):
        (tmp_path / "fyrnheim.yaml").write_text("backend: duckdb\n")
        cfg = load_config(tmp_path)
        assert cfg.output_backend is None
        assert cfg.output_config is None

    def test_output_config_flows_to_resolved(self, tmp_path, monkeypatch):
        (tmp_path / "fyrnheim.yaml").write_text(
            "output_backend: clickhouse\n"
            "output_config:\n"
            "  host: ch.local\n"
        )
        monkeypatch.chdir(tmp_path)
        cfg = resolve_config()
        assert cfg.output_backend == "clickhouse"
        assert cfg.output_config == {"host": "ch.local"}

    def test_output_backend_cli_override(self, tmp_path, monkeypatch):
        (tmp_path / "fyrnheim.yaml").write_text("output_backend: clickhouse\n")
        monkeypatch.chdir(tmp_path)
        cfg = resolve_config(output_backend="bigquery")
        assert cfg.output_backend == "bigquery"

    def test_output_config_cli_override(self, tmp_path, monkeypatch):
        (tmp_path / "fyrnheim.yaml").write_text(
            "output_config:\n"
            "  host: yaml-host\n"
        )
        monkeypatch.chdir(tmp_path)
        cfg = resolve_config(output_config={"host": "cli-host"})
        assert cfg.output_config == {"host": "cli-host"}

    def test_output_config_cli_none_uses_yaml(self, tmp_path, monkeypatch):
        (tmp_path / "fyrnheim.yaml").write_text(
            "output_backend: clickhouse\n"
            "output_config:\n"
            "  host: yaml-host\n"
        )
        monkeypatch.chdir(tmp_path)
        cfg = resolve_config(output_config=None)
        assert cfg.output_config == {"host": "yaml-host"}

    def test_no_config_defaults_to_none(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        cfg = resolve_config()
        assert cfg.output_backend is None
        assert cfg.output_config is None
