"""Tests for ConfigLoader and PipelineConfig schema validation."""

import pytest
import yaml
from pydantic import ValidationError

from src.config.loader import ConfigLoader
from src.config.schema import PipelineConfig


def _base_config(sample_pipeline_config):
    return sample_pipeline_config


class TestConfigLoader:
    def test_load_pipeline_config_from_dict(self, sample_pipeline_config):
        loader = ConfigLoader()
        config = loader.load_from_dict(sample_pipeline_config)
        assert isinstance(config, PipelineConfig)
        assert config.name == "test_pipeline"
        assert config.version == "1.0"
        assert config.source.host == "redshift.example.com"

    def test_invalid_engine_raises(self, sample_pipeline_config):
        """engine='glue' with no glue block must raise ValidationError."""
        sample_pipeline_config["transformation"] = {"engine": "glue"}
        loader = ConfigLoader()
        with pytest.raises(ValidationError):
            loader.load_from_dict(sample_pipeline_config)

    def test_env_var_interpolation(self, sample_pipeline_config, monkeypatch):
        monkeypatch.setenv("TEST_REDSHIFT_HOST", "my-redshift-host.example.com")
        sample_pipeline_config["source"]["host"] = "${TEST_REDSHIFT_HOST}"
        loader = ConfigLoader()
        config = loader.load_from_dict(sample_pipeline_config)
        assert config.source.host == "my-redshift-host.example.com"

    def test_load_from_yaml_file(self, sample_pipeline_config, tmp_path):
        yaml_file = tmp_path / "pipeline.yaml"
        yaml_file.write_text(yaml.dump(sample_pipeline_config))
        loader = ConfigLoader()
        config = loader.load(yaml_file)
        assert config.name == "test_pipeline"
        assert config.source.database == "testdb"

    def test_file_not_found_raises(self):
        loader = ConfigLoader()
        with pytest.raises(FileNotFoundError):
            loader.load("/nonexistent/path/config.yaml")

    def test_invalid_format_raises(self, sample_pipeline_config):
        """An unrecognised storage format must raise ValidationError."""
        sample_pipeline_config["storage"]["format"] = "xlsx"
        loader = ConfigLoader()
        with pytest.raises(ValidationError):
            loader.load_from_dict(sample_pipeline_config)

    def test_emr_engine_without_emr_config_raises(self, sample_pipeline_config):
        sample_pipeline_config["transformation"] = {"engine": "emr"}
        loader = ConfigLoader()
        with pytest.raises(ValidationError):
            loader.load_from_dict(sample_pipeline_config)

    def test_missing_env_var_keeps_placeholder(self, sample_pipeline_config, monkeypatch):
        """Unset env vars should remain as the original placeholder."""
        monkeypatch.delenv("UNDEFINED_VAR_XYZ", raising=False)
        sample_pipeline_config["source"]["host"] = "${UNDEFINED_VAR_XYZ}"
        loader = ConfigLoader()
        config = loader.load_from_dict(sample_pipeline_config)
        assert config.source.host == "${UNDEFINED_VAR_XYZ}"
