"""YAML config loader with environment variable interpolation."""

import os
import re
from pathlib import Path

import yaml

from .schema import PipelineConfig

_ENV_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")


def _interpolate_env_vars(value):
    """Recursively interpolate environment variables in config values."""
    if isinstance(value, str):
        return _ENV_VAR_PATTERN.sub(
            lambda m: os.environ.get(m.group(1), m.group(0)), value
        )
    elif isinstance(value, dict):
        return {k: _interpolate_env_vars(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [_interpolate_env_vars(item) for item in value]
    return value


class ConfigLoader:
    """Loads and validates pipeline YAML configuration files."""

    def load(self, config_path: str | Path) -> PipelineConfig:
        """Load a pipeline config from a YAML file.

        Args:
            config_path: Path to the YAML configuration file.

        Returns:
            A validated PipelineConfig instance.

        Raises:
            FileNotFoundError: If the config file does not exist.
            ValidationError: If the config fails schema validation.
        """
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")
        with open(path) as f:
            raw = yaml.safe_load(f)
        raw = _interpolate_env_vars(raw)
        return PipelineConfig.model_validate(raw)

    def load_from_dict(self, data: dict) -> PipelineConfig:
        """Load a pipeline config from a dictionary.

        Args:
            data: Raw configuration dictionary.

        Returns:
            A validated PipelineConfig instance.
        """
        data = _interpolate_env_vars(data)
        return PipelineConfig.model_validate(data)
