import yaml
from pathlib import Path

class ConfigManager:
    """
    Manages application configurations loaded from YAML files.
    """
    def __init__(self, config_path: Path):
        self.config = self._load_config(config_path)

    def _load_config(self, config_path: Path) -> dict:
        """
        Loads configuration from a YAML file.
        """
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def get(self, key: str, default=None):
        """
        Retrieves a configuration value by key.
        Supports dot notation for nested keys (e.g., 'spark.app_name').
        """
        keys = key.split('.')
        value = self.config
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        return value