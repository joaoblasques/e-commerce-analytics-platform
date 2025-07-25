"""
Configuration Management System with Secrets Integration.

This module provides comprehensive configuration management with:
- Environment-specific configuration loading
- HashiCorp Vault secrets integration
- Kubernetes secrets support
- Configuration hot-reloading
- Hierarchical configuration merging
- Type validation and conversion
"""

import json
import logging
import os
import threading
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

import yaml
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from .vault_client import VaultClient, get_vault_client

logger = logging.getLogger(__name__)


class ConfigSource(Enum):
    """Configuration source types."""

    FILE = "file"
    VAULT = "vault"
    K8S_SECRET = "kubernetes_secret"
    ENVIRONMENT = "environment"
    DEFAULT = "default"


@dataclass
class ConfigValue:
    """Configuration value with metadata."""

    value: Any
    source: ConfigSource
    path: str
    last_updated: datetime = field(default_factory=datetime.now)
    encrypted: bool = False


class ConfigFileHandler(FileSystemEventHandler):
    """File system event handler for configuration hot-reloading."""

    def __init__(self, config_manager: "ConfigManager"):
        self.config_manager = config_manager

    def on_modified(self, event):
        if (
            not event.is_directory
            and event.src_path in self.config_manager._watched_files
        ):
            logger.info(f"Configuration file modified: {event.src_path}")
            self.config_manager._reload_config_file(event.src_path)


class ConfigManager:
    """
    Comprehensive configuration management system.

    Features:
    - Hierarchical configuration loading (defaults < environment < secrets)
    - Multiple configuration sources (files, Vault, K8s secrets, env vars)
    - Hot-reloading of configuration files
    - Type validation and conversion
    - Configuration change callbacks
    - Thread-safe operations
    """

    def __init__(
        self,
        config_dir: str = None,
        environment: str = None,
        vault_client: VaultClient = None,
        enable_hot_reload: bool = True,
        enable_vault: bool = True,
        enable_k8s_secrets: bool = True,
    ):
        """
        Initialize configuration manager.

        Args:
            config_dir: Configuration directory path
            environment: Current environment (dev, staging, prod)
            vault_client: Vault client instance
            enable_hot_reload: Enable hot-reloading of config files
            enable_vault: Enable Vault secrets integration
            enable_k8s_secrets: Enable Kubernetes secrets integration
        """
        self.config_dir = Path(config_dir or os.getenv("CONFIG_DIR", "config"))
        self.environment = environment or os.getenv("ENVIRONMENT", "development")
        self.vault_client = vault_client
        self.enable_hot_reload = enable_hot_reload
        self.enable_vault = enable_vault
        self.enable_k8s_secrets = enable_k8s_secrets

        # Configuration storage
        self._config: Dict[str, ConfigValue] = {}
        self._lock = threading.RLock()
        self._callbacks: List[Callable[[str, Any, Any], None]] = []

        # Hot-reload setup
        self._observer = None
        self._watched_files: set = set()

        # Load initial configuration
        self._load_configuration()

        # Setup hot-reloading
        if self.enable_hot_reload:
            self._setup_hot_reload()

    def _load_configuration(self) -> None:
        """Load configuration from all sources in priority order."""
        logger.info(f"Loading configuration for environment: {self.environment}")

        # 1. Load default configuration
        self._load_config_file("default.yaml", required=False)

        # 2. Load environment-specific configuration
        self._load_config_file(f"{self.environment}.yaml", required=False)

        # 3. Load local overrides
        self._load_config_file("local.yaml", required=False)

        # 4. Load secrets from Vault
        if self.enable_vault:
            self._load_vault_secrets()

        # 5. Load Kubernetes secrets
        if self.enable_k8s_secrets:
            self._load_k8s_secrets()

        # 6. Load environment variables
        self._load_environment_variables()

        logger.info(f"Configuration loaded with {len(self._config)} values")

    def _load_config_file(self, filename: str, required: bool = True) -> None:
        """Load configuration from YAML file."""
        file_path = self.config_dir / filename

        if not file_path.exists():
            if required:
                raise FileNotFoundError(
                    f"Required configuration file not found: {file_path}"
                )
            logger.debug(f"Optional configuration file not found: {file_path}")
            return

        try:
            with open(file_path, "r") as f:
                data = yaml.safe_load(f) or {}

            self._merge_config_data(data, ConfigSource.FILE, str(file_path))
            self._watched_files.add(str(file_path))

            logger.debug(f"Loaded configuration from: {file_path}")

        except Exception as e:
            logger.error(f"Error loading configuration file {file_path}: {e}")
            if required:
                raise

    def _load_vault_secrets(self) -> None:
        """Load secrets from HashiCorp Vault."""
        if not self.vault_client:
            try:
                self.vault_client = get_vault_client()
            except Exception as e:
                logger.warning(f"Vault client not available: {e}")
                return

        try:
            # Load application secrets
            app_secrets = self.vault_client.get_secret(f"ecap/{self.environment}/app")
            if app_secrets:
                self._merge_config_data(
                    app_secrets,
                    ConfigSource.VAULT,
                    f"vault:ecap/{self.environment}/app",
                )

            # Load database secrets
            db_secrets = self.vault_client.get_secret(
                f"ecap/{self.environment}/database"
            )
            if db_secrets:
                self._merge_config_data(
                    {"database": db_secrets},
                    ConfigSource.VAULT,
                    f"vault:ecap/{self.environment}/database",
                )

            # Load service secrets (kafka, redis, etc.)
            for service in ["kafka", "redis", "minio"]:
                service_secrets = self.vault_client.get_secret(
                    f"ecap/{self.environment}/{service}"
                )
                if service_secrets:
                    self._merge_config_data(
                        {service: service_secrets},
                        ConfigSource.VAULT,
                        f"vault:ecap/{self.environment}/{service}",
                    )

            logger.info("Loaded secrets from Vault")

        except Exception as e:
            logger.error(f"Error loading Vault secrets: {e}")

    def _load_k8s_secrets(self) -> None:
        """Load secrets from Kubernetes secret mounts."""
        secrets_dir = Path("/etc/secrets")

        if not secrets_dir.exists():
            logger.debug("Kubernetes secrets directory not found")
            return

        try:
            for secret_file in secrets_dir.iterdir():
                if secret_file.is_file():
                    with open(secret_file, "r") as f:
                        content = f.read().strip()

                    # Map Kubernetes secret files to config keys
                    key = secret_file.name.replace("-", "_").upper()
                    self._set_config_value(
                        key,
                        content,
                        ConfigSource.K8S_SECRET,
                        str(secret_file),
                        encrypted=True,
                    )

            logger.info("Loaded secrets from Kubernetes")

        except Exception as e:
            logger.error(f"Error loading Kubernetes secrets: {e}")

    def _load_environment_variables(self) -> None:
        """Load configuration from environment variables."""
        env_prefixes = ["ECAP_", "APP_", "DATABASE_", "KAFKA_", "REDIS_", "MINIO_"]

        for key, value in os.environ.items():
            for prefix in env_prefixes:
                if key.startswith(prefix):
                    config_key = key.lower()
                    self._set_config_value(
                        config_key,
                        self._convert_env_value(value),
                        ConfigSource.ENVIRONMENT,
                        f"env:{key}",
                    )
                    break

    def _convert_env_value(self, value: str) -> Any:
        """Convert environment variable string to appropriate type."""
        # Boolean conversion
        if value.lower() in ("true", "false"):
            return value.lower() == "true"

        # Number conversion
        try:
            if "." in value:
                return float(value)
            return int(value)
        except ValueError:
            pass

        # JSON conversion
        if value.startswith(("{", "[")):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                pass

        return value

    def _merge_config_data(
        self, data: Dict[str, Any], source: ConfigSource, path: str
    ) -> None:
        """Merge configuration data into current config."""

        def _merge_recursive(current_data: Dict[str, Any], prefix: str = ""):
            for key, value in current_data.items():
                full_key = f"{prefix}.{key}" if prefix else key

                if isinstance(value, dict):
                    _merge_recursive(value, full_key)
                else:
                    self._set_config_value(full_key, value, source, path)

        _merge_recursive(data)

    def _set_config_value(
        self,
        key: str,
        value: Any,
        source: ConfigSource,
        path: str,
        encrypted: bool = False,
    ) -> None:
        """Set configuration value with metadata."""
        with self._lock:
            old_value = self._config.get(key)

            self._config[key] = ConfigValue(
                value=value, source=source, path=path, encrypted=encrypted
            )

            # Notify callbacks of changes
            old_val = old_value.value if old_value else None
            if old_val != value:
                self._notify_callbacks(key, old_val, value)

    def _notify_callbacks(self, key: str, old_value: Any, new_value: Any) -> None:
        """Notify registered callbacks of configuration changes."""
        for callback in self._callbacks:
            try:
                callback(key, old_value, new_value)
            except Exception as e:
                logger.error(f"Error in configuration callback: {e}")

    def _setup_hot_reload(self) -> None:
        """Setup file system watcher for hot-reloading."""
        if not self.config_dir.exists():
            return

        self._observer = Observer()
        handler = ConfigFileHandler(self)
        self._observer.schedule(handler, str(self.config_dir), recursive=False)
        self._observer.start()

        logger.info("Configuration hot-reloading enabled")

    def _reload_config_file(self, file_path: str) -> None:
        """Reload specific configuration file."""
        try:
            filename = Path(file_path).name

            # Clear existing config from this file
            with self._lock:
                keys_to_remove = [
                    key
                    for key, config_val in self._config.items()
                    if config_val.source == ConfigSource.FILE
                    and config_val.path == file_path
                ]
                for key in keys_to_remove:
                    del self._config[key]

            # Reload the file
            self._load_config_file(filename, required=False)

            logger.info(f"Reloaded configuration file: {filename}")

        except Exception as e:
            logger.error(f"Error reloading configuration file {file_path}: {e}")

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        with self._lock:
            config_value = self._config.get(key)
            return config_value.value if config_value else default

    def get_bool(self, key: str, default: bool = False) -> bool:
        """Get boolean configuration value."""
        value = self.get(key, default)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ("true", "1", "yes", "on")
        return bool(value)

    def get_int(self, key: str, default: int = 0) -> int:
        """Get integer configuration value."""
        value = self.get(key, default)
        try:
            return int(value)
        except (ValueError, TypeError):
            logger.warning(f"Could not convert config value '{key}' to int: {value}")
            return default

    def get_float(self, key: str, default: float = 0.0) -> float:
        """Get float configuration value."""
        value = self.get(key, default)
        try:
            return float(value)
        except (ValueError, TypeError):
            logger.warning(f"Could not convert config value '{key}' to float: {value}")
            return default

    def get_list(self, key: str, default: List = None) -> List:
        """Get list configuration value."""
        value = self.get(key, default or [])
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return default or []

    def get_dict(self, key: str, default: Dict = None) -> Dict:
        """Get dictionary configuration value."""
        # First try to get the value directly
        value = self.get(key, None)
        if isinstance(value, dict):
            return value

        # If not found, reconstruct nested dictionary from flattened keys
        result = {}
        prefix = f"{key}."

        for config_key in self._config.keys():
            if config_key.startswith(prefix):
                # Extract the nested key part
                nested_key = config_key[len(prefix) :]
                # Get the value
                nested_value = self._config[config_key].value

                # Handle further nesting
                if "." in nested_key:
                    # Split into parts and create nested structure
                    parts = nested_key.split(".")
                    current = result
                    for part in parts[:-1]:
                        if part not in current:
                            current[part] = {}
                        current = current[part]
                    current[parts[-1]] = nested_value
                else:
                    result[nested_key] = nested_value

        return result if result else (default or {})

    def set(self, key: str, value: Any) -> None:
        """Set configuration value programmatically."""
        self._set_config_value(
            key, value, ConfigSource.DEFAULT, "programmatic", encrypted=False
        )

    def get_config_info(self, key: str) -> Optional[ConfigValue]:
        """Get configuration value with metadata."""
        with self._lock:
            return self._config.get(key)

    def get_all_config(self, include_encrypted: bool = False) -> Dict[str, Any]:
        """Get all configuration values."""
        with self._lock:
            result = {}
            for key, config_val in self._config.items():
                if config_val.encrypted and not include_encrypted:
                    result[key] = "***ENCRYPTED***"
                else:
                    result[key] = config_val.value
            return result

    def get_config_summary(self) -> Dict[str, Any]:
        """Get configuration summary with source information."""
        with self._lock:
            summary = {}
            sources_count = {}

            for key, config_val in self._config.items():
                source = config_val.source.value
                sources_count[source] = sources_count.get(source, 0) + 1

                summary[key] = {
                    "source": source,
                    "path": config_val.path,
                    "encrypted": config_val.encrypted,
                    "last_updated": config_val.last_updated.isoformat(),
                }

            return {
                "total_configs": len(self._config),
                "sources_summary": sources_count,
                "environment": self.environment,
                "configs": summary,
            }

    def register_callback(self, callback: Callable[[str, Any, Any], None]) -> None:
        """Register callback for configuration changes."""
        self._callbacks.append(callback)

    def reload_secrets(self) -> None:
        """Manually reload secrets from external sources."""
        logger.info("Manually reloading secrets...")

        if self.enable_vault:
            self._load_vault_secrets()

        if self.enable_k8s_secrets:
            self._load_k8s_secrets()

    def validate_required_configs(self, required_keys: List[str]) -> None:
        """Validate that required configuration keys are present."""
        missing_keys = []

        for key in required_keys:
            if key not in self._config:
                missing_keys.append(key)

        if missing_keys:
            raise ValueError(f"Missing required configuration keys: {missing_keys}")

    def close(self) -> None:
        """Clean up resources."""
        if self._observer:
            self._observer.stop()
            self._observer.join()

        logger.info("Configuration manager closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


# Global configuration manager instance
_config_manager = None


def get_config_manager(**kwargs) -> ConfigManager:
    """Get global configuration manager instance."""
    global _config_manager
    if _config_manager is None:
        _config_manager = ConfigManager(**kwargs)
    return _config_manager


def get_config(key: str, default: Any = None) -> Any:
    """Convenience function to get configuration value."""
    return get_config_manager().get(key, default)


def close_config_manager() -> None:
    """Close global configuration manager instance."""
    global _config_manager
    if _config_manager:
        _config_manager.close()
        _config_manager = None
