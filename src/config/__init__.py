"""
Comprehensive Configuration and Secrets Management System.

This package provides a complete configuration management solution with:
- Environment-specific configuration loading
- HashiCorp Vault integration
- Kubernetes secrets support
- Configuration encryption
- Hot-reloading capabilities
- Type-safe configuration access
"""

import logging
import os
from typing import Any, Callable, Dict, List, Optional

from .encryption import (
    ConfigEncryption,
    SecretManager,
    get_config_encryption,
    get_secret_manager,
)
from .k8s_secrets import KubernetesSecretsManager, get_k8s_secrets_manager
from .manager import ConfigManager, close_config_manager, get_config, get_config_manager
from .vault_client import VaultClient, close_vault_client, get_vault_client

logger = logging.getLogger(__name__)


class ECAPConfig:
    """
    E-Commerce Analytics Platform Configuration.

    Main configuration class that provides a unified interface to all
    configuration sources and secrets management capabilities.
    """

    def __init__(
        self,
        environment: str = None,
        config_dir: str = None,
        enable_vault: bool = None,
        enable_k8s_secrets: bool = None,
        enable_encryption: bool = True,
        enable_hot_reload: bool = True,
    ):
        """
        Initialize ECAP configuration.

        Args:
            environment: Environment name (dev, staging, prod)
            config_dir: Configuration directory path
            enable_vault: Enable Vault integration
            enable_k8s_secrets: Enable Kubernetes secrets
            enable_encryption: Enable configuration encryption
            enable_hot_reload: Enable hot-reloading
        """
        self.environment = environment or os.getenv("ENVIRONMENT", "development")

        # Auto-detect capabilities based on environment
        if enable_vault is None:
            enable_vault = self._detect_vault_availability()

        if enable_k8s_secrets is None:
            enable_k8s_secrets = self._detect_k8s_availability()

        # Initialize configuration manager
        self.config_manager = ConfigManager(
            config_dir=config_dir,
            environment=self.environment,
            enable_vault=enable_vault,
            enable_k8s_secrets=enable_k8s_secrets,
            enable_hot_reload=enable_hot_reload,
        )

        # Initialize encryption if enabled
        if enable_encryption:
            self.encryption = get_config_encryption()
            self.secret_manager = get_secret_manager()
        else:
            self.encryption = None
            self.secret_manager = None

        # Initialize external clients
        self.vault_client = None
        self.k8s_secrets_manager = None

        if enable_vault:
            try:
                self.vault_client = get_vault_client()
            except Exception as e:
                logger.warning(f"Vault client initialization failed: {e}")

        if enable_k8s_secrets:
            try:
                self.k8s_secrets_manager = get_k8s_secrets_manager()
            except Exception as e:
                logger.warning(f"Kubernetes secrets manager initialization failed: {e}")

        logger.info(
            f"ECAP configuration initialized for environment: {self.environment}"
        )

    def _detect_vault_availability(self) -> bool:
        """Detect if Vault is available."""
        vault_addr = os.getenv("VAULT_ADDR")
        vault_token = os.getenv("VAULT_TOKEN")
        return bool(vault_addr and vault_token)

    def _detect_k8s_availability(self) -> bool:
        """Detect if running in Kubernetes environment."""
        return (
            os.path.exists("/var/run/secrets/kubernetes.io/serviceaccount/token")
            or os.getenv("KUBERNETES_SERVICE_HOST") is not None
        )

    # Configuration access methods
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        return self.config_manager.get(key, default)

    def get_bool(self, key: str, default: bool = False) -> bool:
        """Get boolean configuration value."""
        return self.config_manager.get_bool(key, default)

    def get_int(self, key: str, default: int = 0) -> int:
        """Get integer configuration value."""
        return self.config_manager.get_int(key, default)

    def get_float(self, key: str, default: float = 0.0) -> float:
        """Get float configuration value."""
        return self.config_manager.get_float(key, default)

    def get_list(self, key: str, default: List = None) -> List:
        """Get list configuration value."""
        return self.config_manager.get_list(key, default)

    def get_dict(self, key: str, default: Dict = None) -> Dict:
        """Get dictionary configuration value."""
        return self.config_manager.get_dict(key, default)

    def set(self, key: str, value: Any) -> None:
        """Set configuration value."""
        self.config_manager.set(key, value)

    # Secret management methods
    def get_secret(self, name: str, source: str = "auto") -> Optional[str]:
        """
        Get secret from various sources.

        Args:
            name: Secret name
            source: Source to use (auto, vault, k8s, local)

        Returns:
            Secret value or None
        """
        if source == "auto":
            # Try Vault first, then K8s, then local
            sources = ["vault", "k8s", "local"]
        else:
            sources = [source]

        for src in sources:
            try:
                if src == "vault" and self.vault_client:
                    secret = self.vault_client.get_secret(
                        f"ecap/{self.environment}/secrets", name
                    )
                    if secret:
                        return secret

                elif src == "k8s" and self.k8s_secrets_manager:
                    secrets = self.k8s_secrets_manager.get_secret(
                        f"ecap-secrets-{self.environment}"
                    )
                    if secrets and name in secrets:
                        return secrets[name]

                elif src == "local" and self.secret_manager:
                    secret = self.secret_manager.get_secret(name)
                    if secret:
                        return secret

            except Exception as e:
                logger.debug(f"Error getting secret from {src}: {e}")
                continue

        return None

    def set_secret(self, name: str, value: str, target: str = "auto") -> bool:
        """
        Store secret in target location.

        Args:
            name: Secret name
            value: Secret value
            target: Target location (auto, vault, k8s, local)

        Returns:
            Success status
        """
        success = False

        if target in ("auto", "vault") and self.vault_client:
            try:
                secrets = (
                    self.vault_client.get_secret(f"ecap/{self.environment}/secrets")
                    or {}
                )
                secrets[name] = value
                success = self.vault_client.put_secret(
                    f"ecap/{self.environment}/secrets", secrets
                )
                if success and target == "vault":
                    return True
            except Exception as e:
                logger.error(f"Error storing secret in Vault: {e}")

        if target in ("auto", "local") and self.secret_manager:
            try:
                success = self.secret_manager.set_secret(name, value)
                if success and target == "local":
                    return True
            except Exception as e:
                logger.error(f"Error storing secret locally: {e}")

        return success

    # Database configuration helpers
    def get_database_config(self) -> Dict[str, Any]:
        """Get database configuration with secrets."""
        db_config = {
            "host": self.get("database.host", "localhost"),
            "port": self.get_int("database.port", 5432),
            "database": self.get("database.database", "ecommerce_analytics"),
            "username": self.get_secret("db_username") or self.get("database.username"),
            "password": self.get_secret("db_password") or self.get("database.password"),
            "pool_size": self.get_int("database.pool_size", 10),
            "max_overflow": self.get_int("database.max_overflow", 20),
            "pool_timeout": self.get_int("database.pool_timeout", 30),
            "pool_recycle": self.get_int("database.pool_recycle", 3600),
            "echo": self.get_bool("database.echo", False),
        }

        return db_config

    def get_kafka_config(self) -> Dict[str, Any]:
        """Get Kafka configuration with secrets."""
        kafka_config = {
            "bootstrap_servers": self.get_secret("kafka_bootstrap_servers")
            or self.get("kafka.bootstrap_servers"),
            "security_protocol": self.get("kafka.security_protocol", "PLAINTEXT"),
            "sasl_mechanism": self.get("kafka.sasl_mechanism", "PLAIN"),
            "sasl_username": self.get_secret("kafka_username"),
            "sasl_password": self.get_secret("kafka_password"),
            "consumer": self.get_dict("kafka.consumer", {}),
            "producer": self.get_dict("kafka.producer", {}),
            "topics": self.get_dict("kafka.topics", {}),
        }

        return kafka_config

    def get_redis_config(self) -> Dict[str, Any]:
        """Get Redis configuration with secrets."""
        redis_config = {
            "host": self.get_secret("redis_host")
            or self.get("redis.host", "localhost"),
            "port": self.get_int("redis.port", 6379),
            "password": self.get_secret("redis_password"),
            "db": self.get_int("redis.db", 0),
            "max_connections": self.get_int("redis.max_connections", 20),
            "socket_timeout": self.get_int("redis.socket_timeout", 30),
            "socket_connect_timeout": self.get_int("redis.socket_connect_timeout", 30),
            "socket_keepalive": self.get_bool("redis.socket_keepalive", True),
            "health_check_interval": self.get_int("redis.health_check_interval", 30),
        }

        return redis_config

    def get_minio_config(self) -> Dict[str, Any]:
        """Get MinIO configuration with secrets."""
        minio_config = {
            "endpoint": self.get_secret("minio_endpoint") or self.get("minio.endpoint"),
            "access_key": self.get_secret("minio_access_key")
            or self.get("minio.access_key"),
            "secret_key": self.get_secret("minio_secret_key")
            or self.get("minio.secret_key"),
            "secure": self.get_bool("minio.secure", False),
            "region": self.get("minio.region", "us-east-1"),
            "buckets": self.get_dict("minio.buckets", {}),
            "lifecycle": self.get_dict("minio.lifecycle", {}),
        }

        return minio_config

    def get_jwt_config(self) -> Dict[str, Any]:
        """Get JWT configuration with secrets."""
        jwt_config = {
            "secret_key": self.get_secret("jwt_secret_key")
            or self.get("app.secret_key"),
            "algorithm": self.get("security.jwt.algorithm", "HS256"),
            "access_token_expire_minutes": self.get_int(
                "security.jwt.access_token_expire_minutes", 30
            ),
            "refresh_token_expire_days": self.get_int(
                "security.jwt.refresh_token_expire_days", 7
            ),
        }

        return jwt_config

    # Health and monitoring
    def get_health_status(self) -> Dict[str, Any]:
        """Get health status of all configuration sources."""
        status = {
            "config_manager": True,
            "vault": False,
            "k8s_secrets": False,
            "encryption": bool(self.encryption),
            "environment": self.environment,
        }

        if self.vault_client:
            status["vault"] = self.vault_client.is_healthy()

        if self.k8s_secrets_manager:
            status["k8s_secrets"] = self.k8s_secrets_manager.is_healthy()

        return status

    def reload_configuration(self) -> None:
        """Manually reload configuration from all sources."""
        logger.info("Manually reloading configuration...")
        self.config_manager.reload_secrets()

    def register_change_callback(
        self, callback: Callable[[str, Any, Any], None]
    ) -> None:
        """Register callback for configuration changes."""
        self.config_manager.register_callback(callback)

    def close(self) -> None:
        """Clean up resources."""
        if self.config_manager:
            self.config_manager.close()

        close_vault_client()
        logger.info("ECAP configuration closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


# Global configuration instance
_ecap_config = None


def init_config(**kwargs) -> ECAPConfig:
    """Initialize global ECAP configuration."""
    global _ecap_config
    _ecap_config = ECAPConfig(**kwargs)
    return _ecap_config


def get_ecap_config() -> ECAPConfig:
    """Get global ECAP configuration instance."""
    global _ecap_config
    if _ecap_config is None:
        _ecap_config = ECAPConfig()
    return _ecap_config


def close_ecap_config() -> None:
    """Close global ECAP configuration instance."""
    global _ecap_config
    if _ecap_config:
        _ecap_config.close()
        _ecap_config = None


# Convenience functions
def config_get(key: str, default: Any = None) -> Any:
    """Convenience function to get configuration value."""
    return get_ecap_config().get(key, default)


def config_get_secret(name: str) -> Optional[str]:
    """Convenience function to get secret value."""
    return get_ecap_config().get_secret(name)


# Export main classes and functions
__all__ = [
    "ECAPConfig",
    "ConfigManager",
    "VaultClient",
    "KubernetesSecretsManager",
    "ConfigEncryption",
    "SecretManager",
    "init_config",
    "get_ecap_config",
    "close_ecap_config",
    "config_get",
    "config_get_secret",
]
