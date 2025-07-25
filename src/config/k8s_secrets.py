"""
Kubernetes Secrets Management Integration.

This module provides utilities for managing secrets in Kubernetes environments:
- Secret creation and updates
- ConfigMap management
- Environment-specific secret handling
- Secret rotation capabilities
"""

import base64
import logging
from typing import Any, Dict, List, Optional

from kubernetes import client, config
from kubernetes.client.rest import ApiException

logger = logging.getLogger(__name__)


class KubernetesSecretsManager:
    """
    Kubernetes secrets management with comprehensive operations.

    Features:
    - Secret creation, reading, updating, deletion
    - ConfigMap management
    - Environment-specific handling
    - Base64 encoding/decoding
    - Secret rotation support
    """

    def __init__(self, namespace: str = "default", kubeconfig: str = None):
        """
        Initialize Kubernetes secrets manager.

        Args:
            namespace: Kubernetes namespace
            kubeconfig: Path to kubeconfig file (optional)
        """
        self.namespace = namespace

        try:
            if kubeconfig:
                config.load_kube_config(config_file=kubeconfig)
            else:
                # Try in-cluster config first, then local config
                try:
                    config.load_incluster_config()
                except config.ConfigException:
                    config.load_kube_config()

            self.v1 = client.CoreV1Api()
            logger.info(f"Initialized Kubernetes client for namespace: {namespace}")

        except Exception as e:
            logger.error(f"Failed to initialize Kubernetes client: {e}")
            raise

    def create_secret(
        self,
        name: str,
        data: Dict[str, str],
        secret_type: str = "Opaque",
        labels: Dict[str, str] = None,
        annotations: Dict[str, str] = None,
    ) -> bool:
        """
        Create a Kubernetes secret.

        Args:
            name: Secret name
            data: Secret data (will be base64 encoded)
            secret_type: Secret type (Opaque, kubernetes.io/tls, etc.)
            labels: Secret labels
            annotations: Secret annotations

        Returns:
            Success status
        """
        try:
            # Encode data to base64
            encoded_data = {
                key: base64.b64encode(value.encode()).decode()
                for key, value in data.items()
            }

            secret = client.V1Secret(
                metadata=client.V1ObjectMeta(
                    name=name,
                    namespace=self.namespace,
                    labels=labels or {},
                    annotations=annotations or {},
                ),
                type=secret_type,
                data=encoded_data,
            )

            self.v1.create_namespaced_secret(namespace=self.namespace, body=secret)

            logger.info(f"Created secret: {name}")
            return True

        except ApiException as e:
            if e.status == 409:  # Already exists
                logger.warning(f"Secret {name} already exists")
                return self.update_secret(name, data)
            else:
                logger.error(f"Error creating secret {name}: {e}")
                return False

        except Exception as e:
            logger.error(f"Error creating secret {name}: {e}")
            return False

    def get_secret(self, name: str) -> Optional[Dict[str, str]]:
        """
        Get secret data.

        Args:
            name: Secret name

        Returns:
            Decoded secret data
        """
        try:
            secret = self.v1.read_namespaced_secret(name=name, namespace=self.namespace)

            if not secret.data:
                return {}

            # Decode base64 data
            decoded_data = {
                key: base64.b64decode(value).decode()
                for key, value in secret.data.items()
            }

            return decoded_data

        except ApiException as e:
            if e.status == 404:
                logger.warning(f"Secret {name} not found")
                return None
            else:
                logger.error(f"Error reading secret {name}: {e}")
                return None

        except Exception as e:
            logger.error(f"Error reading secret {name}: {e}")
            return None

    def update_secret(self, name: str, data: Dict[str, str]) -> bool:
        """
        Update existing secret.

        Args:
            name: Secret name
            data: New secret data

        Returns:
            Success status
        """
        try:
            # Get existing secret
            existing_secret = self.v1.read_namespaced_secret(
                name=name, namespace=self.namespace
            )

            # Encode new data
            encoded_data = {
                key: base64.b64encode(value.encode()).decode()
                for key, value in data.items()
            }

            # Update secret data
            existing_secret.data = encoded_data

            self.v1.replace_namespaced_secret(
                name=name, namespace=self.namespace, body=existing_secret
            )

            logger.info(f"Updated secret: {name}")
            return True

        except ApiException as e:
            logger.error(f"Error updating secret {name}: {e}")
            return False

        except Exception as e:
            logger.error(f"Error updating secret {name}: {e}")
            return False

    def delete_secret(self, name: str) -> bool:
        """
        Delete secret.

        Args:
            name: Secret name

        Returns:
            Success status
        """
        try:
            self.v1.delete_namespaced_secret(name=name, namespace=self.namespace)

            logger.info(f"Deleted secret: {name}")
            return True

        except ApiException as e:
            if e.status == 404:
                logger.warning(f"Secret {name} not found for deletion")
                return True
            else:
                logger.error(f"Error deleting secret {name}: {e}")
                return False

        except Exception as e:
            logger.error(f"Error deleting secret {name}: {e}")
            return False

    def list_secrets(self, label_selector: str = None) -> List[str]:
        """
        List secrets in namespace.

        Args:
            label_selector: Label selector filter

        Returns:
            List of secret names
        """
        try:
            secrets = self.v1.list_namespaced_secret(
                namespace=self.namespace, label_selector=label_selector
            )

            return [secret.metadata.name for secret in secrets.items]

        except Exception as e:
            logger.error(f"Error listing secrets: {e}")
            return []

    def create_configmap(
        self,
        name: str,
        data: Dict[str, str],
        labels: Dict[str, str] = None,
        annotations: Dict[str, str] = None,
    ) -> bool:
        """
        Create a ConfigMap.

        Args:
            name: ConfigMap name
            data: ConfigMap data
            labels: ConfigMap labels
            annotations: ConfigMap annotations

        Returns:
            Success status
        """
        try:
            configmap = client.V1ConfigMap(
                metadata=client.V1ObjectMeta(
                    name=name,
                    namespace=self.namespace,
                    labels=labels or {},
                    annotations=annotations or {},
                ),
                data=data,
            )

            self.v1.create_namespaced_config_map(
                namespace=self.namespace, body=configmap
            )

            logger.info(f"Created ConfigMap: {name}")
            return True

        except ApiException as e:
            if e.status == 409:  # Already exists
                logger.warning(f"ConfigMap {name} already exists")
                return self.update_configmap(name, data)
            else:
                logger.error(f"Error creating ConfigMap {name}: {e}")
                return False

        except Exception as e:
            logger.error(f"Error creating ConfigMap {name}: {e}")
            return False

    def get_configmap(self, name: str) -> Optional[Dict[str, str]]:
        """
        Get ConfigMap data.

        Args:
            name: ConfigMap name

        Returns:
            ConfigMap data
        """
        try:
            configmap = self.v1.read_namespaced_config_map(
                name=name, namespace=self.namespace
            )

            return configmap.data or {}

        except ApiException as e:
            if e.status == 404:
                logger.warning(f"ConfigMap {name} not found")
                return None
            else:
                logger.error(f"Error reading ConfigMap {name}: {e}")
                return None

        except Exception as e:
            logger.error(f"Error reading ConfigMap {name}: {e}")
            return None

    def update_configmap(self, name: str, data: Dict[str, str]) -> bool:
        """
        Update existing ConfigMap.

        Args:
            name: ConfigMap name
            data: New ConfigMap data

        Returns:
            Success status
        """
        try:
            # Get existing ConfigMap
            existing_configmap = self.v1.read_namespaced_config_map(
                name=name, namespace=self.namespace
            )

            # Update data
            existing_configmap.data = data

            self.v1.replace_namespaced_config_map(
                name=name, namespace=self.namespace, body=existing_configmap
            )

            logger.info(f"Updated ConfigMap: {name}")
            return True

        except ApiException as e:
            logger.error(f"Error updating ConfigMap {name}: {e}")
            return False

        except Exception as e:
            logger.error(f"Error updating ConfigMap {name}: {e}")
            return False

    def create_ecap_secrets(
        self, environment: str, secrets_data: Dict[str, Any]
    ) -> bool:
        """
        Create ECAP-specific secrets for an environment.

        Args:
            environment: Environment name (dev, staging, prod)
            secrets_data: Dictionary of service secrets

        Returns:
            Success status
        """
        success = True

        # Application secrets
        app_secrets = {
            "secret-key": secrets_data.get("app", {}).get("secret_key", ""),
            "jwt-secret-key": secrets_data.get("app", {}).get("jwt_secret_key", ""),
        }

        if not self.create_secret(
            f"ecap-app-{environment}",
            app_secrets,
            labels={"app": "ecap", "environment": environment, "type": "app"},
        ):
            success = False

        # Database secrets
        db_secrets = {
            "username": secrets_data.get("database", {}).get("username", ""),
            "password": secrets_data.get("database", {}).get("password", ""),
            "host": secrets_data.get("database", {}).get("host", ""),
            "port": str(secrets_data.get("database", {}).get("port", 5432)),
        }

        if not self.create_secret(
            f"ecap-database-{environment}",
            db_secrets,
            labels={"app": "ecap", "environment": environment, "type": "database"},
        ):
            success = False

        # Kafka secrets
        kafka_secrets = {
            "bootstrap-servers": secrets_data.get("kafka", {}).get(
                "bootstrap_servers", ""
            ),
            "username": secrets_data.get("kafka", {}).get("username", ""),
            "password": secrets_data.get("kafka", {}).get("password", ""),
        }

        if not self.create_secret(
            f"ecap-kafka-{environment}",
            kafka_secrets,
            labels={"app": "ecap", "environment": environment, "type": "kafka"},
        ):
            success = False

        # Redis secrets
        redis_secrets = {
            "host": secrets_data.get("redis", {}).get("host", ""),
            "port": str(secrets_data.get("redis", {}).get("port", 6379)),
            "password": secrets_data.get("redis", {}).get("password", ""),
        }

        if not self.create_secret(
            f"ecap-redis-{environment}",
            redis_secrets,
            labels={"app": "ecap", "environment": environment, "type": "redis"},
        ):
            success = False

        # MinIO secrets
        minio_secrets = {
            "endpoint": secrets_data.get("minio", {}).get("endpoint", ""),
            "access-key": secrets_data.get("minio", {}).get("access_key", ""),
            "secret-key": secrets_data.get("minio", {}).get("secret_key", ""),
        }

        if not self.create_secret(
            f"ecap-minio-{environment}",
            minio_secrets,
            labels={"app": "ecap", "environment": environment, "type": "minio"},
        ):
            success = False

        return success

    def rotate_secret(
        self, name: str, new_data: Dict[str, str], backup: bool = True
    ) -> bool:
        """
        Rotate secret with optional backup.

        Args:
            name: Secret name
            new_data: New secret data
            backup: Create backup of old secret

        Returns:
            Success status
        """
        try:
            # Create backup if requested
            if backup:
                old_data = self.get_secret(name)
                if old_data:
                    backup_name = f"{name}-backup-{int(time.time())}"
                    self.create_secret(
                        backup_name,
                        old_data,
                        labels={"backup": "true", "original": name},
                    )

            # Update with new data
            return self.update_secret(name, new_data)

        except Exception as e:
            logger.error(f"Error rotating secret {name}: {e}")
            return False

    def is_healthy(self) -> bool:
        """Check Kubernetes API connectivity."""
        try:
            self.v1.list_namespaced_secret(namespace=self.namespace, limit=1)
            return True
        except Exception:
            return False


def create_k8s_secrets_manager(**kwargs) -> KubernetesSecretsManager:
    """Create and return a Kubernetes secrets manager."""
    return KubernetesSecretsManager(**kwargs)


# Global instance
_k8s_secrets_manager = None


def get_k8s_secrets_manager(**kwargs) -> KubernetesSecretsManager:
    """Get global Kubernetes secrets manager instance."""
    global _k8s_secrets_manager
    if _k8s_secrets_manager is None:
        _k8s_secrets_manager = create_k8s_secrets_manager(**kwargs)
    return _k8s_secrets_manager
