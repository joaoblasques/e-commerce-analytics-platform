"""
HashiCorp Vault client for secure secrets management.

This module provides a comprehensive interface to HashiCorp Vault for:
- Secrets retrieval and storage
- Authentication with multiple methods
- Automatic token renewal
- KV secrets engine integration
- Database secrets engine integration
"""

import logging
import os
import time
from threading import Event, Thread
from typing import Any, Dict, Optional, Union

import hvac
from hvac.exceptions import InvalidPath, InvalidRequest, VaultError

logger = logging.getLogger(__name__)


class VaultClient:
    """
    HashiCorp Vault client with comprehensive secret management capabilities.

    Features:
    - Multiple authentication methods (token, kubernetes, userpass)
    - Automatic token renewal
    - KV v1 and v2 secrets engine support
    - Database secrets engine integration
    - Configurable retry logic
    - Connection health monitoring
    """

    def __init__(
        self,
        vault_url: str = None,
        vault_token: str = None,
        auth_method: str = "token",
        mount_point: str = "secret",
        kv_version: int = 2,
        namespace: str = None,
        verify_ssl: bool = True,
        timeout: int = 30,
        max_retries: int = 3,
        auto_renew: bool = True,
    ):
        """
        Initialize Vault client.

        Args:
            vault_url: Vault server URL (default: VAULT_ADDR env var)
            vault_token: Vault token (default: VAULT_TOKEN env var)
            auth_method: Authentication method (token, kubernetes, userpass)
            mount_point: KV secrets engine mount point
            kv_version: KV secrets engine version (1 or 2)
            namespace: Vault namespace for enterprise
            verify_ssl: Verify SSL certificates
            timeout: Request timeout in seconds
            max_retries: Maximum retry attempts
            auto_renew: Enable automatic token renewal
        """
        self.vault_url = vault_url or os.getenv("VAULT_ADDR", "http://localhost:8200")
        self.vault_token = vault_token or os.getenv("VAULT_TOKEN")
        self.auth_method = auth_method
        self.mount_point = mount_point
        self.kv_version = kv_version
        self.namespace = namespace
        self.verify_ssl = verify_ssl
        self.timeout = timeout
        self.max_retries = max_retries
        self.auto_renew = auto_renew

        # Initialize client
        self.client = hvac.Client(
            url=self.vault_url,
            token=self.vault_token,
            namespace=self.namespace,
            verify=self.verify_ssl,
            timeout=self.timeout,
        )

        # Token renewal thread
        self._renewal_thread = None
        self._stop_renewal = Event()

        # Initialize authentication
        self._authenticate()

        # Start token renewal if enabled
        if self.auto_renew and self.auth_method == "token":
            self._start_token_renewal()

    def _authenticate(self) -> None:
        """Authenticate with Vault using configured method."""
        try:
            if self.auth_method == "token":
                if not self.vault_token:
                    raise ValueError(
                        "VAULT_TOKEN environment variable or vault_token parameter required"
                    )
                self.client.token = self.vault_token

            elif self.auth_method == "kubernetes":
                self._authenticate_kubernetes()

            elif self.auth_method == "userpass":
                self._authenticate_userpass()

            else:
                raise ValueError(
                    f"Unsupported authentication method: {self.auth_method}"
                )

            # Verify authentication
            if not self.client.is_authenticated():
                raise VaultError("Failed to authenticate with Vault")

            logger.info(
                f"Successfully authenticated with Vault using {self.auth_method}"
            )

        except Exception as e:
            logger.error(f"Vault authentication failed: {e}")
            raise

    def _authenticate_kubernetes(self) -> None:
        """Authenticate using Kubernetes service account."""
        jwt_path = "/var/run/secrets/kubernetes.io/serviceaccount/token"
        role = os.getenv("VAULT_K8S_ROLE", "ecap-role")

        if not os.path.exists(jwt_path):
            raise FileNotFoundError("Kubernetes service account token not found")

        with open(jwt_path, "r") as f:
            jwt = f.read().strip()

        auth_response = self.client.auth.kubernetes.login(
            role=role, jwt=jwt, mount_point="kubernetes"
        )

        self.client.token = auth_response["auth"]["client_token"]

    def _authenticate_userpass(self) -> None:
        """Authenticate using username/password."""
        username = os.getenv("VAULT_USERNAME")
        password = os.getenv("VAULT_PASSWORD")

        if not username or not password:
            raise ValueError(
                "VAULT_USERNAME and VAULT_PASSWORD environment variables required"
            )

        auth_response = self.client.auth.userpass.login(
            username=username, password=password, mount_point="userpass"
        )

        self.client.token = auth_response["auth"]["client_token"]

    def _start_token_renewal(self) -> None:
        """Start automatic token renewal thread."""
        if self._renewal_thread and self._renewal_thread.is_alive():
            return

        self._stop_renewal.clear()
        self._renewal_thread = Thread(target=self._token_renewal_loop, daemon=True)
        self._renewal_thread.start()
        logger.info("Started automatic token renewal")

    def _token_renewal_loop(self) -> None:
        """Token renewal background loop."""
        while not self._stop_renewal.is_set():
            try:
                # Get token info
                token_info = self.client.auth.token.lookup_self()
                ttl = token_info["data"].get("ttl", 0)

                # Renew token when TTL is less than 5 minutes
                if ttl > 0 and ttl < 300:
                    logger.info("Renewing Vault token")
                    self.client.auth.token.renew_self()

                # Sleep for 60 seconds before next check
                self._stop_renewal.wait(60)

            except Exception as e:
                logger.error(f"Token renewal failed: {e}")
                self._stop_renewal.wait(60)

    def stop_token_renewal(self) -> None:
        """Stop automatic token renewal."""
        if self._renewal_thread:
            self._stop_renewal.set()
            self._renewal_thread.join(timeout=5)
            logger.info("Stopped automatic token renewal")

    def get_secret(
        self, path: str, key: str = None
    ) -> Union[Dict[str, Any], str, None]:
        """
        Retrieve secret from Vault.

        Args:
            path: Secret path
            key: Specific key within secret (optional)

        Returns:
            Secret data or specific key value
        """
        for attempt in range(self.max_retries):
            try:
                if self.kv_version == 2:
                    response = self.client.secrets.kv.v2.read_secret_version(
                        path=path, mount_point=self.mount_point
                    )
                    data = response["data"]["data"]
                else:
                    response = self.client.secrets.kv.v1.read_secret(
                        path=path, mount_point=self.mount_point
                    )
                    data = response["data"]

                if key:
                    return data.get(key)
                return data

            except InvalidPath:
                logger.warning(f"Secret not found: {path}")
                return None

            except Exception as e:
                logger.error(
                    f"Error retrieving secret {path} (attempt {attempt + 1}): {e}"
                )
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(2**attempt)  # Exponential backoff

    def put_secret(self, path: str, data: Dict[str, Any]) -> bool:
        """
        Store secret in Vault.

        Args:
            path: Secret path
            data: Secret data dictionary

        Returns:
            Success status
        """
        try:
            if self.kv_version == 2:
                self.client.secrets.kv.v2.create_or_update_secret(
                    path=path, secret=data, mount_point=self.mount_point
                )
            else:
                self.client.secrets.kv.v1.create_or_update_secret(
                    path=path, secret=data, mount_point=self.mount_point
                )

            logger.info(f"Successfully stored secret: {path}")
            return True

        except Exception as e:
            logger.error(f"Error storing secret {path}: {e}")
            return False

    def delete_secret(self, path: str) -> bool:
        """
        Delete secret from Vault.

        Args:
            path: Secret path

        Returns:
            Success status
        """
        try:
            if self.kv_version == 2:
                self.client.secrets.kv.v2.delete_metadata_and_all_versions(
                    path=path, mount_point=self.mount_point
                )
            else:
                self.client.secrets.kv.v1.delete_secret(
                    path=path, mount_point=self.mount_point
                )

            logger.info(f"Successfully deleted secret: {path}")
            return True

        except Exception as e:
            logger.error(f"Error deleting secret {path}: {e}")
            return False

    def get_database_credentials(self, role: str) -> Optional[Dict[str, str]]:
        """
        Get dynamic database credentials from Vault.

        Args:
            role: Database role name

        Returns:
            Database credentials (username, password)
        """
        try:
            response = self.client.secrets.database.generate_credentials(name=role)
            return {
                "username": response["data"]["username"],
                "password": response["data"]["password"],
                "lease_id": response["lease_id"],
                "lease_duration": response["lease_duration"],
            }

        except Exception as e:
            logger.error(f"Error getting database credentials for role {role}: {e}")
            return None

    def is_healthy(self) -> bool:
        """Check Vault connection health."""
        try:
            return self.client.sys.is_initialized() and self.client.is_authenticated()
        except Exception:
            return False

    def get_health_info(self) -> Dict[str, Any]:
        """Get detailed health information."""
        try:
            health = self.client.sys.read_health_status()
            return {
                "healthy": True,
                "initialized": health.get("initialized", False),
                "sealed": health.get("sealed", True),
                "version": health.get("version", "unknown"),
                "authenticated": self.client.is_authenticated(),
            }
        except Exception as e:
            return {"healthy": False, "error": str(e), "authenticated": False}

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop_token_renewal()


# Factory function for easy instantiation
def create_vault_client(**kwargs) -> VaultClient:
    """
    Create and return a configured Vault client.

    Args:
        **kwargs: VaultClient initialization parameters

    Returns:
        Configured VaultClient instance
    """
    return VaultClient(**kwargs)


# Global vault client instance (lazy loaded)
_vault_client = None


def get_vault_client() -> VaultClient:
    """Get global vault client instance."""
    global _vault_client
    if _vault_client is None:
        _vault_client = create_vault_client()
    return _vault_client


def close_vault_client() -> None:
    """Close global vault client instance."""
    global _vault_client
    if _vault_client:
        _vault_client.stop_token_renewal()
        _vault_client = None
