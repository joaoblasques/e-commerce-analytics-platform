"""
Configuration and Secrets Encryption Module.

This module provides encryption and decryption capabilities for:
- Configuration values
- Database secrets
- API keys and tokens
- File-based encryption
- Key rotation support
"""

import base64
import hashlib
import logging
import os
import secrets
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

logger = logging.getLogger(__name__)


class ConfigEncryption:
    """
    Configuration encryption and decryption with key management.

    Features:
    - Symmetric encryption for configuration values
    - Asymmetric encryption for secure key exchange
    - Key derivation from passwords
    - Automatic key rotation
    - Secure key storage
    """

    def __init__(
        self,
        master_key: str = None,
        key_file: str = None,
        auto_generate_key: bool = True,
        key_rotation_days: int = 90,
    ):
        """
        Initialize encryption manager.

        Args:
            master_key: Master encryption key
            key_file: Path to key file
            auto_generate_key: Generate key if not provided
            key_rotation_days: Days between key rotation
        """
        self.key_file = key_file or os.getenv("ENCRYPTION_KEY_FILE", ".encryption_key")
        self.key_rotation_days = key_rotation_days

        # Initialize encryption key
        if master_key:
            self.key = self._derive_key_from_password(master_key)
        elif os.path.exists(self.key_file):
            self.key = self._load_key_from_file()
        elif auto_generate_key:
            self.key = self._generate_and_save_key()
        else:
            raise ValueError("No encryption key provided and auto-generation disabled")

        self.cipher = Fernet(self.key)

        # Key metadata
        self.key_created = self._get_key_creation_time()

        logger.info("Configuration encryption initialized")

    def _derive_key_from_password(self, password: str, salt: bytes = None) -> bytes:
        """Derive encryption key from password using PBKDF2."""
        if salt is None:
            salt = os.urandom(16)

        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )

        key = base64.urlsafe_b64encode(kdf.derive(password.encode()))
        return key

    def _generate_and_save_key(self) -> bytes:
        """Generate new encryption key and save to file."""
        key = Fernet.generate_key()

        try:
            with open(self.key_file, "wb") as f:
                f.write(key)

            # Set restrictive permissions
            os.chmod(self.key_file, 0o600)

            logger.info(f"Generated new encryption key: {self.key_file}")
            return key

        except Exception as e:
            logger.error(f"Error saving encryption key: {e}")
            raise

    def _load_key_from_file(self) -> bytes:
        """Load encryption key from file."""
        try:
            with open(self.key_file, "rb") as f:
                key = f.read()

            logger.info(f"Loaded encryption key from: {self.key_file}")
            return key

        except Exception as e:
            logger.error(f"Error loading encryption key: {e}")
            raise

    def _get_key_creation_time(self) -> datetime:
        """Get key file creation time."""
        try:
            if os.path.exists(self.key_file):
                timestamp = os.path.getctime(self.key_file)
                return datetime.fromtimestamp(timestamp)
            return datetime.now()
        except Exception:
            return datetime.now()

    def encrypt_value(self, value: Union[str, dict, list]) -> str:
        """
        Encrypt configuration value.

        Args:
            value: Value to encrypt

        Returns:
            Base64 encoded encrypted value
        """
        try:
            if isinstance(value, (dict, list)):
                import json

                value_str = json.dumps(value)
            else:
                value_str = str(value)

            encrypted = self.cipher.encrypt(value_str.encode())
            return base64.b64encode(encrypted).decode()

        except Exception as e:
            logger.error(f"Error encrypting value: {e}")
            raise

    def decrypt_value(self, encrypted_value: str) -> str:
        """
        Decrypt configuration value.

        Args:
            encrypted_value: Base64 encoded encrypted value

        Returns:
            Decrypted value
        """
        try:
            encrypted_bytes = base64.b64decode(encrypted_value.encode())
            decrypted = self.cipher.decrypt(encrypted_bytes)
            return decrypted.decode()

        except Exception as e:
            logger.error(f"Error decrypting value: {e}")
            raise

    def encrypt_config_dict(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Encrypt sensitive values in configuration dictionary.

        Args:
            config: Configuration dictionary

        Returns:
            Configuration with encrypted sensitive values
        """
        sensitive_keys = {
            "password",
            "secret",
            "key",
            "token",
            "credential",
            "private",
            "auth",
            "cert",
            "api_key",
            "access_key",
        }

        encrypted_config = {}

        for key, value in config.items():
            key_lower = key.lower()

            # Check if key contains sensitive terms
            is_sensitive = any(term in key_lower for term in sensitive_keys)

            if is_sensitive and isinstance(value, str):
                encrypted_config[key] = {
                    "value": self.encrypt_value(value),
                    "encrypted": True,
                    "timestamp": datetime.now().isoformat(),
                }
            elif isinstance(value, dict):
                encrypted_config[key] = self.encrypt_config_dict(value)
            else:
                encrypted_config[key] = value

        return encrypted_config

    def decrypt_config_dict(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Decrypt encrypted values in configuration dictionary.

        Args:
            config: Configuration dictionary with encrypted values

        Returns:
            Configuration with decrypted values
        """
        decrypted_config = {}

        for key, value in config.items():
            if isinstance(value, dict):
                if value.get("encrypted"):
                    # Decrypt encrypted value
                    decrypted_config[key] = self.decrypt_value(value["value"])
                else:
                    # Recursively process nested dictionaries
                    decrypted_config[key] = self.decrypt_config_dict(value)
            else:
                decrypted_config[key] = value

        return decrypted_config

    def rotate_key(self) -> bool:
        """
        Rotate encryption key.

        Returns:
            Success status
        """
        try:
            # Backup old key
            if os.path.exists(self.key_file):
                backup_file = (
                    f"{self.key_file}.backup.{int(datetime.now().timestamp())}"
                )
                os.rename(self.key_file, backup_file)
                logger.info(f"Backed up old key to: {backup_file}")

            # Generate new key
            new_key = self._generate_and_save_key()

            # Update cipher
            old_cipher = self.cipher
            self.key = new_key
            self.cipher = Fernet(new_key)
            self.key_created = datetime.now()

            logger.info("Encryption key rotated successfully")
            return True

        except Exception as e:
            logger.error(f"Error rotating encryption key: {e}")
            return False

    def is_key_rotation_needed(self) -> bool:
        """Check if key rotation is needed based on age."""
        if not self.key_created:
            return False

        age = datetime.now() - self.key_created
        return age.days >= self.key_rotation_days

    def get_key_info(self) -> Dict[str, Any]:
        """Get encryption key information."""
        age_days = (datetime.now() - self.key_created).days if self.key_created else 0

        return {
            "key_file": self.key_file,
            "key_created": self.key_created.isoformat() if self.key_created else None,
            "key_age_days": age_days,
            "rotation_needed": self.is_key_rotation_needed(),
            "rotation_due_days": max(0, self.key_rotation_days - age_days),
        }


class AsymmetricEncryption:
    """
    Asymmetric encryption for secure key exchange and digital signatures.
    """

    def __init__(self, key_size: int = 2048):
        """
        Initialize asymmetric encryption.

        Args:
            key_size: RSA key size in bits
        """
        self.key_size = key_size
        self.private_key = None
        self.public_key = None

    def generate_key_pair(self) -> Tuple[bytes, bytes]:
        """
        Generate RSA key pair.

        Returns:
            Tuple of (private_key, public_key) in PEM format
        """
        self.private_key = rsa.generate_private_key(
            public_exponent=65537, key_size=self.key_size
        )

        self.public_key = self.private_key.public_key()

        # Serialize keys
        private_pem = self.private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        public_pem = self.public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )

        return private_pem, public_pem

    def load_private_key(self, private_key_pem: bytes, password: str = None) -> None:
        """Load private key from PEM format."""
        self.private_key = serialization.load_pem_private_key(
            private_key_pem, password=password.encode() if password else None
        )
        self.public_key = self.private_key.public_key()

    def load_public_key(self, public_key_pem: bytes) -> None:
        """Load public key from PEM format."""
        self.public_key = serialization.load_pem_public_key(public_key_pem)

    def encrypt_with_public_key(self, data: str) -> str:
        """Encrypt data with public key."""
        if not self.public_key:
            raise ValueError("Public key not loaded")

        encrypted = self.public_key.encrypt(
            data.encode(),
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None,
            ),
        )

        return base64.b64encode(encrypted).decode()

    def decrypt_with_private_key(self, encrypted_data: str) -> str:
        """Decrypt data with private key."""
        if not self.private_key:
            raise ValueError("Private key not loaded")

        encrypted_bytes = base64.b64decode(encrypted_data.encode())

        decrypted = self.private_key.decrypt(
            encrypted_bytes,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None,
            ),
        )

        return decrypted.decode()


class SecretManager:
    """
    Comprehensive secret management with encryption and rotation.
    """

    def __init__(
        self,
        encryption: ConfigEncryption = None,
        secret_store: str = "secrets.encrypted",
    ):
        """
        Initialize secret manager.

        Args:
            encryption: ConfigEncryption instance
            secret_store: Path to encrypted secrets file
        """
        self.encryption = encryption or ConfigEncryption()
        self.secret_store = secret_store
        self.secrets: Dict[str, Any] = {}

        # Load existing secrets
        self._load_secrets()

    def _load_secrets(self) -> None:
        """Load secrets from encrypted file."""
        if os.path.exists(self.secret_store):
            try:
                with open(self.secret_store, "r") as f:
                    encrypted_secrets = f.read()

                decrypted_data = self.encryption.decrypt_value(encrypted_secrets)
                import json

                self.secrets = json.loads(decrypted_data)

                logger.info(f"Loaded {len(self.secrets)} secrets")

            except Exception as e:
                logger.error(f"Error loading secrets: {e}")
                self.secrets = {}

    def _save_secrets(self) -> bool:
        """Save secrets to encrypted file."""
        try:
            import json

            secrets_json = json.dumps(self.secrets, indent=2)
            encrypted_data = self.encryption.encrypt_value(secrets_json)

            with open(self.secret_store, "w") as f:
                f.write(encrypted_data)

            # Set restrictive permissions
            os.chmod(self.secret_store, 0o600)

            logger.info(f"Saved {len(self.secrets)} secrets")
            return True

        except Exception as e:
            logger.error(f"Error saving secrets: {e}")
            return False

    def set_secret(
        self, name: str, value: str, metadata: Dict[str, Any] = None
    ) -> bool:
        """
        Store encrypted secret.

        Args:
            name: Secret name
            value: Secret value
            metadata: Optional metadata

        Returns:
            Success status
        """
        self.secrets[name] = {
            "value": value,
            "created": datetime.now().isoformat(),
            "metadata": metadata or {},
        }

        return self._save_secrets()

    def get_secret(self, name: str) -> Optional[str]:
        """
        Retrieve secret value.

        Args:
            name: Secret name

        Returns:
            Secret value or None
        """
        secret_data = self.secrets.get(name)
        return secret_data["value"] if secret_data else None

    def delete_secret(self, name: str) -> bool:
        """
        Delete secret.

        Args:
            name: Secret name

        Returns:
            Success status
        """
        if name in self.secrets:
            del self.secrets[name]
            return self._save_secrets()
        return False

    def list_secrets(self) -> List[Dict[str, Any]]:
        """List all secrets with metadata (without values)."""
        return [
            {"name": name, "created": data["created"], "metadata": data["metadata"]}
            for name, data in self.secrets.items()
        ]

    def rotate_secrets(self, pattern: str = None) -> int:
        """
        Rotate secrets matching pattern.

        Args:
            pattern: Regex pattern to match secret names

        Returns:
            Number of secrets rotated
        """
        import re

        rotated = 0

        for name in list(self.secrets.keys()):
            if pattern and not re.match(pattern, name):
                continue

            # Generate new value for specific secret types
            if "key" in name.lower():
                new_value = self._generate_api_key()
            elif "password" in name.lower():
                new_value = self._generate_password()
            else:
                continue

            if self.set_secret(name, new_value):
                rotated += 1
                logger.info(f"Rotated secret: {name}")

        return rotated

    def _generate_api_key(self, length: int = 32) -> str:
        """Generate secure API key."""
        return secrets.token_urlsafe(length)

    def _generate_password(self, length: int = 16) -> str:
        """Generate secure password."""
        import string

        alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
        return "".join(secrets.choice(alphabet) for _ in range(length))


# Factory functions
def create_config_encryption(**kwargs) -> ConfigEncryption:
    """Create and return a ConfigEncryption instance."""
    return ConfigEncryption(**kwargs)


def create_secret_manager(**kwargs) -> SecretManager:
    """Create and return a SecretManager instance."""
    return SecretManager(**kwargs)


# Global instances
_config_encryption = None
_secret_manager = None


def get_config_encryption() -> ConfigEncryption:
    """Get global ConfigEncryption instance."""
    global _config_encryption
    if _config_encryption is None:
        _config_encryption = create_config_encryption()
    return _config_encryption


def get_secret_manager() -> SecretManager:
    """Get global SecretManager instance."""
    global _secret_manager
    if _secret_manager is None:
        _secret_manager = create_secret_manager()
    return _secret_manager
