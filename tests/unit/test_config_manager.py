"""
Unit tests for configuration management system.
"""

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
import yaml

from src.config.encryption import ConfigEncryption
from src.config.manager import ConfigManager, ConfigSource, ConfigValue
from src.config.vault_client import VaultClient


class TestConfigManager:
    """Test cases for ConfigManager class."""

    @pytest.fixture
    def temp_config_dir(self):
        """Create temporary configuration directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)

            # Create test configuration files
            default_config = {
                "app": {"name": "Test App", "debug": False},
                "database": {"host": "localhost", "port": 5432},
            }

            dev_config = {
                "app": {"debug": True},
                "database": {"host": "dev-db.example.com"},
            }

            with open(config_dir / "default.yaml", "w") as f:
                yaml.dump(default_config, f)

            with open(config_dir / "development.yaml", "w") as f:
                yaml.dump(dev_config, f)

            yield config_dir

    @pytest.fixture
    def mock_vault_client(self):
        """Mock Vault client."""
        mock_vault = Mock(spec=VaultClient)
        mock_vault.get_secret.return_value = {
            "database_password": "secret123",
            "api_key": "key456",
        }
        return mock_vault

    def test_config_manager_initialization(self, temp_config_dir):
        """Test ConfigManager initialization."""
        config_manager = ConfigManager(
            config_dir=str(temp_config_dir),
            environment="development",
            enable_vault=False,
            enable_k8s_secrets=False,
        )

        assert config_manager.environment == "development"
        assert config_manager.config_dir == temp_config_dir
        assert not config_manager.enable_vault
        assert not config_manager.enable_k8s_secrets

    def test_hierarchical_config_loading(self, temp_config_dir):
        """Test hierarchical configuration loading."""
        config_manager = ConfigManager(
            config_dir=str(temp_config_dir),
            environment="development",
            enable_vault=False,
            enable_k8s_secrets=False,
            enable_hot_reload=False,
        )

        # Test that development config overrides default
        assert config_manager.get("app.name") == "Test App"  # From default
        assert config_manager.get("app.debug") == True  # Overridden in development
        assert config_manager.get("database.host") == "dev-db.example.com"  # Overridden
        assert config_manager.get("database.port") == 5432  # From default

    def test_get_methods(self, temp_config_dir):
        """Test configuration getter methods."""
        config_manager = ConfigManager(
            config_dir=str(temp_config_dir),
            environment="development",
            enable_vault=False,
            enable_k8s_secrets=False,
            enable_hot_reload=False,
        )

        # Test get with default
        assert config_manager.get("nonexistent", "default_value") == "default_value"

        # Test typed getters
        assert config_manager.get_bool("app.debug") == True
        assert config_manager.get_int("database.port") == 5432
        assert config_manager.get_dict("app") == {"name": "Test App", "debug": True}

    def test_environment_variable_loading(self, temp_config_dir):
        """Test environment variable loading."""
        # Set test environment variables
        os.environ["ECAP_TEST_VALUE"] = "test123"
        os.environ["APP_SECRET_KEY"] = "secret456"

        try:
            config_manager = ConfigManager(
                config_dir=str(temp_config_dir),
                environment="development",
                enable_vault=False,
                enable_k8s_secrets=False,
                enable_hot_reload=False,
            )

            assert config_manager.get("ecap_test_value") == "test123"
            assert config_manager.get("app_secret_key") == "secret456"

        finally:
            # Clean up environment variables
            os.environ.pop("ECAP_TEST_VALUE", None)
            os.environ.pop("APP_SECRET_KEY", None)

    def test_vault_integration(self, temp_config_dir, mock_vault_client):
        """Test Vault secrets integration."""
        with patch(
            "src.config.manager.get_vault_client", return_value=mock_vault_client
        ):
            config_manager = ConfigManager(
                config_dir=str(temp_config_dir),
                environment="development",
                enable_vault=True,
                enable_k8s_secrets=False,
                enable_hot_reload=False,
            )

            # Verify Vault client was called
            mock_vault_client.get_secret.assert_called()

    def test_config_callbacks(self, temp_config_dir):
        """Test configuration change callbacks."""
        config_manager = ConfigManager(
            config_dir=str(temp_config_dir),
            environment="development",
            enable_vault=False,
            enable_k8s_secrets=False,
            enable_hot_reload=False,
        )

        callback_called = []

        def test_callback(key, old_value, new_value):
            callback_called.append((key, old_value, new_value))

        config_manager.register_callback(test_callback)
        config_manager.set("test.key", "new_value")

        assert len(callback_called) == 1
        assert callback_called[0] == ("test.key", None, "new_value")

    def test_config_value_metadata(self, temp_config_dir):
        """Test configuration value metadata."""
        config_manager = ConfigManager(
            config_dir=str(temp_config_dir),
            environment="development",
            enable_vault=False,
            enable_k8s_secrets=False,
            enable_hot_reload=False,
        )

        config_info = config_manager.get_config_info("app.name")

        assert isinstance(config_info, ConfigValue)
        assert config_info.value == "Test App"
        assert config_info.source == ConfigSource.FILE
        assert config_info.path.endswith("default.yaml")

    def test_required_configs_validation(self, temp_config_dir):
        """Test required configuration validation."""
        config_manager = ConfigManager(
            config_dir=str(temp_config_dir),
            environment="development",
            enable_vault=False,
            enable_k8s_secrets=False,
            enable_hot_reload=False,
        )

        # Should not raise for existing keys
        config_manager.validate_required_configs(["app.name", "database.host"])

        # Should raise for missing keys
        with pytest.raises(ValueError, match="Missing required configuration keys"):
            config_manager.validate_required_configs(["nonexistent.key"])

    def test_config_summary(self, temp_config_dir):
        """Test configuration summary generation."""
        config_manager = ConfigManager(
            config_dir=str(temp_config_dir),
            environment="development",
            enable_vault=False,
            enable_k8s_secrets=False,
            enable_hot_reload=False,
        )

        summary = config_manager.get_config_summary()

        assert "total_configs" in summary
        assert "sources_summary" in summary
        assert "environment" in summary
        assert "configs" in summary
        assert summary["environment"] == "development"
        assert summary["total_configs"] > 0


class TestConfigEncryption:
    """Test cases for ConfigEncryption class."""

    @pytest.fixture
    def temp_key_file(self):
        """Create temporary key file."""
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.close()  # Close the file so ConfigEncryption can write to it
            # Remove the file so auto_generate_key works
            os.unlink(temp_file.name)
            yield temp_file.name

        # Clean up
        if os.path.exists(temp_file.name):
            os.unlink(temp_file.name)

    def test_encryption_initialization(self, temp_key_file):
        """Test encryption initialization."""
        encryption = ConfigEncryption(key_file=temp_key_file, auto_generate_key=True)

        assert encryption.key is not None
        assert encryption.cipher is not None
        assert os.path.exists(temp_key_file)

    def test_value_encryption_decryption(self, temp_key_file):
        """Test value encryption and decryption."""
        encryption = ConfigEncryption(key_file=temp_key_file, auto_generate_key=True)

        test_value = "secret_password_123"

        # Encrypt
        encrypted = encryption.encrypt_value(test_value)
        assert encrypted != test_value
        assert isinstance(encrypted, str)

        # Decrypt
        decrypted = encryption.decrypt_value(encrypted)
        assert decrypted == test_value

    def test_config_dict_encryption(self, temp_key_file):
        """Test configuration dictionary encryption."""
        encryption = ConfigEncryption(key_file=temp_key_file, auto_generate_key=True)

        config = {
            "app_name": "Test App",
            "database_password": "secret123",
            "api_key": "key456",
            "normal_setting": "value789",
        }

        encrypted_config = encryption.encrypt_config_dict(config)

        # Check that sensitive keys are encrypted
        assert encrypted_config["database_password"]["encrypted"] == True
        assert encrypted_config["api_key"]["encrypted"] == True

        # Check that normal keys are not encrypted
        assert encrypted_config["app_name"] == "Test App"
        assert encrypted_config["normal_setting"] == "value789"

        # Decrypt and verify
        decrypted_config = encryption.decrypt_config_dict(encrypted_config)
        assert decrypted_config["database_password"] == "secret123"
        assert decrypted_config["api_key"] == "key456"

    def test_key_rotation(self, temp_key_file):
        """Test encryption key rotation."""
        encryption = ConfigEncryption(key_file=temp_key_file, auto_generate_key=True)

        original_key = encryption.key

        # Rotate key
        success = encryption.rotate_key()
        assert success == True
        assert encryption.key != original_key

        # Check backup file exists
        backup_files = [
            f
            for f in os.listdir(os.path.dirname(temp_key_file))
            if f.startswith(os.path.basename(temp_key_file) + ".backup")
        ]
        assert len(backup_files) > 0


class TestVaultClient:
    """Test cases for VaultClient class."""

    @pytest.fixture
    def mock_hvac_client(self):
        """Mock hvac client."""
        mock_client = MagicMock()
        mock_client.is_authenticated.return_value = True
        mock_client.sys.is_initialized.return_value = True
        return mock_client

    @patch("hvac.Client")
    def test_vault_client_initialization(self, mock_hvac, mock_hvac_client):
        """Test VaultClient initialization."""
        mock_hvac.return_value = mock_hvac_client

        vault_client = VaultClient(
            vault_url="http://localhost:8200",
            vault_token="test-token",
            auth_method="token",
        )

        assert vault_client.vault_url == "http://localhost:8200"
        assert vault_client.vault_token == "test-token"
        assert vault_client.auth_method == "token"
        mock_hvac.assert_called_once()

    @patch("hvac.Client")
    def test_secret_operations(self, mock_hvac, mock_hvac_client):
        """Test secret get/put operations."""
        mock_hvac.return_value = mock_hvac_client

        # Mock secret responses
        mock_hvac_client.secrets.kv.v2.read_secret_version.return_value = {
            "data": {"data": {"password": "secret123", "api_key": "key456"}}
        }

        vault_client = VaultClient(
            vault_url="http://localhost:8200",
            vault_token="test-token",
            auth_method="token",
        )

        # Test get_secret
        secret = vault_client.get_secret("test/path", "password")
        assert secret == "secret123"

        # Test put_secret
        success = vault_client.put_secret("test/path", {"new_key": "new_value"})
        assert success == True

    @patch("hvac.Client")
    def test_health_check(self, mock_hvac, mock_hvac_client):
        """Test health check functionality."""
        mock_hvac.return_value = mock_hvac_client

        vault_client = VaultClient(
            vault_url="http://localhost:8200",
            vault_token="test-token",
            auth_method="token",
        )

        # Test healthy status
        assert vault_client.is_healthy() == True

        # Test health info
        mock_hvac_client.sys.read_health_status.return_value = {
            "initialized": True,
            "sealed": False,
            "version": "1.13.0",
        }

        health_info = vault_client.get_health_info()
        assert health_info["healthy"] == True
        assert health_info["initialized"] == True
        assert health_info["version"] == "1.13.0"


if __name__ == "__main__":
    pytest.main([__file__])
