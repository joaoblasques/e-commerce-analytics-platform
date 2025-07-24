"""
Tests for the FastAPI configuration module.

This module tests the configuration management, settings validation,
and environment-specific behavior.
"""

import os
from unittest.mock import patch

import pytest

from src.api.config import (
    Settings,
    get_database_config,
    get_redis_config,
    get_settings,
    validate_configuration,
)


class TestSettings:
    """Test the Settings class and configuration management."""

    def test_default_settings(self):
        """Test default settings values."""
        settings = Settings()

        assert settings.environment == "development"
        assert settings.debug is False
        assert settings.host == "0.0.0.0"
        assert settings.port == 8000
        assert settings.log_level == "INFO"
        assert settings.default_page_size == 100
        assert settings.max_page_size == 1000

    def test_environment_variable_override(self):
        """Test that environment variables override defaults."""
        with patch.dict(
            os.environ,
            {
                "ECAP_ENVIRONMENT": "production",
                "ECAP_DEBUG": "false",
                "ECAP_PORT": "9000",
                "ECAP_LOG_LEVEL": "ERROR",
            },
        ):
            settings = Settings()

            assert settings.environment == "production"
            assert settings.debug is False
            assert settings.port == 9000
            assert settings.log_level == "ERROR"

    def test_environment_validation(self):
        """Test environment setting validation."""
        with pytest.raises(ValueError, match="Environment must be one of"):
            Settings(environment="invalid")

    def test_log_level_validation(self):
        """Test log level validation."""
        with pytest.raises(ValueError, match="Log level must be one of"):
            Settings(log_level="INVALID")

    def test_port_validation(self):
        """Test port validation."""
        with pytest.raises(ValueError, match="Port must be between 1 and 65535"):
            Settings(port=0)

        with pytest.raises(ValueError, match="Port must be between 1 and 65535"):
            Settings(port=70000)

    def test_page_size_validation(self):
        """Test page size validation."""
        with pytest.raises(ValueError, match="Page size must be positive"):
            Settings(default_page_size=0)

        with pytest.raises(ValueError, match="Page size must be positive"):
            Settings(max_page_size=-1)

    def test_max_page_size_validation(self):
        """Test max page size is greater than default."""
        with pytest.raises(ValueError, match="Max page size must be greater than"):
            Settings(default_page_size=100, max_page_size=50)


class TestConfigurationFunctions:
    """Test configuration helper functions."""

    def test_get_settings_cached(self):
        """Test that get_settings returns cached instance."""
        settings1 = get_settings()
        settings2 = get_settings()

        # Should be the same instance due to lru_cache
        assert settings1 is settings2

    def test_get_database_config(self):
        """Test database configuration generation."""
        config = get_database_config()

        assert "url" in config
        assert "pool_size" in config
        assert "max_overflow" in config
        assert "pool_pre_ping" in config
        assert "pool_recycle" in config
        assert "echo" in config

        # Check types
        assert isinstance(config["pool_size"], int)
        assert isinstance(config["max_overflow"], int)
        assert isinstance(config["pool_pre_ping"], bool)
        assert isinstance(config["pool_recycle"], int)
        assert isinstance(config["echo"], bool)

    def test_get_redis_config(self):
        """Test Redis configuration generation."""
        config = get_redis_config()

        assert "url" in config
        assert "decode_responses" in config
        assert "socket_keepalive" in config
        assert "health_check_interval" in config

        # Check types
        assert isinstance(config["decode_responses"], bool)
        assert isinstance(config["socket_keepalive"], bool)
        assert isinstance(config["health_check_interval"], int)


class TestConfigurationValidation:
    """Test configuration validation logic."""

    def test_development_configuration_valid(self):
        """Test that development configuration passes validation."""
        with patch("src.api.config.get_settings") as mock_settings:
            settings = Settings(environment="development")
            mock_settings.return_value = settings

            # Should not raise exception
            assert validate_configuration() is True

    def test_production_configuration_validation_fails_with_default_secret(self):
        """Test that production fails validation with default secret key."""
        with patch("src.api.config.get_settings") as mock_settings:
            settings = Settings(
                environment="production",
                secret_key="your-secret-key-change-in-production",
            )
            mock_settings.return_value = settings

            with pytest.raises(
                ValueError, match="Secret key must be changed in production"
            ):
                validate_configuration()

    def test_production_configuration_validation_fails_with_debug_enabled(self):
        """Test that production fails validation with debug enabled."""
        with patch("src.api.config.get_settings") as mock_settings:
            settings = Settings(
                environment="production", debug=True, secret_key="production-secret-key"
            )
            mock_settings.return_value = settings

            with pytest.raises(
                ValueError, match="Debug mode should be disabled in production"
            ):
                validate_configuration()

    def test_invalid_database_url_fails_validation(self):
        """Test that invalid database URL fails validation."""
        with patch("src.api.config.get_settings") as mock_settings:
            settings = Settings(
                environment="production",
                secret_key="production-secret-key",
                database_url="invalid://url",
            )
            mock_settings.return_value = settings

            with pytest.raises(
                ValueError, match="Database URL must be PostgreSQL or SQLite"
            ):
                validate_configuration()

    def test_invalid_redis_url_fails_validation(self):
        """Test that invalid Redis URL fails validation."""
        with patch("src.api.config.get_settings") as mock_settings:
            settings = Settings(
                environment="production",
                secret_key="production-secret-key",
                redis_url="invalid://url",
            )
            mock_settings.return_value = settings

            with pytest.raises(ValueError, match="Redis URL must start with redis://"):
                validate_configuration()

    def test_valid_production_configuration(self):
        """Test that valid production configuration passes validation."""
        with patch("src.api.config.get_settings") as mock_settings:
            settings = Settings(
                environment="production",
                debug=False,
                secret_key="production-secret-key-123",
                database_url="postgresql://user:pass@host:5432/db",
                redis_url="redis://host:6379/0",
            )
            mock_settings.return_value = settings

            # Should not raise exception
            assert validate_configuration() is True


class TestEnvironmentSpecificSettings:
    """Test environment-specific configuration behavior."""

    def test_development_allows_debug_and_docs(self):
        """Test that development environment allows debug and docs."""
        settings = Settings(environment="development", debug=True)

        assert settings.environment == "development"
        assert settings.debug is True

        # Should pass validation
        with patch("src.api.config.get_settings", return_value=settings):
            assert validate_configuration() is True

    def test_production_requires_secure_settings(self):
        """Test that production environment requires secure settings."""
        # This should fail validation
        settings = Settings(
            environment="production",
            debug=False,
            secret_key="production-secret-key-123",
        )

        assert settings.environment == "production"
        assert settings.debug is False

        # Should pass validation with proper settings
        with patch("src.api.config.get_settings", return_value=settings):
            assert validate_configuration() is True
