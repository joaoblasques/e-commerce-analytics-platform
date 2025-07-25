"""
Configuration Management System Demonstration.

This script demonstrates the comprehensive configuration and secrets management
capabilities of the E-Commerce Analytics Platform.
"""

import logging
import os
import sys
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config import ECAPConfig, init_config
from config.encryption import ConfigEncryption, SecretManager
from config.k8s_secrets import KubernetesSecretsManager
from config.vault_client import VaultClient

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def demo_basic_configuration():
    """Demonstrate basic configuration management."""
    print("\n" + "=" * 60)
    print("BASIC CONFIGURATION MANAGEMENT DEMO")
    print("=" * 60)

    # Initialize configuration for development environment
    config = ECAPConfig(
        environment="development",
        enable_vault=False,  # Disable for demo
        enable_k8s_secrets=False,  # Disable for demo
        enable_hot_reload=True,
    )

    print(f"Environment: {config.environment}")
    print(f"App Name: {config.get('app.name')}")
    print(f"App Debug: {config.get_bool('app.debug')}")
    print(f"Database Host: {config.get('database.host')}")
    print(f"Database Port: {config.get_int('database.port')}")

    # Get structured configuration
    print(f"Kafka Config: {config.get_dict('kafka.topics')}")

    # Set and get a new configuration value
    config.set("demo.setting", "demo_value")
    print(f"Demo Setting: {config.get('demo.setting')}")

    # Get configuration with default
    print(f"Non-existent Setting: {config.get('non.existent', 'default_value')}")

    config.close()


def demo_environment_specific_config():
    """Demonstrate environment-specific configuration."""
    print("\n" + "=" * 60)
    print("ENVIRONMENT-SPECIFIC CONFIGURATION DEMO")
    print("=" * 60)

    environments = ["development", "staging", "production"]

    for env in environments:
        print(f"\n--- {env.upper()} Environment ---")

        try:
            config = ECAPConfig(
                environment=env,
                enable_vault=False,
                enable_k8s_secrets=False,
                enable_hot_reload=False,
            )

            print(f"Debug Mode: {config.get_bool('app.debug')}")
            print(f"Workers: {config.get_int('app.api.workers', 1)}")
            print(f"Log Level: {config.get('logging.level')}")
            print(f"Cache TTL: {config.get_int('performance.cache_ttl', 300)}")

            config.close()

        except Exception as e:
            print(f"Error loading {env} config: {e}")


def demo_encryption():
    """Demonstrate configuration encryption."""
    print("\n" + "=" * 60)
    print("CONFIGURATION ENCRYPTION DEMO")
    print("=" * 60)

    # Initialize encryption
    encryption = ConfigEncryption(auto_generate_key=True)

    # Encrypt individual values
    secret_value = "super_secret_password_123"
    encrypted = encryption.encrypt_value(secret_value)
    decrypted = encryption.decrypt_value(encrypted)

    print(f"Original: {secret_value}")
    print(f"Encrypted: {encrypted[:50]}...")
    print(f"Decrypted: {decrypted}")
    print(f"Encryption successful: {secret_value == decrypted}")

    # Encrypt configuration dictionary
    config_dict = {
        "app_name": "Demo App",
        "database_password": "secret_db_pass",
        "api_key": "secret_api_key",
        "normal_setting": "normal_value",
    }

    print(f"\nOriginal config: {config_dict}")

    encrypted_config = encryption.encrypt_config_dict(config_dict)
    print(f"Encrypted config: {encrypted_config}")

    decrypted_config = encryption.decrypt_config_dict(encrypted_config)
    print(f"Decrypted config: {decrypted_config}")

    # Key information
    key_info = encryption.get_key_info()
    print(f"\nKey info: {key_info}")


def demo_secret_manager():
    """Demonstrate secret management."""
    print("\n" + "=" * 60)
    print("SECRET MANAGEMENT DEMO")
    print("=" * 60)

    # Initialize secret manager
    secret_manager = SecretManager()

    # Store secrets
    secrets = {
        "database_password": "ultra_secure_db_password",
        "api_key": "very_secret_api_key",
        "jwt_secret": "jwt_signing_secret",
    }

    for name, value in secrets.items():
        success = secret_manager.set_secret(name, value, {"environment": "demo"})
        print(f"Stored {name}: {success}")

    # Retrieve secrets
    for name in secrets.keys():
        value = secret_manager.get_secret(name)
        print(f"Retrieved {name}: {value}")

    # List all secrets (without values)
    secret_list = secret_manager.list_secrets()
    print(f"\nAll secrets: {secret_list}")


def demo_database_config():
    """Demonstrate database configuration retrieval."""
    print("\n" + "=" * 60)
    print("DATABASE CONFIGURATION DEMO")
    print("=" * 60)

    config = ECAPConfig(
        environment="development", enable_vault=False, enable_k8s_secrets=False
    )

    # Get complete database configuration
    db_config = config.get_database_config()

    print("Database Configuration:")
    for key, value in db_config.items():
        if "password" in key.lower():
            print(f"  {key}: {'*' * len(str(value)) if value else None}")
        else:
            print(f"  {key}: {value}")

    config.close()


def demo_kafka_config():
    """Demonstrate Kafka configuration retrieval."""
    print("\n" + "=" * 60)
    print("KAFKA CONFIGURATION DEMO")
    print("=" * 60)

    config = ECAPConfig(
        environment="development", enable_vault=False, enable_k8s_secrets=False
    )

    # Get complete Kafka configuration
    kafka_config = config.get_kafka_config()

    print("Kafka Configuration:")
    for key, value in kafka_config.items():
        if "password" in key.lower() or "secret" in key.lower():
            print(f"  {key}: {'*' * len(str(value)) if value else None}")
        else:
            print(f"  {key}: {value}")

    config.close()


def demo_config_callbacks():
    """Demonstrate configuration change callbacks."""
    print("\n" + "=" * 60)
    print("CONFIGURATION CALLBACKS DEMO")
    print("=" * 60)

    config = ECAPConfig(
        environment="development",
        enable_vault=False,
        enable_k8s_secrets=False,
        enable_hot_reload=False,
    )

    # Register callback
    def config_change_handler(key, old_value, new_value):
        print(f"Configuration changed: {key}")
        print(f"  Old: {old_value}")
        print(f"  New: {new_value}")

    config.register_change_callback(config_change_handler)

    # Make some changes
    print("Setting new values...")
    config.set("demo.value1", "first_value")
    config.set("demo.value2", "second_value")
    config.set("demo.value1", "updated_value")  # This should trigger callback

    config.close()


def demo_health_monitoring():
    """Demonstrate health monitoring."""
    print("\n" + "=" * 60)
    print("HEALTH MONITORING DEMO")
    print("=" * 60)

    config = ECAPConfig(
        environment="development", enable_vault=False, enable_k8s_secrets=False
    )

    # Get health status
    health_status = config.get_health_status()

    print("System Health Status:")
    for component, status in health_status.items():
        status_icon = "✅" if status else "❌"
        print(f"  {component}: {status_icon} {status}")

    config.close()


def demo_configuration_summary():
    """Demonstrate configuration summary."""
    print("\n" + "=" * 60)
    print("CONFIGURATION SUMMARY DEMO")
    print("=" * 60)

    config = ECAPConfig(
        environment="development", enable_vault=False, enable_k8s_secrets=False
    )

    # Get configuration summary
    summary = config.config_manager.get_config_summary()

    print(f"Total configurations: {summary['total_configs']}")
    print(f"Environment: {summary['environment']}")
    print(f"Sources: {summary['sources_summary']}")

    # Show a few example configurations
    print("\nSample configurations:")
    count = 0
    for key, info in summary["configs"].items():
        if count >= 5:  # Limit output
            break
        print(f"  {key}: {info['source']} ({info['path']})")
        count += 1

    config.close()


def main():
    """Run all demonstrations."""
    print("E-Commerce Analytics Platform")
    print("Configuration Management System Demo")
    print("=" * 60)

    try:
        demo_basic_configuration()
        demo_environment_specific_config()
        demo_encryption()
        demo_secret_manager()
        demo_database_config()
        demo_kafka_config()
        demo_config_callbacks()
        demo_health_monitoring()
        demo_configuration_summary()

        print("\n" + "=" * 60)
        print("DEMO COMPLETED SUCCESSFULLY")
        print("=" * 60)

    except Exception as e:
        logger.error(f"Demo failed: {e}")
        raise


if __name__ == "__main__":
    main()
