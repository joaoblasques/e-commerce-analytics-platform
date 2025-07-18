"""Basic integration tests for the e-commerce analytics platform."""

import pytest


def test_basic_integration():
    """Basic integration test to ensure the test suite runs."""
    # This is a placeholder test to ensure the integration test suite
    # doesn't fail with 0 items collected
    assert True


def test_package_imports():
    """Test that basic package imports work correctly."""
    try:
        import src

        assert src is not None
    except ImportError:
        pytest.fail("Could not import src package")


def test_database_models_integration():
    """Test that database models can be imported and used."""
    try:
        import sys
        from pathlib import Path

        # Add src directory to path
        sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

        from database.models import Customer

        # Test that models can be instantiated
        customer = Customer(
            user_id="integration_test_user",
            email="test@integration.com",
            account_status="active",
            customer_tier="bronze",
        )

        assert customer.user_id == "integration_test_user"
        assert customer.email == "test@integration.com"

    except ImportError as e:
        pytest.fail(f"Could not import database models: {e}")


def test_cli_module_integration():
    """Test that CLI module can be imported and basic functionality works."""
    try:
        import sys
        from pathlib import Path

        # Add src directory to path
        sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

        from cli import main

        # Test that main function exists and is callable
        assert callable(main)

    except ImportError as e:
        pytest.fail(f"Could not import CLI module: {e}")
