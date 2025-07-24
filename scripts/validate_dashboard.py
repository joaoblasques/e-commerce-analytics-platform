#!/usr/bin/env python3
"""
Dashboard validation script.

This script validates that the dashboard can be imported and basic functionality works.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def test_imports():
    """Test that all dashboard modules can be imported."""
    try:
        print("Testing dashboard imports...")

        # Test main imports
        import dashboard.main  # noqa: F401
        import dashboard.config.settings  # noqa: F401

        # Test component imports
        import dashboard.components.alerts  # noqa: F401
        import dashboard.components.charts  # noqa: F401
        import dashboard.components.metrics_cards  # noqa: F401
        import dashboard.components.sidebar  # noqa: F401
        import dashboard.components.tables  # noqa: F401

        # Test page imports
        import dashboard.pages.customer_analytics  # noqa: F401
        import dashboard.pages.executive_dashboard  # noqa: F401
        import dashboard.pages.fraud_detection  # noqa: F401
        import dashboard.pages.real_time_monitoring  # noqa: F401
        import dashboard.pages.revenue_analytics  # noqa: F401

        # Test utility imports
        import dashboard.utils.api_client  # noqa: F401
        import dashboard.utils.auth  # noqa: F401
        import dashboard.utils.data_processing  # noqa: F401

        print("✅ All imports successful!")
        return True

    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False


def test_configuration():
    """Test dashboard configuration."""
    try:
        print("Testing configuration...")

        from dashboard.config.settings import get_dashboard_settings

        settings = get_dashboard_settings()

        # Test basic settings
        assert settings.api_base_url
        assert settings.page_title
        assert len(settings.pages) > 0
        assert len(settings.api_endpoints) > 0

        print("✅ Configuration validation successful!")
        return True

    except Exception as e:
        print(f"❌ Configuration error: {e}")
        return False


def test_components():
    """Test component functionality."""
    try:
        print("Testing components...")

        from dashboard.utils.data_processing import format_currency, format_percentage

        # Test formatting functions
        assert format_currency(1234.56) == "$1,234.56"
        assert format_percentage(0.1234) == "12.34%"

        print("✅ Component testing successful!")
        return True

    except Exception as e:
        print(f"❌ Component error: {e}")
        return False


def main():
    """Run all validation tests."""
    print("🚀 Dashboard Validation Starting...\n")

    tests = [
        ("Import Tests", test_imports),
        ("Configuration Tests", test_configuration),
        ("Component Tests", test_components),
    ]

    passed = 0
    total = len(tests)

    for name, test_func in tests:
        print(f"\n📋 Running {name}...")
        if test_func():
            passed += 1
        print("-" * 50)

    print(f"\n📊 Validation Results: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 Dashboard validation completed successfully!")
        print("✅ Dashboard is ready to run!")
        print("\nTo start the dashboard:")
        print("  make run-dashboard")
        print("  or")
        print("  poetry run streamlit run src/dashboard/main.py")
        return True
    else:
        print("❌ Some validation tests failed")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
