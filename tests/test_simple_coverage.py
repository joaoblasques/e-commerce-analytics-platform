"""
Simple test to ensure basic imports work and contribute to coverage.
"""

import os
import sys

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


def test_basic_imports():
    """Test that basic imports work."""
    from src import cli
    from src.database import models
    from src.utils import spark_validator

    # Basic assertion to ensure modules are loaded
    assert cli is not None
    assert models is not None
    assert spark_validator is not None


def test_database_config_import():
    """Test database config import."""
    from src.database import config

    # Just test that the module can be imported
    assert config is not None


def test_cli_import():
    """Test CLI import."""
    from src.cli import main

    assert main is not None
