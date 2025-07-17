"""
Unit tests for package initialization.
"""
import pytest


def test_src_package_import():
    """Test that src package can be imported."""
    import src
    assert hasattr(src, '__version__')
    assert isinstance(src.__version__, str)
    assert len(src.__version__) > 0


def test_src_version_format():
    """Test that version follows semantic versioning."""
    import src
    version_parts = src.__version__.split('.')
    assert len(version_parts) >= 2  # At least major.minor
    assert all(part.isdigit() for part in version_parts if part.isdigit())


def test_subpackage_imports():
    """Test that subpackages can be imported."""
    import src.analytics
    import src.api
    import src.dashboard
    import src.streaming
    import src.utils
    import src.data
    
    # These should import without errors
    assert src.analytics is not None
    assert src.api is not None
    assert src.dashboard is not None
    assert src.streaming is not None
    assert src.utils is not None
    assert src.data is not None


def test_cli_module_import():
    """Test that CLI module can be imported."""
    from src import cli
    assert hasattr(cli, 'main')
    assert callable(cli.main)


def test_spark_validator_import():
    """Test that spark validator can be imported."""
    from src.utils import spark_validator
    assert hasattr(spark_validator, 'validate_spark_imports')
    assert hasattr(spark_validator, 'main')
    assert callable(spark_validator.validate_spark_imports)
    assert callable(spark_validator.main)


def test_package_structure():
    """Test that package has expected structure."""
    import src
    
    # Check that main attributes exist
    assert hasattr(src, '__version__')
    assert hasattr(src, '__author__')
    assert hasattr(src, '__description__')
    
    # Check types
    assert isinstance(src.__version__, str)
    assert isinstance(src.__author__, str)
    assert isinstance(src.__description__, str)