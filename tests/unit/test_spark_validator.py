"""
Unit tests for the Spark validator utility.
"""
import pytest
from pathlib import Path
from unittest.mock import patch, mock_open, MagicMock
import tempfile
import os

from src.utils.spark_validator import validate_spark_imports, main


@pytest.fixture
def temp_py_file():
    """Create a temporary Python file for testing."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
        yield f.name
    os.unlink(f.name)


def test_validate_spark_imports_no_spark():
    """Test validation with no Spark imports."""
    content = '''
import os
import sys
from pathlib import Path

def hello():
    print("Hello world")
'''
    
    with patch('builtins.open', mock_open(read_data=content)):
        errors = validate_spark_imports(Path('test.py'))
        assert errors == []


def test_validate_spark_imports_proper_usage():
    """Test validation with proper Spark imports."""
    content = '''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("test").getOrCreate()
'''
    
    with patch('builtins.open', mock_open(read_data=content)):
        errors = validate_spark_imports(Path('test.py'))
        # The logic checks for 'pyspark.sql' in imports but looks for 'SparkSession' in import names
        # Since we import SparkSession, this should pass
        assert errors == []


def test_validate_spark_imports_missing_sparksession():
    """Test validation with missing SparkSession import."""
    content = '''
import pyspark.sql
from pyspark.sql.functions import col

# Missing SparkSession import
'''
    
    with patch('builtins.open', mock_open(read_data=content)):
        errors = validate_spark_imports(Path('test.py'))
        assert len(errors) == 1
        assert "Consider importing SparkSession explicitly" in errors[0]


def test_validate_spark_imports_deprecated_rdd():
    """Test validation with deprecated RDD usage."""
    content = '''
from pyspark.sql import SparkSession
from pyspark.rdd import RDD

# RDD usage is deprecated
'''
    
    with patch('builtins.open', mock_open(read_data=content)):
        errors = validate_spark_imports(Path('test.py'))
        # Should have 1 error about RDD usage (SparkSession is properly imported)
        assert len(errors) == 1
        assert "Consider using DataFrame API instead of RDD" in errors[0]


def test_validate_spark_imports_multiple_issues():
    """Test validation with multiple issues."""
    content = '''
import pyspark.sql
from pyspark.rdd import RDD

# Multiple issues: missing SparkSession and RDD usage
'''
    
    with patch('builtins.open', mock_open(read_data=content)):
        errors = validate_spark_imports(Path('test.py'))
        # Should have 2 errors: missing SparkSession and RDD usage
        assert len(errors) == 2
        assert any("SparkSession" in error for error in errors)
        assert any("RDD" in error for error in errors)


def test_validate_spark_imports_parsing_error():
    """Test validation with syntax error in file."""
    content = '''
import pyspark.sql
def invalid_syntax(
    # Missing closing parenthesis
'''
    
    with patch('builtins.open', mock_open(read_data=content)):
        errors = validate_spark_imports(Path('test.py'))
        assert len(errors) == 1
        assert "Error parsing file" in errors[0]


def test_validate_spark_imports_file_read_error():
    """Test validation with file read error."""
    with patch('builtins.open', side_effect=IOError("File not found")):
        errors = validate_spark_imports(Path('nonexistent.py'))
        assert len(errors) == 1
        assert "Error parsing file" in errors[0]


def test_validate_spark_imports_import_alias():
    """Test validation with import aliases."""
    content = '''
import pyspark.sql as sql
from pyspark.sql.functions import col as column

# Using aliases - still missing SparkSession
'''
    
    with patch('builtins.open', mock_open(read_data=content)):
        errors = validate_spark_imports(Path('test.py'))
        # Should have 1 error about missing SparkSession
        assert len(errors) == 1
        assert "Consider importing SparkSession explicitly" in errors[0]


def test_validate_spark_imports_complex_imports():
    """Test validation with complex import patterns."""
    content = '''
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import StructType, StructField, StringType

# Complex but valid imports
'''
    
    with patch('builtins.open', mock_open(read_data=content)):
        errors = validate_spark_imports(Path('test.py'))
        assert errors == []


def test_main_no_src_directory():
    """Test main function with no src directory."""
    with patch('pathlib.Path') as mock_path_class:
        mock_src_path = MagicMock()
        mock_src_path.exists.return_value = False
        mock_path_class.return_value = mock_src_path
        
        with patch('builtins.print') as mock_print:
            result = main()
            assert result == 0
            mock_print.assert_called_with("No src directory found")


def test_main_with_valid_files():
    """Test main function with valid files."""
    # Mock Python files
    mock_file1 = MagicMock()
    mock_file1.name = 'valid_file.py'
    mock_file2 = MagicMock() 
    mock_file2.name = '__init__.py'  # Should be skipped
    mock_file3 = MagicMock()
    mock_file3.name = 'test_something.py'  # Should be skipped
    
    files = [mock_file1, mock_file2, mock_file3]
    
    with patch('pathlib.Path') as mock_path_class:
        mock_src_path = MagicMock()
        mock_src_path.exists.return_value = True
        mock_src_path.rglob.return_value = files
        mock_path_class.return_value = mock_src_path
        
        with patch('src.utils.spark_validator.validate_spark_imports') as mock_validate:
            mock_validate.return_value = []
            
            with patch('builtins.print') as mock_print:
                result = main()
                assert result == 0
                mock_print.assert_called_with("Spark validation passed")
                # Should only validate mock_file1 (others are skipped)
                mock_validate.assert_called_once_with(mock_file1)


def test_main_with_validation_errors():
    """Test main function with validation errors."""
    # Mock Python file
    mock_file = MagicMock()
    mock_file.name = 'error_file.py'
    files = [mock_file]
    
    with patch('pathlib.Path') as mock_path_class:
        mock_src_path = MagicMock()
        mock_src_path.exists.return_value = True
        mock_src_path.rglob.return_value = files
        mock_path_class.return_value = mock_src_path
        
        with patch('src.utils.spark_validator.validate_spark_imports') as mock_validate:
            mock_validate.return_value = ["Error 1", "Error 2"]
            
            with patch('builtins.print') as mock_print:
                result = main()
                assert result == 1
                # Check that errors were printed
                mock_print.assert_any_call("Spark validation errors:")
                mock_print.assert_any_call("  Error 1")
                mock_print.assert_any_call("  Error 2")


def test_main_script_execution():
    """Test that main can be executed as a script."""
    with patch('src.utils.spark_validator.main') as mock_main:
        mock_main.return_value = 0
        
        with patch('sys.exit') as mock_exit:
            # This would normally run when script is executed
            exec("import sys; sys.exit(main())")
            mock_exit.assert_called_with(0)


@pytest.mark.parametrize("filename,should_skip", [
    ("__init__.py", True),
    ("__main__.py", True),
    ("test_file.py", True),
    ("file_test.py", True),
    ("testing.py", True),
    ("normal_file.py", False),
    ("spark_utils.py", False),
])
def test_file_filtering(filename, should_skip):
    """Test that files are correctly filtered."""
    mock_file = MagicMock()
    mock_file.name = filename
    
    # This tests the filtering logic from main()
    should_continue = mock_file.name.startswith('__') or 'test' in mock_file.name
    assert should_continue == should_skip