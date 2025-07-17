"""
Example unit test to verify testing framework setup.
"""
import pytest
from src import __version__


def test_version():
    """Test that version is accessible."""
    assert __version__ == "0.1.0"


def test_example_calculation():
    """Example test for basic calculation."""
    result = 2 + 2
    assert result == 4


class TestExampleClass:
    """Example test class."""
    
    def test_string_operations(self):
        """Test string operations."""
        text = "Hello, World!"
        assert text.upper() == "HELLO, WORLD!"
        assert text.lower() == "hello, world!"
    
    def test_list_operations(self):
        """Test list operations."""
        numbers = [1, 2, 3, 4, 5]
        assert len(numbers) == 5
        assert sum(numbers) == 15
        assert max(numbers) == 5
        assert min(numbers) == 1


@pytest.mark.parametrize("input_value,expected", [
    (1, 2),
    (2, 3),
    (3, 4),
    (0, 1),
    (-1, 0),
])
def test_increment_function(input_value, expected):
    """Test increment function with parametrized inputs."""
    def increment(x):
        return x + 1
    
    assert increment(input_value) == expected