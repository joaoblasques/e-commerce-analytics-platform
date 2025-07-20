"""
Comprehensive unit tests for CLI module.
"""
import click
import pytest
from click.testing import CliRunner

from src import __version__
from src.cli import main, start, test, version


def test_main_group():
    """Test that main is a click group."""
    assert isinstance(main, click.Group)
    assert main.name is None or main.name == "main"


def test_version_command_exists():
    """Test that version command is registered."""
    commands = main.list_commands(None)
    assert "version" in commands


def test_start_command_exists():
    """Test that start command is registered."""
    commands = main.list_commands(None)
    assert "start" in commands


def test_test_command_exists():
    """Test that test command is registered."""
    commands = main.list_commands(None)
    assert "test" in commands


def test_version_command():
    """Test version command output."""
    runner = CliRunner()
    result = runner.invoke(version)
    assert result.exit_code == 0
    assert __version__ in result.output
    assert "E-Commerce Analytics Platform" in result.output


def test_start_command_default():
    """Test start command with default config."""
    runner = CliRunner()
    result = runner.invoke(start)
    assert result.exit_code == 0
    assert "Starting E-Commerce Analytics Platform" in result.output
    assert "config/local.yaml" in result.output


def test_start_command_custom_config():
    """Test start command with custom config."""
    runner = CliRunner()
    custom_config = "custom_config.yaml"
    result = runner.invoke(start, ["--config", custom_config])
    assert result.exit_code == 0
    assert "Starting E-Commerce Analytics Platform" in result.output
    assert custom_config in result.output


def test_start_command_config_shorthand():
    """Test start command with config shorthand."""
    runner = CliRunner()
    custom_config = "shorthand_config.yaml"
    result = runner.invoke(start, ["-c", custom_config])
    assert result.exit_code == 0
    assert "Starting E-Commerce Analytics Platform" in result.output
    assert custom_config in result.output


def test_test_command():
    """Test test command output."""
    runner = CliRunner()
    result = runner.invoke(test)
    assert result.exit_code == 0
    assert "Running tests..." in result.output


def test_main_help():
    """Test main command help."""
    runner = CliRunner()
    result = runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "E-Commerce Analytics Platform CLI" in result.output
    assert "Commands:" in result.output


def test_main_version_option():
    """Test main command version option."""
    runner = CliRunner()
    result = runner.invoke(main, ["--version"])
    assert result.exit_code == 0
    assert __version__ in result.output


def test_start_command_help():
    """Test start command help."""
    runner = CliRunner()
    result = runner.invoke(start, ["--help"])
    assert result.exit_code == 0
    assert "Start the analytics platform" in result.output
    assert "--config" in result.output


def test_version_command_help():
    """Test version command help."""
    runner = CliRunner()
    result = runner.invoke(version, ["--help"])
    assert result.exit_code == 0
    assert "Show version information" in result.output


def test_test_command_help():
    """Test test command help."""
    runner = CliRunner()
    result = runner.invoke(test, ["--help"])
    assert result.exit_code == 0
    assert "Run tests" in result.output


def test_invalid_command():
    """Test invalid command handling."""
    runner = CliRunner()
    result = runner.invoke(main, ["invalid_command"])
    assert result.exit_code != 0
    assert "No such command" in result.output


def test_command_functions_callable():
    """Test that command functions are callable."""
    assert callable(main)
    assert callable(version)
    assert callable(start)
    assert callable(test)


def test_version_import():
    """Test that version import works correctly."""
    from src import __version__ as imported_version

    runner = CliRunner()
    result = runner.invoke(version)
    assert imported_version in result.output


def test_all_commands_registered():
    """Test that all expected commands are registered."""
    expected_commands = ["version", "start", "test"]
    actual_commands = main.list_commands(None)

    for cmd in expected_commands:
        assert cmd in actual_commands
