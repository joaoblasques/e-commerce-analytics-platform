"""
Unit tests for the CLI module.
"""
import pytest
from click.testing import CliRunner
from unittest.mock import patch, MagicMock

from src.cli import main, version, start, test


@pytest.fixture
def runner():
    """Create a Click test runner."""
    return CliRunner()


def test_main_help(runner):
    """Test main command help."""
    result = runner.invoke(main, ['--help'])
    assert result.exit_code == 0
    assert "E-Commerce Analytics Platform CLI" in result.output
    assert "Commands:" in result.output


def test_version_option(runner):
    """Test version option."""
    result = runner.invoke(main, ['--version'])
    assert result.exit_code == 0
    assert "version" in result.output.lower()


def test_version_command(runner):
    """Test version command."""
    result = runner.invoke(main, ['version'])
    assert result.exit_code == 0
    assert "E-Commerce Analytics Platform v" in result.output


def test_start_command_default(runner):
    """Test start command with default config."""
    result = runner.invoke(main, ['start'])
    assert result.exit_code == 0
    assert "Starting E-Commerce Analytics Platform with config: config/local.yaml" in result.output


def test_start_command_custom_config(runner):
    """Test start command with custom config."""
    result = runner.invoke(main, ['start', '--config', 'custom.yaml'])
    assert result.exit_code == 0
    assert "Starting E-Commerce Analytics Platform with config: custom.yaml" in result.output


def test_start_command_short_option(runner):
    """Test start command with short config option."""
    result = runner.invoke(main, ['start', '-c', 'short.yaml'])
    assert result.exit_code == 0
    assert "Starting E-Commerce Analytics Platform with config: short.yaml" in result.output


def test_test_command(runner):
    """Test test command."""
    result = runner.invoke(main, ['test'])
    assert result.exit_code == 0
    assert "Running tests..." in result.output


def test_invalid_command(runner):
    """Test invalid command."""
    result = runner.invoke(main, ['invalid'])
    assert result.exit_code == 2
    assert "No such command 'invalid'" in result.output


def test_main_context_object(runner):
    """Test that main sets up context object."""
    with runner.isolated_filesystem():
        result = runner.invoke(main, ['version'])
        assert result.exit_code == 0


@pytest.mark.parametrize("command", ["version", "start", "test"])
def test_all_commands_exist(runner, command):
    """Test that all expected commands exist."""
    result = runner.invoke(main, [command, '--help'])
    assert result.exit_code == 0
    assert "Usage:" in result.output


def test_start_command_help(runner):
    """Test start command help."""
    result = runner.invoke(main, ['start', '--help'])
    assert result.exit_code == 0
    assert "Start the analytics platform" in result.output
    assert "--config" in result.output
    assert "Configuration file path" in result.output


def test_test_command_help(runner):
    """Test test command help."""
    result = runner.invoke(main, ['test', '--help'])
    assert result.exit_code == 0
    assert "Run tests" in result.output