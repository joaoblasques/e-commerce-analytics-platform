"""
Unit tests for logger utilities.
"""
import logging

from src.utils.logger import get_logger, setup_logging


def test_setup_logging_default():
    """Test setup_logging with default parameters."""
    logger = setup_logging()
    assert logger is not None
    assert isinstance(logger, logging.Logger)
    assert logger.name == "ecommerce_analytics"
    assert logger.level == logging.INFO


def test_setup_logging_custom_name():
    """Test setup_logging with custom name."""
    custom_name = "test_logger"
    logger = setup_logging(custom_name)
    assert logger is not None
    assert isinstance(logger, logging.Logger)
    assert logger.name == custom_name
    assert logger.level == logging.INFO


def test_get_logger_default():
    """Test get_logger with default parameters."""
    logger = get_logger()
    assert logger is not None
    assert isinstance(logger, logging.Logger)
    assert logger.name == "ecommerce_analytics"
    assert logger.level == logging.INFO


def test_get_logger_custom_name():
    """Test get_logger with custom name."""
    custom_name = "test_get_logger"
    logger = get_logger(custom_name)
    assert logger is not None
    assert isinstance(logger, logging.Logger)
    assert logger.name == custom_name
    assert logger.level == logging.INFO


def test_logger_has_handlers():
    """Test that logger has console handler configured."""
    logger = setup_logging("test_handlers")
    assert len(logger.handlers) >= 1

    # Check that we have a StreamHandler
    stream_handlers = [
        h for h in logger.handlers if isinstance(h, logging.StreamHandler)
    ]
    assert len(stream_handlers) >= 1

    # Check handler level
    handler = stream_handlers[0]
    assert handler.level == logging.INFO


def test_logger_formatter():
    """Test that logger has proper formatter."""
    logger = setup_logging("test_formatter")
    handler = logger.handlers[0]
    formatter = handler.formatter
    assert formatter is not None

    # Test formatter format
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="test message",
        args=(),
        exc_info=None,
    )
    formatted = formatter.format(record)
    assert "test" in formatted
    assert "INFO" in formatted
    assert "test message" in formatted


def test_logger_no_duplicate_handlers():
    """Test that calling setup_logging multiple times doesn't add duplicate handlers."""
    logger_name = "test_no_duplicates"

    # First call
    logger1 = setup_logging(logger_name)
    initial_handler_count = len(logger1.handlers)

    # Second call with same name
    logger2 = setup_logging(logger_name)
    final_handler_count = len(logger2.handlers)

    # Should be same logger instance and same handler count
    assert logger1 is logger2
    assert initial_handler_count == final_handler_count


def test_get_logger_alias_consistency():
    """Test that get_logger is consistent with setup_logging."""
    logger_name = "test_consistency"

    logger1 = setup_logging(logger_name)
    logger2 = get_logger(logger_name)

    # Should return the same logger instance
    assert logger1 is logger2
    assert logger1.name == logger2.name
    assert logger1.level == logger2.level
    assert len(logger1.handlers) == len(logger2.handlers)


def test_logger_logging_functionality():
    """Test that logger can actually log messages."""
    import io

    # Capture stdout
    captured_output = io.StringIO()

    # Create logger and redirect to captured output
    logger = setup_logging("test_logging")

    # Remove existing handlers and add our test handler
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    test_handler = logging.StreamHandler(captured_output)
    test_handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
    test_handler.setFormatter(formatter)
    logger.addHandler(test_handler)

    # Test logging
    test_message = "This is a test message"
    logger.info(test_message)

    # Check output
    output = captured_output.getvalue()
    assert "test_logging" in output
    assert "INFO" in output
    assert test_message in output
