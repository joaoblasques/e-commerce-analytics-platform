import logging


def setup_logging(name: str = "ecommerce_analytics") -> logging.Logger:
    """
    Sets up a standardized logger.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Create console handler and set level to info
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    # Create formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Add formatter to ch
    ch.setFormatter(formatter)

    # Add ch to logger
    if not logger.handlers:
        logger.addHandler(ch)

    return logger


def get_logger(name: str = "ecommerce_analytics") -> logging.Logger:
    """
    Gets a standardized logger (alias for setup_logging).
    """
    return setup_logging(name)
