#!/usr/bin/env python3
"""
FastAPI application runner script.

This script provides a convenient way to run the FastAPI application
with proper configuration and logging setup.
"""

import logging
import sys
from pathlib import Path

import uvicorn

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.api.config import get_settings, validate_configuration
from src.utils.logger import setup_logger


def setup_logging():
    """Set up logging for the application."""
    settings = get_settings()

    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("api.log")
            if settings.environment != "development"
            else logging.NullHandler(),
        ],
    )

    # Set specific loggers
    logging.getLogger("uvicorn.access").setLevel(logging.INFO)
    logging.getLogger("uvicorn.error").setLevel(logging.INFO)
    logging.getLogger("sqlalchemy.engine").setLevel(
        logging.INFO if settings.debug else logging.WARNING
    )


def main():
    """Main entry point for the API server."""
    try:
        # Setup logging
        setup_logging()
        logger = logging.getLogger(__name__)

        # Validate configuration
        logger.info("Validating configuration...")
        validate_configuration()
        logger.info("Configuration validation passed")

        # Get settings
        settings = get_settings()

        # Log startup information
        logger.info(f"Starting E-Commerce Analytics Platform API")
        logger.info(f"Environment: {settings.environment}")
        logger.info(f"Debug mode: {settings.debug}")
        logger.info(f"Host: {settings.host}")
        logger.info(f"Port: {settings.port}")
        logger.info(f"Log level: {settings.log_level}")

        # Run the server
        uvicorn.run(
            "src.api.main:app",
            host=settings.host,
            port=settings.port,
            reload=settings.environment == "development",
            log_level=settings.log_level.lower(),
            access_log=True,
            server_header=False,  # Security: don't expose server info
            date_header=False,  # Security: don't expose server time
        )

    except Exception as e:
        logger.error(f"Failed to start API server: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
