"""Logging configuration for the proxy application with file rotation."""

import logging
import sys
from logging.handlers import TimedRotatingFileHandler

from proxy.settings import settings


def setup_logging() -> None:
    """
    Configure logging for the application with file rotation.

    Log files are rotated each day and only the last 7 days are kept.
    """
    # Use settings values
    level = settings.log_level
    format_string = settings.log_format
    log_file = settings.log_file

    # Convert string level to logging level
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    # Create formatter
    formatter = logging.Formatter(format_string)

    # Setup root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)

    # Remove existing handlers to avoid duplicates
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # File handler with rotation (if specified)
    if log_file:
        # TimedRotatingFileHandler rotates logs every midnight and keeps (7 days)
        file_handler = TimedRotatingFileHandler(
            log_file,
            when="midnight",
            interval=1,
            backupCount=7,
            encoding="utf-8",
            delay=True,  # Only open the file when the first log message is emitted
            utc=True,  # Rotate in UTC time (optional, set to False for local time)
        )
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    # Set specific loggers to reduce noise from dependencies
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
