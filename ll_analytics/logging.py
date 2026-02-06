"""Logging configuration for LL Analytics."""

import logging
import sys


def setup_logging(level: str = "INFO", log_file: str | None = None) -> None:
    """
    Configure logging for the application.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional path to a log file
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]

    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
        force=True,
    )


def get_logger(name: str) -> logging.Logger:
    """Get a logger for a module. Pass __name__ as the argument."""
    return logging.getLogger(name)
