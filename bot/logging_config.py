"""
Structured JSON logging configuration.

Dual output:
  - stdout: for `docker compose logs` (all levels)
  - logs/bot.log: rotating file, INFO+, 10MB max, keep 5 files
  - logs/bot.error.log: rotating file, WARNING+, 5MB max, keep 5 files

Uses python-json-logger for structured output with contextual fields.
"""

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from pythonjsonlogger import jsonlogger


LOG_DIR = Path("/app/logs")
LOG_FILE = LOG_DIR / "bot.log"
ERROR_LOG_FILE = LOG_DIR / "bot.error.log"


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    """Add standard fields to every log entry."""

    def add_fields(self, log_record, record, message_dict):
        super().add_fields(log_record, record, message_dict)
        log_record["timestamp"] = self.formatTime(record)
        log_record["level"] = record.levelname
        log_record["logger"] = record.name

        # Remove default fields that are redundant
        log_record.pop("message", None)
        log_record["msg"] = record.getMessage()


def setup_logging(log_level: str = "INFO"):
    """
    Configure logging for the entire application.
    Call once at startup before any other module initializes.
    """
    # Ensure log directory exists
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # Clear any existing handlers
    root_logger.handlers.clear()

    # JSON formatter
    json_formatter = CustomJsonFormatter(
        fmt="%(timestamp)s %(level)s %(logger)s %(msg)s"
    )

    # Simple formatter for stdout (more readable during development)
    simple_formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(name)-20s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Handler 1: stdout (simple format for docker compose logs readability)
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)
    stdout_handler.setFormatter(simple_formatter)
    root_logger.addHandler(stdout_handler)

    # Handler 2: Rotating file (JSON format, all levels)
    file_handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(json_formatter)
    root_logger.addHandler(file_handler)

    # Handler 3: Error-only rotating file (JSON format)
    error_handler = RotatingFileHandler(
        ERROR_LOG_FILE,
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=5,
        encoding="utf-8",
    )
    error_handler.setLevel(logging.WARNING)
    error_handler.setFormatter(json_formatter)
    root_logger.addHandler(error_handler)

    # Suppress noisy third-party loggers
    logging.getLogger("pyrogram").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("redis").setLevel(logging.WARNING)

    logging.getLogger(__name__).info(
        f"Logging initialized: level={log_level}, file={LOG_FILE}"
    )
