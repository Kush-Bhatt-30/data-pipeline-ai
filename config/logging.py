from __future__ import annotations

import json
import logging
import os
from typing import Optional

_CONFIGURED = False


def configure_logging(level: Optional[str] = None) -> None:
    """
    Central logging setup used across all modules.
    """
    global _CONFIGURED
    if _CONFIGURED:
        return

    resolved = (level or os.getenv("LOG_LEVEL") or "INFO").upper()

    class JSONFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            log_record = {
                "timestamp": self.formatTime(record, self.datefmt),
                "level": record.levelname,
                "name": record.name,
                "message": record.getMessage(),
            }
            if record.exc_info:
                log_record["exception"] = self.formatException(record.exc_info)
            return json.dumps(log_record)

    # Set up root logger with JSON handler
    root_logger = logging.getLogger()
    root_logger.setLevel(resolved)

    # Remove existing handlers to avoid duplicates during testing/reloads
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(JSONFormatter())
    root_logger.addHandler(console_handler)

    # Quiet noisy libs a bit.
    logging.getLogger("boto3").setLevel(max(logging.WARNING, logging.getLogger().level))
    logging.getLogger("botocore").setLevel(max(logging.WARNING, logging.getLogger().level))
    logging.getLogger("urllib3").setLevel(max(logging.WARNING, logging.getLogger().level))
    _CONFIGURED = True
