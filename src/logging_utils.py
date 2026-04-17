"""Shared logging setup for API and worker processes."""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone

_RESERVED_LOG_RECORD_FIELDS = set(logging.makeLogRecord({}).__dict__.keys())


class JsonFormatter(logging.Formatter):
    """Minimal JSON formatter for structured log shipping."""

    def __init__(self, service_name: str):
        super().__init__()
        self._default_service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        service = getattr(record, "service", None)
        if not service and record.name:
            service = record.name.split(".", 1)[0]
        if not service:
            service = self._default_service_name

        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "service": service,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

        for key, value in record.__dict__.items():
            if key in _RESERVED_LOG_RECORD_FIELDS:
                continue
            payload[key] = value

        return json.dumps(payload, default=str)


def configure_logging(service_name: str) -> logging.Logger:
    """Configure root logging once and return the service logger."""
    root_logger = logging.getLogger()
    if getattr(root_logger, "_aic_logging_configured", False):
        return logging.getLogger(service_name)

    level = os.getenv("LOG_LEVEL", "INFO").strip().upper()
    log_format = os.getenv("LOG_FORMAT", "json").strip().lower()

    handler = logging.StreamHandler(sys.stdout)
    if log_format == "json":
        handler.setFormatter(JsonFormatter(service_name))
    else:
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        )

    root_logger.handlers.clear()
    root_logger.setLevel(level)
    root_logger.addHandler(handler)
    root_logger._aic_logging_configured = True
    return logging.getLogger(service_name)
