"""Logging setup that redacts subject identifiers from every record."""

from __future__ import annotations

import logging
import sys

from afterimage.subject import Redactor


class RedactingFilter(logging.Filter):
    """Render the message, redact it, and freeze it so later formatting cannot re-leak args."""

    def __init__(self, redactor: Redactor) -> None:
        super().__init__()
        self.redactor = redactor

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = self.redactor(record.getMessage())
        record.args = None
        if record.exc_info and not record.exc_text:
            record.exc_text = logging.Formatter().formatException(record.exc_info)
        if record.exc_text:
            record.exc_text = self.redactor(record.exc_text)
        return True


def configure_logging(level: str | int, redactor: Redactor | None = None) -> logging.Handler:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    if redactor is not None:
        handler.addFilter(RedactingFilter(redactor))
    root = logging.getLogger("afterimage")
    for existing in list(root.handlers):
        root.removeHandler(existing)
    root.addHandler(handler)
    root.setLevel(level)
    root.propagate = False
    return handler
