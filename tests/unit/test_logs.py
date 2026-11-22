from __future__ import annotations

import logging

import pytest

from afterimage.logs import configure_logging
from afterimage.subject import Identifier, Subject


def test_logs_never_contain_raw_identifiers(capsys: pytest.CaptureFixture[str]) -> None:
    subject = Subject([Identifier("email", "secret.person@example.com")], salt="l" * 32)
    configure_logging("DEBUG", subject.redactor())
    log = logging.getLogger("afterimage.test")
    log.info("reading %s", "s3://b/data/email=secret.person%40example.com/f.parquet")
    try:
        raise RuntimeError("boom secret.person@example.com")
    except RuntimeError:
        log.exception("failed")
    err = capsys.readouterr().err
    assert "secret.person" not in err
    assert "[redacted:" in err
    assert "RuntimeError" in err


def test_configure_logging_without_redactor_replaces_handlers() -> None:
    first = configure_logging("INFO")
    second = configure_logging("WARNING")
    root = logging.getLogger("afterimage")
    assert root.handlers == [second]
    assert first is not second
