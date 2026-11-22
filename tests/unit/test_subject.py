from __future__ import annotations

import uuid
from pathlib import Path

import pytest
from pyiceberg.types import BooleanType, IntegerType, LongType, StringType, UUIDType

from afterimage.subject import (
    Identifier,
    IdentifierError,
    Subject,
    bound_form,
    coerce_value,
    read_identifier_file,
)

SALT = "s" * 32


def test_parse_splits_on_first_equals_sign() -> None:
    ident = Identifier.parse("note=a=b")
    assert ident.column == "note"
    assert ident.raw == "a=b"


@pytest.mark.parametrize("spec", ["no-separator", "=value", "column="])
def test_parse_rejects_malformed(spec: str) -> None:
    with pytest.raises(IdentifierError):
        Identifier.parse(spec)


def test_identifier_repr_and_str_never_show_the_value() -> None:
    ident = Identifier("email", "secret@example.com")
    assert "secret" not in repr(ident)
    assert "secret" not in str(ident)


def test_subject_repr_never_shows_values() -> None:
    subject = Subject([Identifier("email", "secret@example.com")], salt=SALT)
    assert "secret" not in repr(subject)


def test_salted_hash_is_deterministic_and_keyed() -> None:
    ident = Identifier("email", "a@example.com")
    first = Subject([ident], salt=SALT).salted_hash(ident)
    assert first == Subject([ident], salt=SALT).salted_hash(ident)
    assert first != Subject([ident], salt="t" * 32).salted_hash(ident)
    assert len(first) == 64


def test_hash_binds_the_column_name() -> None:
    subject = Subject([Identifier("a", "1"), Identifier("b", "1")], salt=SALT)
    assert subject.salted_hash(Identifier("a", "1")) != subject.salted_hash(Identifier("b", "1"))


def test_generated_salt_is_flagged() -> None:
    subject = Subject([Identifier("email", "a@example.com")])
    assert subject.salt_was_generated
    assert len(subject.salt_fingerprint) == 16


def test_short_salt_rejected() -> None:
    with pytest.raises(IdentifierError):
        Subject([Identifier("email", "a@example.com")], salt="short")


def test_subject_requires_identifiers() -> None:
    with pytest.raises(IdentifierError):
        Subject([], salt=SALT)


def test_redactor_replaces_plain_and_url_encoded_forms() -> None:
    subject = Subject([Identifier("email", "Alice+1@example.com")], salt=SALT)
    redact = subject.redactor()
    text = "s3://b/t/data/email=Alice%2B1%40example.com/f.parquet and ALICE+1@EXAMPLE.COM"
    out = redact(text)
    assert "example.com" not in out.lower()
    assert out.count("[redacted:") == 2


def test_redactor_only_redacts_short_values_in_key_value_segments() -> None:
    redact = Subject([Identifier("customer_id", "7")], salt=SALT).redactor()
    out = redact("file:///wh/data/customer_id=7/00000-7-abc.parquet snapshot 17")
    assert "customer_id=7/" not in out
    assert "00000-7-abc" in out
    assert "snapshot 17" in out


def test_redactor_extra_values() -> None:
    redact = Subject([Identifier("email", "a@example.com")], salt=SALT).redactor(extra_values=["other-value"])
    assert "other-value" not in redact("x other-value y")


def test_read_identifier_file(tmp_path: Path) -> None:
    path = tmp_path / "subject.txt"
    path.write_text("# comment\n\nemail=a@example.com\ncustomer_id=42\n", encoding="utf-8")
    idents = read_identifier_file(path)
    assert [i.column for i in idents] == ["email", "customer_id"]


def test_read_identifier_file_reports_line_without_value(tmp_path: Path) -> None:
    path = tmp_path / "subject.txt"
    path.write_text("email=a@example.com\nbroken-line\n", encoding="utf-8")
    with pytest.raises(IdentifierError, match=r"subject.txt:2"):
        read_identifier_file(path)


def test_coerce_value() -> None:
    assert coerce_value("abc", StringType()) == "abc"
    assert coerce_value(" 42 ", LongType()) == 42
    assert coerce_value("7", IntegerType()) == 7
    value = uuid.uuid4()
    assert coerce_value(str(value), UUIDType()) == value
    assert bound_form(value) == value.bytes
    assert bound_form(5) == 5


@pytest.mark.parametrize(
    ("raw", "iceberg_type"),
    [("abc", LongType()), ("not-a-uuid", UUIDType()), ("true", BooleanType())],
)
def test_coerce_value_errors(raw: str, iceberg_type: object) -> None:
    with pytest.raises(IdentifierError):
        coerce_value(raw, iceberg_type)  # type: ignore[arg-type]
