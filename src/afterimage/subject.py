"""Data-subject identifiers, salted hashing and redaction.

Raw identifier values are sensitive: they are the very data an erasure request is
about. They live only in memory inside :class:`Subject`. Everything that leaves
the process (logs, reports, plans) goes through :class:`Redactor` or carries a
keyed hash produced by :meth:`Subject.salted_hash`.
"""

from __future__ import annotations

import hashlib
import hmac
import re
import secrets
import urllib.parse
import uuid
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pyiceberg.types import (
    IcebergType,
    IntegerType,
    LongType,
    StringType,
    UUIDType,
)

HASH_ALGORITHM = "hmac-sha256"
#: Values shorter than this are only redacted when they appear as a whole
#: ``key=value`` path segment, otherwise a customer id like ``7`` would mangle
#: every path and snapshot id in a report.
MIN_FREE_TEXT_REDACTION_LENGTH = 4


class IdentifierError(ValueError):
    """Raised when an identifier cannot be parsed or applied to a table."""


@dataclass(frozen=True)
class Identifier:
    """One identifying value of a data subject, bound to a column name.

    ``column`` may be a dotted path for nested fields (``customer.email``).
    ``raw`` is the value exactly as supplied by the operator, before it is
    coerced to the column type.
    """

    column: str
    raw: str = field(repr=False)

    def __post_init__(self) -> None:
        if not self.column.strip():
            raise IdentifierError("identifier column must not be empty")
        if self.raw == "":
            raise IdentifierError(f"identifier value for column {self.column!r} must not be empty")

    def __str__(self) -> str:
        return f"{self.column}=<redacted>"

    @classmethod
    def parse(cls, spec: str) -> Identifier:
        """Parse ``column=value``. Only the first ``=`` separates column and value."""
        column, sep, raw = spec.partition("=")
        if not sep:
            raise IdentifierError("identifier must look like column=value (value redacted)")
        return cls(column=column.strip(), raw=raw)


def read_identifier_file(path: Path) -> list[Identifier]:
    """Read ``column=value`` lines from a file. Blank lines and ``#`` comments are ignored.

    Using a file (or stdin) keeps raw values out of shell history and process lists.
    """
    identifiers: list[Identifier] = []
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        try:
            identifiers.append(Identifier.parse(stripped))
        except IdentifierError as exc:
            raise IdentifierError(f"{path}:{lineno}: {exc}") from None
    return identifiers


class Subject:
    """A data subject described by one or more identifiers plus a hashing salt.

    The salt keys an HMAC so that the report can prove *which* subject it covers
    to someone who knows the salt and the value, without letting anyone else
    brute-force low-entropy identifiers such as emails or numeric ids.
    """

    def __init__(self, identifiers: Sequence[Identifier], salt: str | bytes | None = None) -> None:
        if not identifiers:
            raise IdentifierError("at least one identifier is required")
        self.identifiers: tuple[Identifier, ...] = tuple(identifiers)
        self.salt_was_generated = salt is None
        if salt is None:
            salt = secrets.token_bytes(32)
        self._salt = salt.encode("utf-8") if isinstance(salt, str) else bytes(salt)
        if len(self._salt) < 16:
            raise IdentifierError("salt must be at least 16 bytes long")

    def __repr__(self) -> str:
        cols = ", ".join(i.column for i in self.identifiers)
        return f"Subject(columns=[{cols}], salt_fingerprint={self.salt_fingerprint})"

    @property
    def columns(self) -> list[str]:
        return [i.column for i in self.identifiers]

    @property
    def salt_fingerprint(self) -> str:
        """A short, non-reversible fingerprint that identifies which salt was used."""
        return hashlib.sha256(b"afterimage-salt-fingerprint:" + self._salt).hexdigest()[:16]

    def salted_hash(self, identifier: Identifier) -> str:
        message = f"{identifier.column}\x00{identifier.raw}".encode()
        return hmac.new(self._salt, message, hashlib.sha256).hexdigest()

    def redactor(self, extra_values: Iterable[Any] = ()) -> Redactor:
        values: list[tuple[str, str]] = []
        for ident in self.identifiers:
            token = f"[redacted:{self.salted_hash(ident)[:12]}]"
            values.append((ident.raw, token))
        for value in extra_values:
            values.append((str(value), "[redacted]"))
        return Redactor(values)


class Redactor:
    """Replace raw identifier values (and their URL-encoded forms) in free text."""

    def __init__(self, replacements: Iterable[tuple[str, str]]) -> None:
        patterns: list[tuple[re.Pattern[str], str]] = []
        seen: set[str] = set()
        for raw, token in sorted(replacements, key=lambda item: -len(item[0])):
            for form in _value_forms(raw):
                if form in seen or not form:
                    continue
                seen.add(form)
                escaped = re.escape(form)
                if len(form) >= MIN_FREE_TEXT_REDACTION_LENGTH:
                    pattern = re.compile(escaped, re.IGNORECASE)
                    patterns.append((pattern, token))
                else:
                    # Short values: only whole `key=value` segments (Hive-style partition paths).
                    pattern = re.compile(rf"(?<==){escaped}(?=$|[/&,;\s\"'\]\)])")
                    patterns.append((pattern, token))
        self._patterns = patterns

    def __call__(self, text: str) -> str:
        return self.redact(text)

    def redact(self, text: str) -> str:
        for pattern, token in self._patterns:
            text = pattern.sub(token, text)
        return text


def _value_forms(raw: str) -> list[str]:
    forms = [raw, urllib.parse.quote(raw, safe=""), urllib.parse.quote_plus(raw, safe="")]
    return list(dict.fromkeys(forms))


# ---------------------------------------------------------------------------
# Type coercion
# ---------------------------------------------------------------------------

SUPPORTED_TYPES = (StringType, IntegerType, LongType, UUIDType)


def coerce_value(raw: str, iceberg_type: IcebergType) -> Any:
    """Convert an operator-supplied string to the Python value PyIceberg uses for expressions."""
    if isinstance(iceberg_type, StringType):
        return raw
    if isinstance(iceberg_type, IntegerType | LongType):
        try:
            return int(raw.strip())
        except ValueError:
            raise IdentifierError(f"value for an {iceberg_type} column is not an integer") from None
    if isinstance(iceberg_type, UUIDType):
        try:
            return uuid.UUID(raw.strip())
        except ValueError:
            raise IdentifierError("value for a uuid column is not a valid UUID") from None
    raise IdentifierError(
        f"identifier columns of type {iceberg_type} are not supported yet (supported: string, int, long, uuid)"
    )


def bound_form(value: Any) -> Any:
    """Return the value in the representation produced by ``pyiceberg.conversions.from_bytes``."""
    if isinstance(value, uuid.UUID):
        return value.bytes
    return value
