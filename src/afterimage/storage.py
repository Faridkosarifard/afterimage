"""Storage helpers built on PyIceberg's FileIO (local filesystem, S3, MinIO, ...)."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum

from pyarrow.fs import FileSelector, FileType
from pyiceberg.io import FileIO
from pyiceberg.io.pyarrow import PyArrowFileIO

logger = logging.getLogger(__name__)

S3_SCHEMES = {"s3", "s3a", "s3n"}


def normalize_uri(uri: str) -> str:
    """Normalise a location so that ``file:/x``, ``file:///x`` and ``/x`` compare equal."""
    scheme, _netloc, path = PyArrowFileIO.parse_location(uri)
    if scheme in S3_SCHEMES:
        return f"s3://{path}"
    if scheme == "file":
        return f"file://{path}"
    return f"{scheme}://{path}"


class FileState(Enum):
    """Whether a file is in storage. ``UNKNOWN`` means storage could not answer."""

    EXISTS = "exists"
    MISSING = "missing"
    UNKNOWN = "unknown"


def file_state(io: FileIO, uri: str) -> FileState:
    """Probe ``uri`` without ever mistaking a storage error for a deleted file.

    Only a definite "not found" (``exists()`` returning ``False``, which is how
    FileIO reports a 404, or :class:`FileNotFoundError`) counts as missing. Any
    other error, such as a 403 or a network failure, is ``UNKNOWN`` so callers can
    fail closed instead of reporting a file as erased.
    """
    try:
        return FileState.EXISTS if io.new_input(uri).exists() else FileState.MISSING
    except FileNotFoundError:
        return FileState.MISSING
    except Exception:
        logger.debug("could not stat %s", uri, exc_info=True)
        return FileState.UNKNOWN


def file_exists(io: FileIO, uri: str) -> bool:
    try:
        return io.new_input(uri).exists()
    except Exception:  # permission errors etc. mean "cannot prove it exists"
        logger.debug("could not stat %s", uri, exc_info=True)
        return False


def list_files(io: FileIO, location: str) -> list[tuple[str, int]] | None:
    """Recursively list ``(uri, size)`` for every file under ``location``.

    Returns ``None`` when the FileIO implementation does not support listing, so the
    caller can mark the orphan check as skipped instead of silently passing it.
    """
    if not isinstance(io, PyArrowFileIO):
        return None
    scheme, netloc, path = PyArrowFileIO.parse_location(location)
    fs = io.fs_by_scheme(scheme, netloc)
    selector = FileSelector(path.rstrip("/"), recursive=True, allow_not_found=True)
    uris: list[tuple[str, int]] = []
    for info in fs.get_file_info(selector):
        if info.type != FileType.File:
            continue
        if scheme in S3_SCHEMES:
            uri = f"s3://{info.path}"
        elif scheme == "file":
            uri = f"file://{info.path}"
        else:
            uri = f"{scheme}://{info.path}"
        uris.append((uri, int(info.size or 0)))
    return uris


@dataclass(frozen=True)
class S3Location:
    bucket: str
    prefix: str

    @classmethod
    def parse(cls, uri: str) -> S3Location | None:
        scheme, netloc, path = PyArrowFileIO.parse_location(uri)
        if scheme not in S3_SCHEMES or not netloc:
            return None
        key = path[len(netloc) :].lstrip("/") if path.startswith(netloc) else path.lstrip("/")
        return cls(bucket=netloc, prefix=key)

    def uri(self, key: str | None = None) -> str:
        return f"s3://{self.bucket}/{key if key is not None else self.prefix}"
