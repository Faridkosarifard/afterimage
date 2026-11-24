from __future__ import annotations

from pathlib import Path

import pytest
from pyiceberg.io import FileIO
from pyiceberg.io.pyarrow import PyArrowFileIO

from afterimage.storage import FileState, file_state


class _RaisingInput:
    def __init__(self, exc: Exception) -> None:
        self.exc = exc

    def exists(self) -> bool:
        raise self.exc


class _RaisingIO(PyArrowFileIO):
    def __init__(self, exc: Exception) -> None:
        super().__init__()
        self.exc = exc

    def new_input(self, location: str):  # type: ignore[no-untyped-def]
        return _RaisingInput(self.exc)


def test_existing_file(tmp_path: Path) -> None:
    path = tmp_path / "data.parquet"
    path.write_bytes(b"x")
    assert file_state(PyArrowFileIO(), f"file://{path}") is FileState.EXISTS


def test_missing_file(tmp_path: Path) -> None:
    assert file_state(PyArrowFileIO(), f"file://{tmp_path}/gone.parquet") is FileState.MISSING


def test_file_not_found_error_is_missing() -> None:
    io: FileIO = _RaisingIO(FileNotFoundError("no such key"))
    assert file_state(io, "s3://bucket/gone.parquet") is FileState.MISSING


@pytest.mark.parametrize(
    "exc",
    [
        OSError("AWS Error ACCESS_DENIED during HeadObject operation: 403 Forbidden"),
        PermissionError("permission denied"),
        TimeoutError("connection timed out"),
    ],
)
def test_storage_errors_are_unknown(exc: Exception) -> None:
    io: FileIO = _RaisingIO(exc)
    assert file_state(io, "s3://bucket/data.parquet") is FileState.UNKNOWN
