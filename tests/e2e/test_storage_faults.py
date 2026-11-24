"""Storage and file faults must never turn into a clean verdict."""

from __future__ import annotations

import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.expressions import EqualTo
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.table import Table

from afterimage import FindingKind, Verdict, scan_table
from afterimage.models import CheckState, Report
from afterimage.subject import Identifier, Subject

from ..conftest import SALT

pytestmark = pytest.mark.e2e

FORBIDDEN = OSError("AWS Error ACCESS_DENIED during HeadObject operation: 403 Forbidden")

# customer 2 is neither the minimum nor the maximum, so column bounds cannot reveal it
# and only the file contents and time travel prove the residue.
BOB = Subject([Identifier("customer_id", "2")], salt=SALT)


def _local(uri: str) -> Path:
    return Path(uri.removeprefix("file://").removeprefix("file:"))


def _data_files(table: Table) -> list[str]:
    return [task.file.file_path for task in table.scan().plan_files()]


def _deny(
    monkeypatch: pytest.MonkeyPatch, names: set[str], *, on_exists: bool = True, error: Exception = FORBIDDEN
) -> None:
    """Make ``open`` (and optionally ``exists``) fail for files whose name is in ``names``."""
    original = PyArrowFileIO.new_input

    def new_input(self: PyArrowFileIO, location: str) -> Any:
        inp = original(self, location)
        if location.rsplit("/", 1)[-1] in names:

            def fail(*_args: object, **_kwargs: object) -> Any:
                raise error

            if on_exists:
                inp.exists = fail  # type: ignore[method-assign]
            inp.open = fail  # type: ignore[method-assign]
        return inp

    monkeypatch.setattr(PyArrowFileIO, "new_input", new_input)


def _check(report: Report, name: str) -> tuple[CheckState, str]:
    check = next(c for c in report.checks if c.name == name)
    return check.state, check.detail


@pytest.fixture
def deleted(catalog: SqlCatalog, make_table: Callable[..., Table]) -> tuple[Table, str]:
    """An unpartitioned table after a copy-on-write delete of customer 2, plus the old data file."""
    table = make_table(partitioned=False)
    (old_file,) = _data_files(table)
    table.delete(EqualTo("customer_id", 2))
    return catalog.load_table("crm.customers"), old_file


def test_baseline_finds_the_historical_file(deleted: tuple[Table, str]) -> None:
    table, _ = deleted
    report = scan_table(table, BOB)
    assert report.verdict is Verdict.RESIDUES_FOUND
    assert {FindingKind.HISTORICAL_DATA_FILE, FindingKind.HISTORICAL_SNAPSHOT_ROWS} <= {f.kind for f in report.findings}


@pytest.mark.parametrize(
    "error",
    [FORBIDDEN, PermissionError("permission denied"), TimeoutError("connection timed out")],
    ids=["403", "permission", "timeout"],
)
def test_storage_error_on_exists_and_open_is_incomplete(
    deleted: tuple[Table, str], monkeypatch: pytest.MonkeyPatch, error: Exception
) -> None:
    # Regression: a 403 on the old data file used to be read as "already deleted" and the
    # verdict came out clean although the rows were still in storage.
    table, old_file = deleted
    _deny(monkeypatch, {old_file.rsplit("/", 1)[-1]}, error=error)
    report = scan_table(table, BOB)
    assert report.verdict is Verdict.INCOMPLETE
    state, detail = _check(report, "file_contents")
    assert state is CheckState.ERROR
    assert "1 file(s) could not be checked" in detail
    assert _check(report, "time_travel")[0] is CheckState.ERROR


def test_storage_error_on_open_only_is_incomplete(deleted: tuple[Table, str], monkeypatch: pytest.MonkeyPatch) -> None:
    table, old_file = deleted
    _deny(monkeypatch, {old_file.rsplit("/", 1)[-1]}, on_exists=False)
    report = scan_table(table, BOB)
    assert report.verdict is Verdict.INCOMPLETE
    assert _check(report, "file_contents")[0] is CheckState.ERROR


def test_truncated_parquet_is_incomplete(deleted: tuple[Table, str]) -> None:
    table, old_file = deleted
    path = _local(old_file)
    path.write_bytes(path.read_bytes()[:64])
    report = scan_table(table, BOB)
    assert report.verdict is Verdict.INCOMPLETE
    assert _check(report, "file_contents")[0] is CheckState.ERROR


def test_file_that_is_really_gone_is_reported_as_missing(deleted: tuple[Table, str]) -> None:
    table, old_file = deleted
    _local(old_file).unlink()
    report = scan_table(table, BOB)
    state, detail = _check(report, "file_contents")
    assert state is CheckState.RAN
    assert "1 referenced file(s) missing from storage" in detail
    assert report.verdict is Verdict.CLEAN


def test_unreadable_orphan_is_incomplete(deleted: tuple[Table, str], monkeypatch: pytest.MonkeyPatch) -> None:
    table, old_file = deleted
    orphan = _local(old_file).with_name("orphan-copy.parquet")
    shutil.copyfile(_local(old_file), orphan)
    _deny(monkeypatch, {orphan.name})
    report = scan_table(table, BOB)
    state, detail = _check(report, "orphan_files")
    assert state is CheckState.ERROR
    assert "could not be checked" in detail
    assert report.verdict is not Verdict.CLEAN


def test_unreadable_previous_metadata_is_incomplete(
    deleted: tuple[Table, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    table, _ = deleted
    previous = table.metadata.metadata_log[-1].metadata_file
    _deny(monkeypatch, {previous.rsplit("/", 1)[-1]})
    report = scan_table(table, BOB)
    assert _check(report, "metadata_log")[0] is CheckState.ERROR
    assert report.verdict is not Verdict.CLEAN
