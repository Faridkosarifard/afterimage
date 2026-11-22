"""End-to-end: a local Iceberg table (sqlite catalog, filesystem warehouse) from insert to clean."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.expressions import EqualTo
from pyiceberg.table import Table
from pyiceberg.typedef import Record

from afterimage import FindingKind, Scanner, ScanOptions, Verdict, scan_table
from afterimage.models import CheckState, Confidence
from afterimage.render import to_json, to_markdown
from afterimage.subject import Identifier, Subject

from ..conftest import ALICE_EMAIL, customers
from ..helpers import commit_delete_files, write_equality_deletes, write_position_deletes

pytestmark = pytest.mark.e2e

CLEANUP_PROPS = {
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "1",
}


def remediate(catalog: SqlCatalog, name: str, subject: Subject) -> Table:
    """Expire history and delete orphans, the way a maintenance job would."""
    table = catalog.load_table(name)
    # One more commit so the manifest holding DELETED entries is no longer current.
    table.append(customers([(4, "dave@example.com", "AT")]))
    table = catalog.load_table(name)
    current = table.metadata.current_snapshot_id
    table.maintenance.expire_snapshots().by_ids(
        [s.snapshot_id for s in table.metadata.snapshots if s.snapshot_id != current]
    ).commit()
    # Rotate the metadata written before the expiry out of the metadata log.
    with catalog.load_table(name).transaction() as tx:
        tx.set_properties({"maintenance.last-run": "erasure"})
    table = catalog.load_table(name)
    scanner = Scanner(table, subject)
    scanner.run()
    for uri in scanner.unreferenced_files:  # stands in for remove_orphan_files
        Path(uri.removeprefix("file://")).unlink()
    return catalog.load_table(name)


def kinds(report) -> set[FindingKind]:  # type: ignore[no-untyped-def]
    return {f.kind for f in report.findings}


def test_full_lifecycle_from_live_rows_to_clean(
    catalog: SqlCatalog, make_table: Callable[..., Table], alice: Subject
) -> None:
    table = make_table(properties=CLEANUP_PROPS)

    before = scan_table(table, alice)
    assert before.verdict is Verdict.RESIDUES_FOUND
    assert FindingKind.LIVE_ROWS in kinds(before)

    table.delete(EqualTo("customer_id", 1))
    table = catalog.load_table("crm.customers")
    assert table.scan(row_filter=EqualTo("customer_id", 1)).to_arrow().num_rows == 0

    after_delete = scan_table(table, alice)
    found = kinds(after_delete)
    assert FindingKind.LIVE_ROWS not in found
    assert {
        FindingKind.HISTORICAL_SNAPSHOT_ROWS,
        FindingKind.HISTORICAL_DATA_FILE,
        FindingKind.COLUMN_BOUND,
        FindingKind.PARTITION_VALUE,
        FindingKind.PARTITION_SUMMARY,
    } <= found
    bounds = after_delete.findings_of(FindingKind.COLUMN_BOUND)
    assert any(f.confidence is Confidence.POSSIBLE and f.column == "email" for f in bounds)  # truncate(16)
    assert any(f.confidence is Confidence.CONFIRMED and f.column == "customer_id" for f in bounds)
    current = after_delete.table.current_snapshot_id
    summaries = after_delete.findings_of(FindingKind.PARTITION_SUMMARY)
    assert any(current in f.snapshot_ids for f in summaries), "the DELETE snapshot keeps the summary"

    serialized = to_json(after_delete) + to_markdown(after_delete)
    assert ALICE_EMAIL not in serialized
    assert "customer_id=1/" not in serialized
    assert json.loads(to_json(after_delete))["subject"]["identifiers"][0]["salted_hash"]

    table = remediate(catalog, "crm.customers", alice)
    clean = scan_table(table, alice)
    assert clean.findings == [], [f.kind for f in clean.findings]
    assert clean.verdict is Verdict.CLEAN
    assert {c.state for c in clean.checks} <= {CheckState.RAN, CheckState.NOT_APPLICABLE}


def test_expired_snapshots_without_file_cleanup_leave_orphans(
    catalog: SqlCatalog, make_table: Callable[..., Table], alice: Subject
) -> None:
    table = make_table(properties=CLEANUP_PROPS)
    table.delete(EqualTo("customer_id", 1))
    table = catalog.load_table("crm.customers")
    table.append(customers([(4, "dave@example.com", "AT")]))
    table = catalog.load_table("crm.customers")
    current = table.metadata.current_snapshot_id
    table.maintenance.expire_snapshots().by_ids(
        [s.snapshot_id for s in table.metadata.snapshots if s.snapshot_id != current]
    ).commit()
    # Right after expiry the previous metadata.json still lists the expired snapshots.
    assert kinds(scan_table(catalog.load_table("crm.customers"), alice)) == {FindingKind.METADATA_LOG_SNAPSHOT}
    with catalog.load_table("crm.customers").transaction() as tx:
        tx.set_properties({"maintenance.last-run": "expire"})
    report = scan_table(catalog.load_table("crm.customers"), alice)
    found = kinds(report)
    assert FindingKind.ORPHAN_DATA_FILE in found
    assert FindingKind.ORPHAN_METADATA_FILE in found
    assert FindingKind.HISTORICAL_SNAPSHOT_ROWS not in found


def test_metadata_log_keeps_expired_snapshots_reachable(
    catalog: SqlCatalog, make_table: Callable[..., Table], alice: Subject
) -> None:
    table = make_table()  # default: previous metadata files are kept
    table.delete(EqualTo("customer_id", 1))
    table = catalog.load_table("crm.customers")
    table.append(customers([(4, "dave@example.com", "AT")]))
    table = catalog.load_table("crm.customers")
    current = table.metadata.current_snapshot_id
    table.maintenance.expire_snapshots().by_ids(
        [s.snapshot_id for s in table.metadata.snapshots if s.snapshot_id != current]
    ).commit()
    report = scan_table(catalog.load_table("crm.customers"), alice)
    log_findings = report.findings_of(FindingKind.METADATA_LOG_SNAPSHOT)
    assert log_findings
    assert all(f.confidence is Confidence.CONFIRMED for f in log_findings)
    assert "not encrypted" in log_findings[0].detail
    # Files reachable through old metadata are not double-reported as orphans.
    assert FindingKind.ORPHAN_DATA_FILE not in kinds(report)


def test_tag_pins_old_snapshot(catalog: SqlCatalog, make_table: Callable[..., Table], alice: Subject) -> None:
    table = make_table(properties=CLEANUP_PROPS)
    first = table.metadata.current_snapshot_id
    assert first is not None
    table.manage_snapshots().create_tag(first, "audit-2026").commit()
    table = catalog.load_table("crm.customers")
    table.delete(EqualTo("customer_id", 1))
    report = scan_table(catalog.load_table("crm.customers"), alice)
    history = report.findings_of(FindingKind.HISTORICAL_SNAPSHOT_ROWS)
    assert any("audit-2026" in f.detail for f in history)


def test_merge_on_read_position_delete_leaves_unrewritten_file(
    catalog: SqlCatalog, make_table: Callable[..., Table], alice: Subject
) -> None:
    table = make_table(partitioned=False, properties={"format-version": "2"})
    data_path = table.inspect.data_files()["file_path"][0].as_py()
    commit_delete_files(table, [write_position_deletes(table, data_path, [0], Record())])
    table = catalog.load_table("crm.customers")
    assert table.scan(row_filter=EqualTo("customer_id", 1)).to_arrow().num_rows == 0

    report = scan_table(table, alice)
    found = kinds(report)
    assert FindingKind.UNREWRITTEN_DATA_FILE in found
    assert FindingKind.LIVE_ROWS not in found
    unrewritten = report.findings_of(FindingKind.UNREWRITTEN_DATA_FILE)[0]
    assert unrewritten.match_count == 1
    assert "merge-on-read" in unrewritten.detail


def test_equality_delete_file_stores_the_identifier(
    catalog: SqlCatalog, make_table: Callable[..., Table], alice: Subject
) -> None:
    table = make_table(partitioned=False, properties={"format-version": "2"})
    commit_delete_files(table, [write_equality_deletes(table, "email", [ALICE_EMAIL], Record())])
    report = scan_table(catalog.load_table("crm.customers"), alice)
    found = kinds(report)
    assert FindingKind.EQUALITY_DELETE_FILE in found
    time_travel = next(c for c in report.checks if c.name == "time_travel")
    assert time_travel.state is CheckState.RAN
    assert "equality deletes" in time_travel.detail
    live = report.findings_of(FindingKind.LIVE_ROWS)
    assert live and all(f.confidence is Confidence.POSSIBLE for f in live)


def test_disabled_checks_make_the_verdict_incomplete(make_table: Callable[..., Table]) -> None:
    table = make_table()
    nobody = Subject([Identifier("email", "nobody@example.com")], salt="x" * 32)
    report = scan_table(table, nobody, ScanOptions(orphan_files=False, time_travel=False))
    assert report.findings == []
    assert report.verdict is Verdict.INCOMPLETE
    assert {c.name: c.state for c in report.checks}["orphan_files"] is CheckState.SKIPPED


def test_subject_never_present_is_clean(make_table: Callable[..., Table]) -> None:
    table = make_table()
    nobody = Subject([Identifier("customer_id", "99"), Identifier("email", "nobody@example.com")], salt="x" * 32)
    report = scan_table(table, nobody)
    assert report.verdict is Verdict.CLEAN
    contents = next(c for c in report.checks if c.name == "file_contents")
    assert contents.detail.startswith("read 0 file(s)"), "bounds pruning should skip every file"


def test_object_versions_forced_on_local_table_is_skipped(make_table: Callable[..., Table], alice: Subject) -> None:
    report = scan_table(make_table(), alice, ScanOptions(object_versions=True))
    state = {c.name: c.state for c in report.checks}["object_versions"]
    assert state is CheckState.SKIPPED
