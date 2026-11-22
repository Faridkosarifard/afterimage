from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from pyiceberg.table import Table

from afterimage.inspect_files import FileInspector, Inspection, file_kind
from afterimage.matching import resolve_identifiers
from afterimage.models import Confidence
from afterimage.subject import Subject


def inspector_for(table: Table, subject: Subject) -> FileInspector:
    meta = table.metadata
    return FileInspector(meta.schema(), meta.specs(), resolve_identifiers(subject, meta.schema()))


def test_file_kind() -> None:
    assert file_kind("s3://b/t/data/x.parquet") == "parquet"
    assert file_kind("s3://b/t/metadata/snap-1-0-abc.avro") == "manifest_list"
    assert file_kind("s3://b/t/metadata/abc-m0.avro") == "manifest"
    assert file_kind("s3://b/t/metadata/00001-abc.metadata.json") == "table_metadata"
    assert file_kind("s3://b/t/metadata/version-hint.text") == "other"


def test_inspect_real_manifest_and_manifest_list(make_table: Callable[..., Table], alice: Subject) -> None:
    table = make_table()
    inspector = inspector_for(table, alice)
    snapshot = table.current_snapshot()
    assert snapshot is not None
    manifest = snapshot.manifests(table.io)[0]
    found = inspector.inspect(table.io, manifest.manifest_path)
    assert found is not None
    assert found.confidence is Confidence.CONFIRMED
    email_hash = alice.salted_hash(alice.identifiers[1])
    assert found.confidence_for(email_hash) is Confidence.POSSIBLE  # truncated bounds only
    listing = inspector.inspect(table.io, snapshot.manifest_list)
    assert listing is not None
    assert "summary" in listing.describe()


def test_inspect_bytes_of_parquet(make_table: Callable[..., Table], alice: Subject) -> None:
    table = make_table(partitioned=False)
    inspector = inspector_for(table, alice)
    path = table.inspect.data_files()["file_path"][0].as_py()
    data = Path(path.removeprefix("file://")).read_bytes()
    found = inspector.inspect_bytes("data/00000-0-x.parquet", data)
    assert found is not None
    assert found.matched_rows == 2  # customer_id and email both match the same row
    assert "2 matching row(s)" in found.describe()


def test_unparseable_files_are_possible(make_table: Callable[..., Table], alice: Subject) -> None:
    inspector = inspector_for(make_table(), alice)
    found = inspector.inspect_bytes("metadata/abc-m0.avro", b"not avro at all")
    assert found is not None
    assert not found.parsed
    assert found.confidence is Confidence.POSSIBLE
    broken_parquet = inspector.inspect_bytes("data/x.parquet", b"garbage")
    assert broken_parquet is not None
    assert broken_parquet.confidence is Confidence.POSSIBLE
    meta = inspector.inspect_bytes("metadata/00001-x.metadata.json", b"{}")
    assert meta is not None
    assert meta.confidence is Confidence.POSSIBLE
    assert inspector.inspect_bytes("metadata/version-hint.text", b"1") is None


def test_inspection_without_signals_is_none() -> None:
    assert Inspection().confidence is None
    assert Inspection().describe() == ""
