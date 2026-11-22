from __future__ import annotations

import io
from collections.abc import Callable, Iterator

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from moto import mock_aws
from pyiceberg.table import Table

from afterimage.inspect_files import FileInspector
from afterimage.matching import resolve_identifiers
from afterimage.models import Confidence
from afterimage.objects import (
    ObjectVersionScanner,
    bucket_versioning_status,
    iter_noncurrent_versions,
    purge_versions,
    s3_client_from_properties,
)
from afterimage.storage import S3Location
from afterimage.subject import Subject

from ..conftest import ALICE_EMAIL

BUCKET = "lake"
PREFIX = "wh/crm/customers"


def parquet_bytes(emails: list[str]) -> bytes:
    field = pa.field("email", pa.string(), metadata={b"PARQUET:field_id": b"2"})
    buffer = io.BytesIO()
    pq.write_table(pa.table({"email": emails}, schema=pa.schema([field])), buffer)
    return buffer.getvalue()


@pytest.fixture
def s3() -> Iterator[object]:
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        client.put_bucket_versioning(Bucket=BUCKET, VersioningConfiguration={"Status": "Enabled"})
        yield client


@pytest.fixture
def inspector(make_table: Callable[..., Table], alice: Subject) -> FileInspector:
    table = make_table()
    meta = table.metadata
    return FileInspector(meta.schema(), meta.specs(), resolve_identifiers(alice, meta.schema()))


def test_versioning_status(s3) -> None:  # type: ignore[no-untyped-def]
    assert bucket_versioning_status(s3, BUCKET) == "Enabled"
    s3.create_bucket(Bucket="plain")
    assert bucket_versioning_status(s3, "plain") == "Disabled"


def test_finds_deleted_and_overwritten_versions(s3, inspector: FileInspector) -> None:  # type: ignore[no-untyped-def]
    deleted_key = f"{PREFIX}/data/customer_id=1/a.parquet"
    s3.put_object(Bucket=BUCKET, Key=deleted_key, Body=parquet_bytes([ALICE_EMAIL]))
    s3.delete_object(Bucket=BUCKET, Key=deleted_key)

    overwritten_key = f"{PREFIX}/data/b.parquet"
    s3.put_object(Bucket=BUCKET, Key=overwritten_key, Body=parquet_bytes([ALICE_EMAIL, "x@y.z"]))
    s3.put_object(Bucket=BUCKET, Key=overwritten_key, Body=parquet_bytes(["x@y.z"]))

    clean_key = f"{PREFIX}/data/c.parquet"
    s3.put_object(Bucket=BUCKET, Key=clean_key, Body=parquet_bytes(["bob@example.com"]))
    s3.delete_object(Bucket=BUCKET, Key=clean_key)

    s3.put_object(Bucket=BUCKET, Key=f"{PREFIX}/metadata/version-hint.text", Body=b"1")
    s3.delete_object(Bucket=BUCKET, Key=f"{PREFIX}/metadata/version-hint.text")
    s3.put_object(Bucket=BUCKET, Key="other-table/data/d.parquet", Body=parquet_bytes([ALICE_EMAIL]))
    s3.delete_object(Bucket=BUCKET, Key="other-table/data/d.parquet")

    location = S3Location(BUCKET, PREFIX)
    versions = list(iter_noncurrent_versions(s3, location))
    assert sum(v.is_delete_marker for v in versions) == 3

    hits = ObjectVersionScanner(s3, inspector).scan(location)
    assert sorted(h.version.key for h in hits) == sorted([deleted_key, overwritten_key])
    assert all(h.confidence is Confidence.CONFIRMED for h in hits)
    assert {h.inspection.matched_rows for h in hits} == {1}


def test_large_versions_are_flagged_without_download(s3, inspector: FileInspector) -> None:  # type: ignore[no-untyped-def]
    key = f"{PREFIX}/data/big.parquet"
    s3.put_object(Bucket=BUCKET, Key=key, Body=parquet_bytes(["bob@example.com"]))
    s3.delete_object(Bucket=BUCKET, Key=key)
    hits = ObjectVersionScanner(s3, inspector, max_download_bytes=1).scan(S3Location(BUCKET, PREFIX))
    assert len(hits) == 1
    assert hits[0].confidence is Confidence.POSSIBLE
    assert "download limit" in hits[0].inspection.describe()


def test_purge_is_dry_run_unless_confirmed(s3, inspector: FileInspector) -> None:  # type: ignore[no-untyped-def]
    key = f"{PREFIX}/data/a.parquet"
    s3.put_object(Bucket=BUCKET, Key=key, Body=parquet_bytes([ALICE_EMAIL]))
    s3.delete_object(Bucket=BUCKET, Key=key)
    location = S3Location(BUCKET, PREFIX)
    hits = ObjectVersionScanner(s3, inspector).scan(location)

    dry = purge_versions(s3, hits, confirm=False)
    assert [o.deleted for o in dry] == [False]
    assert ObjectVersionScanner(s3, inspector).scan(location)

    done = purge_versions(s3, hits, confirm=True)
    assert [o.deleted for o in done] == [True]
    assert ObjectVersionScanner(s3, inspector).scan(location) == []


def test_purge_reports_errors_per_version(s3, inspector: FileInspector) -> None:  # type: ignore[no-untyped-def]
    key = f"{PREFIX}/data/a.parquet"
    s3.put_object(Bucket=BUCKET, Key=key, Body=parquet_bytes([ALICE_EMAIL]))
    s3.delete_object(Bucket=BUCKET, Key=key)
    hits = ObjectVersionScanner(s3, inspector).scan(S3Location(BUCKET, PREFIX))

    class Failing:
        def delete_object(self, **_: object) -> None:
            raise PermissionError("AccessDenied")

    outcomes = purge_versions(Failing(), hits, confirm=True)  # type: ignore[arg-type]
    assert outcomes[0].error is not None
    assert not outcomes[0].deleted


def test_s3_client_from_properties_uses_path_style_for_custom_endpoints() -> None:
    client = s3_client_from_properties(
        {
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "minio",
            "s3.secret-access-key": "minio123",
            "s3.session-token": "token",
            "s3.region": "eu-central-1",
        }
    )
    assert client.meta.endpoint_url == "http://localhost:9000"
    assert client.meta.region_name == "eu-central-1"
    assert client.meta.config.s3 == {"addressing_style": "path"}


def test_s3_location_parse() -> None:
    assert S3Location.parse("s3://lake/wh/t") == S3Location("lake", "wh/t")
    assert S3Location.parse("s3a://lake/wh/t").uri("k") == "s3://lake/k"  # type: ignore[union-attr]
    assert S3Location.parse("file:///tmp/x") is None
