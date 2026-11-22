"""End-to-end against a mocked, versioned S3 bucket (moto server) through PyIceberg's S3 FileIO."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import boto3
import pytest
from moto.server import ThreadedMotoServer
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.expressions import EqualTo
from typer.testing import CliRunner

from afterimage import FindingKind, Scanner, ScanOptions, Verdict, scan_table
from afterimage.cli import EXIT_CLEAN, app
from afterimage.objects import purge_versions
from afterimage.subject import Subject

from ..conftest import BY_CUSTOMER, SCHEMA, SEED, customers

pytestmark = pytest.mark.e2e

BUCKET = "lake"


@pytest.fixture
def moto_endpoint() -> Iterator[str]:
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    try:
        yield f"http://{host}:{port}"
    finally:
        server.stop()


@pytest.fixture
def s3_props(moto_endpoint: str) -> dict[str, str]:
    return {
        "s3.endpoint": moto_endpoint,
        "s3.access-key-id": "testing",
        "s3.secret-access-key": "testing",
        "s3.region": "us-east-1",
    }


@pytest.fixture
def s3_catalog(tmp_path: Path, s3_props: dict[str, str]) -> SqlCatalog:
    client = boto3.client(
        "s3",
        endpoint_url=s3_props["s3.endpoint"],
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        region_name="us-east-1",
    )
    client.create_bucket(Bucket=BUCKET)
    client.put_bucket_versioning(Bucket=BUCKET, VersioningConfiguration={"Status": "Enabled"})
    catalog = SqlCatalog(
        "s3test",
        uri=f"sqlite:///{tmp_path}/catalog.db",
        warehouse=f"s3://{BUCKET}/warehouse",
        **s3_props,
    )
    catalog.create_namespace("crm")
    return catalog


def test_noncurrent_versions_survive_iceberg_maintenance_until_purged(
    s3_catalog: SqlCatalog, alice: Subject, tmp_path: Path, s3_props: dict[str, str]
) -> None:
    name = "crm.customers"
    table = s3_catalog.create_table(
        name,
        SCHEMA,
        partition_spec=BY_CUSTOMER,
        properties={
            "write.metadata.delete-after-commit.enabled": "true",
            "write.metadata.previous-versions-max": "1",
        },
    )
    table.append(customers(SEED))
    table = s3_catalog.load_table(name)
    table.delete(EqualTo("customer_id", 1))
    table = s3_catalog.load_table(name)
    table.append(customers([(4, "dave@example.com", "AT")]))
    table = s3_catalog.load_table(name)
    current = table.metadata.current_snapshot_id
    table.maintenance.expire_snapshots().by_ids(
        [s.snapshot_id for s in table.metadata.snapshots if s.snapshot_id != current]
    ).commit()
    with s3_catalog.load_table(name).transaction() as tx:
        tx.set_properties({"maintenance.last-run": "erasure"})
    table = s3_catalog.load_table(name)

    # Remove orphans: in a versioned bucket this only hides the bytes behind a delete marker.
    scanner = Scanner(table, alice)
    before = scanner.run()
    assert FindingKind.ORPHAN_DATA_FILE in {f.kind for f in before.findings}
    for uri in scanner.unreferenced_files:
        table.io.delete(uri)

    report = scan_table(s3_catalog.load_table(name), alice)
    versions = report.findings_of(FindingKind.NONCURRENT_OBJECT_VERSION)
    assert versions, report.findings
    assert {f.kind for f in report.findings} == {FindingKind.NONCURRENT_OBJECT_VERSION}
    assert all(f.object_version_id for f in versions)
    assert any((f.match_count or 0) > 0 for f in versions)  # the parquet data file itself
    assert "Enabled" in next(c.detail for c in report.checks if c.name == "object_versions")

    # Library purge (dry run first) ...
    scanner = Scanner(s3_catalog.load_table(name), alice, ScanOptions(object_versions=True))
    scanner.run()
    assert scanner.s3_client is not None
    dry = purge_versions(scanner.s3_client, scanner.version_hits, confirm=False)
    assert dry and not any(o.deleted for o in dry)

    # ... then the CLI with --confirm.
    salt_env = {"AFTERIMAGE_SALT": "unit-test-salt-0123456789"}
    subject_file = tmp_path / "subject.txt"
    subject_file.write_text("customer_id=1\nemail=alice.wonderland@example.com\n", encoding="utf-8")
    catalog_args = [
        "-c",
        "s3test",
        "-p",
        "type=sql",
        "-p",
        f"uri=sqlite:///{tmp_path}/catalog.db",
        "-p",
        f"warehouse=s3://{BUCKET}/warehouse",
        *[arg for key, value in s3_props.items() for arg in ("-p", f"{key}={value}")],
    ]
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "purge-versions",
            "-t",
            name,
            *catalog_args,
            "--match-file",
            str(subject_file),
            "--include-possible",
            "--confirm",
        ],
        env=salt_env,
    )
    assert result.exit_code == 0, result.output
    assert "alice.wonderland" not in result.output

    result = runner.invoke(app, ["scan", "-t", name, *catalog_args, "--match-file", str(subject_file)], env=salt_env)
    assert result.exit_code == EXIT_CLEAN, result.output
    assert '"verdict": "clean"' in result.output

    final = scan_table(s3_catalog.load_table(name), alice)
    assert final.verdict is Verdict.CLEAN
