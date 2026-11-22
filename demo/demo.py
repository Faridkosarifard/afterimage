"""Walk through an erasure request end to end against the docker-compose playground.

Steps: create a partitioned customer table, delete one subject, show what is still
recoverable, run the maintenance a platform team would run, show that the bytes now
only live in noncurrent object versions, purge them, and archive a clean report.

Catalog properties default to the docker-compose services and can be overridden with
AFTERIMAGE_DEMO_<PROP> environment variables (for example AFTERIMAGE_DEMO_URI).
"""

from __future__ import annotations

import contextlib
import os
import sys
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.expressions import EqualTo
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import LongType, NestedField, StringType

from afterimage import Identifier, Report, Scanner, ScanOptions, Subject
from afterimage.objects import purge_versions
from afterimage.plan import build_plan, render_plan
from afterimage.render import to_json, to_markdown

DEFAULTS = {
    "type": "rest",
    "uri": "http://localhost:8181",
    "s3.endpoint": "http://localhost:9000",
    "s3.access-key-id": "admin",
    "s3.secret-access-key": "password",
    "s3.region": "us-east-1",
}
TABLE = "crm.customers"
OUT = Path(__file__).parent / "out"
# Demo-only salt. In production, load it from a secret store into AFTERIMAGE_SALT.
SALT = os.environ.get("AFTERIMAGE_SALT", "demo-salt-do-not-use-in-production")


def catalog_properties() -> dict[str, str]:
    props = dict(DEFAULTS)
    for key, value in os.environ.items():
        if key.startswith("AFTERIMAGE_DEMO_"):
            props[key.removeprefix("AFTERIMAGE_DEMO_").lower().replace("__", "-").replace("_", ".")] = value
    return props


def rows(data: list[tuple[int, str, str]]) -> pa.Table:
    return pa.table(
        {
            "customer_id": pa.array([r[0] for r in data], pa.int64()),
            "email": pa.array([r[1] for r in data], pa.string()),
            "country": pa.array([r[2] for r in data], pa.string()),
        }
    )


def banner(text: str) -> None:
    print(f"\n=== {text} ===")


def scan(catalog: Catalog, subject: Subject, label: str) -> tuple[Scanner, Report]:
    scanner = Scanner(catalog.load_table(TABLE), subject, ScanOptions(), table_identifier=TABLE)
    report = scanner.run()
    OUT.mkdir(exist_ok=True)
    (OUT / f"{label}.json").write_text(to_json(report), encoding="utf-8")
    (OUT / f"{label}.md").write_text(to_markdown(report), encoding="utf-8")
    print(f"verdict: {report.verdict.value}")
    for kind, count in report.summary.items():
        print(f"  {kind:28} {count}")
    return scanner, report


def main() -> int:
    catalog = load_catalog("demo", **catalog_properties())
    with contextlib.suppress(NamespaceAlreadyExistsError):
        catalog.create_namespace("crm")
    with contextlib.suppress(NoSuchTableError):
        catalog.purge_table(TABLE)

    schema = Schema(
        NestedField(1, "customer_id", LongType(), required=False),
        NestedField(2, "email", StringType(), required=False),
        NestedField(3, "country", StringType(), required=False),
    )
    table = catalog.create_table(
        TABLE,
        schema,
        partition_spec=PartitionSpec(PartitionField(1, 1000, IdentityTransform(), "customer_id")),
        properties={
            "write.metadata.delete-after-commit.enabled": "true",
            "write.metadata.previous-versions-max": "1",
        },
    )
    table.append(
        rows(
            [
                (48213, "erika.mustermann@example.de", "DE"),
                (48214, "jan.jansen@example.nl", "NL"),
                (48215, "marie.dupont@example.fr", "FR"),
            ]
        )
    )
    subject = Subject(
        [Identifier("customer_id", "48213"), Identifier("email", "erika.mustermann@example.de")], salt=SALT
    )

    banner("1. Logical DELETE")
    catalog.load_table(TABLE).delete(EqualTo("customer_id", 48213))
    visible = catalog.load_table(TABLE).scan(row_filter=EqualTo("customer_id", 48213)).to_arrow().num_rows
    print(f"rows visible in the current snapshot: {visible}")

    banner("2. Scan after DELETE")
    _, after_delete = scan(catalog, subject, "01-after-delete")
    print("\nRemediation plan:\n")
    print(render_plan(build_plan(after_delete, catalog="demo")))

    banner("3. Maintenance: commit, expire snapshots, rotate metadata, remove orphans")
    catalog.load_table(TABLE).append(rows([(48216, "anna.svensson@example.se", "SE")]))
    table = catalog.load_table(TABLE)
    current = table.metadata.current_snapshot_id
    table.maintenance.expire_snapshots().by_ids(
        [s.snapshot_id for s in table.metadata.snapshots if s.snapshot_id != current]
    ).commit()
    with catalog.load_table(TABLE).transaction() as tx:
        tx.set_properties({"maintenance.last-erasure": "demo"})
    orphans = Scanner(catalog.load_table(TABLE), subject, table_identifier=TABLE)
    orphans.run()
    io = catalog.load_table(TABLE).io
    for uri in orphans.unreferenced_files:
        io.delete(uri)
    print(f"removed {len(orphans.unreferenced_files)} orphan file(s)")

    banner("4. Scan after maintenance")
    after_maintenance, _ = scan(catalog, subject, "02-after-maintenance")

    banner("5. Purge noncurrent object versions")
    if after_maintenance.s3_client is not None:
        outcomes = purge_versions(after_maintenance.s3_client, after_maintenance.version_hits, confirm=True)
        print(f"deleted {sum(o.deleted for o in outcomes)} noncurrent version(s)")

    banner("6. Final scan")
    _, final = scan(catalog, subject, "03-final")
    print(f"\nreports written to {OUT}")
    return 0 if final.is_clean else 1


if __name__ == "__main__":
    sys.exit(main())
