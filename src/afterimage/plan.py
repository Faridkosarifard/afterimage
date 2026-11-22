"""Turn a report into an ordered remediation plan.

The order matters: rewriting data files creates new snapshots, so it must happen
before snapshots are expired; expiring snapshots turns files into orphans; and
removing orphans (or expiring with file deletion) creates noncurrent versions in a
versioned bucket, which therefore have to be purged last.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from afterimage.models import Finding, FindingKind, Report

_METADATA_KINDS = {FindingKind.COLUMN_BOUND, FindingKind.PARTITION_VALUE, FindingKind.PARTITION_SUMMARY}


@dataclass(frozen=True)
class Step:
    key: str
    title: str
    why: str
    spark_sql: list[str] = field(default_factory=list)
    pyiceberg: list[str] = field(default_factory=list)
    shell: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    addresses: list[FindingKind] = field(default_factory=list)


def _in_current(finding: Finding, current: int | None) -> bool:
    return current is not None and current in finding.snapshot_ids


def build_plan(report: Report, catalog: str = "my_catalog") -> list[Step]:
    """Return the remediation steps needed for the findings in ``report`` (empty when clean)."""
    if not report.findings:
        return []
    table = report.table.identifier
    current = report.table.current_snapshot_id
    kinds = {f.kind for f in report.findings}
    metadata_current = any(f.kind in _METADATA_KINDS and _in_current(f, current) for f in report.findings)
    metadata_history = any(f.kind in _METADATA_KINDS and not _in_current(f, current) for f in report.findings)
    steps: list[Step] = []

    if FindingKind.LIVE_ROWS in kinds:
        steps.append(
            Step(
                key="logical_delete",
                title="Delete the subject's rows",
                why="The current snapshot still returns the subject. Nothing else matters until this is done.",
                spark_sql=[f"DELETE FROM {catalog}.{table} WHERE <subject predicate>;"],
                pyiceberg=["table.delete(delete_filter=EqualTo('<column>', <value>))"],
                notes=["Keep the predicate out of shared logs and query history where possible."],
                addresses=[FindingKind.LIVE_ROWS],
            )
        )

    if kinds & {FindingKind.UNREWRITTEN_DATA_FILE, FindingKind.EQUALITY_DELETE_FILE} or metadata_current:
        steps.append(
            Step(
                key="rewrite_data_files",
                title="Rewrite data files and delete files, then rewrite manifests",
                why=(
                    "Merge-on-read deletes only mask rows; equality-delete files store the identifier "
                    "itself; and manifests keep the old files' bounds and partition values. Compaction "
                    "writes new files without the subject and drops the delete files."
                ),
                spark_sql=[
                    f"CALL {catalog}.system.rewrite_data_files(table => '{table}', "
                    "where => '<partition predicate covering the subject>', "
                    "options => map('delete-file-threshold', '1', 'rewrite-all', 'true'));",
                    f"CALL {catalog}.system.rewrite_position_delete_files(table => '{table}', "
                    "options => map('rewrite-all', 'true'));",
                    f"CALL {catalog}.system.rewrite_manifests('{table}');",
                ],
                notes=[
                    "PyIceberg cannot compact merge-on-read tables yet; use Spark, Flink, Trino "
                    "(ALTER TABLE ... EXECUTE optimize) or your platform's compaction service.",
                ],
                addresses=[
                    FindingKind.UNREWRITTEN_DATA_FILE,
                    FindingKind.EQUALITY_DELETE_FILE,
                    *sorted(_METADATA_KINDS),
                ],
            )
        )

    history_kinds = {FindingKind.HISTORICAL_SNAPSHOT_ROWS, FindingKind.HISTORICAL_DATA_FILE}
    if kinds & history_kinds or metadata_history or metadata_current:
        steps.append(
            Step(
                key="expire_snapshots",
                title="Expire every snapshot that can still reach the subject",
                why=(
                    "Time travel reads old snapshots, and their manifests keep bounds and partition values. "
                    "Snapshots pinned by a tag or branch are never expired; drop those refs first."
                ),
                spark_sql=[
                    f"ALTER TABLE {catalog}.{table} DROP TAG <tag>;  -- for tags pinning old snapshots",
                    f"CALL {catalog}.system.expire_snapshots(table => '{table}', "
                    "older_than => TIMESTAMP '<now>', retain_last => 1);",
                ],
                pyiceberg=[
                    "table.maintenance.expire_snapshots().older_than(datetime.now(timezone.utc)).commit()",
                ],
                notes=[
                    "PyIceberg's expire_snapshots only rewrites metadata; it does not delete files. "
                    "Follow it with orphan file removal.",
                    "If the deleting snapshot is the current one, commit one more change first so that "
                    "manifests with DELETED entries stop being referenced.",
                ],
                addresses=sorted(history_kinds | _METADATA_KINDS),
            )
        )

    if FindingKind.METADATA_LOG_SNAPSHOT in kinds or FindingKind.ORPHAN_METADATA_FILE in kinds:
        steps.append(
            Step(
                key="clean_metadata_log",
                title="Stop keeping old metadata.json files",
                why=(
                    "Each previous metadata.json lists the snapshots that existed at the time and is not "
                    "encrypted, even when table encryption is enabled."
                ),
                spark_sql=[
                    f"ALTER TABLE {catalog}.{table} SET TBLPROPERTIES ("
                    "'write.metadata.delete-after-commit.enabled' = 'true', "
                    "'write.metadata.previous-versions-max' = '1');",
                ],
                pyiceberg=[
                    "with table.transaction() as tx: tx.set_properties({"
                    "'write.metadata.delete-after-commit.enabled': 'true', "
                    "'write.metadata.previous-versions-max': '1'})",
                ],
                notes=[
                    "Old entries are deleted on the next commits; commit twice after expiring snapshots so "
                    "the metadata written before the expiry rotates out as well.",
                ],
                addresses=[FindingKind.METADATA_LOG_SNAPSHOT],
            )
        )

    orphan_kinds = {FindingKind.ORPHAN_DATA_FILE, FindingKind.ORPHAN_METADATA_FILE}
    if kinds & (orphan_kinds | history_kinds) or metadata_history:
        steps.append(
            Step(
                key="remove_orphan_files",
                title="Remove orphan files",
                why="Files no metadata references are invisible to queries but remain readable in storage.",
                spark_sql=[
                    f"CALL {catalog}.system.remove_orphan_files(table => '{table}', "
                    "older_than => TIMESTAMP '<cutoff>', dry_run => true);",
                    f"CALL {catalog}.system.remove_orphan_files(table => '{table}', "
                    "older_than => TIMESTAMP '<cutoff>');",
                ],
                notes=[
                    "The cutoff protects files of in-flight writes. Pick one later than the deleting commit "
                    "but earlier than any job still running against the table.",
                ],
                addresses=sorted(orphan_kinds),
            )
        )

    deletes_files = bool(kinds & (orphan_kinds | history_kinds)) or metadata_history
    on_object_store = report.table.location.startswith(("s3://", "s3a://", "s3n://"))
    if FindingKind.NONCURRENT_OBJECT_VERSION in kinds or (deletes_files and on_object_store):
        steps.append(
            Step(
                key="purge_noncurrent_versions",
                title="Purge noncurrent object versions",
                why=(
                    "With bucket versioning, every file deleted in the previous steps becomes a noncurrent "
                    "version that can be restored by version id."
                ),
                shell=[
                    f"afterimage purge-versions --table {table} --match-file subject.txt            # dry run",
                    f"afterimage purge-versions --table {table} --match-file subject.txt --confirm  # deletes",
                ],
                notes=[
                    "Only applies to versioned S3/MinIO buckets. Object Lock retention or legal holds will "
                    "block deletion; document those as a retention exception.",
                    "Consider a NoncurrentVersionExpiration lifecycle rule so this happens automatically.",
                ],
                addresses=[FindingKind.NONCURRENT_OBJECT_VERSION],
            )
        )

    if kinds & _METADATA_KINDS:
        steps.append(
            Step(
                key="prevent",
                title="Prevent identifiers from reaching metadata again",
                why="Bounds and partition values are copied into every manifest and every manifest list.",
                spark_sql=[
                    f"ALTER TABLE {catalog}.{table} SET TBLPROPERTIES "
                    "('write.metadata.metrics.column.<identifier column>' = 'none');",
                ],
                notes=[
                    "Avoid identity or truncate partitioning on identifier columns; bucket[N] does not "
                    "reveal the value.",
                ],
            )
        )

    steps.append(
        Step(
            key="rescan",
            title="Scan again and archive the clean report",
            why="The evidence report is only meaningful once the verdict is clean.",
            shell=[f"afterimage scan --table {table} --match-file subject.txt --json-out evidence.json"],
        )
    )
    return steps


def render_plan(steps: list[Step]) -> str:
    if not steps:
        return "Nothing to remediate: the report is clean.\n"
    lines: list[str] = []
    for number, step in enumerate(steps, start=1):
        lines.append(f"{number}. {step.title}")
        lines.append(f"   Why: {step.why}")
        for label, commands in (
            ("Spark SQL", step.spark_sql),
            ("PyIceberg", step.pyiceberg),
            ("Shell", step.shell),
        ):
            if commands:
                lines.append(f"   {label}:")
                lines.extend(f"     {command}" for command in commands)
        for note in step.notes:
            lines.append(f"   Note: {note}")
        if step.addresses:
            lines.append("   Addresses: " + ", ".join(k.value for k in step.addresses))
        lines.append("")
    return "\n".join(lines)
