"""The scanner: walks every reachable and unreachable artefact of an Iceberg table.

Checks, in order:

1. ``manifest_metadata``: column lower/upper bounds, partition tuples and manifest-list
   partition summaries in every manifest of every snapshot, including ``DELETED``
   entries that Iceberg keeps until the deleting snapshot expires.
2. ``file_contents``: reads every data and equality-delete file whose bounds do not rule
   the subject out, ignoring delete files, to find physically retained rows.
3. ``time_travel``: rows readable per snapshot with deletes applied (what a ``SELECT ...
   VERSION AS OF`` returns).
4. ``metadata_log``: previous ``metadata.json`` files that still list expired snapshots.
5. ``orphan_files``: files under the table location that no metadata references.
6. ``object_versions``: noncurrent versions in versioned S3 / MinIO buckets.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from pyiceberg.expressions import EqualTo
from pyiceberg.io import FileIO
from pyiceberg.manifest import DataFile, DataFileContent, ManifestEntry, ManifestEntryStatus, ManifestFile
from pyiceberg.serializers import FromInputFile
from pyiceberg.table import Table
from pyiceberg.table.metadata import TableMetadata
from pyiceberg.table.snapshots import Snapshot

import afterimage
from afterimage.inspect_files import FileInspector, file_kind
from afterimage.matching import (
    DEFAULT_TRUNCATE_LENGTH,
    ResolvedIdentifier,
    count_parquet_matches,
    match_bounds,
    match_partition,
    match_partition_summaries,
    may_contain,
    resolve_identifiers,
)
from afterimage.models import (
    CheckResult,
    CheckState,
    Confidence,
    Finding,
    FindingKind,
    IdentifierEvidence,
    Report,
    SubjectEvidence,
    TableInfo,
    compute_verdict,
    summarize,
)
from afterimage.objects import (
    DEFAULT_MAX_DOWNLOAD_BYTES,
    ObjectVersionScanner,
    VersionHit,
    bucket_versioning_status,
    s3_client_from_properties,
)
from afterimage.storage import S3Location, file_exists, list_files, normalize_uri
from afterimage.subject import HASH_ALGORITHM, Redactor, Subject

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

logger = logging.getLogger(__name__)

CHECK_NAMES = (
    "manifest_metadata",
    "file_contents",
    "time_travel",
    "metadata_log",
    "orphan_files",
    "object_versions",
)


@dataclass
class ScanOptions:
    """Which checks to run and how. All checks are on by default."""

    manifest_metadata: bool = True
    file_contents: bool = True
    time_travel: bool = True
    metadata_log: bool = True
    orphan_files: bool = True
    object_versions: bool | None = None
    """``None`` means "run when the table lives in S3-compatible storage"."""
    truncate_length: int = DEFAULT_TRUNCATE_LENGTH
    max_download_bytes: int = DEFAULT_MAX_DOWNLOAD_BYTES
    s3_client: S3Client | None = None
    s3_properties: dict[str, str] = field(default_factory=dict)
    """Overrides for ``s3.*`` properties used to build the boto3 client."""


@dataclass
class _FileRef:
    data_file: DataFile
    live_in: set[int] = field(default_factory=set)
    deleted_in: set[int] = field(default_factory=set)

    @property
    def path(self) -> str:
        return self.data_file.file_path

    @property
    def content(self) -> DataFileContent:
        return self.data_file.content

    @property
    def snapshots(self) -> list[int]:
        return sorted(self.live_in | self.deleted_in)


@dataclass
class _Index:
    """Every manifest, manifest list and file reachable from a set of snapshots."""

    files: dict[str, _FileRef] = field(default_factory=dict)
    manifests: dict[str, tuple[ManifestFile, set[int]]] = field(default_factory=dict)
    manifest_lists: set[str] = field(default_factory=set)
    snapshot_files: dict[int, set[str]] = field(default_factory=dict)

    @classmethod
    def build(cls, snapshots: Iterable[Snapshot], io: FileIO) -> _Index:
        index = cls()
        cache: dict[str, list[ManifestEntry]] = {}
        for snapshot in snapshots:
            index.manifest_lists.add(normalize_uri(snapshot.manifest_list))
            files_in_snapshot = index.snapshot_files.setdefault(snapshot.snapshot_id, set())
            for manifest in snapshot.manifests(io):
                key = normalize_uri(manifest.manifest_path)
                if key not in index.manifests:
                    index.manifests[key] = (manifest, set())
                index.manifests[key][1].add(snapshot.snapshot_id)
                if key not in cache:
                    cache[key] = manifest.fetch_manifest_entry(io, discard_deleted=False)
                for entry in cache[key]:
                    path = normalize_uri(entry.data_file.file_path)
                    ref = index.files.setdefault(path, _FileRef(entry.data_file))
                    if entry.status == ManifestEntryStatus.DELETED:
                        ref.deleted_in.add(snapshot.snapshot_id)
                    else:
                        ref.live_in.add(snapshot.snapshot_id)
                        files_in_snapshot.add(path)
        return index

    def referenced_uris(self) -> set[str]:
        return set(self.files) | set(self.manifests) | self.manifest_lists


@dataclass(frozen=True)
class _Signal:
    kind: FindingKind
    confidence: Confidence
    location: str
    ident: ResolvedIdentifier | None
    snapshot_ids: tuple[int, ...] = ()
    match_count: int | None = None
    detail: str = ""
    object_version_id: str | None = None


class Scanner:
    """Scan one table for one subject. Use :func:`scan_table` for the one-call API."""

    def __init__(
        self,
        table: Table,
        subject: Subject,
        options: ScanOptions | None = None,
        *,
        table_identifier: str | None = None,
    ) -> None:
        self.table = table
        self.subject = subject
        self.options = options or ScanOptions()
        self.table_identifier = table_identifier or ".".join(table.name())
        self.redact: Redactor = subject.redactor()
        self.version_hits: list[VersionHit] = []
        #: Raw URIs of files under the table location that no metadata references.
        #: Populated by the orphan check; never written to reports unredacted.
        self.unreferenced_files: list[str] = []
        self._checks: dict[str, CheckResult] = {}
        self._findings: dict[tuple[object, ...], Finding] = {}
        self._contents: dict[str, dict[str, int] | None] = {}
        self._referenced: set[str] = set()

    # ------------------------------------------------------------------ API
    def run(self) -> Report:
        metadata = self.table.metadata
        io = self.table.io
        self.idents = resolve_identifiers(self.subject, metadata.schema())
        self.inspector = FileInspector(
            metadata.schema(), metadata.specs(), self.idents, self.options.truncate_length
        )
        logger.info(
            "scanning %s for %d identifier(s) across %d snapshot(s)",
            self.redact(self.table_identifier),
            len(self.idents),
            len(metadata.snapshots),
        )
        index = _Index.build(metadata.snapshots, io)
        self._referenced = index.referenced_uris()
        self._referenced.add(normalize_uri(self.table.metadata_location))
        for stats in [*metadata.statistics, *metadata.partition_statistics]:
            self._referenced.add(normalize_uri(stats.statistics_path))

        current_id = metadata.current_snapshot_id
        live_counts: dict[int, int] = {}

        self._run_check(
            "manifest_metadata",
            self.options.manifest_metadata,
            lambda: self._emit(self._metadata_signals(index, metadata)),
        )

        def contents() -> str:
            if current_id is not None:
                live_counts[current_id] = self._snapshot_rows(current_id, index)
            signals, read, missing = self._content_signals(index, current_id, live_counts.get(current_id, 0))
            self._emit(signals)
            return f"read {read} file(s); {missing} referenced file(s) already missing from storage"

        self._run_check("file_contents", self.options.file_contents, contents)
        self._run_check(
            "time_travel", self.options.time_travel, lambda: self._time_travel(metadata, index, live_counts)
        )
        self._run_check("metadata_log", self.options.metadata_log, lambda: self._metadata_log(metadata))
        self._run_check("orphan_files", self.options.orphan_files, self._orphans)
        self._object_versions()

        findings = sorted(
            self._findings.values(), key=lambda f: (list(FindingKind).index(f.kind), f.location, f.snapshot_ids)
        )
        checks = [self._checks[name] for name in CHECK_NAMES if name in self._checks]
        return Report(
            tool_version=afterimage.__version__,
            table=TableInfo(
                identifier=self.redact(self.table_identifier),
                location=self.redact(self.table.location()),
                format_version=metadata.format_version,
                current_snapshot_id=current_id,
                snapshot_count=len(metadata.snapshots),
                metadata_location=self.redact(self.table.metadata_location),
            ),
            subject=self._subject_evidence(findings),
            verdict=compute_verdict(findings, checks),
            checks=checks,
            findings=findings,
            summary=summarize(findings),
        )

    # ------------------------------------------------------------ plumbing
    def _run_check(self, name: str, enabled: bool, func: Callable[[], str | None]) -> None:
        if not enabled:
            self._checks[name] = CheckResult(name=name, state=CheckState.SKIPPED, detail="disabled by option")
            return
        try:
            detail = func() or ""
            self._checks[name] = CheckResult(name=name, state=CheckState.RAN, detail=self.redact(detail))
        except _Skip as skip:
            self._checks[name] = CheckResult(name=name, state=CheckState.SKIPPED, detail=self.redact(str(skip)))
        except Exception as exc:
            logger.error("check %s failed: %s", name, self.redact(f"{type(exc).__name__}: {exc}"))
            logger.debug("traceback", exc_info=logger.isEnabledFor(logging.DEBUG))
            self._checks[name] = CheckResult(
                name=name, state=CheckState.ERROR, detail=self.redact(f"{type(exc).__name__}: {exc}")
            )

    def _emit(self, signals: Iterable[_Signal]) -> None:
        for signal in signals:
            location = self.redact(signal.location)
            key = (
                signal.kind,
                location,
                signal.ident.salted_hash if signal.ident else None,
                signal.detail,
                signal.object_version_id,
            )
            existing = self._findings.get(key)
            snapshot_ids = sorted(set(signal.snapshot_ids) | set(existing.snapshot_ids if existing else ()))
            self._findings[key] = Finding(
                kind=signal.kind,
                confidence=signal.confidence,
                location=location,
                identifier_hash=signal.ident.salted_hash if signal.ident else None,
                column=signal.ident.column if signal.ident else None,
                snapshot_ids=snapshot_ids,
                match_count=signal.match_count,
                object_version_id=signal.object_version_id,
                detail=self.redact(signal.detail),
            )

    def _count(self, io: FileIO, path: str) -> dict[str, int] | None:
        """Raw match counts for a file, or ``None`` if the file no longer exists."""
        key = normalize_uri(path)
        if key not in self._contents:
            if not file_exists(io, path):
                self._contents[key] = None
            else:
                with io.new_input(path).open() as stream:
                    self._contents[key] = count_parquet_matches(stream, self.idents)
        return self._contents[key]

    # --------------------------------------------------------------- checks
    def _metadata_signals(self, index: _Index, metadata: TableMetadata) -> list[_Signal]:
        signals: list[_Signal] = []
        specs = metadata.specs()
        schema = metadata.schema()
        for ref in index.files.values():
            data_file = ref.data_file
            only_deleted = not ref.live_in
            suffix = " (only in DELETED manifest entries)" if only_deleted else ""
            for ident in self.idents:
                lower = data_file.lower_bounds.get(ident.field_id) if data_file.lower_bounds else None
                upper = data_file.upper_bounds.get(ident.field_id) if data_file.upper_bounds else None
                by_confidence: dict[Confidence, list[str]] = {}
                for bound in match_bounds(ident, lower, upper, self.options.truncate_length):
                    by_confidence.setdefault(bound.confidence, []).append(bound.detail)
                for confidence, details in by_confidence.items():
                    signals.append(
                        _Signal(
                            FindingKind.COLUMN_BOUND,
                            confidence,
                            ref.path,
                            ident,
                            tuple(ref.snapshots),
                            None,
                            f"{' and '.join(details)} in the manifest entry of a "
                            f"{data_file.content.name.lower().replace('_', ' ')} file{suffix}",
                        )
                    )
            spec = specs.get(data_file.spec_id)
            if spec is None:
                continue
            for pmatch in match_partition(spec, data_file.partition, self.idents):
                signals.append(
                    _Signal(
                        FindingKind.PARTITION_VALUE,
                        pmatch.confidence,
                        ref.path,
                        pmatch.ident,
                        tuple(ref.snapshots),
                        None,
                        pmatch.detail + suffix,
                    )
                )
        for manifest, snapshot_ids in index.manifests.values():
            spec = specs.get(manifest.partition_spec_id)
            if spec is None:
                continue
            for pmatch in match_partition_summaries(spec, schema, manifest.partitions, self.idents):
                signals.append(
                    _Signal(
                        FindingKind.PARTITION_SUMMARY,
                        pmatch.confidence,
                        manifest.manifest_path,
                        pmatch.ident,
                        tuple(sorted(snapshot_ids)),
                        None,
                        pmatch.detail,
                    )
                )
        return signals

    def _content_signals(
        self, index: _Index, current_id: int | None, current_live_rows: int
    ) -> tuple[list[_Signal], int, int]:
        signals: list[_Signal] = []
        io = self.table.io
        read = missing = 0
        for ref in index.files.values():
            if ref.content == DataFileContent.POSITION_DELETES:
                continue
            data_file = ref.data_file
            candidates = [
                ident
                for ident in self.idents
                if may_contain(
                    ident,
                    data_file.lower_bounds.get(ident.field_id) if data_file.lower_bounds else None,
                    data_file.upper_bounds.get(ident.field_id) if data_file.upper_bounds else None,
                )
            ]
            if not candidates:
                continue
            counts = self._count(io, ref.path)
            if counts is None:
                missing += 1
                continue
            read += 1
            for ident in candidates:
                rows = counts.get(ident.salted_hash, 0)
                if not rows:
                    continue
                if ref.content == DataFileContent.EQUALITY_DELETES:
                    kind = FindingKind.EQUALITY_DELETE_FILE
                    detail = "equality-delete file stores the identifier value it deletes"
                elif current_id is not None and current_id in ref.live_in:
                    if current_live_rows:
                        continue  # reported as live_rows
                    kind = FindingKind.UNREWRITTEN_DATA_FILE
                    detail = (
                        "data file in the current snapshot still contains the rows; they are hidden "
                        "only by delete files (merge-on-read) until the file is rewritten"
                    )
                else:
                    kind = FindingKind.HISTORICAL_DATA_FILE
                    detail = "data file is no longer in the current snapshot but still exists in storage"
                signals.append(
                    _Signal(kind, Confidence.CONFIRMED, ref.path, ident, tuple(ref.snapshots), rows, detail)
                )
        return signals, read, missing

    def _snapshot_rows(self, snapshot_id: int, index: _Index, ident: ResolvedIdentifier | None = None) -> int:
        """Rows matching the subject in a snapshot, with delete files applied."""
        idents = [ident] if ident else self.idents
        total = 0
        for item in idents:
            if self.options.file_contents and not self._snapshot_may_match(snapshot_id, index, item):
                continue
            scan = self.table.scan(
                row_filter=EqualTo(item.field_name, item.value),
                selected_fields=(item.field_name,),
                snapshot_id=snapshot_id,
            )
            total += scan.to_arrow().num_rows
        return total

    def _snapshot_may_match(self, snapshot_id: int, index: _Index, ident: ResolvedIdentifier) -> bool:
        for path in index.snapshot_files.get(snapshot_id, set()):
            counts = self._contents.get(path)
            if counts is None and path not in self._contents:
                return True  # not inspected (pruned by bounds is fine, unknown is not)
            if counts and counts.get(ident.salted_hash):
                return True
        return False

    def _time_travel(self, metadata: TableMetadata, index: _Index, live_counts: dict[int, int]) -> str:
        refs: dict[int, list[str]] = {}
        for name, ref in metadata.refs.items():
            refs.setdefault(ref.snapshot_id, []).append(name)
        unreadable = 0
        signals: list[_Signal] = []
        for snapshot in metadata.snapshots:
            sid = snapshot.snapshot_id
            for ident in self.idents:
                try:
                    rows = self._snapshot_rows(sid, index, ident)
                except (FileNotFoundError, OSError):
                    unreadable += 1
                    continue
                if not rows:
                    continue
                named = f" (referenced by {', '.join(sorted(refs[sid]))})" if sid in refs else ""
                if sid == metadata.current_snapshot_id:
                    kind = FindingKind.LIVE_ROWS
                    detail = "rows are visible in the current snapshot; the logical DELETE has not happened"
                else:
                    kind = FindingKind.HISTORICAL_SNAPSHOT_ROWS
                    detail = f"rows are readable with time travel (VERSION AS OF {sid}){named}"
                signals.append(
                    _Signal(
                        kind,
                        Confidence.CONFIRMED,
                        f"{self.table_identifier}@{sid}",
                        ident,
                        (sid,),
                        rows,
                        detail,
                    )
                )
        self._emit(signals)
        detail = f"queried {len(metadata.snapshots)} snapshot(s)"
        if unreadable:
            detail += f"; {unreadable} snapshot/identifier scan(s) failed because files are missing"
        return detail

    def _metadata_log(self, metadata: TableMetadata) -> str:
        io = self.table.io
        current_ids = {s.snapshot_id for s in metadata.snapshots}
        inspected = 0
        for log_entry in metadata.metadata_log:
            uri = log_entry.metadata_file
            self._referenced.add(normalize_uri(uri))
            if not file_exists(io, uri):
                continue
            inspected += 1
            old = FromInputFile.table_metadata(io.new_input(uri))
            expired = [
                s
                for s in old.snapshots
                if s.snapshot_id not in current_ids and file_exists(io, s.manifest_list)
            ]
            if not expired:
                continue
            sub_index = _Index.build(expired, io)
            self._referenced |= sub_index.referenced_uris()
            signals = self._metadata_signals(sub_index, old)
            if self.options.file_contents:
                content, _read, _missing = self._content_signals(sub_index, None, 0)
                signals.extend(content)
            if not signals:
                continue
            confidence = (
                Confidence.CONFIRMED
                if any(s.confidence is Confidence.CONFIRMED for s in signals)
                else Confidence.POSSIBLE
            )
            rows = sum(s.match_count or 0 for s in signals)
            kinds: dict[str, int] = {}
            for s in signals:
                kinds[s.kind.value] = kinds.get(s.kind.value, 0) + 1
            breakdown = ", ".join(f"{k}={v}" for k, v in sorted(kinds.items()))
            snapshot_ids = sorted({sid for s in signals for sid in s.snapshot_ids})
            idents = {s.ident.salted_hash: s.ident for s in signals if s.ident}
            for ident in idents.values() or [None]:
                self._emit(
                    [
                        _Signal(
                            FindingKind.METADATA_LOG_SNAPSHOT,
                            confidence,
                            uri,
                            ident,
                            tuple(snapshot_ids),
                            rows or None,
                            f"previous metadata file still references {len(expired)} expired snapshot(s) "
                            f"whose files reveal the subject ({breakdown}); metadata.json is not "
                            "encrypted even for encrypted tables",
                        )
                    ]
                )
        return f"inspected {inspected} previous metadata file(s)"

    def _orphans(self) -> str:
        io = self.table.io
        listed = list_files(io, self.table.location())
        if listed is None:
            raise _Skip("the table FileIO does not support listing; use PyArrowFileIO")
        orphans = [(uri, size) for uri, size in listed if normalize_uri(uri) not in self._referenced]
        self.unreferenced_files = [uri for uri, _ in orphans]
        signals: list[_Signal] = []
        for uri, size in orphans:
            if file_kind(uri) == "parquet":
                counts = self._count(io, uri) or {}
                for ident in self.idents:
                    if counts.get(ident.salted_hash):
                        signals.append(
                            _Signal(
                                FindingKind.ORPHAN_DATA_FILE,
                                Confidence.CONFIRMED,
                                uri,
                                ident,
                                (),
                                counts[ident.salted_hash],
                                "file is not referenced by any table metadata but is still in storage",
                            )
                        )
                continue
            inspection = self.inspector.inspect(io, uri, size)
            if inspection is None or inspection.confidence is None:
                continue
            idents: list[ResolvedIdentifier | None] = [
                i for i in self.idents if i.salted_hash in inspection.hashes
            ] or [None]
            for ident in idents:
                ident_hash = ident.salted_hash if ident else None
                confidence = inspection.confidence_for(ident_hash) or Confidence.POSSIBLE
                signals.append(
                    _Signal(
                        FindingKind.ORPHAN_METADATA_FILE,
                        confidence,
                        uri,
                        ident,
                        (),
                        None,
                        f"unreferenced {file_kind(uri).replace('_', ' ')}: {inspection.describe(ident_hash)}",
                    )
                )
        self._emit(signals)
        return f"listed {len(listed)} file(s), {len(orphans)} not referenced by table metadata"

    def _object_versions(self) -> None:
        name = "object_versions"
        location = S3Location.parse(self.table.location())
        enabled = self.options.object_versions
        if enabled is False:
            self._checks[name] = CheckResult(name=name, state=CheckState.SKIPPED, detail="disabled by option")
            return
        if location is None:
            state = CheckState.SKIPPED if enabled else CheckState.NOT_APPLICABLE
            self._checks[name] = CheckResult(
                name=name, state=state, detail="table is not stored in S3-compatible object storage"
            )
            return

        def run() -> str:
            assert location is not None
            client = self.options.s3_client or s3_client_from_properties(
                dict(self.table.io.properties), self.options.s3_properties
            )
            status = bucket_versioning_status(client, location.bucket)
            scanner = ObjectVersionScanner(client, self.inspector, self.options.max_download_bytes)
            self.version_hits = scanner.scan(location)
            signals = []
            for hit in self.version_hits:
                idents: list[ResolvedIdentifier | None] = [
                    i for i in self.idents if i.salted_hash in hit.inspection.hashes
                ] or [None]
                for ident in idents:
                    ident_hash = ident.salted_hash if ident else None
                    signals.append(
                        _Signal(
                            FindingKind.NONCURRENT_OBJECT_VERSION,
                            hit.inspection.confidence_for(ident_hash) or hit.confidence,
                            hit.uri,
                            ident,
                            (),
                            hit.inspection.rows.get(ident_hash, 0) if ident_hash else None,
                            "noncurrent object version is still retrievable: "
                            + hit.inspection.describe(ident_hash),
                            hit.version.version_id,
                        )
                    )
            self._emit(signals)
            return f"bucket versioning is {status}; {len(self.version_hits)} noncurrent version(s) matched"

        self._run_check(name, True, run)

    def _subject_evidence(self, findings: list[Finding]) -> SubjectEvidence:
        evidence: list[IdentifierEvidence] = []
        for ident in self.idents:
            own = [f for f in findings if f.identifier_hash == ident.salted_hash]
            evidence.append(
                IdentifierEvidence(
                    column=ident.column,
                    field_id=ident.field_id,
                    iceberg_type=str(ident.iceberg_type),
                    salted_hash=ident.salted_hash,
                    findings=len(own),
                    confirmed_matches=sum(
                        f.match_count or 0 for f in own if f.confidence is Confidence.CONFIRMED
                    ),
                )
            )
        return SubjectEvidence(
            hash_algorithm=HASH_ALGORITHM,
            salt_fingerprint=self.subject.salt_fingerprint,
            salt_was_generated=self.subject.salt_was_generated,
            identifiers=evidence,
        )


class _Skip(Exception):  # noqa: N818 - control flow, not an error
    """Raised inside a check to mark it as skipped with a reason."""


def scan_table(
    table: Table,
    subject: Subject,
    options: ScanOptions | None = None,
    *,
    table_identifier: str | None = None,
) -> Report:
    """Scan ``table`` for every recoverable trace of ``subject`` and return the evidence report."""
    return Scanner(table, subject, options, table_identifier=table_identifier).run()
