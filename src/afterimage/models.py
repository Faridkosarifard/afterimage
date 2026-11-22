"""Pydantic models for the erasure evidence report.

The JSON produced from :class:`Report` is the stable, machine-readable artefact.
``schema_version`` follows semantic versioning: additive changes bump the minor
version, anything that could break a consumer bumps the major version.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

REPORT_SCHEMA_VERSION = "1.0"

DISCLAIMER = (
    "This report is technical evidence gathered by an automated scan. It is not legal advice and "
    "does not by itself establish compliance with GDPR Article 17 or any other law."
)


class FindingKind(StrEnum):
    """Where a residue of the subject was found."""

    LIVE_ROWS = "live_rows"
    """Rows matching the subject are visible in the current snapshot (no logical delete yet)."""
    HISTORICAL_SNAPSHOT_ROWS = "historical_snapshot_rows"
    """Rows are still readable through time travel to an older snapshot."""
    UNREWRITTEN_DATA_FILE = "unrewritten_data_file"
    """A current data file still physically contains the rows; they are only masked by delete files."""
    HISTORICAL_DATA_FILE = "historical_data_file"
    """A data file referenced only by older snapshots still exists and contains the rows."""
    EQUALITY_DELETE_FILE = "equality_delete_file"
    """An equality-delete file stores the identifier itself in order to delete it."""
    COLUMN_BOUND = "column_bound"
    """A manifest lower/upper bound equals the identifier or its truncated prefix."""
    PARTITION_VALUE = "partition_value"
    """A partition tuple (and usually the Hive-style file path) is derived from the identifier."""
    PARTITION_SUMMARY = "partition_summary"
    """A manifest-list partition summary bound equals the partition value derived from the identifier."""
    METADATA_LOG_SNAPSHOT = "metadata_log_snapshot"
    """A previous metadata.json still lists an expired snapshot whose files reveal the subject."""
    ORPHAN_DATA_FILE = "orphan_data_file"
    """A data file no longer referenced by any metadata still exists in storage and contains the rows."""
    ORPHAN_METADATA_FILE = "orphan_metadata_file"
    """An unreferenced manifest, manifest list or metadata file may still describe the subject."""
    NONCURRENT_OBJECT_VERSION = "noncurrent_object_version"
    """An object store keeps a noncurrent (deleted or overwritten) version of a file that held the subject."""


class Confidence(StrEnum):
    CONFIRMED = "confirmed"
    """The identifier value was matched exactly."""
    POSSIBLE = "possible"
    """A partial match (truncated bound, truncated partition, unparsed metadata) that needs review."""


class Verdict(StrEnum):
    CLEAN = "clean"
    """Every check ran and nothing was found."""
    RESIDUES_FOUND = "residues_found"
    INCOMPLETE = "incomplete"
    """Nothing was found, but at least one check was skipped or failed, so the result proves less."""


class CheckState(StrEnum):
    RAN = "ran"
    SKIPPED = "skipped"
    ERROR = "error"
    NOT_APPLICABLE = "not_applicable"
    """The check does not apply to this table (for example object versions on a local filesystem)."""


class Finding(BaseModel):
    model_config = ConfigDict(frozen=True)

    kind: FindingKind
    confidence: Confidence
    location: str = Field(description="Redacted file URI, object key or metadata location")
    identifier_hash: str | None = Field(
        default=None, description="Salted hash of the identifier that matched (never the raw value)"
    )
    column: str | None = None
    snapshot_ids: list[int] = Field(default_factory=list)
    match_count: int | None = Field(default=None, description="Number of matching rows, where countable")
    object_version_id: str | None = None
    detail: str = ""


class CheckResult(BaseModel):
    name: str
    state: CheckState
    detail: str = ""


class IdentifierEvidence(BaseModel):
    column: str
    field_id: int | None = None
    iceberg_type: str | None = None
    salted_hash: str
    findings: int = 0
    confirmed_matches: int = Field(default=0, description="Sum of match_count over confirmed findings")


class SubjectEvidence(BaseModel):
    hash_algorithm: str
    salt_fingerprint: str
    salt_was_generated: bool = Field(
        description="True when no salt was supplied; the hash then cannot be re-derived later"
    )
    identifiers: list[IdentifierEvidence]


class TableInfo(BaseModel):
    identifier: str
    location: str
    format_version: int
    current_snapshot_id: int | None
    snapshot_count: int
    metadata_location: str | None = None


class Report(BaseModel):
    schema_version: str = REPORT_SCHEMA_VERSION
    tool: str = "afterimage"
    tool_version: str
    generated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    table: TableInfo
    subject: SubjectEvidence
    verdict: Verdict
    checks: list[CheckResult]
    findings: list[Finding]
    summary: dict[str, int] = Field(default_factory=dict, description="Number of findings per kind")
    disclaimer: str = DISCLAIMER

    @property
    def is_clean(self) -> bool:
        return self.verdict is Verdict.CLEAN

    def findings_of(self, *kinds: FindingKind) -> list[Finding]:
        return [f for f in self.findings if f.kind in kinds]


def compute_verdict(findings: list[Finding], checks: list[CheckResult]) -> Verdict:
    if findings:
        return Verdict.RESIDUES_FOUND
    if any(c.state in (CheckState.SKIPPED, CheckState.ERROR) for c in checks):
        return Verdict.INCOMPLETE
    return Verdict.CLEAN


def summarize(findings: list[Finding]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for finding in findings:
        counts[finding.kind.value] = counts.get(finding.kind.value, 0) + 1
    return dict(sorted(counts.items()))
