from __future__ import annotations

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


def finding(kind: FindingKind, snapshot_ids: list[int] | None = None, **kwargs: object) -> Finding:
    values: dict[str, object] = {
        "kind": kind,
        "confidence": Confidence.CONFIRMED,
        "location": "file:///wh/crm/customers/data/x.parquet",
        "snapshot_ids": snapshot_ids or [],
    }
    values.update(kwargs)
    return Finding.model_validate(values)


def report(findings: list[Finding], location: str = "file:///wh/crm/customers", current: int | None = 10) -> Report:
    checks = [CheckResult(name="manifest_metadata", state=CheckState.RAN)]
    return Report(
        tool_version="0.0.0",
        table=TableInfo(
            identifier="crm.customers",
            location=location,
            format_version=2,
            current_snapshot_id=current,
            snapshot_count=2,
        ),
        subject=SubjectEvidence(
            hash_algorithm="hmac-sha256",
            salt_fingerprint="f" * 16,
            salt_was_generated=False,
            identifiers=[IdentifierEvidence(column="email", salted_hash="a" * 64, iceberg_type="string")],
        ),
        verdict=compute_verdict(findings, checks),
        checks=checks,
        findings=findings,
        summary=summarize(findings),
    )
