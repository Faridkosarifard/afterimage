"""Render reports as JSON (the machine-readable artefact) or Markdown (for humans and tickets)."""

from __future__ import annotations

from afterimage.models import CheckState, Report, Verdict

_VERDICT_TEXT = {
    Verdict.CLEAN: "CLEAN: no recoverable trace of the subject was found and every check ran.",
    Verdict.RESIDUES_FOUND: "RESIDUES FOUND: the subject can still be recovered from the locations below.",
    Verdict.INCOMPLETE: "INCOMPLETE: nothing was found, but at least one check did not run.",
}


def to_json(report: Report) -> str:
    return report.model_dump_json(indent=2) + "\n"


def _cell(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")


def to_markdown(report: Report) -> str:
    lines = [
        f"# Erasure evidence report: `{report.table.identifier}`",
        "",
        f"**Verdict:** {_VERDICT_TEXT[report.verdict]}",
        "",
        f"- Generated: {report.generated_at.isoformat()}",
        f"- Tool: {report.tool} {report.tool_version} (report schema {report.schema_version})",
        f"- Table location: `{report.table.location}`",
        f"- Format version: {report.table.format_version}; snapshots: {report.table.snapshot_count}; "
        f"current snapshot: {report.table.current_snapshot_id}",
        f"- Subject hash: {report.subject.hash_algorithm}, salt fingerprint "
        f"`{report.subject.salt_fingerprint}`"
        + (" (salt generated for this run and not stored)" if report.subject.salt_was_generated else ""),
        "",
        "## Identifiers",
        "",
        "| Column | Type | Salted hash | Findings | Confirmed matching rows |",
        "|---|---|---|---|---|",
    ]
    for ident in report.subject.identifiers:
        lines.append(
            f"| `{ident.column}` | {ident.iceberg_type} | `{ident.salted_hash[:16]}…` | {ident.findings} | "
            f"{ident.confirmed_matches} |"
        )
    lines += ["", "## Checks", "", "| Check | State | Detail |", "|---|---|---|"]
    for check in report.checks:
        marker = "" if check.state in (CheckState.RAN, CheckState.NOT_APPLICABLE) else " ⚠"
        lines.append(f"| {check.name} | {check.state.value}{marker} | {_cell(check.detail)} |")
    lines += ["", "## Findings", ""]
    if not report.findings:
        lines.append("No findings.")
    else:
        lines += [
            "| # | Kind | Confidence | Column | Location | Snapshots | Rows | Detail |",
            "|---|---|---|---|---|---|---|---|",
        ]
        for number, finding in enumerate(report.findings, start=1):
            location = finding.location
            if finding.object_version_id:
                location += f" (version {finding.object_version_id})"
            snapshots = ", ".join(str(s) for s in finding.snapshot_ids) or "-"
            rows = "-" if finding.match_count is None else str(finding.match_count)
            lines.append(
                f"| {number} | {finding.kind.value} | {finding.confidence.value} | {finding.column or '-'} | "
                f"`{_cell(location)}` | {snapshots} | {rows} | {_cell(finding.detail)} |"
            )
    lines += ["", "---", "", f"_{report.disclaimer}_", ""]
    return "\n".join(lines)
