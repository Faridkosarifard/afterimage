from __future__ import annotations

from afterimage.models import (
    CheckResult,
    CheckState,
    FindingKind,
    Report,
    Verdict,
    compute_verdict,
    summarize,
)
from afterimage.render import to_json

from .factories import finding, report


def test_verdicts() -> None:
    ran = [CheckResult(name="a", state=CheckState.RAN), CheckResult(name="b", state=CheckState.NOT_APPLICABLE)]
    assert compute_verdict([], ran) is Verdict.CLEAN
    assert compute_verdict([], [*ran, CheckResult(name="c", state=CheckState.SKIPPED)]) is Verdict.INCOMPLETE
    assert compute_verdict([], [CheckResult(name="c", state=CheckState.ERROR)]) is Verdict.INCOMPLETE
    assert compute_verdict([finding(FindingKind.LIVE_ROWS)], ran) is Verdict.RESIDUES_FOUND


def test_summarize_counts_by_kind() -> None:
    findings = [finding(FindingKind.COLUMN_BOUND), finding(FindingKind.COLUMN_BOUND), finding(FindingKind.LIVE_ROWS)]
    assert summarize(findings) == {"column_bound": 2, "live_rows": 1}


def test_json_round_trip() -> None:
    original = report([finding(FindingKind.ORPHAN_DATA_FILE, match_count=3)])
    restored = Report.model_validate_json(to_json(original))
    assert restored == original
    assert not restored.is_clean
    assert restored.findings_of(FindingKind.ORPHAN_DATA_FILE)
    assert "not legal advice" in restored.disclaimer
