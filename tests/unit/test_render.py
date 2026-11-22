from __future__ import annotations

from afterimage.models import FindingKind
from afterimage.render import to_markdown

from .factories import finding, report


def test_markdown_clean() -> None:
    text = to_markdown(report([]))
    assert "CLEAN" in text
    assert "No findings." in text
    assert "not legal advice" in text


def test_markdown_findings_table_escapes_pipes() -> None:
    text = to_markdown(
        report(
            [
                finding(
                    FindingKind.NONCURRENT_OBJECT_VERSION,
                    object_version_id="v1",
                    detail="a | b",
                    match_count=2,
                )
            ]
        )
    )
    assert "RESIDUES FOUND" in text
    assert "(version v1)" in text
    assert "a \\| b" in text
