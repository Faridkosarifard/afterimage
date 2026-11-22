from __future__ import annotations

from afterimage.models import FindingKind
from afterimage.plan import build_plan, render_plan

from .factories import finding, report


def keys(findings, **kwargs):  # type: ignore[no-untyped-def]
    return [step.key for step in build_plan(report(findings, **kwargs))]


def test_clean_report_has_no_plan() -> None:
    assert build_plan(report([])) == []
    assert "Nothing to remediate" in render_plan([])


def test_live_rows_start_with_logical_delete() -> None:
    assert keys([finding(FindingKind.LIVE_ROWS, [10])])[0] == "logical_delete"


def test_merge_on_read_residues_need_rewrite_before_expiry() -> None:
    result = keys([finding(FindingKind.UNREWRITTEN_DATA_FILE, [10]), finding(FindingKind.COLUMN_BOUND, [10])])
    assert result.index("rewrite_data_files") < result.index("expire_snapshots")
    assert result[-2:] == ["prevent", "rescan"]


def test_history_on_local_storage_skips_object_versions() -> None:
    result = keys([finding(FindingKind.HISTORICAL_SNAPSHOT_ROWS, [5])])
    assert result == ["expire_snapshots", "remove_orphan_files", "rescan"]


def test_history_on_s3_ends_with_version_purge() -> None:
    result = keys([finding(FindingKind.HISTORICAL_DATA_FILE, [5])], location="s3://lake/wh/crm/customers")
    assert result == ["expire_snapshots", "remove_orphan_files", "purge_noncurrent_versions", "rescan"]


def test_noncurrent_versions_only() -> None:
    result = keys([finding(FindingKind.NONCURRENT_OBJECT_VERSION)])
    assert result == ["purge_noncurrent_versions", "rescan"]


def test_metadata_log_step() -> None:
    result = keys([finding(FindingKind.METADATA_LOG_SNAPSHOT, [1])])
    assert "clean_metadata_log" in result


def test_render_plan_contains_commands_and_no_values() -> None:
    steps = build_plan(report([finding(FindingKind.EQUALITY_DELETE_FILE, [10])]))
    text = render_plan(steps)
    assert "rewrite_data_files" in text
    assert "Spark SQL:" in text
    assert "Addresses: unrewritten_data_file, equality_delete_file" in text
    assert text.startswith("1. ")
