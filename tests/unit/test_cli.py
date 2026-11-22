from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import pytest
from pyiceberg.expressions import EqualTo
from pyiceberg.table import Table
from typer.testing import CliRunner

import afterimage
from afterimage.cli import EXIT_CLEAN, EXIT_INCOMPLETE, EXIT_RESIDUES, EXIT_USAGE, app

from ..conftest import ALICE_EMAIL, SALT

runner = CliRunner()
ENV = {"AFTERIMAGE_SALT": SALT}


@pytest.fixture
def catalog_args(warehouse: Path) -> list[str]:
    return [
        "-c",
        "test",
        "-p",
        "type=sql",
        "-p",
        f"uri=sqlite:///{warehouse}/catalog.db",
        "-p",
        f"warehouse=file://{warehouse}",
    ]


@pytest.fixture
def subject_file(tmp_path: Path) -> Path:
    path = tmp_path / "subject.txt"
    path.write_text(f"# subject\ncustomer_id=1\nemail={ALICE_EMAIL}\n", encoding="utf-8")
    return path


def test_version() -> None:
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert afterimage.__version__ in result.output


def test_scan_writes_reports_and_exits_with_residue_code(
    make_table: Callable[..., Table], catalog_args: list[str], subject_file: Path, tmp_path: Path
) -> None:
    table = make_table()
    table.delete(EqualTo("customer_id", 1))
    json_out = tmp_path / "report.json"
    md_out = tmp_path / "report.md"
    result = runner.invoke(
        app,
        [
            "scan",
            "-t",
            "crm.customers",
            *catalog_args,
            "--match-file",
            str(subject_file),
            "--json-out",
            str(json_out),
            "--markdown-out",
            str(md_out),
            "--log-level",
            "debug",
        ],
        env=ENV,
    )
    assert result.exit_code == EXIT_RESIDUES, result.output
    assert "verdict: residues_found" in result.output
    data = json.loads(json_out.read_text())
    assert data["verdict"] == "residues_found"
    assert data["subject"]["salt_was_generated"] is False
    for text in (json_out.read_text(), md_out.read_text(), result.output):
        assert ALICE_EMAIL not in text
        assert "customer_id=1/" not in text

    plan = runner.invoke(app, ["plan", "--report", str(json_out), "--catalog", "lake"])
    assert plan.exit_code == 0
    assert "CALL lake.system.expire_snapshots" in plan.output

    rendered = runner.invoke(app, ["render", "--report", str(json_out)])
    assert rendered.exit_code == 0
    assert rendered.output.startswith("# Erasure evidence report")


def test_scan_clean_to_stdout_with_inline_match(make_table: Callable[..., Table], catalog_args: list[str]) -> None:
    make_table()
    result = runner.invoke(
        app, ["scan", "-t", "crm.customers", *catalog_args, "-m", "email=nobody@example.com"], env=ENV
    )
    assert result.exit_code == EXIT_CLEAN, result.output
    assert json.loads(result.stdout)["verdict"] == "clean"


def test_scan_with_disabled_checks_is_incomplete(make_table: Callable[..., Table], catalog_args: list[str]) -> None:
    make_table()
    result = runner.invoke(
        app,
        ["scan", "-t", "crm.customers", *catalog_args, "-m", "email=nobody@example.com", "--no-orphans"],
        env=ENV,
    )
    assert result.exit_code == EXIT_INCOMPLETE


def test_generated_salt_warns(make_table: Callable[..., Table], catalog_args: list[str]) -> None:
    make_table()
    result = runner.invoke(
        app, ["scan", "-t", "crm.customers", *catalog_args, "-m", "email=nobody@example.com"], env={}
    )
    assert "random salt was generated" in result.stderr


def test_match_from_stdin(make_table: Callable[..., Table], catalog_args: list[str]) -> None:
    make_table()
    result = runner.invoke(
        app,
        ["scan", "-t", "crm.customers", *catalog_args, "--match-file", "-"],
        input=f"# from a secret store\nemail={ALICE_EMAIL}\n",
        env=ENV,
    )
    assert result.exit_code == EXIT_RESIDUES
    assert ALICE_EMAIL not in result.output


@pytest.mark.parametrize(
    "args",
    [
        [],
        ["-m", "no-separator"],
        ["-m", "email=x", "-p", "broken"],
    ],
)
def test_usage_errors(args: list[str], make_table: Callable[..., Table], catalog_args: list[str]) -> None:
    make_table()
    result = runner.invoke(app, ["scan", "-t", "crm.customers", *catalog_args, *args], env=ENV)
    assert result.exit_code == EXIT_USAGE


def test_purge_versions_requires_object_storage(
    make_table: Callable[..., Table], catalog_args: list[str], subject_file: Path
) -> None:
    make_table()
    result = runner.invoke(
        app, ["purge-versions", "-t", "crm.customers", *catalog_args, "--match-file", str(subject_file)], env=ENV
    )
    assert result.exit_code == EXIT_INCOMPLETE
    assert "not stored in S3" in result.stderr
