"""Command line interface: ``afterimage scan | plan | purge-versions | render``."""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path
from typing import Annotated

import typer

import afterimage
from afterimage.catalog import open_table, parse_properties
from afterimage.logs import configure_logging
from afterimage.models import Report, Verdict
from afterimage.objects import purge_versions
from afterimage.plan import build_plan, render_plan
from afterimage.render import to_json, to_markdown
from afterimage.scanner import Scanner, ScanOptions
from afterimage.subject import Identifier, IdentifierError, Subject, read_identifier_file

EXIT_CLEAN = 0
EXIT_RESIDUES = 3
EXIT_INCOMPLETE = 4
EXIT_USAGE = 2

DEFAULT_SALT_ENV = "AFTERIMAGE_SALT"

app = typer.Typer(
    name="afterimage",
    help=(
        "Find every place a data subject can still be recovered in an Apache Iceberg table after a "
        "DELETE, and produce erasure evidence. Evidence gathering only; not legal advice."
    ),
    no_args_is_help=True,
    add_completion=False,
    pretty_exceptions_enable=False,
)

logger = logging.getLogger("afterimage.cli")

CatalogOpt = Annotated[str, typer.Option("--catalog", "-c", help="PyIceberg catalog name.")]
PropOpt = Annotated[
    list[str] | None,
    typer.Option("--catalog-prop", "-p", help="Catalog property key=value (repeatable), e.g. uri=..."),
]
TableOpt = Annotated[str, typer.Option("--table", "-t", help="Table identifier, e.g. crm.customers.")]
MatchOpt = Annotated[
    list[str] | None,
    typer.Option(
        "--match",
        "-m",
        help="Identifier column=value (repeatable). Prefer --match-file to keep values out of shell history.",
    ),
]
MatchFileOpt = Annotated[
    Path | None,
    typer.Option("--match-file", help="File with one column=value per line; '-' reads stdin."),
]
SaltEnvOpt = Annotated[
    str,
    typer.Option("--salt-env", help="Environment variable holding the HMAC salt for identifier hashes."),
]
LogLevelOpt = Annotated[str, typer.Option("--log-level", help="DEBUG, INFO, WARNING or ERROR.")]


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"afterimage {afterimage.__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        bool, typer.Option("--version", callback=_version_callback, is_eager=True, help="Show the version.")
    ] = False,
) -> None:
    """Afterimage command line."""


def _subject(matches: list[str] | None, match_file: Path | None, salt_env: str) -> Subject:
    identifiers: list[Identifier] = []
    try:
        for spec in matches or []:
            identifiers.append(Identifier.parse(spec))
        if match_file is not None:
            if str(match_file) == "-":
                text = sys.stdin.read()
                tmp = [line.strip() for line in text.splitlines() if line.strip() and not line.startswith("#")]
                identifiers.extend(Identifier.parse(line) for line in tmp)
            else:
                identifiers.extend(read_identifier_file(match_file))
        if not identifiers:
            raise IdentifierError("provide at least one --match or --match-file")
        salt = os.environ.get(salt_env)
        subject = Subject(identifiers, salt=salt)
    except IdentifierError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(EXIT_USAGE) from None
    if subject.salt_was_generated:
        typer.echo(
            f"warning: ${salt_env} is not set; a random salt was generated, so identifier hashes in this "
            "report cannot be re-derived later.",
            err=True,
        )
    return subject


def _exit_code(report: Report) -> int:
    return {
        Verdict.CLEAN: EXIT_CLEAN,
        Verdict.RESIDUES_FOUND: EXIT_RESIDUES,
        Verdict.INCOMPLETE: EXIT_INCOMPLETE,
    }[report.verdict]


def _load(catalog: str, props: list[str] | None, table: str):  # type: ignore[no-untyped-def]
    try:
        properties = parse_properties(props or [])
    except ValueError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(EXIT_USAGE) from None
    return open_table(catalog, table, properties), properties


def _s3_overrides(properties: dict[str, str]) -> dict[str, str]:
    return {k: v for k, v in properties.items() if k.startswith(("s3.", "client."))}


@app.command()
def scan(
    table: TableOpt,
    catalog: CatalogOpt = "default",
    catalog_prop: PropOpt = None,
    match: MatchOpt = None,
    match_file: MatchFileOpt = None,
    salt_env: SaltEnvOpt = DEFAULT_SALT_ENV,
    json_out: Annotated[Path | None, typer.Option("--json-out", help="Write the JSON report here.")] = None,
    markdown_out: Annotated[Path | None, typer.Option("--markdown-out", help="Write the Markdown report here.")] = None,
    time_travel: Annotated[bool, typer.Option(help="Query every snapshot with deletes applied.")] = True,
    file_contents: Annotated[bool, typer.Option(help="Read data and equality-delete files.")] = True,
    metadata_log: Annotated[bool, typer.Option(help="Inspect previous metadata.json files.")] = True,
    orphans: Annotated[bool, typer.Option(help="List the table location for unreferenced files.")] = True,
    object_versions: Annotated[
        bool | None,
        typer.Option(help="Check noncurrent S3 object versions (default: when the table is on S3)."),
    ] = None,
    truncate_length: Annotated[int, typer.Option(help="String metrics truncation length.")] = 16,
    log_level: LogLevelOpt = "WARNING",
) -> None:
    """Scan a table for every recoverable trace of a data subject."""
    subject = _subject(match, match_file, salt_env)
    configure_logging(log_level.upper(), subject.redactor())
    iceberg_table, properties = _load(catalog, catalog_prop, table)
    options = ScanOptions(
        time_travel=time_travel,
        file_contents=file_contents,
        metadata_log=metadata_log,
        orphan_files=orphans,
        object_versions=object_versions,
        truncate_length=truncate_length,
        s3_properties=_s3_overrides(properties),
    )
    report = Scanner(iceberg_table, subject, options, table_identifier=table).run()
    if json_out:
        json_out.write_text(to_json(report), encoding="utf-8")
    if markdown_out:
        markdown_out.write_text(to_markdown(report), encoding="utf-8")
    if not json_out and not markdown_out:
        typer.echo(to_json(report), nl=False)
    else:
        typer.echo(f"verdict: {report.verdict.value}; findings: {len(report.findings)}", err=True)
    raise typer.Exit(_exit_code(report))


@app.command()
def plan(
    report: Annotated[Path, typer.Option("--report", "-r", help="JSON report produced by 'scan'.")],
    catalog: CatalogOpt = "my_catalog",
) -> None:
    """Print the ordered remediation steps for a report."""
    parsed = Report.model_validate_json(report.read_text(encoding="utf-8"))
    typer.echo(render_plan(build_plan(parsed, catalog=catalog)), nl=False)


@app.command()
def render(
    report: Annotated[Path, typer.Option("--report", "-r", help="JSON report produced by 'scan'.")],
) -> None:
    """Render a JSON report as Markdown."""
    parsed = Report.model_validate_json(report.read_text(encoding="utf-8"))
    typer.echo(to_markdown(parsed), nl=False)


@app.command("purge-versions")
def purge_versions_command(
    table: TableOpt,
    catalog: CatalogOpt = "default",
    catalog_prop: PropOpt = None,
    match: MatchOpt = None,
    match_file: MatchFileOpt = None,
    salt_env: SaltEnvOpt = DEFAULT_SALT_ENV,
    include_possible: Annotated[
        bool, typer.Option(help="Also purge versions that only possibly reveal the subject.")
    ] = False,
    confirm: Annotated[
        bool,
        typer.Option("--confirm", help="Actually delete. Without it, only prints what would be deleted."),
    ] = False,
    log_level: LogLevelOpt = "WARNING",
) -> None:
    """Permanently delete noncurrent S3 object versions that reveal the subject (dry run by default).

    Versions are re-discovered by a fresh scan rather than read from an old report, so
    nothing is deleted on the basis of stale or tampered evidence.
    """
    subject = _subject(match, match_file, salt_env)
    redact = subject.redactor()
    configure_logging(log_level.upper(), redact)
    iceberg_table, properties = _load(catalog, catalog_prop, table)
    options = ScanOptions(
        manifest_metadata=False,
        file_contents=False,
        time_travel=False,
        metadata_log=False,
        orphan_files=False,
        object_versions=True,
        s3_properties=_s3_overrides(properties),
    )
    scanner = Scanner(iceberg_table, subject, options, table_identifier=table)
    report = scanner.run()
    check = next(c for c in report.checks if c.name == "object_versions")
    if check.state.value != "ran":
        typer.echo(f"error: object version check {check.state.value}: {check.detail}", err=True)
        raise typer.Exit(EXIT_INCOMPLETE)
    targets = [hit for hit in scanner.version_hits if include_possible or hit.confidence.value == "confirmed"]
    skipped = len(scanner.version_hits) - len(targets)
    if scanner.s3_client is None:  # pragma: no cover - set whenever the check ran
        raise typer.Exit(EXIT_INCOMPLETE)
    outcomes = purge_versions(scanner.s3_client, targets, confirm=confirm)
    results = []
    for outcome in outcomes:
        results.append(
            {
                "location": redact(outcome.hit.uri),
                "version_id": outcome.hit.version.version_id,
                "confidence": outcome.hit.confidence.value,
                "deleted": outcome.deleted,
                "error": redact(outcome.error) if outcome.error else None,
            }
        )
    summary = {
        "mode": "delete" if confirm else "dry-run",
        "targets": len(targets),
        "deleted": sum(1 for o in outcomes if o.deleted),
        "failed": sum(1 for o in outcomes if o.error),
        "skipped_possible": skipped,
        "versions": results,
    }
    typer.echo(json.dumps(summary, indent=2))
    if not confirm and targets:
        typer.echo("dry run: re-run with --confirm to permanently delete these versions.", err=True)
    if any(o.error for o in outcomes):
        raise typer.Exit(1)


if __name__ == "__main__":  # pragma: no cover
    app()
