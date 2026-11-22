"""Inspect individual Iceberg files (data, delete, manifest, manifest list) for a subject.

Used for files that are not reachable from the current table metadata, such as orphan
files and noncurrent object versions, where we only have a path or raw bytes.
"""

from __future__ import annotations

import logging
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from pyiceberg.io import FileIO
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import ManifestContent, ManifestEntryStatus, ManifestFile, read_manifest_list
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema

from afterimage.matching import (
    DEFAULT_TRUNCATE_LENGTH,
    ResolvedIdentifier,
    count_parquet_matches,
    match_bounds,
    match_partition,
    match_partition_summaries,
)
from afterimage.models import Confidence

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Signal:
    """One piece of evidence inside a file. ``ident_hash`` is ``None`` for unattributed signals."""

    ident_hash: str | None
    confidence: Confidence
    text: str


@dataclass
class Inspection:
    """What a single file reveals about the subject."""

    rows: dict[str, int] = field(default_factory=dict)
    signals: list[Signal] = field(default_factory=list)
    parsed: bool = True

    @property
    def matched_rows(self) -> int:
        return sum(self.rows.values())

    @property
    def hashes(self) -> set[str]:
        found = {h for h, n in self.rows.items() if n}
        found |= {s.ident_hash for s in self.signals if s.ident_hash}
        return found

    def add(self, ident_hash: str | None, confidence: Confidence, text: str) -> None:
        self.signals.append(Signal(ident_hash, confidence, text))

    def confidence_for(self, ident_hash: str | None = None) -> Confidence | None:
        rows = self.rows.get(ident_hash, 0) if ident_hash else self.matched_rows
        relevant = [s for s in self.signals if ident_hash is None or s.ident_hash in (ident_hash, None)]
        if rows or any(s.confidence is Confidence.CONFIRMED for s in relevant):
            return Confidence.CONFIRMED
        if relevant or not self.parsed:
            return Confidence.POSSIBLE
        return None

    @property
    def confidence(self) -> Confidence | None:
        return self.confidence_for(None)

    def describe(self, ident_hash: str | None = None) -> str:
        parts: list[str] = []
        rows = self.rows.get(ident_hash, 0) if ident_hash else self.matched_rows
        if rows:
            parts.append(f"{rows} matching row(s)")
        ordered = sorted(self.signals, key=lambda s: s.confidence is not Confidence.CONFIRMED)
        parts.extend(s.text for s in ordered if ident_hash is None or s.ident_hash in (ident_hash, None))
        if not self.parsed:
            parts.append("file could not be parsed; review manually")
        return "; ".join(dict.fromkeys(parts))


def file_kind(uri: str) -> str:
    name = uri.rsplit("/", 1)[-1]
    if name.endswith(".parquet"):
        return "parquet"
    if name.endswith(".avro"):
        return "manifest_list" if name.startswith("snap-") else "manifest"
    if name.endswith(".metadata.json") or name.endswith(".metadata.json.gz"):
        return "table_metadata"
    return "other"


class FileInspector:
    def __init__(
        self,
        schema: Schema,
        specs: dict[int, PartitionSpec],
        idents: list[ResolvedIdentifier],
        truncate_length: int = DEFAULT_TRUNCATE_LENGTH,
    ) -> None:
        self.schema = schema
        self.specs = specs
        self.idents = idents
        self.truncate_length = truncate_length

    # -- parquet -----------------------------------------------------------
    def parquet(self, io: FileIO, uri: str) -> Inspection:
        result = Inspection()
        try:
            with io.new_input(uri).open() as stream:
                counts = count_parquet_matches(stream, self.idents)
        except Exception:
            logger.warning("could not read parquet file %s", uri, exc_info=logger.isEnabledFor(logging.DEBUG))
            result.parsed = False
            return result
        result.rows = {h: n for h, n in counts.items() if n}
        return result

    # -- manifests ---------------------------------------------------------
    def _spec_for(self, spec_id: int | None, partition_len: int) -> PartitionSpec | None:
        if spec_id is not None and spec_id in self.specs:
            return self.specs[spec_id]
        candidates = [s for s in self.specs.values() if len(s.fields) == partition_len]
        return candidates[0] if len(candidates) == 1 else None

    def manifest(self, io: FileIO, uri: str, length: int) -> Inspection:
        result = Inspection()
        manifest = ManifestFile.from_args(
            manifest_path=uri,
            manifest_length=length,
            partition_spec_id=0,
            content=ManifestContent.DATA,
            sequence_number=0,
            min_sequence_number=0,
            added_snapshot_id=0,
        )
        try:
            entries = manifest.fetch_manifest_entry(io, discard_deleted=False)
        except Exception:
            logger.debug("could not parse %s as a manifest", uri, exc_info=True)
            result.parsed = False
            return result
        for entry in entries:
            data_file = entry.data_file
            deleted = " (DELETED entry)" if entry.status == ManifestEntryStatus.DELETED else ""
            for ident in self.idents:
                for bound in match_bounds(
                    ident,
                    data_file.lower_bounds.get(ident.field_id) if data_file.lower_bounds else None,
                    data_file.upper_bounds.get(ident.field_id) if data_file.upper_bounds else None,
                    self.truncate_length,
                ):
                    result.add(ident.salted_hash, bound.confidence, f"{bound.detail}{deleted}")
            spec = self._spec_for(None, len(data_file.partition))
            if spec is None:
                continue
            for pmatch in match_partition(spec, data_file.partition, self.idents):
                result.add(pmatch.ident.salted_hash, pmatch.confidence, pmatch.detail + deleted)
        return result

    def manifest_list(self, io: FileIO, uri: str) -> Inspection:
        result = Inspection()
        try:
            manifests = list(read_manifest_list(io.new_input(uri)))
        except Exception:
            logger.debug("could not parse %s as a manifest list", uri, exc_info=True)
            result.parsed = False
            return result
        for manifest in manifests:
            spec = self.specs.get(manifest.partition_spec_id)
            if spec is None:
                continue
            for pmatch in match_partition_summaries(spec, self.schema, manifest.partitions, self.idents):
                result.add(pmatch.ident.salted_hash, pmatch.confidence, pmatch.detail)
        return result

    # -- dispatch ----------------------------------------------------------
    def inspect(self, io: FileIO, uri: str, length: int | None = None) -> Inspection | None:
        """Inspect a file by kind. Returns ``None`` for file kinds that cannot hold the subject."""
        kind = file_kind(uri)
        if kind == "parquet":
            return self.parquet(io, uri)
        if kind == "manifest":
            if length is None:
                length = io.new_input(uri).__len__()
            result = self.manifest(io, uri, length)
            if not result.parsed:  # might be a manifest list with a non-standard name
                fallback = self.manifest_list(io, uri)
                if fallback.parsed:
                    return fallback
            return result
        if kind == "manifest_list":
            return self.manifest_list(io, uri)
        if kind == "table_metadata":
            # Table metadata is not encrypted even in encrypted tables and can point at
            # snapshots that are no longer referenced. Flag it for review.
            result = Inspection()
            result.add(None, Confidence.POSSIBLE, "table metadata file may list snapshots that reveal the subject")
            return result
        return None

    def inspect_bytes(self, name: str, data: bytes) -> Inspection | None:
        """Inspect raw bytes (for example a noncurrent S3 object version)."""
        if file_kind(name) == "other":
            return None
        suffix = "".join(Path(name).suffixes[-2:]) or ".bin"
        local_name = Path(name).name if not Path(name).name.startswith(".") else "object" + suffix
        with tempfile.TemporaryDirectory(prefix="afterimage-") as tmp:
            path = Path(tmp) / local_name
            path.write_bytes(data)
            return self.inspect(PyArrowFileIO(), f"file://{path}", len(data))
