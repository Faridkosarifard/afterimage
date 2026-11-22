"""Pure functions that decide whether Iceberg metadata or file contents reveal a subject."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import IO, Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyiceberg.conversions import from_bytes
from pyiceberg.manifest import PartitionFieldSummary
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform, TruncateTransform
from pyiceberg.typedef import Record
from pyiceberg.types import IcebergType, PrimitiveType, StringType

from afterimage.models import Confidence
from afterimage.subject import Identifier, IdentifierError, Subject, bound_form, coerce_value

#: PyIceberg, Spark and Flink all default to ``truncate(16)`` metrics for strings.
DEFAULT_TRUNCATE_LENGTH = 16


@dataclass(frozen=True)
class ResolvedIdentifier:
    """An :class:`Identifier` bound to a field of the table schema."""

    identifier: Identifier
    field_id: int
    field_name: str
    iceberg_type: PrimitiveType
    value: Any = field(repr=False)
    salted_hash: str

    @property
    def column(self) -> str:
        return self.identifier.column

    @property
    def bound_value(self) -> Any:
        return bound_form(self.value)


def resolve_identifiers(subject: Subject, schema: Schema) -> list[ResolvedIdentifier]:
    resolved: list[ResolvedIdentifier] = []
    for ident in subject.identifiers:
        try:
            nested = schema.find_field(ident.column, case_sensitive=False)
        except ValueError:
            raise IdentifierError(f"column {ident.column!r} does not exist in the table schema") from None
        if not isinstance(nested.field_type, PrimitiveType):
            raise IdentifierError(f"column {ident.column!r} is not a primitive column")
        name = schema.find_column_name(nested.field_id) or ident.column
        resolved.append(
            ResolvedIdentifier(
                identifier=ident,
                field_id=nested.field_id,
                field_name=name,
                iceberg_type=nested.field_type,
                value=coerce_value(ident.raw, nested.field_type),
                salted_hash=subject.salted_hash(ident),
            )
        )
    return resolved


# ---------------------------------------------------------------------------
# Column bounds
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BoundMatch:
    confidence: Confidence
    which: str  # "lower" | "upper"
    detail: str


def _decode(iceberg_type: IcebergType, raw: bytes | None) -> Any:
    if raw is None:
        return None
    try:
        return from_bytes(iceberg_type, raw)  # type: ignore[arg-type]
    except Exception:  # corrupt or foreign bounds should not abort a scan
        return None


def match_bounds(
    ident: ResolvedIdentifier,
    lower: bytes | None,
    upper: bytes | None,
    truncate_length: int = DEFAULT_TRUNCATE_LENGTH,
) -> list[BoundMatch]:
    """Return how the lower/upper bounds of one column reveal the identifier, if at all."""
    matches: list[BoundMatch] = []
    value = ident.bound_value
    lo = _decode(ident.iceberg_type, lower)
    hi = _decode(ident.iceberg_type, upper)
    if lo is not None and lo == value:
        matches.append(BoundMatch(Confidence.CONFIRMED, "lower", "lower bound equals the identifier"))
    if hi is not None and hi == value:
        matches.append(BoundMatch(Confidence.CONFIRMED, "upper", "upper bound equals the identifier"))
    if matches or not isinstance(ident.iceberg_type, StringType) or not isinstance(value, str):
        return matches
    width = truncate_length
    if len(value) > width:
        if isinstance(lo, str) and len(lo) == width and value.startswith(lo):
            matches.append(
                BoundMatch(
                    Confidence.POSSIBLE,
                    "lower",
                    f"lower bound is the identifier truncated to {width} characters",
                )
            )
        if (
            isinstance(hi, str)
            and len(hi) == width
            and hi[:-1] == value[: width - 1]
            and ord(hi[-1]) == ord(value[width - 1]) + 1
        ):
            matches.append(
                BoundMatch(
                    Confidence.POSSIBLE,
                    "upper",
                    f"upper bound is the identifier truncated to {width} characters with the last "
                    "character incremented",
                )
            )
    return matches


def may_contain(ident: ResolvedIdentifier, lower: bytes | None, upper: bytes | None) -> bool:
    """Bounds-based pruning: False only when the bounds prove the value cannot be in the file."""
    value = ident.bound_value
    lo = _decode(ident.iceberg_type, lower)
    hi = _decode(ident.iceberg_type, upper)
    try:
        if lo is not None and value < lo:
            return False
        if hi is not None and value > hi:
            return False
    except TypeError:
        return True
    return True


# ---------------------------------------------------------------------------
# Partitions
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PartitionMatch:
    ident: ResolvedIdentifier
    partition_field: str
    confidence: Confidence
    detail: str


def _transformed(ident: ResolvedIdentifier, transform: Any) -> tuple[Any, Confidence] | None:
    if isinstance(transform, IdentityTransform):
        return ident.value, Confidence.CONFIRMED
    if isinstance(transform, TruncateTransform):
        func = transform.transform(ident.iceberg_type)
        return func(ident.value), Confidence.POSSIBLE
    # bucket[N] and time transforms are shared by many subjects: not identifying on their own.
    return None


def _equal(left: Any, right: Any) -> bool:
    return bool(left == right or bound_form(left) == bound_form(right))


def match_partition(
    spec: PartitionSpec,
    partition: Record,
    idents: list[ResolvedIdentifier],
) -> list[PartitionMatch]:
    matches: list[PartitionMatch] = []
    for pos, pfield in enumerate(spec.fields):
        if pos >= len(partition):
            break
        for ident in idents:
            if pfield.source_id != ident.field_id:
                continue
            transformed = _transformed(ident, pfield.transform)
            if transformed is None:
                continue
            expected, confidence = transformed
            if _equal(partition[pos], expected):
                matches.append(
                    PartitionMatch(
                        ident,
                        pfield.name,
                        confidence,
                        f"partition field {pfield.name!r} ({pfield.transform}) is derived from the identifier",
                    )
                )
    return matches


def match_partition_summaries(
    spec: PartitionSpec,
    schema: Schema,
    summaries: list[PartitionFieldSummary] | None,
    idents: list[ResolvedIdentifier],
) -> list[PartitionMatch]:
    matches: list[PartitionMatch] = []
    if not summaries:
        return matches
    for pos, pfield in enumerate(spec.fields):
        if pos >= len(summaries):
            break
        summary = summaries[pos]
        for ident in idents:
            if pfield.source_id != ident.field_id:
                continue
            transformed = _transformed(ident, pfield.transform)
            if transformed is None:
                continue
            expected, confidence = transformed
            result_type = pfield.transform.result_type(schema.find_type(pfield.source_id))
            lo = _decode(result_type, summary.lower_bound)
            hi = _decode(result_type, summary.upper_bound)
            hit = [name for name, bound in (("lower", lo), ("upper", hi)) if _equal(bound, expected)]
            if hit:
                matches.append(
                    PartitionMatch(
                        ident,
                        pfield.name,
                        confidence,
                        f"manifest-list summary {' and '.join(hit)} bound of partition field "
                        f"{pfield.name!r} is derived from the identifier",
                    )
                )
    return matches


# ---------------------------------------------------------------------------
# Parquet contents
# ---------------------------------------------------------------------------


def _field_id(arrow_field: pa.Field) -> int | None:
    metadata = arrow_field.metadata or {}
    raw = metadata.get(b"PARQUET:field_id")
    return int(raw) if raw is not None else None


def _find_path(fields: list[pa.Field], field_id: int, prefix: tuple[str, ...] = ()) -> tuple[str, ...] | None:
    for arrow_field in fields:
        path = (*prefix, arrow_field.name)
        if _field_id(arrow_field) == field_id:
            return path
        if pa.types.is_struct(arrow_field.type):
            found = _find_path(list(arrow_field.type), field_id, path)
            if found:
                return found
    return None


def _find_by_name(schema: pa.Schema, dotted: str) -> tuple[str, ...] | None:
    parts = tuple(dotted.split("."))
    fields = list(schema)
    for i, part in enumerate(parts):
        match = next((f for f in fields if f.name.lower() == part.lower()), None)
        if match is None:
            return None
        if i < len(parts) - 1:
            if not pa.types.is_struct(match.type):
                return None
            fields = list(match.type)
    return parts


def _column(table: pa.Table, path: tuple[str, ...]) -> pa.Array:
    array = table.column(path[0]).combine_chunks()
    for name in path[1:]:
        array = pc.struct_field(array, name)
    if isinstance(array.type, pa.ExtensionType):
        array = array.storage
    return array


def count_parquet_matches(source: str | IO[bytes] | Any, idents: list[ResolvedIdentifier]) -> dict[str, int]:
    """Count rows whose identifier columns equal the subject, ignoring any delete files.

    Columns are located by Iceberg field id (``PARQUET:field_id``) so renamed columns
    are still found, falling back to the column name for files written without ids.
    Returns a mapping of identifier hash to row count.
    """
    parquet = pq.ParquetFile(source)
    schema = parquet.schema_arrow
    paths: dict[str, tuple[str, ...]] = {}
    for ident in idents:
        path = _find_path(list(schema), ident.field_id) or _find_by_name(schema, ident.field_name)
        if path is not None:
            paths[ident.salted_hash] = path
    counts = {ident.salted_hash: 0 for ident in idents}
    if not paths:
        return counts
    top_level = sorted({p[0] for p in paths.values()})
    table = parquet.read(columns=top_level)
    for ident in idents:
        path = paths.get(ident.salted_hash)
        if path is None:
            continue
        array = _column(table, path)
        value = ident.bound_value
        try:
            mask = pc.equal(array, pa.scalar(value, type=array.type))
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError):
            continue
        counts[ident.salted_hash] = int(pc.sum(mask).as_py() or 0)
    return counts
