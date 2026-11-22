from __future__ import annotations

import io
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.conversions import to_bytes
from pyiceberg.manifest import PartitionFieldSummary
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import BucketTransform, IdentityTransform, TruncateTransform
from pyiceberg.typedef import Record
from pyiceberg.types import LongType, NestedField, StringType, StructType, UUIDType

from afterimage.matching import (
    count_parquet_matches,
    match_bounds,
    match_partition,
    match_partition_summaries,
    may_contain,
    resolve_identifiers,
)
from afterimage.models import Confidence
from afterimage.subject import Identifier, IdentifierError, Subject

SALT = "m" * 32
LONG_EMAIL = "alice.wonderland@example.com"

SCHEMA = Schema(
    NestedField(1, "customer_id", LongType(), required=False),
    NestedField(2, "email", StringType(), required=False),
    NestedField(3, "ref", UUIDType(), required=False),
    NestedField(
        4,
        "profile",
        StructType(NestedField(5, "handle", StringType(), required=False)),
        required=False,
    ),
)


def resolve(*specs: str):  # type: ignore[no-untyped-def]
    subject = Subject([Identifier.parse(s) for s in specs], salt=SALT)
    return resolve_identifiers(subject, SCHEMA)


def s(value: str) -> bytes:
    return to_bytes(StringType(), value)


def test_resolve_identifiers_case_insensitive_and_nested() -> None:
    email, handle = resolve(f"EMAIL={LONG_EMAIL}", "profile.handle=al")
    assert email.field_id == 2
    assert handle.field_id == 5
    assert handle.field_name == "profile.handle"


def test_resolve_unknown_column() -> None:
    with pytest.raises(IdentifierError, match="does not exist"):
        resolve("nope=1")


def test_resolve_non_primitive_column() -> None:
    with pytest.raises(IdentifierError, match="primitive"):
        resolve("profile=x")


def test_exact_bounds_confirmed() -> None:
    (cid,) = resolve("customer_id=1")
    matches = match_bounds(cid, to_bytes(LongType(), 1), to_bytes(LongType(), 1))
    assert [m.which for m in matches] == ["lower", "upper"]
    assert all(m.confidence is Confidence.CONFIRMED for m in matches)


def test_truncated_string_bounds_are_possible_matches() -> None:
    (email,) = resolve(f"email={LONG_EMAIL}")
    matches = match_bounds(email, s(LONG_EMAIL[:16]), s(LONG_EMAIL[:15] + chr(ord(LONG_EMAIL[15]) + 1)))
    assert {m.which for m in matches} == {"lower", "upper"}
    assert all(m.confidence is Confidence.POSSIBLE for m in matches)


def test_custom_truncate_length() -> None:
    (email,) = resolve(f"email={LONG_EMAIL}")
    assert match_bounds(email, s(LONG_EMAIL[:8]), None, truncate_length=8)
    assert not match_bounds(email, s(LONG_EMAIL[:8]), None, truncate_length=16)


def test_unrelated_bounds_do_not_match() -> None:
    (email,) = resolve(f"email={LONG_EMAIL}")
    assert not match_bounds(email, s("aaa@example.com"), s("zzz@example.com"))
    assert not match_bounds(email, None, None)
    assert not match_bounds(email, b"\xff\xfe", None)  # undecodable bound is ignored


def test_short_value_is_not_matched_by_prefix() -> None:
    (email,) = resolve("email=bob@x.io")
    assert not match_bounds(email, s("bob@x.i"), None)


def test_uuid_bounds() -> None:
    value = uuid.uuid4()
    (ref,) = resolve(f"ref={value}")
    assert match_bounds(ref, value.bytes, None)[0].confidence is Confidence.CONFIRMED


def test_may_contain() -> None:
    (cid,) = resolve("customer_id=5")
    assert may_contain(cid, to_bytes(LongType(), 1), to_bytes(LongType(), 9))
    assert not may_contain(cid, to_bytes(LongType(), 6), to_bytes(LongType(), 9))
    assert not may_contain(cid, to_bytes(LongType(), 1), to_bytes(LongType(), 4))
    assert may_contain(cid, None, None)


def test_may_contain_with_truncated_bounds() -> None:
    (email,) = resolve(f"email={LONG_EMAIL}")
    assert may_contain(email, s(LONG_EMAIL[:16]), s(LONG_EMAIL[:15] + chr(ord(LONG_EMAIL[15]) + 1)))


SPEC = PartitionSpec(
    PartitionField(1, 1000, IdentityTransform(), "customer_id"),
    PartitionField(2, 1001, TruncateTransform(5), "email_trunc"),
    PartitionField(2, 1002, BucketTransform(16), "email_bucket"),
)


def test_match_partition_identity_and_truncate() -> None:
    idents = resolve("customer_id=7", f"email={LONG_EMAIL}")
    bucket = BucketTransform(16).transform(StringType())(LONG_EMAIL)
    matches = match_partition(SPEC, Record(7, LONG_EMAIL[:5], bucket), idents)
    by_field = {m.partition_field: m.confidence for m in matches}
    assert by_field == {"customer_id": Confidence.CONFIRMED, "email_trunc": Confidence.POSSIBLE}


def test_match_partition_no_match_and_short_record() -> None:
    idents = resolve("customer_id=7")
    assert not match_partition(SPEC, Record(8, "x", 0), idents)
    assert not match_partition(SPEC, Record(), idents)


def test_match_partition_summaries() -> None:
    (cid,) = resolve("customer_id=7")
    summaries = [
        PartitionFieldSummary.from_args(
            contains_null=False,
            contains_nan=False,
            lower_bound=to_bytes(LongType(), 7),
            upper_bound=to_bytes(LongType(), 9),
        )
    ]
    matches = match_partition_summaries(SPEC, SCHEMA, summaries, [cid])
    assert len(matches) == 1
    assert "lower" in matches[0].detail
    assert not match_partition_summaries(SPEC, SCHEMA, None, [cid])
    summaries[0] = PartitionFieldSummary.from_args(
        contains_null=False,
        contains_nan=False,
        lower_bound=to_bytes(LongType(), 1),
        upper_bound=to_bytes(LongType(), 9),
    )
    assert not match_partition_summaries(SPEC, SCHEMA, summaries, [cid])


def _parquet(table: pa.Table) -> io.BytesIO:
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)
    return buffer


def _field(name: str, arrow_type: pa.DataType, field_id: int | None) -> pa.Field:
    metadata = {b"PARQUET:field_id": str(field_id).encode()} if field_id is not None else None
    return pa.field(name, arrow_type, metadata=metadata)


def test_count_parquet_matches_by_field_id_survives_renames() -> None:
    schema = pa.schema([_field("old_email_name", pa.string(), 2), _field("customer_id", pa.int64(), 1)])
    data = pa.table({"old_email_name": [LONG_EMAIL, "b@x", LONG_EMAIL], "customer_id": [1, 2, 1]}, schema=schema)
    idents = resolve(f"email={LONG_EMAIL}", "customer_id=1")
    counts = count_parquet_matches(_parquet(data), idents)
    assert set(counts.values()) == {2}


def test_count_parquet_matches_falls_back_to_names() -> None:
    data = pa.table({"Email": [LONG_EMAIL, "b@x"]})
    (email,) = resolve(f"email={LONG_EMAIL}")
    assert count_parquet_matches(_parquet(data), [email]) == {email.salted_hash: 1}


def test_count_parquet_matches_missing_column() -> None:
    data = pa.table({"other": [1, 2]})
    (email,) = resolve(f"email={LONG_EMAIL}")
    assert count_parquet_matches(_parquet(data), [email]) == {email.salted_hash: 0}


def test_count_parquet_matches_nested_and_uuid() -> None:
    value = uuid.uuid4()
    struct_type = pa.struct([_field("handle", pa.string(), 5)])
    schema = pa.schema([_field("profile", struct_type, 4), _field("ref", pa.binary(16), 3)])
    data = pa.table(
        {"profile": [{"handle": "al"}, {"handle": "bo"}], "ref": [value.bytes, uuid.uuid4().bytes]},
        schema=schema,
    )
    handle, ref = resolve("profile.handle=al", f"ref={value}")
    counts = count_parquet_matches(_parquet(data), [handle, ref])
    assert counts == {handle.salted_hash: 1, ref.salted_hash: 1}


def test_count_parquet_matches_type_mismatch_is_ignored() -> None:
    schema = pa.schema([_field("customer_id", pa.string(), 1)])
    data = pa.table({"customer_id": ["not-a-number"]}, schema=schema)
    (cid,) = resolve("customer_id=1")
    assert count_parquet_matches(_parquet(data), [cid]) == {cid.salted_hash: 0}
