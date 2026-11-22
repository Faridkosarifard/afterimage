"""Test helpers that build Iceberg tables in states PyIceberg cannot produce on its own.

PyIceberg writes copy-on-write deletes only. To exercise merge-on-read residues we commit
position- and equality-delete files through a thin subclass of PyIceberg's fast-append
producer. This relies on private PyIceberg APIs and lives in the test suite only.
"""

from __future__ import annotations

import uuid
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.conversions import to_bytes
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestFile,
    ManifestWriterV2,
)
from pyiceberg.table import Table
from pyiceberg.table.update.snapshot import _FastAppendFiles
from pyiceberg.typedef import Record

POS_FILE_PATH_ID = 2147483546
POS_POS_ID = 2147483545


class _DeleteManifestWriter(ManifestWriterV2):
    def content(self) -> ManifestContent:
        return ManifestContent.DELETES


class _AppendDeleteFiles(_FastAppendFiles):
    def __init__(self, delete_files: list[DataFile], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._delete_files = delete_files

    def _manifests(self) -> list[ManifestFile]:
        manifests = super()._manifests()
        meta = self._transaction.table_metadata
        writer = _DeleteManifestWriter(
            meta.spec(), meta.schema(), self.new_manifest_output(), self._snapshot_id, self._compression
        )
        with writer:
            for delete_file in self._delete_files:
                writer.add(
                    ManifestEntry.from_args(
                        status=ManifestEntryStatus.ADDED,
                        snapshot_id=self._snapshot_id,
                        sequence_number=None,
                        file_sequence_number=None,
                        data_file=delete_file,
                    )
                )
        return [*manifests, writer.to_manifest_file()]


def commit_delete_files(table: Table, delete_files: list[DataFile]) -> None:
    from pyiceberg.table import Transaction
    from pyiceberg.table.update.snapshot import Operation

    with Transaction(table, autocommit=True) as tx:
        producer = _AppendDeleteFiles(
            delete_files,
            operation=Operation.OVERWRITE,
            transaction=tx,
            io=table.io,
        )
        producer.commit()


def _local(uri: str) -> str:
    return uri.removeprefix("file://")


def _field(name: str, arrow_type: pa.DataType, field_id: int) -> pa.Field:
    return pa.field(name, arrow_type, metadata={b"PARQUET:field_id": str(field_id).encode()})


def write_position_deletes(table: Table, data_file_path: str, positions: list[int], partition: Record) -> DataFile:
    location = f"{table.location()}/data/pos-deletes-{uuid.uuid4()}.parquet"
    schema = pa.schema([_field("file_path", pa.string(), POS_FILE_PATH_ID), _field("pos", pa.int64(), POS_POS_ID)])
    batch = pa.table({"file_path": [data_file_path] * len(positions), "pos": positions}, schema=schema)
    with table.io.new_output(location).create(overwrite=True) as stream:
        pq.write_table(batch, stream)
    size = len(table.io.new_input(location))
    return DataFile.from_args(
        content=DataFileContent.POSITION_DELETES,
        file_path=location,
        file_format=FileFormat.PARQUET,
        partition=partition,
        record_count=len(positions),
        file_size_in_bytes=size,
        spec_id=table.metadata.default_spec_id,
    )


def write_equality_deletes(table: Table, column: str, values: list[Any], partition: Record) -> DataFile:
    nested = table.schema().find_field(column)
    location = f"{table.location()}/data/eq-deletes-{uuid.uuid4()}.parquet"
    arrow_type = pa.string() if str(nested.field_type) == "string" else pa.int64()
    schema = pa.schema([_field(nested.name, arrow_type, nested.field_id)])
    with table.io.new_output(location).create(overwrite=True) as stream:
        pq.write_table(pa.table({nested.name: values}, schema=schema), stream)
    size = len(table.io.new_input(location))
    ordered = sorted(values)
    return DataFile.from_args(
        content=DataFileContent.EQUALITY_DELETES,
        file_path=location,
        file_format=FileFormat.PARQUET,
        partition=partition,
        record_count=len(values),
        file_size_in_bytes=size,
        equality_ids=[nested.field_id],
        lower_bounds={nested.field_id: to_bytes(nested.field_type, ordered[0])},
        upper_bounds={nested.field_id: to_bytes(nested.field_type, ordered[-1])},
        spec_id=table.metadata.default_spec_id,
    )
