How each residue is detected
============================

Afterimage loads the table through PyIceberg and runs six checks. Every check either runs, is skipped,
fails with an error, or does not apply (for example object versions on a local filesystem). Skipped and
failed checks make the verdict ``incomplete``.

::

   subject identifiers ---> resolve against schema (field ids, types)
                                     |
                 +-------------------+--------------------+
                 |                   |                    |
         manifest_metadata     file_contents         time_travel
         (every manifest of    (data and equality-   (scan each snapshot
          every snapshot)       delete files)         with deletes applied)
                 |                   |                    |
           metadata_log         orphan_files        object_versions
           (previous            (list table          (noncurrent S3 / MinIO
            metadata.json)       location)            versions under prefix)
                 |                   |                    |
                 +---------> findings (redacted) <--------+
                                     |
                         report: JSON + Markdown + plan

manifest_metadata
-----------------

Walks every manifest of every snapshot, including ``DELETED`` entries. A copy-on-write ``DELETE`` writes
a manifest whose ``DELETED`` entry keeps the removed file's full metrics, so bounds survive in the
*current* snapshot until the next commit.

* **Column bounds.** Decodes ``lower_bounds`` and ``upper_bounds`` for each identifier's field id. An
  exact match is ``confirmed``. For strings longer than the truncation width (default 16, the PyIceberg,
  Spark and Flink default), a lower bound equal to the prefix, or an upper bound equal to the prefix with
  its last character incremented, is ``possible``.
* **Partition values.** For partition fields sourced from an identifier column: ``identity`` matches are
  ``confirmed``; ``truncate[W]`` matches are ``possible``. ``bucket[N]`` and time transforms are ignored
  because they do not identify a person.
* **Partition summaries.** Manifest-list summaries store per-manifest lower and upper partition values. A
  bound equal to the identifier-derived value is reported.

file_contents
-------------

Reads data files and equality-delete files whose bounds do not rule the subject out, *ignoring* delete
files, and counts matching rows by Parquet field id (falling back to column names). Classification:

* file live in the current snapshot and the subject is not visible: ``unrewritten_data_file``;
* file referenced only by older snapshots: ``historical_data_file``;
* equality-delete file: ``equality_delete_file``.

Referenced files that no longer exist are counted in the check detail, not reported as residues.

time_travel
-----------

Scans every snapshot with an ``EqualTo`` filter per identifier, with delete files applied, which is what
``SELECT ... VERSION AS OF`` returns. The current snapshot yields ``live_rows``; others yield
``historical_snapshot_rows`` and name any tag or branch pinning them. Snapshots whose files contain no
match are skipped without scanning. PyIceberg cannot apply equality deletes; for such snapshots the check
falls back to physical row counts and marks the finding ``possible``.

metadata_log
------------

Opens each previous ``metadata.json`` listed in the metadata log that still exists, finds snapshots that
are no longer in the current metadata but whose manifest lists still exist, and runs the metadata and
content checks on them. The ``metadata.json`` file is not encrypted even when Iceberg table encryption is
enabled, see https://iceberg.apache.org/docs/latest/encryption/.

orphan_files
------------

Lists every file under the table location and subtracts everything referenced by the current metadata,
the metadata log, all manifest lists, manifests, data and delete files, and statistics files. Orphan
Parquet files are read; orphan manifests and manifest lists are parsed and checked for bounds, partition
values and summaries; orphan ``metadata.json`` files are flagged ``possible``. Requires a FileIO that can
list (PyArrow FileIO); otherwise the check is skipped.

object_versions
---------------

For tables in S3-compatible storage, lists object versions under the table prefix and inspects every
noncurrent version: Parquet files are downloaded and counted, manifests and manifest lists are parsed,
``metadata.json`` versions are flagged ``possible``, versions above the download limit (512 MiB by
default) are flagged ``possible`` without being downloaded. The bucket's versioning status is recorded in
the check detail.
