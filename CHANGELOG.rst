Changelog
=========

All notable changes to this project are documented here. The format follows
`Keep a Changelog <https://keepachangelog.com/en/1.1.0/>`_ and the project uses
`Semantic Versioning <https://semver.org/>`_. The JSON report has its own ``schema_version``.

Unreleased
----------

0.1.0 - 2026-11-22
------------------

Added
~~~~~

* Scanner for subject residues in Apache Iceberg tables: live rows, time-travel rows, unrewritten
  (merge-on-read) data files, historical data files, equality-delete files, column bounds including
  ``truncate(16)`` prefixes, partition values and paths, manifest-list partition summaries, previous
  ``metadata.json`` files, orphan files, and noncurrent S3/MinIO object versions.
* Field-id based matching, so renamed columns are still found in old files.
* Bounds-based pruning so files that cannot contain the subject are never read.
* Erasure evidence report (JSON schema 1.0, Markdown rendering) with ``clean``, ``residues_found`` and
  ``incomplete`` verdicts, HMAC-SHA256 identifier hashes and redacted locations.
* ``afterimage scan``, ``plan``, ``render`` and ``purge-versions`` commands. Purging is a dry run unless
  ``--confirm`` is given.
* Redacting log handler.
* docker-compose playground (MinIO with versioning and an Iceberg REST catalog), a walkthrough script, and
  a Docker-free variant on moto.
* Unit and end-to-end tests on local filesystem tables and a versioned mock S3 bucket; GitHub Actions CI.
