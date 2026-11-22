Report format
=============

``afterimage scan --json-out`` writes a JSON document produced from the pydantic model
``afterimage.models.Report``. ``schema_version`` follows semantic versioning: additive changes bump the
minor version; anything that could break a consumer bumps the major version.

Top level
---------

==================  =========================================================================
Field               Description
==================  =========================================================================
schema_version      Report schema version, currently ``1.0``.
tool, tool_version  ``afterimage`` and the package version that produced the report.
generated_at        UTC timestamp (ISO 8601).
table               ``identifier``, ``location``, ``format_version``, ``current_snapshot_id``,
                    ``snapshot_count``, ``metadata_location`` (all redacted).
subject             ``hash_algorithm`` (``hmac-sha256``), ``salt_fingerprint``,
                    ``salt_was_generated`` and one entry per identifier (below).
verdict             ``clean``, ``residues_found`` or ``incomplete``.
checks              One entry per check: ``name``, ``state`` (``ran``, ``skipped``, ``error``,
                    ``not_applicable``) and ``detail``.
findings            List of findings (below), sorted by kind and location.
summary             Number of findings per kind.
disclaimer          States that the report is technical evidence, not legal advice.
==================  =========================================================================

Identifier evidence
-------------------

==================  =========================================================================
column              Column name as supplied (never the value).
field_id            Iceberg field id the column resolved to.
iceberg_type        Iceberg type of the column.
salted_hash         ``HMAC-SHA256(salt, column + NUL + value)`` as hex.
findings            Number of findings attributed to this identifier.
confirmed_matches   Sum of ``match_count`` over confirmed findings.
==================  =========================================================================

Finding
-------

==================  =========================================================================
kind                One of the residue kinds listed in the README.
confidence          ``confirmed`` (exact match) or ``possible`` (needs review).
location            Redacted file URI, object key, metadata file or ``table@snapshot``.
identifier_hash     Salted hash of the identifier that matched, or ``null`` if unattributed.
column              Identifier column, or ``null``.
snapshot_ids        Snapshots through which the residue is reachable.
match_count         Matching rows where countable, else ``null``.
object_version_id   For ``noncurrent_object_version``: the S3 version id.
detail              Human-readable explanation (redacted).
==================  =========================================================================

Exit codes
----------

``afterimage scan`` exits ``0`` for ``clean``, ``3`` for ``residues_found`` and ``4`` for
``incomplete``; ``2`` signals a usage error. This makes it easy to gate an erasure workflow in CI or an
orchestrator.

Verifying a hash during an audit
--------------------------------

.. code-block:: python

   import hashlib, hmac
   hmac.new(salt, b"email\x00erika.mustermann@example.de", hashlib.sha256).hexdigest()

Compare the result with ``subject.identifiers[].salted_hash``. The ``salt_fingerprint`` tells you which
salt the report was produced with without revealing it.
