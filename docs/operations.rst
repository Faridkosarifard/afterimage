Running Afterimage in an erasure workflow
=========================================

A typical pipeline for one request:

1. Resolve the request to identifiers in each table (surrogate key first, then email or other natural
   keys that may appear in bounds or partitions).
2. ``afterimage scan`` before the delete to record where the subject lives (optional but useful as a
   baseline).
3. Run the logical ``DELETE`` in your engine.
4. ``afterimage scan --json-out`` and ``afterimage plan`` to get the ordered maintenance steps.
5. Run the steps with your engine: rewrite data files and manifests, expire snapshots (drop tags first),
   rotate metadata, remove orphan files.
6. ``afterimage purge-versions`` (dry run, review, then ``--confirm``) for versioned buckets.
7. ``afterimage scan`` again. Archive the clean JSON report with the ticket.

Configuration tips
------------------

* Set ``write.metadata.metrics.column.<identifier> = none`` for identifier columns so their bounds are
  never written, and avoid identity or truncate partitioning on them.
* Set ``write.metadata.delete-after-commit.enabled = true`` and a small
  ``write.metadata.previous-versions-max`` on tables with personal data.
* Add a ``NoncurrentVersionExpiration`` lifecycle rule inside your erasure deadline.
* Keep snapshot retention shorter than the erasure deadline, or the plan will always need a manual
  expiry.

Exit codes for orchestration
----------------------------

``0`` clean, ``3`` residues found, ``4`` incomplete, ``2`` usage error. Treat ``4`` as a failure: a
skipped check proves nothing.
