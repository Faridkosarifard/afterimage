Privacy and safety model
========================

Afterimage exists to prove that personal data is gone, so it must not become another copy of it.

* **Identifiers stay in memory.** ``Identifier`` and ``Subject`` never include values in ``repr`` or
  ``str``. Prefer ``--match-file`` or ``--match-file -`` (stdin) over ``--match`` so values stay out of
  shell history and process listings.
* **Keyed hashes, not plain hashes.** Emails and numeric ids have low entropy; a plain SHA-256 could be
  reversed by brute force. Reports use HMAC-SHA256 keyed with ``AFTERIMAGE_SALT``. Keep the salt in a
  secret manager. Without a salt a random one is generated and the hash cannot be re-derived later; the
  report says so in ``salt_was_generated``.
* **Redaction everywhere.** Locations, check details, error messages and log records go through a
  redactor that replaces the raw value, its URL-encoded forms, and short values appearing as
  ``key=value`` path segments (for example ``customer_id=7/``).
* **Destructive actions are opt-in.** ``purge-versions`` re-discovers versions with a fresh scan instead of
  trusting an old report, prints a dry run by default, and deletes only with ``--confirm``. Failures such
  as Object Lock retention are reported per version and never retried.
* **Reports are still sensitive.** A report proves that a person with a given hash was in a table. Store
  reports with the same access controls as your erasure tickets and do not commit them to source control.

What a clean report means
-------------------------

Every check ran and none of them found the subject in this table at the time of the scan. It does not
cover replicas, downstream copies, backups, caches, logs, other tables, or data written after the scan.
