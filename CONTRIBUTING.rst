Contributing to Afterimage
==========================

Thanks for considering a contribution. Afterimage handles evidence about personal data, so correctness
and not leaking identifiers matter more than features.

Getting started
---------------

.. code-block:: bash

   git clone https://github.com/Faridkosarifard/afterimage
   cd afterimage
   make install      # creates .venv and installs the package with dev dependencies
   make check        # ruff, mypy --strict and the full test suite

Requirements: Python 3.11 or newer. Docker is only needed for ``make demo-up``; ``make demo-local`` runs
the same walkthrough against a moto S3 server.

Workflow
--------

1. Open an issue first for anything larger than a small fix, so the approach can be agreed on.
2. Create a branch from ``main``.
3. Add or update tests. New residue checks need a unit test and, where possible, an end-to-end test
   in ``tests/e2e`` that builds a real table.
4. Run ``make check`` and make sure it passes.
5. Update ``CHANGELOG.rst`` under *Unreleased*.
6. Open a pull request describing the residue class or behaviour you changed.

Rules for code that touches identifiers
---------------------------------------

* Raw identifier values must never be written to logs, reports, exceptions that reach the user, or test
  snapshots. Use ``Subject.salted_hash`` or the ``Redactor``.
* Every new string that can contain a path or an object key must go through the scanner's redactor
  before it ends up in a ``Finding``.
* Destructive operations (anything that deletes data or object versions) must default to a dry run and
  require an explicit confirmation flag.
* A check that cannot run must say so (``skipped`` or ``error``) instead of reporting nothing: a silent
  pass would make the verdict lie.

Verify library behaviour against the installed version
------------------------------------------------------

PyIceberg moves quickly. When you rely on a PyIceberg API, check it against the version pinned in
``pyproject.toml`` (read the installed source or write a test) instead of relying on documentation for a
different release.

Style
-----

* ``ruff format`` and ``ruff check`` (configuration in ``pyproject.toml``).
* ``mypy --strict`` for ``src/``.
* Docstrings explain *why* something is a residue, not only *what* the code does.
* Commit messages: imperative mood, a short summary line, and a body that explains the reason for the
  change.

Reporting security issues
-------------------------

If you find a way to make Afterimage leak an identifier or delete data without confirmation, please do
not open a public issue. Use GitHub's private vulnerability reporting for this repository instead.

License
-------

By contributing you agree that your contributions are licensed under the Apache License 2.0.
