"""Afterimage: erasure evidence for Apache Iceberg lakehouses.

Afterimage inventories every place a data subject can still be recovered in an
Iceberg table after a logical ``DELETE`` and produces a machine-readable evidence
report. It gathers technical evidence; it does not provide legal advice.
"""

__version__ = "0.1.0"

from afterimage.models import Confidence, Finding, FindingKind, Report, Verdict
from afterimage.scanner import ScanOptions, Scanner, scan_table
from afterimage.subject import Identifier, Subject

__all__ = [
    "Confidence",
    "Finding",
    "FindingKind",
    "Identifier",
    "Report",
    "ScanOptions",
    "Scanner",
    "Subject",
    "Verdict",
    "__version__",
    "scan_table",
]
