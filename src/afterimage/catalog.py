"""Catalog and table loading on top of :func:`pyiceberg.catalog.load_catalog`.

Catalog configuration follows PyIceberg conventions: ``~/.pyiceberg.yaml``,
``PYICEBERG_CATALOG__<NAME>__<PROP>`` environment variables, and explicit
``key=value`` properties (which win).
"""

from __future__ import annotations

from collections.abc import Iterable

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.table import Table


def parse_properties(pairs: Iterable[str]) -> dict[str, str]:
    props: dict[str, str] = {}
    for pair in pairs:
        key, sep, value = pair.partition("=")
        if not sep or not key.strip():
            raise ValueError(f"catalog property must look like key=value, got {key.strip() or pair!r}")
        props[key.strip()] = value
    return props


def open_catalog(name: str, properties: dict[str, str] | None = None) -> Catalog:
    return load_catalog(name, **(properties or {}))


def open_table(name: str, table: str, properties: dict[str, str] | None = None) -> Table:
    return open_catalog(name, properties).load_table(table)
