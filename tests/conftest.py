from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import LongType, NestedField, StringType

from afterimage.subject import Identifier, Subject

SALT = "unit-test-salt-0123456789"
ALICE_EMAIL = "alice.wonderland@example.com"  # longer than 16 characters: exercises truncate(16) bounds

SCHEMA = Schema(
    NestedField(1, "customer_id", LongType(), required=False),
    NestedField(2, "email", StringType(), required=False),
    NestedField(3, "country", StringType(), required=False),
)
BY_CUSTOMER = PartitionSpec(
    PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="customer_id")
)


def customers(rows: list[tuple[int, str, str]]) -> pa.Table:
    return pa.table(
        {
            "customer_id": pa.array([r[0] for r in rows], pa.int64()),
            "email": pa.array([r[1] for r in rows], pa.string()),
            "country": pa.array([r[2] for r in rows], pa.string()),
        }
    )


SEED = [
    (1, ALICE_EMAIL, "DE"),
    (2, "bob@example.com", "FR"),
    (3, "carol@example.com", "DE"),
]


@pytest.fixture
def warehouse(tmp_path: Path) -> Path:
    path = tmp_path / "warehouse"
    path.mkdir()
    return path


@pytest.fixture
def catalog(warehouse: Path) -> SqlCatalog:
    cat = SqlCatalog("test", uri=f"sqlite:///{warehouse}/catalog.db", warehouse=f"file://{warehouse}")
    cat.create_namespace("crm")
    return cat


@pytest.fixture
def make_table(catalog: SqlCatalog) -> Callable[..., Table]:
    def factory(
        name: str = "crm.customers",
        partitioned: bool = True,
        properties: dict[str, str] | None = None,
        rows: list[tuple[int, str, str]] | None = None,
    ) -> Table:
        table = catalog.create_table(
            name,
            SCHEMA,
            partition_spec=BY_CUSTOMER if partitioned else PartitionSpec(),
            properties=properties or {},
        )
        table.append(customers(rows if rows is not None else SEED))
        return catalog.load_table(name)

    return factory


@pytest.fixture
def alice() -> Subject:
    return Subject([Identifier("customer_id", "1"), Identifier("email", ALICE_EMAIL)], salt=SALT)
