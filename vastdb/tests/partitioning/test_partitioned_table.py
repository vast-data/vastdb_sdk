from datetime import datetime

import pyarrow as pa
import pytest
from pyiceberg.transforms import (
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    Transform,
    YearTransform,
)

from vastdb.partitioning import PartitionKey, PartitionSpec
from vastdb.session import Session

_ARROW_SCHEMA = pa.schema(
    [
        ("a", pa.int64()),
        ("b", pa.int32()),
        ("s", pa.utf8()),
        ("y", pa.timestamp("us")),
        ("m", pa.timestamp("us")),
        ("d", pa.timestamp("us")),
        ("h", pa.timestamp("us")),
    ]
)
_DATA = pa.Table.from_pydict(
    {
        "a": [1, 1, 2, 2, 3],
        "b": [4, 4, 6, 6, 7],
        "s": ["a", "b", "c", "d", "e"],
        "y": [
            datetime(2024, 1, 1),
            datetime(2024, 1, 1),
            datetime(2025, 1, 1),
            datetime(2025, 1, 1),
            datetime(2026, 1, 1),
        ],
        "m": [
            datetime(2025, 1, 1),
            datetime(2025, 1, 1),
            datetime(2025, 2, 1),
            datetime(2025, 2, 1),
            datetime(2025, 3, 1),
        ],
        "d": [
            datetime(2025, 1, 1),
            datetime(2025, 1, 1),
            datetime(2025, 1, 2),
            datetime(2025, 1, 2),
            datetime(2025, 1, 3),
        ],
        "h": [
            datetime(2025, 1, 1, 1),
            datetime(2025, 1, 1, 1),
            datetime(2025, 1, 1, 2),
            datetime(2025, 1, 1, 2),
            datetime(2025, 1, 1, 3),
        ],
    },
    schema=_ARROW_SCHEMA,
)
_TRANSFORMS: tuple[tuple[str, Transform], ...] = (
    ("b", IdentityTransform()),
    # ("d", DayTransform()),
    ("m", MonthTransform()),
    ("y", YearTransform()),
    ("h", HourTransform()),
)


def _post_transform_name(column_name: str, transform: Transform) -> str:
    transform_suffix = {
        IdentityTransform: "",
        HourTransform: "_hour",
        DayTransform: "_day",
        MonthTransform: "_month",
        YearTransform: "_year",
    }[type(transform)]
    return f"{column_name}{transform_suffix}"


def _create_partition_spec(
    column_name: str, transform: Transform, post_transform: bool = False
) -> PartitionSpec:
    if post_transform:
        column_name = _post_transform_name(column_name, transform)

    return PartitionSpec(
        partition_keys=[
            PartitionKey(column="a", transform=IdentityTransform()),
            PartitionKey(column_name, transform),
        ]
    )


@pytest.mark.parametrize("column_name, transform", _TRANSFORMS)
def test_create_partitioned_table(
    session: Session, clean_bucket_name: str, column_name: str, transform: Transform
):
    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema(f"s_{column_name}")
        t = s.create_table(
            f"partitioned_table_{column_name}",
            _ARROW_SCHEMA,
            partition_spec=_create_partition_spec(column_name, transform),
        )

        assert t._metadata.partitioning == _create_partition_spec(
            column_name, transform, post_transform=True
        )

        t.drop()
        s.drop()
