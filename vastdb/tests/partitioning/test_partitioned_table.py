from datetime import datetime

import pyarrow as pa
import pytest
from pyarrow import compute as pc
from pyiceberg.transforms import (
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    Transform,
    YearTransform,
)
from pyiceberg.types import TimestampType

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


def _transform_table(table: pa.Table, column: str, transform) -> pa.Table:
    post_transform_column_name = _post_transform_name(column, transform)
    transform_fn = transform.pyarrow_transform(_DATA.schema.field(column).type)
    transformed_column = transform_fn(table[post_transform_column_name])
    table.schema.get_field_index(post_transform_column_name)
    return table.set_column(
        table.schema.get_field_index(post_transform_column_name),
        post_transform_column_name,
        transformed_column,
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


@pytest.mark.parametrize("column_name, transform", _TRANSFORMS)
def test_pit(
    session: Session, clean_bucket_name: str, column_name: str, transform: Transform
):
    post_transform_column_name = _post_transform_name(column_name, transform)
    expected_partitions = (
        _DATA.select(["a", column_name])
        .take((0, 2, 4))
        .rename_columns(["a", post_transform_column_name])
    )
    expected_partitions = _transform_table(expected_partitions, column_name, transform)
    schema_name = f"s_{column_name}"
    table_name = f"partitioned_table_{column_name}"

    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema(schema_name)
        t = s.create_table(
            table_name,
            _ARROW_SCHEMA,
            partition_spec=_create_partition_spec(column_name, transform),
        )
        t.insert(_DATA)

    with session.transaction() as tx:
        t = tx.bucket(clean_bucket_name).schema(schema_name).table(table_name)
        pit = t.partitions()
        assert pit.arrow_schema == expected_partitions.schema
        assert pit["a"].equals(pit._pit["keys_0"])
        assert pit[post_transform_column_name].equals(pit._pit["keys_1"])

        with pytest.raises(ValueError):
            pit["keys_0"]

        partitions = pa.Table.from_batches(list(pit.select()))
        assert expected_partitions.equals(partitions)

        value_to_filter_by = transform.transform(TimestampType())(
            _DATA[column_name][2].as_py()
        )

        partitions = pa.Table.from_batches(
            list(
                pit.select(
                    predicate=(
                        (pit["a"] == 2)
                        & (pit[post_transform_column_name] == value_to_filter_by)
                    )
                )
            )
        )
        assert expected_partitions.filter(
            (pc.field("a") == 2)
            & (pc.field(post_transform_column_name) == value_to_filter_by)
        ).equals(partitions)

        for col in ("a", post_transform_column_name):
            partitions = pa.Table.from_batches(list(pit.select([col])))
            assert expected_partitions.select([col]).equals(partitions)
