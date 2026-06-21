import itertools
from datetime import datetime
from typing import Callable

import ibis
import pyarrow as pa
import pytest
from ibis.common.exceptions import IbisTypeError
from pyarrow import compute as pc
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    Transform,
    TruncateTransform,
    YearTransform,
)

from vastdb import _internal, errors, table
from vastdb.partitioning import PartitionKey, PartitionSpec
from vastdb.session import Session
from vastdb.table_metadata import PartitionsMetadata

_ARROW_SCHEMA = pa.schema(
    [
        ("id", pa.int64()),
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
        "id": [0, 1, 2, 3, 4],
        "a": [1, 1, 2, 2, 3],
        "b": [4, 4, 6, 6, 7],
        "s": ["aaz", "aaz", "bbz", "bbz", "ccz"],
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
    ("b", BucketTransform(4)),
    ("s", TruncateTransform(2)),
    ("d", DayTransform()),
    ("m", MonthTransform()),
    ("y", YearTransform()),
    ("h", HourTransform()),
)


def _post_transform_name(column_name: str, transform: Transform) -> str:
    transform_suffix = {
        IdentityTransform: "",
        BucketTransform: "_bucket",
        TruncateTransform: "_trunc",
        HourTransform: "_hour",
        DayTransform: "_day",
        MonthTransform: "_month",
        YearTransform: "_year",
    }[type(transform)]
    return f"{column_name}{transform_suffix}"


def _create_partition_spec(column_name: str, transform: Transform) -> PartitionSpec:
    return PartitionSpec(
        partition_keys=[
            PartitionKey(column="a", transform=IdentityTransform()),
            PartitionKey(column_name, transform),
        ]
    )


def _transform_table(table: pa.Table, column: str, transform) -> pa.Table:
    transform_fn = transform.pyarrow_transform(table.schema.field(column).type)
    transformed_column = transform_fn(table[column])
    table.schema.get_field_index(column)
    return table.set_column(
        table.schema.get_field_index(column),
        column,
        transformed_column,
    )


def _updated_values_for_field(field: pa.Field, num_rows: int) -> pa.Array | pa.ChunkedArray:
    if pa.types.is_integer(field.type):
        return pa.array([900 + i for i in range(num_rows)], type=field.type)

    if pa.types.is_string(field.type):
        return pa.array([f"updated_{i}" for i in range(num_rows)], type=field.type)

    if pa.types.is_timestamp(field.type):
        return pa.array([datetime(2030, 1, 1, 0, i) for i in range(num_rows)], type=field.type)

    raise AssertionError(f"Unsupported type for test updates: {field.type}")


@pytest.fixture
def hydra_session(session: Session) -> Session:
    try:
        session.features.check_partitioned()
        return session
    except errors.NotSupportedVersion:
        pytest.skip("Test requires hydra to run")


@pytest.mark.parametrize("column_name, transform", _TRANSFORMS)
def test_create_partitioned_table(
    hydra_session: Session, clean_bucket_name: str, column_name: str, transform: Transform
):
    with hydra_session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema(f"s_{column_name}")
        t = s.create_table(
            f"partitioned_table_{column_name}",
            _ARROW_SCHEMA,
            partition_spec=_create_partition_spec(column_name, transform),
        )

        assert t._metadata.partitioning == PartitionsMetadata(
            _create_partition_spec(column_name, transform),
            ["a", _post_transform_name(column_name, transform)]
        )

        t.drop()
        s.drop()


def test_create_partitioned_table_negative(hydra_session: Session, clean_bucket_name: str):
    with hydra_session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema("s")

        with pytest.raises(ValueError, match="Partition key column 'B' does not exist in the provided Arrow schema"):
            s.create_table(
            "partitioned_table",
            _ARROW_SCHEMA,
            partition_spec=_create_partition_spec("B", IdentityTransform()),
        )


@pytest.mark.parametrize("column_name, transform", _TRANSFORMS)
def test_pit(
    hydra_session: Session, clean_bucket_name: str, column_name: str, transform: Transform
):
    if isinstance(transform, DayTransform):
        pytest.skip("Server currently returns a date type on the transformed column and not an int like iceberg")

    post_transform_column_name = _post_transform_name(column_name, transform)
    expected_partitions = (
        _DATA.select(["a", column_name])
        .take((0, 2, 4))
        .rename_columns(["a", post_transform_column_name])
    )
    expected_partitions = _transform_table(expected_partitions, post_transform_column_name, transform)
    schema_name = f"s_{column_name}"
    table_name = f"partitioned_table_{column_name}"

    with hydra_session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema(schema_name)
        t = s.create_table(
            table_name,
            _ARROW_SCHEMA,
            partition_spec=_create_partition_spec(column_name, transform),
        )
        t.insert(_DATA)

    with hydra_session.transaction() as tx:
        t = tx.bucket(clean_bucket_name).schema(schema_name).table(table_name)
        pit = t.partitions()
        expected_pit_schema = pa.schema(
            itertools.chain(_internal.PIT_METADATA_COLUMNS_TO_INCLUDE, expected_partitions.schema)
        )
        assert pit.arrow_schema == expected_pit_schema
        assert pit["a"] is not None
        assert pit[post_transform_column_name] is not None

        with pytest.raises(IbisTypeError, match="Column 'sorting_score' is not found in table"):
            pit["sorting_score"]

        if not isinstance(transform, IdentityTransform):
            with pytest.raises(IbisTypeError, match=f"Column '{column_name}' is not found in table"):
                pit[column_name]

        partitions = pit.select().read_all()
        assert expected_partitions.equals(partitions.select(["a", post_transform_column_name]))

        column_type = _DATA.schema.field(column_name).type
        transform_fn = transform.pyarrow_transform(column_type)
        value_to_filter_by = transform_fn(
            pa.array([_DATA[column_name][2]], type=column_type)
        )[0].as_py()

        partitions = pit.select(
            predicate=(
                (pit["a"] == 2)
                & (pit[post_transform_column_name] == value_to_filter_by)
            )
        ).read_all()

        assert expected_partitions.filter(
            (pc.field("a") == 2)
            & (pc.field(post_transform_column_name) == value_to_filter_by)
        ).equals(partitions.select(["a", post_transform_column_name]))

        for col in ("a", post_transform_column_name):
            partitions = pit.select([col]).read_all()
            assert expected_partitions.select([col]).equals(partitions)


@pytest.mark.parametrize("column_name, transform", _TRANSFORMS)
def test_query_partitioned_table(
    hydra_session: Session, clean_bucket_name: str, column_name: str, transform: Transform
):
    schema_name = f"s_{column_name}"
    table_name = f"partitioned_table_{column_name}"

    with hydra_session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema(schema_name)
        t = s.create_table(
            table_name,
            _ARROW_SCHEMA,
            partition_spec=_create_partition_spec(column_name, transform),
        )
        t.insert(_DATA)

    with hydra_session.transaction() as tx:
        t = tx.bucket(clean_bucket_name).schema(schema_name).table(table_name)
        queried_data = t.select().read_all()
        assert _DATA.equals(queried_data)

        value_to_filter_by = _DATA[column_name][2].as_py()

        queried_data = t.select(
                predicate=((t["a"] == 2) & (t[column_name] == value_to_filter_by))
        ).read_all()

        assert _DATA.filter(
            (pc.field("a") == 2) & (pc.field(column_name) == value_to_filter_by)
        ).equals(queried_data)

        str_value_to_filter_by = _DATA["s"][2].as_py()
        queried_data = t.select(
                predicate=(
                    (t["a"] == 2)
                    & (t[column_name] == value_to_filter_by)
                    & (t["s"] == str_value_to_filter_by)
                )
            ).read_all()

        assert _DATA.filter(
            (pc.field("a") == 2)
            & (pc.field(column_name) == value_to_filter_by)
            & (pc.field("s") == str_value_to_filter_by)
        ).equals(queried_data)

        assert list(t.select(predicate=t["a"] == 0)) == []

        for col in ("a", column_name, "s"):
            queried_data = t.select([col]).read_all()
            assert _DATA.select([col]).equals(queried_data)


@pytest.mark.parametrize(
    "predicate_generator, arrow_filter",
    [
        (lambda t: t["a"] == -1, pc.field("a") == -1),
        (lambda t: (t["a"] == 1) & (t["a"] == 2), (pc.field("a") == 1) & (pc.field("a") == 2)),
        (lambda t: t["a"] == 1, pc.field("a") == 1),
        (lambda t: t["b"] >= 6, pc.field("b") >= 6),
    ],
)
@pytest.mark.parametrize("column_name, transform", _TRANSFORMS)
def test_predicate_partitioned_table(
    hydra_session: Session,
    clean_bucket_name: str,
    column_name: str,
    transform: Transform,
    predicate_generator: Callable,
    arrow_filter,
):
    schema_name = f"s_{column_name}"
    table_name = f"partitioned_table_{column_name}"

    with hydra_session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema(schema_name)
        t = s.create_table(
            table_name,
            _ARROW_SCHEMA,
            partition_spec=_create_partition_spec(column_name, transform),
        )
        t.insert(_DATA)

    with hydra_session.transaction() as tx:
        t = tx.bucket(clean_bucket_name).schema(schema_name).table(table_name)
        actual = t.select(["a", "s"], predicate=predicate_generator(t)).read_all()
        expected_table = _DATA.filter(arrow_filter).select(["a", "s"])
        assert expected_table.equals(actual), f"Predicate {predicate_generator(t)} did not return expected results"


@pytest.mark.parametrize("column_name, transform", _TRANSFORMS)
@pytest.mark.parametrize("limit_rows", [0, 1, 3, 5, 6])
def test_query_partitioned_table_with_limit(
    hydra_session: Session,
    clean_bucket_name: str,
    column_name: str,
    transform: Transform,
    limit_rows: int,
):
    schema_name = f"s_limit_{column_name}_{limit_rows}"
    table_name = f"partitioned_table_{column_name}"

    with hydra_session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema(schema_name)
        t = s.create_table(
            table_name,
            _ARROW_SCHEMA,
            partition_spec=_create_partition_spec(column_name, transform),
        )
        t.insert(_DATA)

    with hydra_session.transaction() as tx:
        t = tx.bucket(clean_bucket_name).schema(schema_name).table(table_name)
        query_result = t.select(limit_rows=limit_rows).read_all()

        expected_rows = min(limit_rows, _DATA.num_rows)
        assert query_result.num_rows == expected_rows
        row_ids = query_result["id"].to_pylist()
        assert len(row_ids) == len(set(row_ids))

        expected_data = _DATA.filter(pc.field("id").isin(row_ids))
        assert expected_data.equals(query_result.sort_by("id"))


@pytest.mark.parametrize("column_name, transform", _TRANSFORMS)
@pytest.mark.parametrize(
    "filter_generator",
    [
        lambda t: t["a"] == 2,
        lambda _: None,
    ],
)
def test_delete_partition(
    hydra_session: Session,
    clean_bucket_name: str,
    column_name: str,
    transform: Transform,
    filter_generator,
):
    schema_name = f"s_{column_name}_{hash(filter_generator)}"
    table_name = f"partitioned_table_{column_name}"

    with hydra_session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema(schema_name)
        t = s.create_table(
            table_name,
            _ARROW_SCHEMA,
            partition_spec=_create_partition_spec(column_name, transform),
        )
        t.insert(_DATA)

    with hydra_session.transaction() as tx:
        t = tx.bucket(clean_bucket_name).schema(schema_name).table(table_name)
        pit = t.partitions()

        for b in pit.select(predicate=filter_generator(pit)):
            t.delete_partitions(b, allow_non_acid=True)

    with hydra_session.transaction() as tx:
        t = tx.bucket(clean_bucket_name).schema(schema_name).table(table_name)
        pit = t.partitions()
        assert list(pit.select(predicate=filter_generator(pit))) == []


@pytest.mark.parametrize("column_name, transform", _TRANSFORMS)
def test_delete_partition_invalid_input(
    hydra_session: Session, clean_bucket_name: str, column_name: str, transform: Transform
):
    if isinstance(transform, DayTransform):
        pytest.skip("Server currently returns a date type on the transformed column and not an int like iceberg")

    schema_name = f"s_{column_name}"
    table_name = f"partitioned_table_{column_name}"

    with hydra_session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema(schema_name)
        t = s.create_table(
            table_name,
            _ARROW_SCHEMA,
            partition_spec=_create_partition_spec(column_name, transform),
        )
        t.insert(_DATA)

    with hydra_session.transaction() as tx:
        t = tx.bucket(clean_bucket_name).schema(schema_name).table(table_name)
        pit = t.partitions()
        partitions_batches_count = len(list(pit.select()))

        # non acid flag is required
        with pytest.raises(
            AssertionError,
            match="Beware, deleting partitions is a non acid operation! In order to proceed please use allow_non_acid=True",
        ):
            b = next(pit.select())
            t.delete_partitions(b)

        # partial partition spec is not allowed
        with pytest.raises(errors.BadRequest, match="InvalidArgument"):
            batch = pa.RecordBatch.from_arrays(
                [
                    pa.array([2]),
                ],
                schema=_DATA.select(["a"]).schema,
            )
            t.delete_partitions(batch, allow_non_acid=True)

        # a partition with these values doesnt exist
        with pytest.raises(errors.BadRequest, match="InvalidArgument"):
            batch = pa.RecordBatch.from_arrays(
                [
                    pa.array([2]),
                    pa.array([4]),
                ],
                schema=pa.schema(
                    [pit.arrow_schema.field("a"), pit.arrow_schema.field(_post_transform_name(column_name, transform))]
                ),
            )
            t.delete_partitions(batch, allow_non_acid=True)

    with hydra_session.transaction() as tx:
        t = tx.bucket(clean_bucket_name).schema(schema_name).table(table_name)
        assert len(list(t.partitions().select())) == partitions_batches_count


def test_non_partitioned_table(hydra_session: Session, clean_bucket_name: str):
    schema_name = "s"
    table_name = "non_partitioned_table"

    with hydra_session.transaction() as tx:
        (
            tx.bucket(clean_bucket_name)
            .create_schema(schema_name)
            .create_table(table_name, _ARROW_SCHEMA)
        )

    with hydra_session.transaction() as tx:
        t = tx.bucket(clean_bucket_name).schema(schema_name).table(table_name)

        with pytest.raises(
            AssertionError, match="partitions can only be used on a partitioned table"
        ):
            t.partitions()

        with pytest.raises(
            AssertionError, match="partitions can only be used on a partitioned table"
        ):
            t.delete_partitions(None)


def test_data_engine_example(hydra_session: Session, clean_bucket_name: str):
    schema_name = "s"
    table_name = "t"

    with hydra_session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema(schema_name)
        t = s.create_table(
            "t",
            _ARROW_SCHEMA,
            partition_spec=_create_partition_spec("d", DayTransform()),
        )
        t.insert(_DATA)

    with hydra_session.transaction() as tx:
        t = tx.bucket(clean_bucket_name).schema(schema_name).table(table_name)
        partitions = t.partitions()

        for b in partitions.select(predicate=partitions["d_day"] < _DATA["d"][4].as_py()):
            t.delete_partitions(b, allow_non_acid=True)

    with hydra_session.transaction() as tx:
        t = tx.bucket(clean_bucket_name).schema(schema_name).table(table_name)
        queried_data = t.select().read_all()
        assert queried_data == _DATA.filter(pc.field("d") == _DATA["d"][4])


def test_data_engine_example2(hydra_session: Session, clean_bucket_name: str):
    schema_name = "s"
    table_name = "t"

    with hydra_session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema(schema_name)
        t = s.create_table(
            "t",
            _ARROW_SCHEMA,
            partition_spec=_create_partition_spec("d", DayTransform()),
        )
        t.insert(_DATA)

    with hydra_session.transaction() as tx:
        t = tx.bucket(clean_bucket_name).schema(schema_name).table(table_name)
        predicate = (
            (ibis._["h"] >= datetime(2025, 1, 1, 1, 5)) &
            (ibis._["h"] <= datetime(2025, 1, 1, 2, 5)) &
            (ibis._["s"].isin({"aaz", "bbz"}))
        )

        queried_data = t.select(predicate=predicate, internal_row_id=True).read_all()
        assert queried_data.schema.field(_internal.INTERNAL_ROW_ID) is not None
        queried_data = queried_data.select(_DATA.schema.names)
        assert queried_data == _DATA.filter((
            (pc.field("h") >= datetime(2025, 1, 1, 1, 5)) &
            (pc.field("h") <= datetime(2025, 1, 1, 2, 5)) &
            ((pc.field("s") == "aaz") | (pc.field("s") == "bbz"))
        ))


@pytest.mark.parametrize(
    "delete_predicate_generator, expected_remaining_filter",
    [
        (lambda t: t["a"] == 1, pc.field("a") != 1),
        (lambda t: t["a"] >= 2, pc.field("a") < 2),
        (lambda t: t["id"] == 4, pc.field("id") != 4),
        (lambda t: t["b"] == 6, pc.field("b") != 6),
        (lambda t: t["s"] == "bbz", pc.field("s") != "bbz"),
        (
                lambda t: (t["id"] >= 1) & (t["id"] <= 3),
                (pc.field("id") < 1) | (pc.field("id") > 3)),
        (
                lambda t: (t["a"] > 1) & (t["b"] < 7),
                (pc.field("a") <= 1) | (pc.field("b") >= 7)),
        (
                lambda t: (t["a"].isin([1, 3])) & (t["id"] < 4),
                ~(pc.field("a").isin([1, 3]) & (pc.field("id") < 4)),
        ),
    ],
)
@pytest.mark.parametrize("column_name, transform", _TRANSFORMS)
def test_delete_rows_partitioned_table(
    hydra_session: Session,
    clean_bucket_name: str,
    column_name: str,
    transform: Transform,
    delete_predicate_generator: Callable,
    expected_remaining_filter,
):
    schema_name = f"s_delete_rows_{column_name}"
    table_name = f"partitioned_table_{column_name}"

    with hydra_session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema(schema_name)
        t = s.create_table(
            table_name,
            _ARROW_SCHEMA,
            partition_spec=_create_partition_spec(column_name, transform),
        )
        t.insert(_DATA)

    with hydra_session.transaction() as tx:
        t = tx.bucket(clean_bucket_name).schema(schema_name).table(table_name)
        rows_to_delete = t.select(
            predicate=delete_predicate_generator(t),
            internal_row_id=True,
        ).read_all()
        t.delete(rows_to_delete.select([table.INTERNAL_ROW_ID]))

    with hydra_session.transaction() as tx:
        t = tx.bucket(clean_bucket_name).schema(schema_name).table(table_name)
        actual = t.select().read_all().sort_by("id")
        expected = _DATA.filter(expected_remaining_filter).sort_by("id")
        assert expected.equals(actual)


@pytest.mark.parametrize("column_name, transform", _TRANSFORMS)
def test_update_rows_partitioned_table(
    hydra_session: Session,
    clean_bucket_name: str,
    column_name: str,
    transform: Transform,
):
    schema_name = f"s_update_rows_{column_name}"
    table_name = f"partitioned_table_{column_name}"

    with hydra_session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema(schema_name)
        t = s.create_table(
            table_name,
            _ARROW_SCHEMA,
            partition_spec=_create_partition_spec(column_name, transform),
        )
        t.insert(_DATA)

    column_to_update = "s" if column_name == "b" else "b"

    with hydra_session.transaction() as tx:
        t = tx.bucket(clean_bucket_name).schema(schema_name).table(table_name)

        rows_to_update = t.select(
            internal_row_id=True,
        ).read_all()

        update_data = pa.table(
            {
                table.INTERNAL_ROW_ID: rows_to_update[table.INTERNAL_ROW_ID],
                column_to_update: _updated_values_for_field(_ARROW_SCHEMA.field(column_to_update), rows_to_update.num_rows),
            }
        )

        t.update(update_data)

    with (hydra_session.transaction() as tx):
        t = tx.bucket(clean_bucket_name).schema(schema_name).table(table_name)
        actual_data = t.select().read_all().sort_by("id")
        all_columns_without_updated_one = _ARROW_SCHEMA.names
        all_columns_without_updated_one.remove(column_to_update)

        updated_data = pa.table(
            {
                column_to_update: _updated_values_for_field(_ARROW_SCHEMA.field(column_to_update), rows_to_update.num_rows),
            }
        )

        assert _DATA.select(all_columns_without_updated_one).equals(actual_data.select(all_columns_without_updated_one))
        assert actual_data.select([column_to_update]).equals(updated_data)


@pytest.mark.parametrize("column_name, transform", _TRANSFORMS)
def test_update_rows_fails_on_partition_column(
        hydra_session: Session,
        clean_bucket_name: str,
        column_name: str,
        transform: Transform,
):
    schema_name = f"s_update_rows_partition_columns_{column_name}_fails"
    table_name = f"partitioned_table_{column_name}"

    with hydra_session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema(schema_name)
        t = s.create_table(
            table_name,
            _ARROW_SCHEMA,
            partition_spec=_create_partition_spec(column_name, transform),
        )
        t.insert(_DATA)

    with hydra_session.transaction() as tx:
        t = tx.bucket(clean_bucket_name).schema(schema_name).table(table_name)

        rows_to_update = t.select(
            predicate=(t["id"] >= 2) & (t["id"] <= 3),
            internal_row_id=True,
        ).read_all()
        assert rows_to_update.num_rows == 2

        update_data_column_a = pa.table(
            {
                table.INTERNAL_ROW_ID: rows_to_update[table.INTERNAL_ROW_ID],
                "a": _updated_values_for_field(_ARROW_SCHEMA.field("a"), rows_to_update.num_rows),
            }
        )

        with pytest.raises(errors.BadRequest, match="InvalidArgument"):
            t.update(update_data_column_a)

        update_data_other_partition_column = pa.table(
            {
                table.INTERNAL_ROW_ID: rows_to_update[table.INTERNAL_ROW_ID],
                column_name: _updated_values_for_field(_ARROW_SCHEMA.field(column_name), rows_to_update.num_rows),
            }
        )

        with pytest.raises(errors.BadRequest, match="InvalidArgument"):
            t.update(update_data_other_partition_column)

    with hydra_session.transaction() as tx:
        t = tx.bucket(clean_bucket_name).schema(schema_name).table(table_name)
        assert _DATA.equals(t.select().read_all().sort_by("id"))
