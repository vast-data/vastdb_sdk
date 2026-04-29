import time
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta

import pyarrow as pa
import pytest
from dateutil.relativedelta import relativedelta
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    TruncateTransform,
    YearTransform,
)

from vastdb import errors
from vastdb._by_partition_insert import ByPartitionBuffer, ByPartitionBufferedInserter
from vastdb.config import BufferedInsertConfig
from vastdb.partitioning import PartitionKey, PartitionSpec
from vastdb.session import Session
from vastdb.table_metadata import TableRef

_ARROW_SCHEMA = pa.schema(
    [
        ("id", pa.uint64()),
        ("a", pa.int64()),
        ("b", pa.int32()),
        ("s", pa.utf8()),
        ("y", pa.timestamp("us")),
        ("m", pa.timestamp("us")),
        ("d", pa.timestamp("us")),
        ("h", pa.timestamp("us")),
    ]
)


def _create_batch(num_partitions: int, num_rows_per_partition: int, first_id: int = 0) -> pa.RecordBatch:
    num_rows = num_partitions * num_rows_per_partition
    data = {
        "id": [i + first_id for i in range(num_rows)],
        "a": [i % num_partitions for i in range(num_rows)],
        "b": [i % num_partitions for i in range(num_rows)],
        "s": [f"s{i % num_partitions}" for i in range(num_rows)],
        "d": [datetime(2025, 1, 1) + timedelta(days=i % num_partitions) for i in range(num_rows)],
        "m": [datetime(2025, 1, 1) + relativedelta(months=i % num_partitions) for i in range(num_rows)],
        "y": [datetime(2025, 1, 1) + relativedelta(years=i % num_partitions) for i in range(num_rows)],
        "h": [datetime(2025, 1, 1) + timedelta(hours=i % num_partitions) for i in range(num_rows)],
    }
    return pa.RecordBatch.from_pydict(data, schema=_ARROW_SCHEMA)


def _assert_data_is_complete(created_batches: Iterable[pa.RecordBatch], ready_tables: Iterable[pa.Table]):
    concatenated_batch = pa.Table.from_batches(batch for batch in created_batches)
    concatenated_table = pa.concat_tables(ready_tables)
    sorted_table = concatenated_table.sort_by("id")
    assert concatenated_batch.equals(sorted_table)


@dataclass(frozen=True)
class AddCommand:
    num_partitions: int
    num_rows_per_partition: int


@dataclass(frozen=True)
class FlushAllCommand:
    pass


BufferCommand = AddCommand | FlushAllCommand


@pytest.mark.parametrize(
    "partition_spec",
    (
        PartitionSpec(partition_keys=[PartitionKey(column="a", transform=IdentityTransform())]),
        PartitionSpec(partition_keys=[PartitionKey(column="s", transform=TruncateTransform(8))]),
        PartitionSpec(partition_keys=[PartitionKey(column="d", transform=DayTransform())]),
        PartitionSpec(partition_keys=[PartitionKey(column="m", transform=MonthTransform())]),
        PartitionSpec(partition_keys=[PartitionKey(column="y", transform=YearTransform())]),
        PartitionSpec(partition_keys=[PartitionKey(column="h", transform=HourTransform())]),
        PartitionSpec(
            partition_keys=[
                PartitionKey(column="a", transform=IdentityTransform()),
                PartitionKey(column="b", transform=IdentityTransform()),
            ]
        ),
        PartitionSpec(
            partition_keys=[
                PartitionKey(column="a", transform=IdentityTransform()),
                PartitionKey(column="b", transform=BucketTransform(4)),
            ]
        ),
        PartitionSpec(
            partition_keys=[
                PartitionKey(column="a", transform=IdentityTransform()),
                PartitionKey(column="s", transform=TruncateTransform(8)),
            ]
        ),
        PartitionSpec(
            partition_keys=[
                PartitionKey(column="a", transform=IdentityTransform()),
                PartitionKey(column="d", transform=DayTransform()),
            ]
        ),
        PartitionSpec(
            partition_keys=[
                PartitionKey(column="a", transform=IdentityTransform()),
                PartitionKey(column="m", transform=MonthTransform()),
            ]
        ),
        PartitionSpec(
            partition_keys=[
                PartitionKey(column="a", transform=IdentityTransform()),
                PartitionKey(column="y", transform=YearTransform()),
            ]
        ),
        PartitionSpec(
            partition_keys=[
                PartitionKey(column="a", transform=IdentityTransform()),
                PartitionKey(column="h", transform=HourTransform()),
            ]
        ),
    ),
)
@pytest.mark.parametrize(
    "buffer_capacity_in_bytes, num_target_rows_per_partition",
    (
        (0, 10),
        (2**20, 1),
        (2**10, 50),
        (2**10, 100),
    ),
)
@pytest.mark.parametrize(
    "commands",
    (
        (AddCommand(0, 0),),
        (AddCommand(1, 1),),
        (AddCommand(1, 50),),
        (AddCommand(1, 100),),
        (AddCommand(10, 1),),
        (AddCommand(10, 1),),
        (AddCommand(100, 1),),
        (AddCommand(10, 10),),
        (AddCommand(100, 100),),
        (AddCommand(5, 100), AddCommand(2, 20), AddCommand(6, 50)),
        (FlushAllCommand(),),
        (FlushAllCommand(), FlushAllCommand()),
        (
            AddCommand(1, 100),
            AddCommand(2, 100),
            AddCommand(4, 50),
            AddCommand(5, 40),
            AddCommand(10, 70),
            AddCommand(20, 0),
            AddCommand(20, 50),
            AddCommand(40, 25),
            AddCommand(50, 0),
            AddCommand(50, 20),
            AddCommand(100, 10),
            AddCommand(100, 5),
            AddCommand(100, 4),
            AddCommand(50, 2),
            AddCommand(60, 1),
            AddCommand(10, 0),
        ),
        (
            AddCommand(100, 1),
            AddCommand(1, 100),
            AddCommand(100, 2),
            AddCommand(2, 100),
            AddCommand(30, 4),
            AddCommand(4, 60),
            AddCommand(100, 10),
            AddCommand(10, 0),
            AddCommand(10, 100),
            AddCommand(50, 20),
            AddCommand(20, 0),
            AddCommand(20, 50),
        ),
        (
            AddCommand(1, 100),
            AddCommand(1, 900),
            AddCommand(1, 800),
            AddCommand(1, 700),
            AddCommand(1, 600),
            AddCommand(1, 500),
            AddCommand(1, 400),
            AddCommand(1, 300),
            AddCommand(1, 200),
            AddCommand(1, 100),
        ),
        (
            AddCommand(100, 1),
            AddCommand(900, 1),
            AddCommand(800, 1),
            AddCommand(700, 1),
            AddCommand(600, 1),
            AddCommand(500, 1),
            AddCommand(400, 1),
            AddCommand(300, 1),
            AddCommand(200, 1),
            AddCommand(100, 1),
        ),
        (
            AddCommand(1, 1000),
            AddCommand(1000, 1),
            AddCommand(2, 500),
            AddCommand(500, 2),
            AddCommand(4, 250),
            AddCommand(250, 4),
            AddCommand(5, 200),
            AddCommand(200, 0),
            AddCommand(200, 5),
            AddCommand(10, 100),
            AddCommand(100, 10),
            AddCommand(20, 0),
            AddCommand(20, 50),
            AddCommand(50, 20),
            AddCommand(40, 25),
            AddCommand(25, 0),
            AddCommand(25, 40),
        ),
        (
            AddCommand(10, 10),
            AddCommand(5, 0),
            AddCommand(5, 20),
            AddCommand(4, 25),
            FlushAllCommand(),
            AddCommand(20, 0),
            AddCommand(20, 10),
        ),
        (
            AddCommand(25, 10),
            AddCommand(10, 0),
            AddCommand(10, 10),
            AddCommand(5, 20),
            AddCommand(2, 0),
            AddCommand(2, 50),
            FlushAllCommand(),
            AddCommand(10, 10),
            AddCommand(10, 0),
        ),
        (
            AddCommand(10, 10),
            AddCommand(10, 0),
            AddCommand(10, 10),
            AddCommand(10, 10),
            AddCommand(10, 0),
            AddCommand(10, 10),
            AddCommand(10, 10),
        ),
        (
            AddCommand(2, 100),
            AddCommand(4, 0),
            AddCommand(4, 50),
            AddCommand(5, 40),
            FlushAllCommand(),
            AddCommand(10, 0),
            AddCommand(10, 10),
            AddCommand(20, 5),
        ),
        (
            AddCommand(10, 10),
            AddCommand(10, 0),
            AddCommand(10, 10),
            AddCommand(10, 10),
            FlushAllCommand(),
            AddCommand(10, 0),
            AddCommand(10, 10),
            AddCommand(10, 10),
        ),
        (AddCommand(100, 10), FlushAllCommand()),
        (
            AddCommand(10, 10),
            AddCommand(10, 0),
            AddCommand(10, 10),
            AddCommand(10, 10),
            FlushAllCommand(),
            AddCommand(10, 0),
            AddCommand(10, 10),
            AddCommand(10, 10),
            FlushAllCommand(),
            AddCommand(10, 0),
            AddCommand(10, 10),
            AddCommand(10, 10),
            FlushAllCommand(),
        ),
        (
            AddCommand(20, 10),
            AddCommand(10, 0),
            AddCommand(10, 20),
            AddCommand(5, 0),
            AddCommand(5, 40),
            AddCommand(4, 50),
            FlushAllCommand(),
            AddCommand(2, 0),
            AddCommand(2, 100),
            FlushAllCommand(),
        ),
        (
            AddCommand(10, 10),
            AddCommand(5, 0),
            AddCommand(5, 20),
            AddCommand(4, 0),
            AddCommand(4, 25),
            AddCommand(2, 50),
            AddCommand(1, 0),
            AddCommand(1, 100),
        ),
        (
            AddCommand(10, 10),
            AddCommand(10, 10),
            AddCommand(10, 10),
            AddCommand(10, 10),
            AddCommand(10, 10),
            AddCommand(10, 10),
            AddCommand(10, 10),
            FlushAllCommand(),
            FlushAllCommand(),
            AddCommand(10, 10),
            AddCommand(10, 10),
            AddCommand(10, 10),
            AddCommand(10, 10),
            AddCommand(10, 10),
            AddCommand(10, 10),
        ),
        (AddCommand(5, 50), FlushAllCommand(), AddCommand(10, 10), AddCommand(2, 100)),
        (FlushAllCommand(), AddCommand(1, 500), AddCommand(2, 200), FlushAllCommand()),
        (
            AddCommand(2, 300),
            AddCommand(3, 200),
            AddCommand(4, 100),
            FlushAllCommand(),
            AddCommand(5, 50),
        ),
        (
            AddCommand(2, 400),
            AddCommand(4, 200),
            AddCommand(5, 100),
            AddCommand(10, 50),
        ),
        (
            AddCommand(1, 100),
            AddCommand(2, 200),
            FlushAllCommand(),
            AddCommand(4, 100),
            AddCommand(5, 50),
            AddCommand(10, 10),
            FlushAllCommand(),
            AddCommand(3, 300),
            AddCommand(2, 100),
        ),
        (
            AddCommand(6, 10),
            AddCommand(7, 20),
            AddCommand(8, 30),
            FlushAllCommand(),
            FlushAllCommand(),
            AddCommand(2, 400),
            AddCommand(5, 100),
            AddCommand(10, 50),
        ),
        (
            AddCommand(3, 100),
            AddCommand(4, 200),
            AddCommand(5, 100),
            AddCommand(2, 250),
            FlushAllCommand(),
            AddCommand(1, 1000),
        ),
        (
            AddCommand(2, 100),
            AddCommand(3, 200),
            AddCommand(4, 150),
            AddCommand(5, 100),
            FlushAllCommand(),
            AddCommand(1, 1000),
            FlushAllCommand(),
            AddCommand(10, 50),
            AddCommand(5, 200),
        ),
        (
            AddCommand(25, 4),
            AddCommand(20, 5),
            AddCommand(10, 10),
            AddCommand(8, 12),
            AddCommand(5, 20),
            FlushAllCommand(),
            AddCommand(4, 25),
        ),
        (
            AddCommand(2, 250),
            AddCommand(4, 125),
            AddCommand(5, 100),
            AddCommand(10, 50),
            AddCommand(20, 25),
            AddCommand(25, 20),
            FlushAllCommand(),
        ),
    ),
)
def test_split_by_partitions(
    partition_spec: PartitionSpec,
    buffer_capacity_in_bytes: int,
    num_target_rows_per_partition: int,
    commands: tuple[BufferCommand, ...],
):
    created_batches: list[pa.RecordBatch] = []
    flushed_tables = []
    buffer = ByPartitionBuffer(buffer_capacity_in_bytes, num_target_rows_per_partition, partition_spec)

    for command in commands:
        if isinstance(command, FlushAllCommand):
            ready_tables = _flush_buffer(buffer)
            flushed_tables.extend(ready_tables)
            continue

        if isinstance(command, AddCommand):
            batch = _create_batch(
                command.num_partitions,
                command.num_rows_per_partition,
                first_id=sum(b.num_rows for b in created_batches),
            )

            created_batches.append(batch)
            ready_tables = _add_batch(buffer, batch)
            flushed_tables.extend(ready_tables)
            continue

        raise TypeError(f"Unknown buffer command type: {type(command)} - this is a test issue")

    remaining_tables = _flush_buffer(buffer)
    flushed_tables.extend(remaining_tables)

    if len(flushed_tables) == 0:
        assert sum(b.num_rows for b in created_batches) == 0
    else:
        _assert_data_is_complete(created_batches, flushed_tables)


def _flush_buffer(buffer: ByPartitionBuffer) -> list[pa.Table]:
    original_size = buffer.current_size_in_bytes()
    ready_tables = list(buffer.flush_all())

    if len(ready_tables) == 0:
        assert original_size == 0
    else:
        assert original_size > 0

    for table in ready_tables:
        assert table.schema.equals(_ARROW_SCHEMA)
        assert 0 < table.num_rows < buffer.target_num_rows_per_batch

        for key in buffer.partition_spec.partition_keys:
            key_value = table[key.column][0].as_py()
            assert all(value == key_value for value in table[key.column].to_pylist())

    return ready_tables


def _add_batch(buffer: ByPartitionBuffer, batch: pa.RecordBatch) -> list[pa.Table]:
    ready_tables = list(buffer.add(batch))

    if len(ready_tables) == 0:
        assert batch.num_rows == 0 or buffer.current_size_in_bytes() <= buffer.capacity_in_bytes
        return ready_tables

    assert batch.num_rows > 0
    found_small_partition = False
    total_small_partitions_size = 0
    last_small_partition_num_rows = None

    for table in ready_tables:
        assert table.schema.equals(_ARROW_SCHEMA)

        if table.num_rows >= buffer.target_num_rows_per_batch:
            assert not found_small_partition
            continue

        found_small_partition = True
        total_small_partitions_size += table.nbytes

        if last_small_partition_num_rows is not None:
            assert table.num_rows <= last_small_partition_num_rows

        last_small_partition_num_rows = table.num_rows

        for key in buffer.partition_spec.partition_keys:
            key_value = table[key.column][0].as_py()
            assert all(value == key_value for value in table[key.column].to_pylist())

    if found_small_partition:
        buffer_size_with_small_partitions = buffer.current_size_in_bytes() + total_small_partitions_size
        # assert buffer_size_with_small_partitions - ready_tables[-1].nbytes <= buffer.capacity_in_bytes
        assert buffer_size_with_small_partitions > buffer.capacity_in_bytes

    return ready_tables


@dataclass(frozen=True)
class IdleCommand:
    time_in_seconds: float


UserCommand = AddCommand | IdleCommand


@pytest.mark.parametrize(
    "commands",
    (
        [AddCommand(3, 32)],
        [AddCommand(3, 32), AddCommand(3, 32)],
        [
            AddCommand(2, 10),
            IdleCommand(0.5),
            AddCommand(2, 60),
            IdleCommand(1.2),
            AddCommand(2, 5),
            AddCommand(0, 0),
            AddCommand(2, 80),
        ],
        [
            AddCommand(5, 20),
            AddCommand(0, 0),
            IdleCommand(0.2),
            AddCommand(3, 70),
            IdleCommand(1.5),
            AddCommand(2, 10),
            AddCommand(5, 100),
        ],
        [
            AddCommand(1, 100),
            IdleCommand(0.8),
            AddCommand(1, 10),
            AddCommand(1, 1),
            IdleCommand(1.1),
            AddCommand(1, 80),
            AddCommand(0, 0),
        ],
        [
            AddCommand(4, 5),
            AddCommand(4, 5),
            IdleCommand(0.3),
            AddCommand(4, 60),
            AddCommand(0, 0),
            IdleCommand(1.3),
            AddCommand(4, 100),
        ],
        [
            AddCommand(7, 8),
            IdleCommand(0.7),
            AddCommand(3, 90),
            AddCommand(0, 0),
            IdleCommand(1.7),
            AddCommand(2, 2),
            AddCommand(7, 70),
        ],
        [
            AddCommand(10, 1),
            AddCommand(10, 1),
            IdleCommand(0.4),
            AddCommand(10, 80),
            AddCommand(0, 0),
            IdleCommand(1.8),
            AddCommand(10, 100),
        ],
        [
            AddCommand(3, 64),
            IdleCommand(0.6),
            AddCommand(2, 32),
            AddCommand(0, 0),
            IdleCommand(1.4),
            AddCommand(3, 1),
            AddCommand(3, 100),
        ],
        [
            AddCommand(6, 10),
            AddCommand(0, 0),
            IdleCommand(0.9),
            AddCommand(6, 70),
            IdleCommand(1.6),
            AddCommand(1, 5),
            AddCommand(6, 90),
        ],
        [
            AddCommand(8, 5),
            IdleCommand(0.3),
            AddCommand(8, 60),
            AddCommand(0, 0),
            IdleCommand(1.9),
            AddCommand(2, 10),
            AddCommand(8, 100),
        ],
        [
            AddCommand(9, 7),
            AddCommand(0, 0),
            IdleCommand(0.1),
            AddCommand(9, 90),
            IdleCommand(1.3),
            AddCommand(3, 3),
            AddCommand(9, 99),
        ],
        [
            AddCommand(2, 17),
            IdleCommand(0.5),
            AddCommand(2, 31),
            IdleCommand(1.2),
            AddCommand(2, 13),
            AddCommand(0, 0),
            AddCommand(2, 29),
            AddCommand(2, 7),
            IdleCommand(0.8),
            AddCommand(2, 61),
            AddCommand(2, 3),
            AddCommand(2, 11),
            IdleCommand(1.1),
            AddCommand(2, 19),
            AddCommand(2, 23),
            AddCommand(2, 37),
            IdleCommand(0.4),
            AddCommand(2, 41),
            AddCommand(2, 43),
            AddCommand(2, 47),
            AddCommand(2, 53),
            AddCommand(2, 59),
            IdleCommand(1.3),
            AddCommand(2, 67),
            AddCommand(2, 71),
            AddCommand(2, 73),
            AddCommand(2, 79),
            AddCommand(2, 83),
            AddCommand(2, 89),
            IdleCommand(0.7),
            AddCommand(2, 97),
            AddCommand(2, 5),
            AddCommand(2, 13),
            AddCommand(2, 17),
            AddCommand(2, 19),
            AddCommand(2, 23),
            IdleCommand(1.7),
            AddCommand(2, 29),
            AddCommand(2, 31),
            AddCommand(2, 37),
            AddCommand(2, 41),
            AddCommand(2, 43),
            AddCommand(2, 47),
            AddCommand(2, 53),
            IdleCommand(0.2),
            AddCommand(2, 59),
            AddCommand(2, 61),
            AddCommand(2, 67),
            AddCommand(2, 71),
            AddCommand(2, 73),
            IdleCommand(1.9),
            AddCommand(2, 79),
            AddCommand(2, 83),
            AddCommand(2, 89),
            AddCommand(2, 97),
            AddCommand(0, 0),
        ],
        [
            AddCommand(5, 19),
            AddCommand(0, 0),
            IdleCommand(0.2),
            AddCommand(3, 23),
            IdleCommand(1.5),
            AddCommand(2, 37),
            AddCommand(5, 41),
            AddCommand(5, 5),
            IdleCommand(0.6),
            AddCommand(5, 59),
            AddCommand(5, 17),
            AddCommand(5, 13),
            IdleCommand(0.8),
            AddCommand(5, 53),
            AddCommand(5, 7),
            AddCommand(5, 11),
            IdleCommand(1.1),
            AddCommand(5, 23),
            AddCommand(5, 29),
            AddCommand(5, 31),
            AddCommand(5, 37),
            AddCommand(5, 41),
            IdleCommand(1.3),
            AddCommand(5, 43),
            AddCommand(5, 47),
            AddCommand(5, 53),
            AddCommand(5, 59),
            AddCommand(5, 61),
            AddCommand(5, 67),
            IdleCommand(0.7),
            AddCommand(5, 71),
            AddCommand(5, 73),
            AddCommand(5, 79),
            AddCommand(5, 83),
            AddCommand(5, 89),
            AddCommand(5, 97),
            IdleCommand(1.7),
            AddCommand(5, 5),
            AddCommand(5, 13),
            AddCommand(5, 17),
            AddCommand(5, 19),
            AddCommand(5, 23),
            AddCommand(5, 29),
            IdleCommand(1.9),
            AddCommand(5, 31),
            AddCommand(5, 37),
            AddCommand(5, 41),
            AddCommand(5, 43),
            AddCommand(5, 47),
            AddCommand(5, 53),
            AddCommand(5, 59),
            AddCommand(0, 0),
        ],
    ),
)
def test_partitioned_insert(session: Session, clean_bucket_name: str, commands: list[UserCommand]):
    try:
        session.features.check_partitioned()
    except errors.NotSupportedVersion:
        pytest.skip("Test requires hydra to run")

    ref = _create_partitioned_table(session, clean_bucket_name)
    executor = ThreadPoolExecutor(max_workers=3)

    with session.buffered_inserter(
        ref, executor, flush_config=BufferedInsertConfig(target_num_rows_per_partition=64, timeout=timedelta(seconds=1))
    ) as inserter:
        created_batches = _insert_batches(inserter, commands)

    with session.transaction() as tx:
        t = tx.bucket(clean_bucket_name).schema(ref.schema).table(ref.table)
        result = pa.Table.from_batches(t.select()).sort_by("id")
        _assert_data_is_complete(created_batches, [result])

    executor.shutdown(wait=True)


def _create_partitioned_table(session: Session, clean_bucket_name: str) -> TableRef:
    schema_name = "s"
    table_name = "t"

    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema(schema_name)
        s.create_table(
            table_name,
            _ARROW_SCHEMA,
            partition_spec=PartitionSpec(
                [
                    PartitionKey(transform=IdentityTransform(), column="a"),
                    PartitionKey(transform=BucketTransform(16), column="b"),
                ]
            ),
        )

    return TableRef(clean_bucket_name, schema_name, table_name)


def _insert_batches(inserter: ByPartitionBufferedInserter, commands: list[UserCommand]) -> list[pa.RecordBatch]:
    created_batches: list[pa.RecordBatch] = []

    for command in commands:
        if isinstance(command, AddCommand):
            next_id = sum(b.num_rows for b in created_batches)
            batch = _create_batch(command.num_partitions, command.num_rows_per_partition, next_id)
            created_batches.append(batch)
            inserter.add_batch(batch)
        elif isinstance(command, IdleCommand):
            time.sleep(command.time_in_seconds)
        else:
            raise TypeError(f"Unsupported command type: {type(command)} - this is a test issue")

    return created_batches


def test_partitioned_insert_explicit_tx(session: Session, clean_bucket_name: str):
    try:
        session.features.check_partitioned()
    except errors.NotSupportedVersion:
        pytest.skip("Test requires hydra to run")

    ref = _create_partitioned_table(session, clean_bucket_name)
    executor = ThreadPoolExecutor(max_workers=3)

    with (
        session.transaction() as tx,
        session.buffered_inserter(
            ref, executor, flush_config=BufferedInsertConfig(target_num_rows_per_partition=64, tx=tx)
        ) as inserter,
    ):
        created_batches = _insert_batches(inserter, [
            AddCommand(3, 32),
            AddCommand(3, 32),
            AddCommand(2, 10),
            AddCommand(2, 60),
            AddCommand(2, 5),
            AddCommand(5, 20),
            AddCommand(5, 100),
            AddCommand(10, 0),
            AddCommand(10, 10),
            AddCommand(20, 5),
        ])

    with session.transaction() as tx:
        t = tx.bucket(clean_bucket_name).schema(ref.schema).table(ref.table)
        result = pa.Table.from_batches(t.select()).sort_by("id")
        _assert_data_is_complete(created_batches, [result])

    executor.shutdown(wait=True)
