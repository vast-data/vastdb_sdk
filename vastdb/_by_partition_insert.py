import threading
from collections.abc import Callable, Iterable
from concurrent.futures import Future, ThreadPoolExecutor

import numpy as np
import pyarrow as pa

from . import _by_column_inserter, _internal, util
from .config import BufferedInsertConfig
from .partitioning import PartitionSpec
from .table_metadata import TableMetadata
from .transaction import Transaction


class ByPartitionBuffer:
    def __init__(
        self,
        capacity_in_bytes: int,
        target_num_rows_per_batch: int,
        partition_spec: PartitionSpec,
    ):
        if capacity_in_bytes < 0:
            raise ValueError("Capacity in bytes must be a non negative integer")

        if target_num_rows_per_batch <= 0:
            raise ValueError("Target number of rows per batch must be a positive integer")

        self._capacity_in_bytes = capacity_in_bytes
        self._target_num_rows_per_batch = target_num_rows_per_batch
        self._partition_spec = partition_spec
        self._buffers: dict[tuple, list[pa.RecordBatch]] = dict()
        self._buffers_lock = threading.Lock()

    @property
    def partition_spec(self) -> PartitionSpec:
        return self._partition_spec

    @property
    def target_num_rows_per_batch(self) -> int:
        return self._target_num_rows_per_batch

    @property
    def capacity_in_bytes(self) -> int:
        return self._capacity_in_bytes

    def current_size_in_bytes(self) -> int:
        with self._buffers_lock:
            return self._approximate_size()

    def add(self, record_batch: pa.RecordBatch) -> Iterable[pa.Table]:
        if record_batch.num_rows == 0:
            return ()

        new_partitions = self._split_batch_by_partitions(record_batch)
        buffers_to_send = []

        with self._buffers_lock:
            for partition_key, batch in new_partitions:
                current = self._buffers.get(partition_key)

                if current is None:
                    current = [batch]
                    self._buffers[partition_key] = current
                else:
                    current.append(batch)

                if sum(b.num_rows for b in current) >= self._target_num_rows_per_batch:
                    batches_list = self._buffers.pop(partition_key)
                    buffers_to_send.append(pa.Table.from_batches(batches_list))

            if self._approximate_size() > self._capacity_in_bytes:
                buffers_to_send.extend(self._flush_until_below_capacity())

            return buffers_to_send

    def _split_batch_by_partitions(self, batch: pa.RecordBatch) -> Iterable[tuple[tuple, pa.RecordBatch]]:
        transformed_columns = tuple(
            key.transform.pyarrow_transform(batch.schema.field(key.column).type)(batch[key.column])
            for key in self._partition_spec.partition_keys
        )
        partition_column_names = [key.column for key in self._partition_spec.partition_keys]
        partitions_table = pa.Table.from_arrays(transformed_columns, names=partition_column_names)
        partitions_table = partitions_table.append_column("row_index", pa.array(np.arange(len(batch))))
        partition_rows_table = partitions_table.group_by(partition_column_names).aggregate([("row_index", "list")])
        partition_column_chunks = (partition_rows_table[name].chunks for name in partition_column_names)
        indices_chunks = partition_rows_table["row_index_list"].chunks

        for columns in zip(indices_chunks, *partition_column_chunks):
            idx_chunk = columns[0]
            cols = columns[1:]
            offsets = idx_chunk.offsets
            chunks_as_lists = [chunk.to_pylist() for chunk in cols]

            for i in range(len(idx_chunk)):
                start = offsets[i].as_py()
                end = offsets[i + 1].as_py()
                sub_indices = idx_chunk.values.slice(start, end - start)
                sub_batch = batch.take(sub_indices)
                key = tuple(c[i] for c in chunks_as_lists)
                yield key, sub_batch

    def _approximate_size(self) -> int:
        return sum(sum(b.nbytes for b in batches_list) for batches_list in self._buffers.values())

    def _flush_until_below_capacity(self) -> Iterable[pa.Table]:
        sorted_buffers = sorted(
            self._buffers.items(),
            key=lambda item: sum(b.nbytes for b in item[1]),
            reverse=True,
        )
        current_size = self._approximate_size()
        accumulated_size_to_flush = 0
        keys_to_send = []

        for key, batches_list in sorted_buffers:
            keys_to_send.append(key)
            accumulated_size_to_flush += sum(b.nbytes for b in batches_list)

            if current_size - accumulated_size_to_flush < self._capacity_in_bytes:
                break

        return (pa.Table.from_batches(self._buffers.pop(key)) for key in keys_to_send)

    def flush_all(self) -> Iterable[pa.Table]:
        with self._buffers_lock:
            buffers_to_send = self._buffers.values()
            self._buffers = dict()
            return (pa.Table.from_batches(batches) for batches in buffers_to_send)


class ByPartitionBufferedInserter:
    def __init__(self,
                 transaction_creator: Callable[[], Transaction],
                 api: _internal.VastdbApi,
                 table_metadata: TableMetadata,
                 executor: ThreadPoolExecutor,
                 flush_config: BufferedInsertConfig) -> None:
        if table_metadata.partitioning is None:
            raise ValueError("Table must be partitioned to use a buffered inserter")

        self._api = api
        self._transaction_creator = transaction_creator
        self._tx = flush_config.tx
        self._by_partition_buffer = ByPartitionBuffer(
            flush_config.capacity_in_bytes, flush_config.target_num_rows_per_partition, table_metadata.partitioning.spec)
        self._metadata = table_metadata
        self._executor = executor
        self._timer = None if flush_config.timeout is None else util.Timer(self._flush_all, flush_config.timeout)
        self._results: list[Future] = []
        self._is_finished = False

    def _flush_batch(self, table: pa.Table) -> None:
        self._results.append(self._executor.submit(self._send_batch_to_server, table))

    def _flush_all(self) -> None:
        for table in self._by_partition_buffer.flush_all():
            self._flush_batch(table)

    def _assert_not_finished(self) -> None:
        if self._is_finished:
            raise AssertionError("Cannot use the buffered inserter after call to finish")

    def add_batch(self, batch: pa.RecordBatch) -> None:
        self._assert_not_finished()

        for table in self._by_partition_buffer.add(batch):
            if self._timer is not None:
                self._timer.reset()

            self._flush_batch(table)

    def finish(self) -> None:
        if self._is_finished:
            return

        self._is_finished = True

        if self._timer is not None:
            self._timer.stop()

        self._flush_all()

        for result in self._results:
            result.result()

    def __enter__(self) -> "ByPartitionBufferedInserter":
        self._assert_not_finished()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.finish()

    def _send_batch_to_server(self, table: pa.Table) -> None:
        tx, should_close_tx = (self._transaction_creator(), True) if self._tx is None else (self._tx, False)

        def insert_rows_rpc(batch: pa.RecordBatch) -> pa.Array:
            res = self._api.insert_rows(
                self._metadata.ref.bucket,
                self._metadata.ref.schema,
                self._metadata.ref.table,
                util.serialize_record_batch(batch),
                tx.active_txid
            )
            (ids_batch,) = pa.ipc.open_stream(res.content)
            return ids_batch[_internal.INTERNAL_ROW_ID]

        def update_rows_rpc(batch: pa.RecordBatch) -> None:
            self._api.update_rows(
                self._metadata.ref.bucket,
                self._metadata.ref.schema,
                self._metadata.ref.table,
                util.serialize_record_batch(batch),
                tx.active_txid
            )

        inserter = _by_column_inserter.Inserter(
            insert_rows_rpc,
            update_rows_rpc,
            _internal.MAX_REQUEST_BODY_SIZE,
            self._metadata.arrow_schema,
            self._metadata.non_updatable_column_names()
        )
        table_as_batch = table.combine_chunks().to_batches()[0]

        if should_close_tx:
            with tx:
                inserter.insert(table_as_batch)
        else:
            inserter.insert(table_as_batch)
