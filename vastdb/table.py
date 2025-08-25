"""VAST Database table."""

import concurrent.futures
import itertools
import logging
import os
import queue
import sys
from dataclasses import dataclass
from math import ceil
from queue import Queue
from threading import Event
from typing import (
    TYPE_CHECKING,
    Callable,
    Iterable,
    Optional,
    Union,
)

import ibis
import pyarrow as pa
import urllib3

from vastdb._table_interface import ITable
from vastdb.table_metadata import TableMetadata, TableRef, TableStats, TableType

from . import _internal, errors, util
from ._ibis_support import validate_ibis_support_schema
from .config import ImportConfig, QueryConfig

if TYPE_CHECKING:
    from .transaction import Transaction

log = logging.getLogger(__name__)


INTERNAL_ROW_ID = "$row_id"
INTERNAL_ROW_ID_FIELD = pa.field(INTERNAL_ROW_ID, pa.uint64())
INTERNAL_ROW_ID_SORTED_FIELD = pa.field(
    INTERNAL_ROW_ID, pa.decimal128(38, 0))  # Sorted tables have longer row ids

MAX_ROWS_PER_BATCH = 512 * 1024
# for insert we need a smaller limit due to response amplification
# for example insert of 512k uint8 result in 512k*8bytes response since row_ids are uint64
MAX_INSERT_ROWS_PER_PATCH = 512 * 1024
# in case insert has TooWideRow - need to insert in smaller batches - each cell could contain up to 128K, and our wire is limited to 5MB
MAX_COLUMN_IN_BATCH = int(5 * 1024 / 128)
SORTING_SCORE_BITS = 63


class _EmptyResultException(Exception):
    response_schema: pa.Schema

    def __init__(self, response_schema: pa.Schema):
        self.response_schema = response_schema


@dataclass
class SplitWorkerConfig:
    """Split worker configuration."""

    num_splits: int
    num_sub_splits: int
    num_row_groups_per_sub_split: int
    limit_rows_per_sub_split: int
    use_semi_sorted_projections: bool
    queue_priority: Optional[int]
    semi_sorted_projection_name: Optional[str]


class SplitWorker:
    """State of a specific query split execution."""

    def __init__(self,
                 api: _internal.VastdbApi,
                 query_data_request: _internal.QueryDataRequest,
                 bucket_name: str,
                 schema_name: str,
                 table_name: str,
                 txid: Optional[int],
                 query_imports_table: bool,
                 split_id: int,
                 config: SplitWorkerConfig) -> None:
        """Initialize query split state."""
        self.api = api
        self.split_id = split_id
        self.subsplits_state = {i: 0 for i in range(config.num_sub_splits)}
        self.query_data_request = query_data_request
        self.bucket_name = bucket_name
        self.schema_name = schema_name
        self.table_name = table_name
        self.txid = txid
        self.query_imports_table = query_imports_table
        self.config = config

    def __iter__(self):
        """Execute a sequence of QueryData requests, and queue the parsed RecordBatch objects.

        Can be called repeatedly, to support resuming the query after a disconnection / retriable error.
        """
        try:
            # contains RecordBatch parts received from the server, must be re-created in case of a retry
            while not self.done:
                # raises if request parsing fails or throttled due to server load, and will be externally retried
                response = self.api.query_data(
                    bucket=self.bucket_name,
                    schema=self.schema_name,
                    table=self.table_name,
                    params=self.query_data_request.serialized,
                    split=(self.split_id, self.config.num_splits,
                           self.config.num_row_groups_per_sub_split),
                    num_sub_splits=self.config.num_sub_splits,
                    response_row_id=False,
                    txid=self.txid,
                    limit_rows=self.config.limit_rows_per_sub_split,
                    sub_split_start_row_ids=self.subsplits_state.items(),
                    schedule_id=self.config.queue_priority,
                    enable_sorted_projections=self.config.use_semi_sorted_projections,
                    query_imports_table=self.query_imports_table,
                    projection=self.config.semi_sorted_projection_name)

                # can raise during response parsing (e.g. due to disconnections), and will be externally retried
                # the pagination state is stored in `self.subsplits_state` and must be correct in case of a reconnection
                # the partial RecordBatch chunks are managed internally in `parse_query_data_response`
                response_iter = _internal.parse_query_data_response(
                    conn=response.raw,
                    schema=self.query_data_request.response_schema,
                    parser=self.query_data_request.response_parser)

                for stream_id, next_row_id, table_chunk in response_iter:
                    # in case of I/O error, `response_iter` will be closed and an appropriate exception will be thrown.
                    self.subsplits_state[stream_id] = next_row_id
                    # we have parsed a pyarrow.Table successfully, self.subsplits_state is now correctly updated
                    # if the below loop fails, the query is not retried
                    for batch in table_chunk.to_batches():
                        yield batch
        except urllib3.exceptions.ProtocolError as err:
            fully_qualified_table_name = f"\"{self.bucket_name}/{self.schema_name}\".{self.table_name}"
            log.warning("Failed parsing QueryData response table=%s txid=%s split=%s/%s offsets=%s cause=%s",
                        fully_qualified_table_name, self.txid,
                        self.split_id, self.config.num_splits, self.subsplits_state, err)
            # since this is a read-only idempotent operation, it is safe to retry
            raise errors.ConnectionError(cause=err, may_retry=True)

    def split_record_batch_reader(self) -> pa.RecordBatchReader:
        """Return pa.RecordBatchReader for split."""
        return pa.RecordBatchReader.from_batches(self.query_data_request.response_schema,
                                                 self)

    def _process_split(self, record_batches_queue: Queue[pa.RecordBatch], check_stop: Callable):
        """Process split and enqueues batches into the queue."""
        for batch in self:
            check_stop()  # may raise StoppedException to early-exit the query (without retries)
            if batch:
                record_batches_queue.put(batch)

    @property
    def done(self):
        """Returns true iff the pagination over."""
        return all(row_id == _internal.TABULAR_INVALID_ROW_ID for row_id in self.subsplits_state.values())


class TableInTransaction(ITable):
    """VAST Table."""

    _metadata: TableMetadata
    _tx: "Transaction"

    def __init__(self,
                 metadata: TableMetadata,
                 tx: "Transaction"):
        """VastDB Table."""
        self._metadata = metadata
        self._tx = tx

    @property
    def ref(self) -> TableRef:
        """Table Reference."""
        return self._metadata.ref

    def __eq__(self, other: object) -> bool:
        """Table __eq__."""
        if not isinstance(other, type(self)):
            return False

        return self.ref == other.ref

    @property
    def name(self) -> str:
        """Table name."""
        return self.ref.table

    @property
    def arrow_schema(self) -> pa.Schema:
        """Table arrow schema."""
        return self._metadata.arrow_schema

    @property
    def stats(self) -> Optional[TableStats]:
        """Table's statistics."""
        return self._metadata.stats

    def reload_schema(self) -> None:
        """Reload Arrow Schema."""
        self._metadata.load_schema(self._tx)

    def reload_stats(self) -> None:
        """Reload Table Stats."""
        self._metadata.load_stats(self._tx)

    def reload_sorted_columns(self) -> None:
        """Reload Sorted Columns."""
        self._metadata.load_sorted_columns(self._tx)

    @property
    def path(self) -> str:
        """Return table's path."""
        return self.ref.full_path

    @property
    def _internal_rowid_field(self) -> pa.Field:
        return INTERNAL_ROW_ID_SORTED_FIELD if self._is_sorted_table else INTERNAL_ROW_ID_FIELD

    def sorted_columns(self) -> list[str]:
        """Return sorted columns' metadata."""
        return self._metadata.sorted_columns

    def _assert_not_imports_table(self):
        if self._metadata.is_imports_table:
            raise errors.NotSupportedCommand(
                self.ref.bucket, self.ref.schema, self.ref.table)

    def projection(self, name: str) -> "Projection":
        """Get a specific semi-sorted projection of this table."""
        self._assert_not_imports_table()

        projs = tuple(self.projections(projection_name=name))
        if not projs:
            raise errors.MissingProjection(
                self.ref.bucket, self.ref.schema, self.ref.table, name)
        if len(projs) != 1:
            raise AssertionError(
                f"Expected to receive only a single projection, but got: {len(projs)}. projections: {projs}")
        log.debug("Found projection: %s", projs[0])
        return projs[0]

    def projections(self, projection_name: str = "") -> Iterable["Projection"]:
        """List all semi-sorted projections of this table if `projection_name` is empty.

        Otherwise, list only the specific projection (if exists).
        """
        self._assert_not_imports_table()

        projections = []
        next_key = 0
        name_prefix = projection_name if projection_name else ""
        exact_match = bool(projection_name)
        while True:
            _bucket_name, _schema_name, _table_name, curr_projections, next_key, is_truncated, _ = \
                self._tx._rpc.api.list_projections(
                    bucket=self.ref.bucket, schema=self.ref.schema, table=self.ref.table, next_key=next_key, txid=self._tx.active_txid,
                    exact_match=exact_match, name_prefix=name_prefix)
            if not curr_projections:
                break
            projections.extend(curr_projections)
            if not is_truncated:
                break
        return [_parse_projection_info(projection, self._metadata, self._tx) for projection in projections]

    def import_files(self,
                     files_to_import: Iterable[str],
                     config: Optional[ImportConfig] = None) -> None:
        """Import a list of Parquet files into this table.

        The files must be on VAST S3 server and be accessible using current credentials.
        """
        self._assert_not_imports_table()

        source_files = {}
        for f in files_to_import:
            bucket_name, object_path = _parse_bucket_and_object_names(f)
            source_files[(bucket_name, object_path)] = b''

        self._execute_import(source_files, config=config)

    def import_partitioned_files(self,
                                 files_and_partitions: dict[str, pa.RecordBatch],
                                 config: Optional[ImportConfig] = None) -> None:
        """Import a list of Parquet files into this table.

        The files must be on VAST S3 server and be accessible using current credentials.
        Each file must have its own partition values defined as an Arrow RecordBatch.
        """
        self._assert_not_imports_table()

        source_files = {}
        for f, record_batch in files_and_partitions.items():
            bucket_name, object_path = _parse_bucket_and_object_names(f)
            serialized_batch = _serialize_record_batch(record_batch)
            source_files[(bucket_name, object_path)] = serialized_batch.to_pybytes()

        self._execute_import(source_files, config=config)

    def _execute_import(self,
                        source_files: dict[tuple[str, str], bytes],
                        config: Optional[ImportConfig]):
        config = config or ImportConfig()
        # TODO: Do we want to validate concurrency isn't too high?
        assert config.import_concurrency > 0
        max_batch_size = 10  # Enforced in server side.
        # TODO: use valid endpoints...
        endpoints = [self._tx._rpc.api.url for _ in range(
            config.import_concurrency)]
        files_queue: Queue = Queue()

        key_names = config.key_names or []
        if key_names:
            self._tx._rpc.features.check_zip_import()

        for source_file in source_files.items():
            files_queue.put(source_file)

        stop_event = Event()
        num_files_in_batch = min(
            ceil(len(source_files) / len(endpoints)), max_batch_size)

        def import_worker(q, endpoint):
            try:
                with self._tx._rpc.api.with_endpoint(endpoint) as session:
                    while not q.empty():
                        if stop_event.is_set():
                            log.debug("stop_event is set, exiting")
                            break
                        files_batch = {}
                        try:
                            for _ in range(num_files_in_batch):
                                files_batch.update({q.get(block=False)})
                        except queue.Empty:
                            pass
                        if files_batch:
                            log.info(
                                "Starting import batch of %s files", len(files_batch))
                            log.debug(f"starting import of {files_batch}")
                            session.import_data(
                                self.ref.bucket, self.ref.schema, self.ref.table, files_batch, txid=self._tx.active_txid,
                                key_names=key_names)
            except (Exception, KeyboardInterrupt) as e:
                stop_event.set()
                log.error("Got exception inside import_worker. exception: %s", e)
                raise

        futures = []
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=config.import_concurrency, thread_name_prefix='import_thread') as pool:
            try:
                for endpoint in endpoints:
                    futures.append(pool.submit(
                        import_worker, files_queue, endpoint))

                log.debug("Waiting for import workers to finish")
                for future in concurrent.futures.as_completed(futures):
                    future.result()
            finally:
                stop_event.set()
                # ThreadPoolExecutor will be joined at the end of the context

    def _get_row_estimate(self,
                          columns: list[str],
                          predicate: ibis.expr.types.BooleanColumn,
                          arrow_schema: pa.Schema):
        query_data_request = _internal.build_query_data_request(
            schema=arrow_schema,
            predicate=predicate,
            field_names=columns)
        response = self._tx._rpc.api.query_data(
            bucket=self.ref.bucket,
            schema=self.ref.schema,
            table=self.ref.table,
            params=query_data_request.serialized,
            split=(0xffffffff - 3, 1, 1),
            txid=self._tx.active_txid)
        batch = _internal.read_first_batch(response.raw)
        return batch.num_rows * 2**16 if batch is not None else 0

    def _select_prepare(self,
                        config: QueryConfig,
                        columns: Optional[list[str]] = None,
                        predicate: Union[ibis.expr.types.BooleanColumn,
                                         ibis.common.deferred.Deferred] = None,
                        *,
                        internal_row_id: bool = False,
                        limit_rows: Optional[int] = None) -> tuple[SplitWorkerConfig, _internal.QueryDataRequest, tuple[str, ...]]:

        if config.data_endpoints is None:
            endpoints = tuple([self._tx._rpc.api.url])
        else:
            endpoints = tuple(config.data_endpoints)
        log.debug("endpoints: %s", endpoints)

        if columns is None:
            columns = [f.name for f in self.arrow_schema]

        query_schema = self.arrow_schema
        if internal_row_id:
            queried_fields = [self._internal_rowid_field]
            queried_fields.extend(column for column in self.arrow_schema)
            query_schema = pa.schema(queried_fields)
            columns.append(INTERNAL_ROW_ID)

        if predicate is True:
            predicate = None
        if predicate is False:
            raise _EmptyResultException(
                response_schema=_internal.get_response_schema(schema=query_schema, field_names=columns))

        if isinstance(predicate, ibis.common.deferred.Deferred):
            # may raise if the predicate is invalid (e.g. wrong types / missing column)
            predicate = predicate.resolve(self._metadata.ibis_table)

        if config.num_splits:
            num_splits = config.num_splits
        else:
            num_rows = 0
            if self._is_sorted_table:
                num_rows = self._get_row_estimate(
                    columns, predicate, query_schema)
                log.debug(f'sorted estimate: {num_rows}')

            if num_rows == 0:
                if self.stats is None:
                    raise AssertionError("Select requires either config.num_splits or loaded stats.")

                num_rows = self.stats.num_rows

            num_splits = max(1, num_rows // config.rows_per_split)

        log.debug("config: %s", config)

        if config.semi_sorted_projection_name:
            self._tx._rpc.features.check_enforce_semisorted_projection()

        query_data_request = _internal.build_query_data_request(
            schema=query_schema,
            predicate=predicate,
            field_names=columns)
        if len(query_data_request.serialized) > util.MAX_QUERY_DATA_REQUEST_SIZE:
            raise errors.TooLargeRequest(
                f"{len(query_data_request.serialized)} bytes")

        split_config = SplitWorkerConfig(
            num_splits=num_splits,
            num_sub_splits=config.num_sub_splits,
            num_row_groups_per_sub_split=config.num_row_groups_per_sub_split,
            limit_rows_per_sub_split=limit_rows or config.limit_rows_per_sub_split,
            use_semi_sorted_projections=config.use_semi_sorted_projections,
            queue_priority=config.queue_priority,
            semi_sorted_projection_name=config.semi_sorted_projection_name)

        return split_config, query_data_request, endpoints

    def select_splits(self, columns: Optional[list[str]] = None,
                      predicate: Union[ibis.expr.types.BooleanColumn,
                                       ibis.common.deferred.Deferred] = None,
                      config: Optional[QueryConfig] = None,
                      *,
                      internal_row_id: bool = False,
                      limit_rows: Optional[int] = None) -> list[pa.RecordBatchReader]:
        """Return pa.RecordBatchReader for each split."""
        config = config or QueryConfig()

        try:
            split_config, query_data_request, endpoints = self._select_prepare(
                config, columns, predicate, internal_row_id=internal_row_id, limit_rows=limit_rows)
        except _EmptyResultException:
            return []

        endpoint_api = itertools.cycle([
            self._tx._rpc.api.with_endpoint(endpoint)
            for endpoint in endpoints])

        return [
            SplitWorker(
                api=next(endpoint_api),
                query_data_request=query_data_request,
                bucket_name=self.ref.bucket,
                schema_name=self.ref.schema,
                table_name=self.ref.table,
                txid=self._tx.active_txid,
                query_imports_table=self._metadata.is_imports_table,
                split_id=split,
                config=split_config
            ).split_record_batch_reader()
            for split in range(split_config.num_splits)
        ]

    def select(self, columns: Optional[list[str]] = None,
               predicate: Union[ibis.expr.types.BooleanColumn,
                                ibis.common.deferred.Deferred] = None,
               config: Optional[QueryConfig] = None,
               *,
               internal_row_id: bool = False,
               limit_rows: Optional[int] = None) -> pa.RecordBatchReader:
        """Execute a query over this table.

        To read a subset of the columns, specify their names via `columns` argument. Otherwise, all columns will be read.

        In order to apply a filter, a predicate can be specified. See https://github.com/vast-data/vastdb_sdk/blob/main/README.md#filters-and-projections for more details.

        Query-execution configuration options can be specified via the optional `config` argument.
        """
        config = config or QueryConfig()

        try:
            split_config, query_data_request, endpoints = self._select_prepare(config,
                                                                               columns,
                                                                               predicate,
                                                                               internal_row_id=internal_row_id,
                                                                               limit_rows=limit_rows)
        except _EmptyResultException as e:
            return pa.RecordBatchReader.from_batches(e.response_schema, [])

        splits_queue: Queue[int] = Queue()

        for split in range(split_config.num_splits):
            splits_queue.put(split)

        # this queue shouldn't be large it is merely a pipe through which the results
        # are sent to the main thread. Most of the pages actually held in the
        # threads that fetch the pages.
        # also, this queue should be at least the amount of workers. otherwise a deadlock may arise.
        # each worker must be able to send the final None message without blocking.
        log.warn("Using the number of endpoints as a heuristic for concurrency.")
        max_workers = len(endpoints)
        record_batches_queue: Queue[pa.RecordBatch] = Queue(
            maxsize=max_workers)

        stop_event = Event()

        class StoppedException(Exception):
            pass

        def check_stop():
            if stop_event.is_set():
                raise StoppedException

        def single_endpoint_worker(endpoint: str):
            try:
                with self._tx._rpc.api.with_endpoint(endpoint) as host_api:
                    backoff_decorator = self._tx._rpc.api._backoff_decorator
                    while True:
                        check_stop()
                        try:
                            split = splits_queue.get_nowait()
                        except queue.Empty:
                            log.debug("splits queue is empty")
                            break

                        split_state = SplitWorker(
                            api=host_api,
                            query_data_request=query_data_request,
                            bucket_name=self.ref.bucket,
                            schema_name=self.ref.schema,
                            table_name=self.ref.table,
                            txid=self._tx.active_txid,
                            query_imports_table=self._metadata.is_imports_table,
                            split_id=split,
                            config=split_config)

                        process_with_retries = backoff_decorator(
                            split_state._process_split)
                        process_with_retries(record_batches_queue, check_stop)

            except StoppedException:
                log.debug("stop signal.", exc_info=True)
                return
            finally:
                # signal that this thread has ended
                log.debug("exiting")
                record_batches_queue.put(None)

        def batches_iterator() -> Iterable[pa.RecordBatch]:
            def propagate_first_exception(futures: set[concurrent.futures.Future], block=False) -> set[concurrent.futures.Future]:
                done, not_done = concurrent.futures.wait(
                    futures, None if block else 0, concurrent.futures.FIRST_EXCEPTION)
                if not self._tx.is_active:
                    raise errors.MissingTransaction()
                for future in done:
                    future.result()
                return not_done

            threads_prefix = "query-data"
            # This is mainly for testing, it helps to identify running threads in runtime.
            if config.query_id:
                threads_prefix = threads_prefix + "-" + config.query_id

            total_num_rows = limit_rows if limit_rows else sys.maxsize
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix=threads_prefix) as tp:
                futures: set[concurrent.futures.Future] = {tp.submit(single_endpoint_worker, endpoint)
                                                           for endpoint in endpoints[:config.num_splits]}
                tasks_running = len(futures)
                try:
                    while tasks_running > 0:
                        futures = propagate_first_exception(
                            futures, block=False)

                        batch = record_batches_queue.get()
                        if batch is not None:
                            if batch.num_rows < total_num_rows:
                                yield batch
                                total_num_rows -= batch.num_rows
                            else:
                                yield batch.slice(length=total_num_rows)
                                log.info(
                                    "reached limit rows per query: %d - stop query", limit_rows)
                                stop_event.set()
                                break
                        else:
                            tasks_running -= 1
                            log.debug(
                                "one worker thread finished, remaining: %d", tasks_running)

                    # all host threads ended - wait for all futures to complete
                    propagate_first_exception(futures, block=True)
                finally:
                    stop_event.set()
                    while tasks_running > 0:
                        if record_batches_queue.get() is None:
                            tasks_running -= 1

        return pa.RecordBatchReader.from_batches(query_data_request.response_schema, batches_iterator())

    def insert_in_column_batches(self, rows: pa.RecordBatch) -> pa.ChunkedArray:
        """Split the RecordBatch into max_columns that can be inserted in single RPC.

        Insert first MAX_COLUMN_IN_BATCH columns and get the row_ids. Then loop on the rest of the columns and
        update in groups of MAX_COLUMN_IN_BATCH.
        """
        column_record_batch = pa.RecordBatch.from_arrays([_combine_chunks(rows.column(i)) for i in range(0, MAX_COLUMN_IN_BATCH)],
                                                         schema=pa.schema([rows.schema.field(i) for i in range(0, MAX_COLUMN_IN_BATCH)]))
        row_ids = self.insert(rows=column_record_batch)  # type: ignore

        columns_names = [field.name for field in rows.schema]
        columns = list(rows.schema)
        arrays = [_combine_chunks(rows.column(i))
                  for i in range(len(rows.schema))]
        for start in range(MAX_COLUMN_IN_BATCH, len(rows.schema), MAX_COLUMN_IN_BATCH):
            end = start + MAX_COLUMN_IN_BATCH if start + \
                MAX_COLUMN_IN_BATCH < len(rows.schema) else len(rows.schema)
            columns_name_chunk = columns_names[start:end]
            columns_chunks = columns[start:end]
            arrays_chunks = arrays[start:end]
            columns_chunks.append(self._internal_rowid_field)
            arrays_chunks.append(row_ids.to_pylist())
            column_record_batch = pa.RecordBatch.from_arrays(
                arrays_chunks, schema=pa.schema(columns_chunks))
            self.update(rows=column_record_batch, columns=columns_name_chunk)
        return row_ids

    def insert(self,
               rows: Union[pa.RecordBatch, pa.Table],
               by_columns: bool = False) -> pa.ChunkedArray:
        """Insert a RecordBatch into this table."""
        self._assert_not_imports_table()

        if 0 == rows.num_rows:
            log.debug("Ignoring empty insert into %s", self.ref)
            return pa.chunked_array([], type=self._internal_rowid_field.type)

        if by_columns:
            self._tx._rpc.features.check_return_row_ids()
            return self.insert_in_column_batches(rows)

        try:
            row_ids = []
            serialized_slices = util.iter_serialized_slices(
                rows, MAX_INSERT_ROWS_PER_PATCH)
            for slice in serialized_slices:
                res = self._tx._rpc.api.insert_rows(self.ref.bucket,
                                                    self.ref.schema,
                                                    self.ref.table,
                                                    record_batch=slice,
                                                    txid=self._tx.active_txid)
                (batch,) = pa.RecordBatchStreamReader(res.content)
                row_ids.append(batch[INTERNAL_ROW_ID])
            try:
                self._tx._rpc.features.check_return_row_ids()
            except errors.NotSupportedVersion:
                return  # type: ignore
            return pa.chunked_array(row_ids, type=self._internal_rowid_field.type)
        except errors.TooWideRow:
            self._tx._rpc.features.check_return_row_ids()
            return self.insert_in_column_batches(rows)

    def update(self,
               rows: Union[pa.RecordBatch, pa.Table],
               columns: Optional[list[str]] = None) -> None:
        """Update a subset of cells in this table.

        Row IDs are specified using a special field (named "$row_id" of uint64 type) - this function assume that this
        special field is part of arguments.

        A subset of columns to be updated can be specified via the `columns` argument.
        """
        self._assert_not_imports_table()

        try:
            rows_chunk = rows[INTERNAL_ROW_ID]
        except KeyError:
            raise errors.MissingRowIdColumn

        if columns is None:
            columns = [
                name for name in rows.schema.names if name != INTERNAL_ROW_ID]

        update_fields = [self._internal_rowid_field]
        update_values = [_combine_chunks(rows_chunk)]
        for col in columns:
            update_fields.append(rows.field(col))
            update_values.append(_combine_chunks(rows[col]))

        update_rows_rb = pa.record_batch(
            schema=pa.schema(update_fields), data=update_values)

        update_rows_rb = util.sort_record_batch_if_needed(
            update_rows_rb, INTERNAL_ROW_ID)

        serialized_slices = util.iter_serialized_slices(
            update_rows_rb, MAX_ROWS_PER_BATCH)
        for slice in serialized_slices:
            self._tx._rpc.api.update_rows(self.ref.bucket, self.ref.schema, self.ref.table, record_batch=slice,
                                          txid=self._tx.active_txid)

    def delete(self, rows: Union[pa.RecordBatch, pa.Table]) -> None:
        """Delete a subset of rows in this table.

        Row IDs are specified using a special field (named "$row_id" of uint64 type).
        """
        self._assert_not_imports_table()

        try:
            rows_chunk = rows[INTERNAL_ROW_ID]
        except KeyError:
            raise errors.MissingRowIdColumn
        delete_rows_rb = pa.record_batch(schema=pa.schema([self._internal_rowid_field]),
                                         data=[_combine_chunks(rows_chunk)])

        delete_rows_rb = util.sort_record_batch_if_needed(
            delete_rows_rb, INTERNAL_ROW_ID)

        serialized_slices = util.iter_serialized_slices(
            delete_rows_rb, MAX_ROWS_PER_BATCH)
        for slice in serialized_slices:
            self._tx._rpc.api.delete_rows(self.ref.bucket,
                                          self.ref.schema,
                                          self.ref.table,
                                          record_batch=slice,
                                          txid=self._tx.active_txid,
                                          delete_from_imports_table=self._metadata.is_imports_table)

    def imports_table(self) -> Optional[ITable]:
        """Get the imports table of this table."""
        imports_table_metadata = self.imports_table_metadata()
        return TableInTransaction(metadata=imports_table_metadata,
                                  tx=self._tx)

    def imports_table_metadata(self) -> TableMetadata:
        """Get TableMetadata for import table."""
        self._tx._rpc.features.check_imports_table()

        return TableMetadata(ref=self.ref,
                             table_type=TableType.TableImports)

    def __getitem__(self, col_name: str) -> ibis.Column:
        """Allow constructing ibis-like column expressions from this table.

        It is useful for constructing expressions for predicate pushdown in `Table.select()` method.
        """
        return self._metadata.ibis_table[col_name]

    def sorting_done(self) -> bool:
        """Sorting done indicator for the table.  Always False for unsorted tables."""
        if not self._is_sorted_table:
            return False
        raw_sorting_score = self._tx._rpc.api.raw_sorting_score(self.ref.bucket,
                                                                self.ref.schema,
                                                                self._tx.active_txid,
                                                                self.ref.table)
        return bool(raw_sorting_score >> SORTING_SCORE_BITS)

    def sorting_score(self) -> int:
        """Sorting score for the table.  Always 0 for unsorted tables."""
        if not self._is_sorted_table:
            return 0
        raw_sorting_score = self._tx._rpc.api.raw_sorting_score(self.ref.bucket,
                                                                self.ref.schema,
                                                                self._tx.active_txid,
                                                                self.ref.table)
        return raw_sorting_score & ((1 << SORTING_SCORE_BITS) - 1)

    @property
    def _is_sorted_table(self) -> bool:
        return self._metadata.table_type is TableType.Elysium


class Table(TableInTransaction):
    """Vast Interactive Table."""

    _handle: int

    def __init__(self,
                 metadata: TableMetadata,
                 handle: int,
                 tx: "Transaction"):
        """Vast Interactive Table."""
        super().__init__(metadata, tx)
        self._metadata.load_schema(tx)

        self._handle = handle

    @property
    def schema(self):
        """Deprecated property."""
        return DeprecationWarning("`schema` property is deprecated, to get the schema name use `self.ref.schema`")

    @property
    def bucket(self):
        """Deprecated property."""
        return DeprecationWarning("`bucket` property is deprecated, to get the bucket name use `self.ref.bucket`")

    @property
    def handle(self) -> int:
        """Table Handle."""
        return self._handle

    @property
    def tx(self):
        """Return transaction."""
        return self._tx

    @property
    def stats(self) -> TableStats:
        """Fetch table's statistics from server."""
        self.reload_stats()
        assert self._metadata.stats is not None
        return self._metadata.stats

    def columns(self) -> pa.Schema:
        """Return columns' metadata."""
        self.reload_schema()
        return self._metadata.arrow_schema

    def sorted_columns(self) -> list:
        """Return sorted columns' metadata."""
        try:
            self.reload_sorted_columns()
        except Exception:
            pass

        return self._metadata.sorted_columns

    def get_stats(self) -> TableStats:
        """Get the statistics of this table."""
        return self.stats

    def imports_table(self) -> Optional["Table"]:
        """Get the imports table of this table."""
        imports_table_metadata = self.imports_table_metadata()
        imports_table_metadata.load(self.tx)
        return Table(handle=self.handle,
                     metadata=imports_table_metadata,
                     tx=self.tx)

    @property
    def sorted_table(self) -> bool:
        """Is table a sorted table."""
        return self._is_sorted_table

    def __getitem__(self, col_name: str):
        """Allow constructing ibis-like column expressions from this table.

        It is useful for constructing expressions for predicate pushdown in `Table.select()` method.
        """
        return self._metadata.ibis_table[col_name]

    def drop(self) -> None:
        """Drop this table."""
        self._tx._rpc.api.drop_table(self.ref.bucket,
                                     self.ref.schema,
                                     self.ref.table,
                                     txid=self._tx.active_txid,
                                     remove_imports_table=self._metadata.is_imports_table)
        log.info("Dropped table: %s", self.ref.table)

    def rename(self, new_name: str) -> None:
        """Rename this table."""
        self._assert_not_imports_table()

        self._tx._rpc.api.alter_table(self.ref.bucket,
                                      self.ref.schema,
                                      self.ref.table,
                                      txid=self._tx.active_txid,
                                      new_name=new_name)
        log.info("Renamed table from %s to %s ", self.ref.table, new_name)
        self._metadata.rename_table(new_name)

    def add_sorting_key(self, sorting_key: list[int]) -> None:
        """Add a sorting key to a table that doesn't have any."""
        self._tx._rpc.features.check_elysium()
        self._tx._rpc.api.alter_table(self.ref.bucket,
                                      self.ref.schema,
                                      self.ref.table,
                                      txid=self._tx.active_txid,
                                      sorting_key=sorting_key)
        log.info("Enabled Elysium for table %s with sorting key %s ",
                 self.ref.table, str(sorting_key))

    def add_column(self, new_column: pa.Schema) -> None:
        """Add a new column."""
        self._assert_not_imports_table()

        validate_ibis_support_schema(new_column)
        self._tx._rpc.api.add_columns(
            self.ref.bucket, self.ref.schema, self.ref.table, new_column, txid=self._tx.active_txid)
        log.info("Added column(s): %s", new_column)
        self._metadata.load_schema(self._tx)

    def drop_column(self, column_to_drop: pa.Schema) -> None:
        """Drop an existing column."""
        self._tx._rpc.api.drop_columns(self.ref.bucket,
                                       self.ref.schema,
                                       self.ref.table, column_to_drop, txid=self._tx.active_txid)
        log.info("Dropped column(s): %s", column_to_drop)
        self._metadata.load_schema(self._tx)

    def rename_column(self, current_column_name: str, new_column_name: str) -> None:
        """Rename an existing column."""
        self._assert_not_imports_table()

        self._tx._rpc.api.alter_column(self.ref.bucket,
                                       self.ref.schema,
                                       self.ref.table, name=current_column_name,
                                       new_name=new_column_name, txid=self._tx.active_txid)
        log.info("Renamed column: %s to %s",
                 current_column_name, new_column_name)
        self._metadata.load_schema(self._tx)

    def create_projection(self, projection_name: str, sorted_columns: list[str], unsorted_columns: list[str]) -> "Projection":
        """Create a new semi-sorted projection."""
        self._assert_not_imports_table()

        columns = [(sorted_column, "Sorted") for sorted_column in sorted_columns] + \
            [(unsorted_column, "Unorted")
             for unsorted_column in unsorted_columns]
        self._tx._rpc.api.create_projection(self.ref.bucket,
                                            self.ref.schema,
                                            self.ref.table, projection_name, columns=columns, txid=self._tx.active_txid)
        log.info("Created projection: %s", projection_name)
        return self.projection(projection_name)

    def create_imports_table(self, fail_if_exists=True) -> ITable:
        """Create imports table."""
        self._tx._rpc.features.check_imports_table()
        empty_schema = pa.schema([])
        self._tx._rpc.api.create_table(self.ref.bucket,
                                       self.ref.schema,
                                       self.ref.table,
                                       empty_schema,
                                       txid=self._tx.active_txid,
                                       create_imports_table=True)
        log.info("Created imports table for table: %s", self.name)
        return self.imports_table()  # type: ignore[return-value]


@dataclass
class Projection:
    """VAST semi-sorted projection."""

    name: str
    table_metadata: TableMetadata
    stats: TableStats
    handle: int
    tx: "Transaction"

    def columns(self) -> pa.Schema:
        """Return this projections' columns as an Arrow schema."""
        columns = []
        next_key = 0
        while True:
            curr_columns, next_key, is_truncated, _count, _ = \
                self.tx._rpc.api.list_projection_columns(
                    self.table_metadata.ref.bucket,
                    self.table_metadata.ref.schema,
                    self.table_metadata.ref.table,
                    self.name,
                    txid=self.tx.active_txid,
                    next_key=next_key)
            if not curr_columns:
                break
            columns.extend(curr_columns)
            if not is_truncated:
                break
        self.arrow_schema = pa.schema([(col[0], col[1]) for col in columns])
        return self.arrow_schema

    def rename(self, new_name: str) -> None:
        """Rename this projection."""
        self.tx._rpc.api.alter_projection(self.table_metadata.ref.bucket,
                                          self.table_metadata.ref.schema,
                                          self.table_metadata.ref.table,
                                          self.name,
                                          txid=self.tx.active_txid,
                                          new_name=new_name)
        log.info("Renamed projection from %s to %s ", self.name, new_name)
        self.name = new_name

    def drop(self) -> None:
        """Drop this projection."""
        self.tx._rpc.api.drop_projection(self.table_metadata.ref.bucket,
                                         self.table_metadata.ref.schema,
                                         self.table_metadata.ref.table,
                                         self.name,
                                         txid=self.tx.active_txid)
        log.info("Dropped projection: %s", self.name)


def _parse_projection_info(projection_info, table_metadata: "TableMetadata", tx: "Transaction"):
    log.info("Projection info %s", str(projection_info))
    stats = TableStats(num_rows=projection_info.num_rows, size_in_bytes=projection_info.size_in_bytes,
                       sorting_score=0, write_amplification=0, acummulative_row_inserition_count=0)
    return Projection(name=projection_info.name,
                      table_metadata=table_metadata,
                      stats=stats,
                      handle=int(projection_info.handle),
                      tx=tx)


def _parse_bucket_and_object_names(path: str) -> tuple[str, str]:
    if not path.startswith('/'):
        raise errors.InvalidArgument(f"Path {path} must start with a '/'")
    components = path.split(os.path.sep)
    bucket_name = components[1]
    object_path = os.path.sep.join(components[2:])
    return bucket_name, object_path


def _serialize_record_batch(record_batch: pa.RecordBatch) -> pa.lib.Buffer:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, record_batch.schema) as writer:
        writer.write(record_batch)
    return sink.getvalue()


def _combine_chunks(col):
    if hasattr(col, "combine_chunks"):
        return col.combine_chunks()
    else:
        return col


__all__ = ["ITable",
           "Table",
           "TableInTransaction",
           "Projection"]
