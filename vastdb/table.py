"""VAST Database table."""

import concurrent.futures
import logging
import os
import queue
from dataclasses import dataclass, field
from math import ceil
from threading import Event
from typing import Callable, Dict, Iterable, List, Optional, Tuple, Union

import ibis
import pyarrow as pa
import urllib3

from . import _internal, errors, schema, util
from .config import ImportConfig, QueryConfig

log = logging.getLogger(__name__)


INTERNAL_ROW_ID = "$row_id"
INTERNAL_ROW_ID_FIELD = pa.field(INTERNAL_ROW_ID, pa.uint64())

MAX_ROWS_PER_BATCH = 512 * 1024
# for insert we need a smaller limit due to response amplification
# for example insert of 512k uint8 result in 512k*8bytes response since row_ids are uint64
MAX_INSERT_ROWS_PER_PATCH = 512 * 1024
# in case insert has TooWideRow - need to insert in smaller batches - each cell could contain up to 128K, and our wire is limited to 5MB
MAX_COLUMN_IN_BATCH = int(5 * 1024 / 128)


@dataclass
class TableStats:
    """Table-related information."""

    num_rows: int
    size_in_bytes: int
    is_external_rowid_alloc: bool = False
    endpoints: Tuple[str, ...] = ()


class SelectSplitState:
    """State of a specific query split execution."""

    def __init__(self, query_data_request, table: "Table", split_id: int, config: QueryConfig) -> None:
        """Initialize query split state."""
        self.split_id = split_id
        self.subsplits_state = {i: 0 for i in range(config.num_sub_splits)}
        self.config = config
        self.query_data_request = query_data_request
        self.table = table

    def process_split(self, api: _internal.VastdbApi, record_batches_queue: queue.Queue[pa.RecordBatch], check_stop: Callable):
        """Execute a sequence of QueryData requests, and queue the parsed RecordBatch objects.

        Can be called repeatedly, to support resuming the query after a disconnection / retriable error.
        """
        try:
            # contains RecordBatch parts received from the server, must be re-created in case of a retry
            while not self.done:
                # raises if request parsing fails or throttled due to server load, and will be externally retried
                response = api.query_data(
                                bucket=self.table.bucket.name,
                                schema=self.table.schema.name,
                                table=self.table.name,
                                params=self.query_data_request.serialized,
                                split=(self.split_id, self.config.num_splits, self.config.num_row_groups_per_sub_split),
                                num_sub_splits=self.config.num_sub_splits,
                                response_row_id=False,
                                txid=self.table.tx.txid,
                                limit_rows=self.config.limit_rows_per_sub_split,
                                sub_split_start_row_ids=self.subsplits_state.items(),
                                schedule_id=self.config.queue_priority,
                                enable_sorted_projections=self.config.use_semi_sorted_projections,
                                query_imports_table=self.table._imports_table,
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
                        check_stop()  # may raise StoppedException to early-exit the query (without retries)
                        if batch:
                            record_batches_queue.put(batch)
        except urllib3.exceptions.ProtocolError as err:
            log.warning("Failed parsing QueryData response table=%r split=%s/%s offsets=%s cause=%s",
                        self.table, self.split_id, self.config.num_splits, self.subsplits_state, err)
            # since this is a read-only idempotent operation, it is safe to retry
            raise errors.ConnectionError(cause=err, may_retry=True)

    @property
    def done(self):
        """Returns true iff the pagination over."""
        return all(row_id == _internal.TABULAR_INVALID_ROW_ID for row_id in self.subsplits_state.values())


@dataclass
class Table:
    """VAST Table."""

    name: str
    schema: "schema.Schema"
    handle: int
    arrow_schema: pa.Schema = field(init=False, compare=False, repr=False)
    _ibis_table: ibis.Schema = field(init=False, compare=False, repr=False)
    _imports_table: bool

    def __post_init__(self):
        """Also, load columns' metadata."""
        self.arrow_schema = self.columns()

        self._table_path = f'{self.schema.bucket.name}/{self.schema.name}/{self.name}'
        self._ibis_table = ibis.table(ibis.Schema.from_pyarrow(self.arrow_schema), self._table_path)

    @property
    def path(self):
        """Return table's path."""
        return self._table_path

    @property
    def tx(self):
        """Return transaction."""
        return self.schema.tx

    @property
    def bucket(self):
        """Return bucket."""
        return self.schema.bucket

    @property
    def stats(self):
        """Fetch table's statistics from server."""
        return self.get_stats()

    def columns(self) -> pa.Schema:
        """Return columns' metadata."""
        fields = []
        next_key = 0
        while True:
            cur_columns, next_key, is_truncated, _count = self.tx._rpc.api.list_columns(
                bucket=self.bucket.name, schema=self.schema.name, table=self.name, next_key=next_key, txid=self.tx.txid, list_imports_table=self._imports_table)
            fields.extend(cur_columns)
            if not is_truncated:
                break

        self.arrow_schema = pa.schema(fields)
        return self.arrow_schema

    def projection(self, name: str) -> "Projection":
        """Get a specific semi-sorted projection of this table."""
        if self._imports_table:
            raise errors.NotSupportedCommand(self.bucket.name, self.schema.name, self.name)
        projs = tuple(self.projections(projection_name=name))
        if not projs:
            raise errors.MissingProjection(self.bucket.name, self.schema.name, self.name, name)
        assert len(projs) == 1, f"Expected to receive only a single projection, but got: {len(projs)}. projections: {projs}"
        log.debug("Found projection: %s", projs[0])
        return projs[0]

    def projections(self, projection_name: str = "") -> Iterable["Projection"]:
        """List all semi-sorted projections of this table if `projection_name` is empty.

        Otherwise, list only the specific projection (if exists).
        """
        if self._imports_table:
            raise errors.NotSupportedCommand(self.bucket.name, self.schema.name, self.name)
        projections = []
        next_key = 0
        name_prefix = projection_name if projection_name else ""
        exact_match = bool(projection_name)
        while True:
            _bucket_name, _schema_name, _table_name, curr_projections, next_key, is_truncated, _ = \
                self.tx._rpc.api.list_projections(
                    bucket=self.bucket.name, schema=self.schema.name, table=self.name, next_key=next_key, txid=self.tx.txid,
                    exact_match=exact_match, name_prefix=name_prefix)
            if not curr_projections:
                break
            projections.extend(curr_projections)
            if not is_truncated:
                break
        return [_parse_projection_info(projection, self) for projection in projections]

    def import_files(self, files_to_import: Iterable[str], config: Optional[ImportConfig] = None) -> None:
        """Import a list of Parquet files into this table.

        The files must be on VAST S3 server and be accessible using current credentials.
        """
        if self._imports_table:
            raise errors.NotSupportedCommand(self.bucket.name, self.schema.name, self.name)
        source_files = {}
        for f in files_to_import:
            bucket_name, object_path = _parse_bucket_and_object_names(f)
            source_files[(bucket_name, object_path)] = b''

        self._execute_import(source_files, config=config)

    def import_partitioned_files(self, files_and_partitions: Dict[str, pa.RecordBatch], config: Optional[ImportConfig] = None) -> None:
        """Import a list of Parquet files into this table.

        The files must be on VAST S3 server and be accessible using current credentials.
        Each file must have its own partition values defined as an Arrow RecordBatch.
        """
        if self._imports_table:
            raise errors.NotSupportedCommand(self.bucket.name, self.schema.name, self.name)
        source_files = {}
        for f, record_batch in files_and_partitions.items():
            bucket_name, object_path = _parse_bucket_and_object_names(f)
            serialized_batch = _serialize_record_batch(record_batch)
            source_files = {(bucket_name, object_path): serialized_batch.to_pybytes()}

        self._execute_import(source_files, config=config)

    def _execute_import(self, source_files, config):
        config = config or ImportConfig()
        assert config.import_concurrency > 0  # TODO: Do we want to validate concurrency isn't too high?
        max_batch_size = 10  # Enforced in server side.
        endpoints = [self.tx._rpc.api.url for _ in range(config.import_concurrency)]  # TODO: use valid endpoints...
        files_queue = queue.Queue()

        for source_file in source_files.items():
            files_queue.put(source_file)

        stop_event = Event()
        num_files_in_batch = min(ceil(len(source_files) / len(endpoints)), max_batch_size)

        def import_worker(q, endpoint):
            try:
                with self.tx._rpc.api.with_endpoint(endpoint) as session:
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
                            log.debug("Starting import batch of %s files", len(files_batch))
                            session.import_data(
                                self.bucket.name, self.schema.name, self.name, files_batch, txid=self.tx.txid)
            except (Exception, KeyboardInterrupt) as e:
                stop_event.set()
                log.error("Got exception inside import_worker. exception: %s", e)
                raise

        futures = []
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=config.import_concurrency, thread_name_prefix='import_thread') as pool:
            try:
                for endpoint in endpoints:
                    futures.append(pool.submit(import_worker, files_queue, endpoint))

                log.debug("Waiting for import workers to finish")
                for future in concurrent.futures.as_completed(futures):
                    future.result()
            finally:
                stop_event.set()
                # ThreadPoolExecutor will be joined at the end of the context

    def get_stats(self) -> TableStats:
        """Get the statistics of this table."""
        stats_tuple = self.tx._rpc.api.get_table_stats(
            bucket=self.bucket.name, schema=self.schema.name, name=self.name, txid=self.tx.txid,
            imports_table_stats=self._imports_table)
        return TableStats(**stats_tuple._asdict())

    def select(self, columns: Optional[List[str]] = None,
               predicate: Union[ibis.expr.types.BooleanColumn, ibis.common.deferred.Deferred] = None,
               config: Optional[QueryConfig] = None,
               *,
               internal_row_id: bool = False) -> pa.RecordBatchReader:
        """Execute a query over this table.

        To read a subset of the columns, specify their names via `columns` argument. Otherwise, all columns will be read.

        In order to apply a filter, a predicate can be specified. See https://github.com/vast-data/vastdb_sdk/blob/main/README.md#filters-and-projections for more details.

        Query-execution configuration options can be specified via the optional `config` argument.
        """
        if config is None:
            config = QueryConfig()

        # Retrieve snapshots only if needed
        if config.data_endpoints is None or config.num_splits is None:
            stats = self.get_stats()
            log.debug("stats: %s", stats)

        if config.data_endpoints is None:
            endpoints = stats.endpoints
        else:
            endpoints = tuple(config.data_endpoints)
        log.debug("endpoints: %s", endpoints)

        if config.num_splits is None:
            config.num_splits = max(1, stats.num_rows // config.rows_per_split)
        log.debug("config: %s", config)

        if config.semi_sorted_projection_name:
            self.tx._rpc.features.check_enforce_semisorted_projection()

        if columns is None:
            columns = [f.name for f in self.arrow_schema]

        query_schema = self.arrow_schema
        if internal_row_id:
            queried_fields = [INTERNAL_ROW_ID_FIELD]
            queried_fields.extend(column for column in self.arrow_schema)
            query_schema = pa.schema(queried_fields)
            columns.append(INTERNAL_ROW_ID)

        if predicate is True:
            predicate = None
        if predicate is False:
            response_schema = _internal.get_response_schema(schema=query_schema, field_names=columns)
            return pa.RecordBatchReader.from_batches(response_schema, [])

        if isinstance(predicate, ibis.common.deferred.Deferred):
            predicate = predicate.resolve(self._ibis_table)  # may raise if the predicate is invalid (e.g. wrong types / missing column)

        query_data_request = _internal.build_query_data_request(
            schema=query_schema,
            predicate=predicate,
            field_names=columns)
        if len(query_data_request.serialized) > util.MAX_QUERY_DATA_REQUEST_SIZE:
            raise errors.TooLargeRequest(f"{len(query_data_request.serialized)} bytes")

        splits_queue: queue.Queue[int] = queue.Queue()

        for split in range(config.num_splits):
            splits_queue.put(split)

        # this queue shouldn't be large it is marely a pipe through which the results
        # are sent to the main thread. Most of the pages actually held in the
        # threads that fetch the pages.
        record_batches_queue: queue.Queue[pa.RecordBatch] = queue.Queue(maxsize=2)

        stop_event = Event()

        class StoppedException(Exception):
            pass

        def check_stop():
            if stop_event.is_set():
                raise StoppedException

        def single_endpoint_worker(endpoint: str):
            try:
                with self.tx._rpc.api.with_endpoint(endpoint) as host_api:
                    backoff_decorator = self.tx._rpc.api._backoff_decorator
                    while True:
                        check_stop()
                        try:
                            split = splits_queue.get_nowait()
                        except queue.Empty:
                            log.debug("splits queue is empty")
                            break

                        split_state = SelectSplitState(query_data_request=query_data_request,
                                                       table=self,
                                                       split_id=split,
                                                       config=config)

                        process_with_retries = backoff_decorator(split_state.process_split)
                        process_with_retries(host_api, record_batches_queue, check_stop)

            except StoppedException:
                log.debug("stop signal.", exc_info=True)
                return
            finally:
                # signal that this thread has ended
                log.debug("exiting")
                record_batches_queue.put(None)

        def batches_iterator():
            def propagate_first_exception(futures: List[concurrent.futures.Future], block=False):
                done, not_done = concurrent.futures.wait(futures, None if block else 0, concurrent.futures.FIRST_EXCEPTION)
                if self.tx.txid is None:
                    raise errors.MissingTransaction()
                for future in done:
                    future.result()
                return not_done

            threads_prefix = "query-data"
            # This is mainly for testing, it helps to identify running threads in runtime.
            if config.query_id:
                threads_prefix = threads_prefix + "-" + config.query_id

            with concurrent.futures.ThreadPoolExecutor(max_workers=len(endpoints), thread_name_prefix=threads_prefix) as tp:  # TODO: concurrency == enpoints is just a heuristic
                futures = [tp.submit(single_endpoint_worker, endpoint) for endpoint in endpoints]
                tasks_running = len(futures)
                try:
                    while tasks_running > 0:
                        futures = propagate_first_exception(futures, block=False)

                        batch = record_batches_queue.get()
                        if batch is not None:
                            yield batch
                        else:
                            tasks_running -= 1
                            log.debug("one worker thread finished, remaining: %d", tasks_running)

                    # all host threads ended - wait for all futures to complete
                    propagate_first_exception(futures, block=True)
                finally:
                    stop_event.set()
                    while tasks_running > 0:
                        if record_batches_queue.get() is None:
                            tasks_running -= 1

        return pa.RecordBatchReader.from_batches(query_data_request.response_schema, batches_iterator())

    def insert_in_column_batches(self, rows: pa.RecordBatch):
        """Split the RecordBatch into max_columns that can be inserted in single RPC.

        Insert first MAX_COLUMN_IN_BATCH columns and get the row_ids. Then loop on the rest of the columns and
        update in groups of MAX_COLUMN_IN_BATCH.
        """
        column_record_batch = pa.RecordBatch.from_arrays([_combine_chunks(rows.column(i)) for i in range(0, MAX_COLUMN_IN_BATCH)],
                                                         schema=pa.schema([rows.schema.field(i) for i in range(0, MAX_COLUMN_IN_BATCH)]))
        row_ids = self.insert(rows=column_record_batch)  # type: ignore

        columns_names = [field.name for field in rows.schema]
        columns = list(rows.schema)
        arrays = [_combine_chunks(rows.column(i)) for i in range(len(rows.schema))]
        for start in range(MAX_COLUMN_IN_BATCH, len(rows.schema), MAX_COLUMN_IN_BATCH):
            end = start + MAX_COLUMN_IN_BATCH if start + MAX_COLUMN_IN_BATCH < len(rows.schema) else len(rows.schema)
            columns_name_chunk = columns_names[start:end]
            columns_chunks = columns[start:end]
            arrays_chunks = arrays[start:end]
            columns_chunks.append(INTERNAL_ROW_ID_FIELD)
            arrays_chunks.append(row_ids.to_pylist())
            column_record_batch = pa.RecordBatch.from_arrays(arrays_chunks, schema=pa.schema(columns_chunks))
            self.update(rows=column_record_batch, columns=columns_name_chunk)
        return row_ids

    def insert(self, rows: Union[pa.RecordBatch, pa.Table]):
        """Insert a RecordBatch into this table."""
        if self._imports_table:
            raise errors.NotSupportedCommand(self.bucket.name, self.schema.name, self.name)
        try:
            row_ids = []
            serialized_slices = util.iter_serialized_slices(rows, MAX_INSERT_ROWS_PER_PATCH)
            for slice in serialized_slices:
                res = self.tx._rpc.api.insert_rows(self.bucket.name, self.schema.name, self.name, record_batch=slice,
                                                   txid=self.tx.txid)
                (batch,) = pa.RecordBatchStreamReader(res.content)
                row_ids.append(batch[INTERNAL_ROW_ID])
            try:
                self.tx._rpc.features.check_return_row_ids()
            except errors.NotSupportedVersion:
                return  # type: ignore
            return pa.chunked_array(row_ids)
        except errors.TooWideRow:
            self.tx._rpc.features.check_return_row_ids()
            return self.insert_in_column_batches(rows)

    def update(self, rows: Union[pa.RecordBatch, pa.Table], columns: Optional[List[str]] = None) -> None:
        """Update a subset of cells in this table.

        Row IDs are specified using a special field (named "$row_id" of uint64 type) - this function assume that this
        special field is part of arguments.

        A subset of columns to be updated can be specified via the `columns` argument.
        """
        if self._imports_table:
            raise errors.NotSupportedCommand(self.bucket.name, self.schema.name, self.name)
        try:
            rows_chunk = rows[INTERNAL_ROW_ID]
        except KeyError:
            raise errors.MissingRowIdColumn

        if columns is None:
            columns = [name for name in rows.schema.names if name != INTERNAL_ROW_ID]

        update_fields = [(INTERNAL_ROW_ID, pa.uint64())]
        update_values = [_combine_chunks(rows_chunk)]
        for col in columns:
            update_fields.append(rows.field(col))
            update_values.append(_combine_chunks(rows[col]))

        update_rows_rb = pa.record_batch(schema=pa.schema(update_fields), data=update_values)

        update_rows_rb = util.sort_record_batch_if_needed(update_rows_rb, INTERNAL_ROW_ID)

        serialized_slices = util.iter_serialized_slices(update_rows_rb, MAX_ROWS_PER_BATCH)
        for slice in serialized_slices:
            self.tx._rpc.api.update_rows(self.bucket.name, self.schema.name, self.name, record_batch=slice,
                                         txid=self.tx.txid)

    def delete(self, rows: Union[pa.RecordBatch, pa.Table]) -> None:
        """Delete a subset of rows in this table.

        Row IDs are specified using a special field (named "$row_id" of uint64 type).
        """
        if self._imports_table:
            raise errors.NotSupportedCommand(self.bucket.name, self.schema.name, self.name)
        try:
            rows_chunk = rows[INTERNAL_ROW_ID]
        except KeyError:
            raise errors.MissingRowIdColumn
        delete_rows_rb = pa.record_batch(schema=pa.schema([(INTERNAL_ROW_ID, pa.uint64())]),
                                         data=[_combine_chunks(rows_chunk)])

        delete_rows_rb = util.sort_record_batch_if_needed(delete_rows_rb, INTERNAL_ROW_ID)

        serialized_slices = util.iter_serialized_slices(delete_rows_rb, MAX_ROWS_PER_BATCH)
        for slice in serialized_slices:
            self.tx._rpc.api.delete_rows(self.bucket.name, self.schema.name, self.name, record_batch=slice,
                                         txid=self.tx.txid, delete_from_imports_table=self._imports_table)

    def drop(self) -> None:
        """Drop this table."""
        self.tx._rpc.api.drop_table(self.bucket.name, self.schema.name, self.name, txid=self.tx.txid, remove_imports_table=self._imports_table)
        log.info("Dropped table: %s", self.name)

    def rename(self, new_name: str) -> None:
        """Rename this table."""
        if self._imports_table:
            raise errors.NotSupportedCommand(self.bucket.name, self.schema.name, self.name)
        self.tx._rpc.api.alter_table(
            self.bucket.name, self.schema.name, self.name, txid=self.tx.txid, new_name=new_name)
        log.info("Renamed table from %s to %s ", self.name, new_name)
        self.name = new_name

    def add_column(self, new_column: pa.Schema) -> None:
        """Add a new column."""
        if self._imports_table:
            raise errors.NotSupportedCommand(self.bucket.name, self.schema.name, self.name)
        self.tx._rpc.api.add_columns(self.bucket.name, self.schema.name, self.name, new_column, txid=self.tx.txid)
        log.info("Added column(s): %s", new_column)
        self.arrow_schema = self.columns()

    def drop_column(self, column_to_drop: pa.Schema) -> None:
        """Drop an existing column."""
        if self._imports_table:
            raise errors.NotSupported(self.bucket.name, self.schema.name, self.name)
        if self._imports_table:
            raise errors.NotSupportedCommand(self.bucket.name, self.schema.name, self.name)
        self.tx._rpc.api.drop_columns(self.bucket.name, self.schema.name, self.name, column_to_drop, txid=self.tx.txid)
        log.info("Dropped column(s): %s", column_to_drop)
        self.arrow_schema = self.columns()

    def rename_column(self, current_column_name: str, new_column_name: str) -> None:
        """Rename an existing column."""
        if self._imports_table:
            raise errors.NotSupportedCommand(self.bucket.name, self.schema.name, self.name)
        self.tx._rpc.api.alter_column(self.bucket.name, self.schema.name, self.name, name=current_column_name,
                                       new_name=new_column_name, txid=self.tx.txid)
        log.info("Renamed column: %s to %s", current_column_name, new_column_name)
        self.arrow_schema = self.columns()

    def create_projection(self, projection_name: str, sorted_columns: List[str], unsorted_columns: List[str]) -> "Projection":
        """Create a new semi-sorted projection."""
        if self._imports_table:
            raise errors.NotSupportedCommand(self.bucket.name, self.schema.name, self.name)
        columns = [(sorted_column, "Sorted") for sorted_column in sorted_columns] + [(unsorted_column, "Unorted") for unsorted_column in unsorted_columns]
        self.tx._rpc.api.create_projection(self.bucket.name, self.schema.name, self.name, projection_name, columns=columns, txid=self.tx.txid)
        log.info("Created projection: %s", projection_name)
        return self.projection(projection_name)

    def create_imports_table(self, fail_if_exists=True) -> "Table":
        """Create imports table."""
        self.tx._rpc.features.check_imports_table()
        empty_schema = pa.schema([])
        self.tx._rpc.api.create_table(self.bucket.name, self.schema.name, self.name, empty_schema, txid=self.tx.txid,
                                        create_imports_table=True)
        log.info("Created imports table for table: %s", self.name)
        return self.imports_table()  # type: ignore[return-value]

    def imports_table(self) -> Optional["Table"]:
        """Get the imports table of this table."""
        self.tx._rpc.features.check_imports_table()
        return Table(name=self.name, schema=self.schema, handle=int(self.handle), _imports_table=True)

    def __getitem__(self, col_name: str):
        """Allow constructing ibis-like column expressions from this table.

        It is useful for constructing expressions for predicate pushdown in `Table.select()` method.
        """
        return self._ibis_table[col_name]


@dataclass
class Projection:
    """VAST semi-sorted projection."""

    name: str
    table: Table
    handle: int
    stats: TableStats

    @property
    def bucket(self):
        """Return bucket."""
        return self.table.schema.bucket

    @property
    def schema(self):
        """Return schema."""
        return self.table.schema

    @property
    def tx(self):
        """Return transaction."""
        return self.table.schema.tx

    def columns(self) -> pa.Schema:
        """Return this projections' columns as an Arrow schema."""
        columns = []
        next_key = 0
        while True:
            curr_columns, next_key, is_truncated, _count, _ = \
                self.tx._rpc.api.list_projection_columns(
                    self.bucket.name, self.schema.name, self.table.name, self.name, txid=self.table.tx.txid, next_key=next_key)
            if not curr_columns:
                break
            columns.extend(curr_columns)
            if not is_truncated:
                break
        self.arrow_schema = pa.schema([(col[0], col[1]) for col in columns])
        return self.arrow_schema

    def rename(self, new_name: str) -> None:
        """Rename this projection."""
        self.tx._rpc.api.alter_projection(self.bucket.name, self.schema.name,
                                                self.table.name, self.name, txid=self.tx.txid, new_name=new_name)
        log.info("Renamed projection from %s to %s ", self.name, new_name)
        self.name = new_name

    def drop(self) -> None:
        """Drop this projection."""
        self.tx._rpc.api.drop_projection(self.bucket.name, self.schema.name, self.table.name,
                                         self.name, txid=self.tx.txid)
        log.info("Dropped projection: %s", self.name)


def _parse_projection_info(projection_info, table: "Table"):
    log.info("Projection info %s", str(projection_info))
    stats = TableStats(num_rows=projection_info.num_rows, size_in_bytes=projection_info.size_in_bytes)
    return Projection(name=projection_info.name, table=table, stats=stats, handle=int(projection_info.handle))


def _parse_bucket_and_object_names(path: str) -> Tuple[str, str]:
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
