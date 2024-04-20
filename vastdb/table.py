import concurrent.futures
import logging
import os
import queue
from dataclasses import dataclass, field
from math import ceil
from threading import Event
from typing import Dict, List, Optional, Tuple, Union

import ibis
import pyarrow as pa

from . import errors, internal_commands, schema

log = logging.getLogger(__name__)


INTERNAL_ROW_ID = "$row_id"
MAX_ROWS_PER_BATCH = 512 * 1024
# for insert we need a smaller limit due to response amplification
# for example insert of 512k uint8 result in 512k*8bytes response since row_ids are uint64
MAX_INSERT_ROWS_PER_PATCH = 512 * 1024

@dataclass
class TableStats:
    num_rows: int
    size_in_bytes: int
    is_external_rowid_alloc: bool = False
    endpoints: Tuple[str, ...] = ()

@dataclass
class QueryConfig:
    num_sub_splits: int = 4
    num_splits: int = 1
    data_endpoints: Optional[List[str]] = None
    limit_rows_per_sub_split: int = 128 * 1024
    num_row_groups_per_sub_split: int = 8
    use_semi_sorted_projections: bool = True
    rows_per_split: int = 4000000
    query_id: str = ""


@dataclass
class ImportConfig:
    import_concurrency: int = 2

class SelectSplitState():
    def __init__(self, query_data_request, table : "Table", split_id : int, config: QueryConfig) -> None:
        self.split_id = split_id
        self.subsplits_state = {i: 0 for i in range(config.num_sub_splits)}
        self.config = config
        self.query_data_request = query_data_request
        self.table = table

    def batches(self, api : internal_commands.VastdbApi):
        while not self.done:
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
                            enable_sorted_projections=self.config.use_semi_sorted_projections)
            pages_iter = internal_commands.parse_query_data_response(
                conn=response.raw,
                schema=self.query_data_request.response_schema,
                start_row_ids=self.subsplits_state,
                parser = self.query_data_request.response_parser)

            for page in pages_iter:
                for batch in page.to_batches():
                    if len(batch) > 0:
                        yield batch


    @property
    def done(self):
        return all(row_id == internal_commands.TABULAR_INVALID_ROW_ID for row_id in self.subsplits_state.values())

@dataclass
class Table:
    name: str
    schema: "schema.Schema"
    handle: int
    stats: TableStats
    arrow_schema: pa.Schema = field(init=False, compare=False)
    _ibis_table: ibis.Schema = field(init=False, compare=False)

    def __post_init__(self):
        self.arrow_schema = self.columns()

        table_path = f'{self.schema.bucket.name}/{self.schema.name}/{self.name}'
        self._ibis_table = ibis.table(ibis.Schema.from_pyarrow(self.arrow_schema), table_path)

    @property
    def tx(self):
        return self.schema.tx

    @property
    def bucket(self):
        return self.schema.bucket

    def __repr__(self):
        return f"{type(self).__name__}(name={self.name})"

    def columns(self) -> pa.Schema:
        fields = []
        next_key = 0
        while True:
            cur_columns, next_key, is_truncated, _count = self.tx._rpc.api.list_columns(
                bucket=self.bucket.name, schema=self.schema.name, table=self.name, next_key=next_key, txid=self.tx.txid)
            fields.extend(cur_columns)
            if not is_truncated:
                break

        self.arrow_schema = pa.schema(fields)
        return self.arrow_schema

    def projection(self, name: str) -> "Projection":
        projs = self.projections(projection_name=name)
        if not projs:
            raise errors.MissingProjection(self.bucket.name, self.schema.name, self.name, name)
        assert len(projs) == 1, f"Expected to receive only a single projection, but got: {len(projs)}. projections: {projs}"
        log.debug("Found projection: %s", projs[0])
        return projs[0]

    def projections(self, projection_name=None) -> List["Projection"]:
        projections = []
        next_key = 0
        name_prefix = projection_name if projection_name else ""
        exact_match = bool(projection_name)
        while True:
            bucket_name, schema_name, table_name, curr_projections, next_key, is_truncated, _ = \
                self.tx._rpc.api.list_projections(
                    bucket=self.bucket.name, schema=self.schema.name, table=self.name, next_key=next_key, txid=self.tx.txid,
                    exact_match=exact_match, name_prefix=name_prefix)
            if not curr_projections:
                break
            projections.extend(curr_projections)
            if not is_truncated:
                break
        return [_parse_projection_info(projection, self) for projection in projections]

    def import_files(self, files_to_import: List[str], config: Optional[ImportConfig] = None) -> None:
        source_files = {}
        for f in files_to_import:
            bucket_name, object_path = _parse_bucket_and_object_names(f)
            source_files[(bucket_name, object_path)] = b''

        self._execute_import(source_files, config=config)

    def import_partitioned_files(self, files_and_partitions: Dict[str, pa.RecordBatch], config: Optional[ImportConfig] = None) -> None:
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

        def import_worker(q, session):
            try:
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
                    session = internal_commands.VastdbApi(endpoint, self.tx._rpc.api.access_key, self.tx._rpc.api.secret_key)
                    futures.append(pool.submit(import_worker, files_queue, session))

                log.debug("Waiting for import workers to finish")
                for future in concurrent.futures.as_completed(futures):
                    future.result()
            finally:
                stop_event.set()
                # ThreadPoolExecutor will be joined at the end of the context

    def get_stats(self) -> TableStats:
        stats_tuple = self.tx._rpc.api.get_table_stats(
            bucket=self.bucket.name, schema=self.schema.name, name=self.name, txid=self.tx.txid)
        return TableStats(**stats_tuple._asdict())

    def select(self, columns: Optional[List[str]] = None,
               predicate: ibis.expr.types.BooleanColumn = None,
               config: Optional[QueryConfig] = None,
               *,
               internal_row_id: bool = False) -> pa.RecordBatchReader:
        if config is None:
            config = QueryConfig()

        # Take a snapshot of enpoints
        stats = self.get_stats()
        endpoints = stats.endpoints if config.data_endpoints is None else config.data_endpoints

        if stats.num_rows > config.rows_per_split and config.num_splits is None:
            config.num_splits = stats.num_rows // config.rows_per_split
        log.debug(f"num_rows={stats.num_rows} rows_per_splits={config.rows_per_split} num_splits={config.num_splits} ")

        if columns is None:
            columns = [f.name for f in self.arrow_schema]

        query_schema = self.arrow_schema
        if internal_row_id:
            queried_fields = [pa.field(INTERNAL_ROW_ID, pa.uint64())]
            queried_fields.extend(column for column in self.arrow_schema)
            query_schema = pa.schema(queried_fields)
            columns.append(INTERNAL_ROW_ID)

        query_data_request = internal_commands.build_query_data_request(
            schema=query_schema,
            predicate=predicate,
            field_names=columns)

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

        def single_endpoint_worker(endpoint : str):
            try:
                host_api = internal_commands.VastdbApi(endpoint=endpoint, access_key=self.tx._rpc.api.access_key, secret_key=self.tx._rpc.api.secret_key)
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

                    for batch in split_state.batches(host_api):
                        check_stop()
                        record_batches_queue.put(batch)
            except StoppedException:
                log.debug("stop signal.", exc_info=True)
                return
            finally:
                # signal that this thread has ended
                log.debug("exiting")
                record_batches_queue.put(None)

        def batches_iterator():
            def propagate_first_exception(futures : List[concurrent.futures.Future], block = False):
                done, not_done = concurrent.futures.wait(futures, None if block else 0, concurrent.futures.FIRST_EXCEPTION)
                for future in done:
                    future.result()
                return not_done

            threads_prefix = "query-data"
            # This is mainly for testing, it helps to identify running threads in runtime.
            if config.query_id:
                threads_prefix = threads_prefix + "-" + config.query_id

            with concurrent.futures.ThreadPoolExecutor(max_workers=len(endpoints), thread_name_prefix=threads_prefix) as tp: # TODO: concurrency == enpoints is just a heuristic
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

    def _combine_chunks(self, col):
        if hasattr(col, "combine_chunks"):
            return col.combine_chunks()
        else:
            return col

    def insert(self, rows: pa.RecordBatch) -> pa.RecordBatch:
        serialized_slices = self.tx._rpc.api._record_batch_slices(rows, MAX_INSERT_ROWS_PER_PATCH)
        row_ids = []
        for slice in serialized_slices:
            res = self.tx._rpc.api.insert_rows(self.bucket.name, self.schema.name, self.name, record_batch=slice,
                                               txid=self.tx.txid)
            (batch,) = pa.RecordBatchStreamReader(res.raw)
            row_ids.append(batch[INTERNAL_ROW_ID])

        return pa.chunked_array(row_ids)

    def update(self, rows: Union[pa.RecordBatch, pa.Table], columns: Optional[List[str]] = None) -> None:
        if columns is not None:
            update_fields = [(INTERNAL_ROW_ID, pa.uint64())]
            update_values = [self._combine_chunks(rows[INTERNAL_ROW_ID])]
            for col in columns:
                update_fields.append(rows.field(col))
                update_values.append(self._combine_chunks(rows[col]))

            update_rows_rb = pa.record_batch(schema=pa.schema(update_fields), data=update_values)
        else:
            update_rows_rb = rows

        serialized_slices = self.tx._rpc.api._record_batch_slices(update_rows_rb, MAX_ROWS_PER_BATCH)
        for slice in serialized_slices:
            self.tx._rpc.api.update_rows(self.bucket.name, self.schema.name, self.name, record_batch=slice,
                                         txid=self.tx.txid)

    def delete(self, rows: Union[pa.RecordBatch, pa.Table]) -> None:
        delete_rows_rb = pa.record_batch(schema=pa.schema([(INTERNAL_ROW_ID, pa.uint64())]),
                                         data=[self._combine_chunks(rows[INTERNAL_ROW_ID])])

        serialized_slices = self.tx._rpc.api._record_batch_slices(delete_rows_rb, MAX_ROWS_PER_BATCH)
        for slice in serialized_slices:
            self.tx._rpc.api.delete_rows(self.bucket.name, self.schema.name, self.name, record_batch=slice,
                                         txid=self.tx.txid)

    def drop(self) -> None:
        self.tx._rpc.api.drop_table(self.bucket.name, self.schema.name, self.name, txid=self.tx.txid)
        log.info("Dropped table: %s", self.name)

    def rename(self, new_name) -> None:
        self.tx._rpc.api.alter_table(
            self.bucket.name, self.schema.name, self.name, txid=self.tx.txid, new_name=new_name)
        log.info("Renamed table from %s to %s ", self.name, new_name)
        self.name = new_name

    def add_column(self, new_column: pa.Schema) -> None:
        self.tx._rpc.api.add_columns(self.bucket.name, self.schema.name, self.name, new_column, txid=self.tx.txid)
        log.info("Added column(s): %s", new_column)
        self.arrow_schema = self.columns()

    def drop_column(self, column_to_drop: pa.Schema) -> None:
        self.tx._rpc.api.drop_columns(self.bucket.name, self.schema.name, self.name, column_to_drop, txid=self.tx.txid)
        log.info("Dropped column(s): %s", column_to_drop)
        self.arrow_schema = self.columns()

    def rename_column(self, current_column_name: str, new_column_name: str) -> None:
        self.tx._rpc.api.alter_column(self.bucket.name, self.schema.name, self.name, name=current_column_name,
                                       new_name=new_column_name, txid=self.tx.txid)
        log.info("Renamed column: %s to %s", current_column_name, new_column_name)
        self.arrow_schema = self.columns()

    def create_projection(self, projection_name: str, sorted_columns: List[str], unsorted_columns: List[str]) -> "Projection":
        columns = [(sorted_column, "Sorted") for sorted_column in sorted_columns] + [(unsorted_column, "Unorted") for unsorted_column in unsorted_columns]
        self.tx._rpc.api.create_projection(self.bucket.name, self.schema.name, self.name, projection_name, columns=columns, txid=self.tx.txid)
        log.info("Created projection: %s", projection_name)
        return self.projection(projection_name)

    def __getitem__(self, col_name):
        return self._ibis_table[col_name]


@dataclass
class Projection:
    name: str
    table: Table
    handle: int
    stats: TableStats

    @property
    def bucket(self):
        return self.table.schema.bucket

    @property
    def schema(self):
        return self.table.schema

    @property
    def tx(self):
        return self.table.schema.tx

    def __repr__(self):
        return f"{type(self).__name__}(name={self.name})"

    def columns(self) -> pa.Schema:
        columns = []
        next_key = 0
        while True:
            curr_columns, next_key, is_truncated, count, _ = \
                self.tx._rpc.api.list_projection_columns(
                    self.bucket.name, self.schema.name, self.table.name, self.name, txid=self.table.tx.txid, next_key=next_key)
            if not curr_columns:
                break
            columns.extend(curr_columns)
            if not is_truncated:
                break
        self.arrow_schema = pa.schema([(col[0], col[1]) for col in columns])
        return self.arrow_schema

    def rename(self, new_name) -> None:
        self.tx._rpc.api.alter_projection(self.bucket.name, self.schema.name,
                                                self.table.name, self.name, txid=self.tx.txid, new_name=new_name)
        log.info("Renamed projection from %s to %s ", self.name, new_name)
        self.name = new_name

    def drop(self) -> None:
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
