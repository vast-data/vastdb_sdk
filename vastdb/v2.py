from dataclasses import dataclass, field
import logging
import os
from typing import Union

import boto3
import botocore
import ibis
import pyarrow as pa

from vastdb.api import VastdbApi, serialize_record_batch, build_query_data_request, parse_query_data_response, TABULAR_INVALID_ROW_ID


log = logging.getLogger(__name__)
INTERNAL_ROW_ID = "$row_id"


class VastException(Exception):
    pass


class NotFoundError(VastException):
    pass


class AccessDeniedError(VastException):
    pass


class InvalidArgumentError(VastException):
    pass


class RPC:
    def __init__(self, access=None, secret=None, endpoint=None):
        if access is None:
            access = os.environ['AWS_ACCESS_KEY_ID']
        if secret is None:
            secret = os.environ['AWS_SECRET_ACCESS_KEY']
        if endpoint is None:
            endpoint = os.environ['AWS_S3_ENDPOINT_URL']

        self.api = VastdbApi(endpoint, access, secret)
        self.s3 = boto3.client('s3',
            aws_access_key_id=access,
            aws_secret_access_key=secret,
            endpoint_url=endpoint)

    def __repr__(self):
        return f'RPC(endpoint={self.api.url}, access={self.api.access_key})'

    def transaction(self):
        return Transaction(self)


def connect(*args, **kw):
    return RPC(*args, **kw)


@dataclass
class Transaction:
    _rpc: RPC
    txid: int = None

    def __enter__(self):
        response = self._rpc.api.begin_transaction()
        self.txid = int(response.headers['tabular-txid'])
        log.debug("opened txid=%016x", self.txid)
        return self

    def __exit__(self, *args):
        if args == (None, None, None):
            log.debug("committing txid=%016x", self.txid)
            self._rpc.api.commit_transaction(self.txid)
        else:
            log.exception("rolling back txid=%016x", self.txid)
            self._rpc.api.rollback_transaction(self.txid)

    def __repr__(self):
        return f'Transaction(id=0x{self.txid:016x})'

    def bucket(self, name: str) -> "Bucket":
        try:
            self._rpc.s3.head_bucket(Bucket=name)
            return Bucket(name, self)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 403:
                raise AccessDeniedError(f"Access is denied to bucket: {name}") from e
            else:
                raise NotFoundError(f"Bucket {name} does not exist") from e


@dataclass
class Bucket:
    name: str
    tx: Transaction

    def create_schema(self, path: str) -> "Schema":
        self.tx._rpc.api.create_schema(self.name, path, txid=self.tx.txid)
        log.info("Created schema: %s", path)
        return self.schema(path)

    def schema(self, path: str) -> "Schema":
        schema = self.schemas(path)
        log.debug("schema: %s", schema)
        if not schema:
            raise NotFoundError(f"Schema '{path}' was not found in bucket: {self.name}")
        assert len(schema) == 1, f"Expected to receive only a single schema, but got: {len(schema)}. ({schema})"
        log.debug("Found schema: %s", schema[0].name)
        return schema[0]

    def schemas(self, schema: str = None) -> ["Schema"]:
        schemas = []
        next_key = 0
        exact_match = bool(schema)
        log.debug("list schemas param: schema=%s, exact_match=%s", schema, exact_match)
        while True:
            bucket_name, curr_schemas, next_key, is_truncated, _ = \
                self.tx._rpc.api.list_schemas(bucket=self.name, next_key=next_key, txid=self.tx.txid,
                                               name_prefix=schema, exact_match=exact_match)
            if not curr_schemas:
                break
            schemas.extend(curr_schemas)
            if not is_truncated:
                break

        return [Schema(name=name, bucket=self) for name, *_ in schemas]


@dataclass
class Schema:
    name: str
    bucket: Bucket

    @property
    def tx(self):
        return self.bucket.tx

    def create_table(self, table_name: str, columns: pa.Schema) -> "Table":
        self.tx._rpc.api.create_table(self.bucket.name, self.name, table_name, columns, txid=self.tx.txid)
        log.info("Created table: %s", table_name)
        return self.table(table_name)

    def table(self, name: str) -> "Table":
        t = self.tables(table_name=name)
        if not t:
            raise NotFoundError(f"Table '{name}' was not found under schema: {self.name}")
        assert len(t) == 1, f"Expected to receive only a single table, but got: {len(t)}. tables: {t}"
        log.debug("Found table: %s", t[0])
        return t[0]

    def tables(self, table_name=None) -> ["Table"]:
        tables = []
        next_key = 0
        name_prefix = table_name if table_name else ""
        exact_match = bool(table_name)
        while True:
            bucket_name, schema_name, curr_tables, next_key, is_truncated, _ = \
                self.tx._rpc.api.list_tables(
                    bucket=self.bucket.name, schema=self.name, next_key=next_key, txid=self.tx.txid,
                    exact_match=exact_match, name_prefix=name_prefix)
            if not curr_tables:
                break
            tables.extend(curr_tables)
            if not is_truncated:
                break

        return [_parse_table_info(table, self) for table in tables]

    def drop(self) -> None:
        self.tx._rpc.api.drop_schema(self.bucket.name, self.name, txid=self.tx.txid)
        log.info("Dropped schema: %s", self.name)

    def rename(self, new_name) -> None:
        self.tx._rpc.api.alter_schema(self.bucket.name, self.name, txid=self.tx.txid, new_name=new_name)
        log.info("Renamed schema: %s to %s", self.name, new_name)
        self.name = new_name


@dataclass
class TableStats:
    num_rows: int
    size: int


@dataclass
class QueryConfig:
    num_sub_splits: int = 4
    num_splits: int = 1
    data_endpoints: [str] = None
    limit_rows_per_sub_split: int = 128 * 1024
    num_row_groups_per_sub_split: int = 8
    use_semi_sorted_projections: bool = True


@dataclass
class Table:
    name: str
    schema: Schema
    handle: int
    stats: TableStats
    properties: dict = None
    arrow_schema: pa.Schema = field(init=False, compare=False)
    _ibis_table: ibis.Schema = field(init=False, compare=False)

    def __post_init__(self):
        self.properties = self.properties or {}
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
        cols = self.tx._rpc.api._list_table_columns(self.bucket.name, self.schema.name, self.name, txid=self.tx.txid)
        self.arrow_schema = pa.schema([(col[0], col[1]) for col in cols])
        return self.arrow_schema

    def import_files(self, files_to_import: [str]) -> None:
        source_files = {}
        for f in files_to_import:
            bucket_name, object_path = _parse_bucket_and_object_names(f)
            source_files[(bucket_name, object_path)] = b''

        self._execute_import(source_files)

    def import_partitioned_files(self, files_and_partitions: {str: pa.RecordBatch}) -> None:
        source_files = {}
        for f, record_batch in files_and_partitions.items():
            bucket_name, object_path = _parse_bucket_and_object_names(f)
            serialized_batch = _serialize_record_batch(record_batch)
            source_files = {(bucket_name, object_path): serialized_batch.to_pybytes()}

        self._execute_import(source_files)

    def _execute_import(self, source_files):
        self.tx._rpc.api.import_data(
            self.bucket.name, self.schema.name, self.name, source_files, txid=self.tx.txid)

    def select(self, columns: [str] = None,
               predicate: ibis.expr.types.BooleanColumn = None,
               config: "QueryConfig" = None,
               *,
               internal_row_id = False) -> pa.RecordBatchReader:
        if config is None:
            config = QueryConfig()

        query_schema = self.arrow_schema
        if internal_row_id:
            queried_fields = [pa.field(INTERNAL_ROW_ID, pa.uint64())]
            queried_fields.extend(column for column in self.arrow_schema)
            query_schema = pa.schema(queried_fields)
            columns.append(INTERNAL_ROW_ID)

        query_data_request = build_query_data_request(
            schema=query_schema,
            predicate=predicate,
            field_names=columns)

        start_row_ids = {i: 0 for i in range(config.num_sub_splits)}
        assert config.num_splits == 1  # TODO()
        split = (0, 1, config.num_row_groups_per_sub_split)
        response_row_id = False
        def batches_iterator():
            while not all(row_id == TABULAR_INVALID_ROW_ID for row_id in start_row_ids.values()):
                response = self.tx._rpc.api.query_data(
                    bucket=self.bucket.name,
                    schema=self.schema.name,
                    table=self.name,
                    params=query_data_request.serialized,
                    split=split,
                    num_sub_splits=config.num_sub_splits,
                    response_row_id=response_row_id,
                    txid=self.tx.txid,
                    limit_rows=config.limit_rows_per_sub_split,
                    sub_split_start_row_ids=start_row_ids.items(),
                    enable_sorted_projections=config.use_semi_sorted_projections)

                pages_iter = parse_query_data_response(
                    conn=response.raw,
                    schema=query_data_request.response_schema,
                    start_row_ids=start_row_ids)

                for page in pages_iter:
                    for batch in page.to_batches():
                        if len(batch) > 0:
                            yield batch

        return pa.RecordBatchReader.from_batches(query_data_request.response_schema.arrow_schema, batches_iterator())

    def insert(self, rows: pa.RecordBatch) -> pa.RecordBatch:
        blob = serialize_record_batch(rows)
        res = self.tx._rpc.api.insert_rows(self.bucket.name, self.schema.name, self.name, record_batch=blob, txid=self.tx.txid)
        reader = pa.ipc.RecordBatchStreamReader(res.raw)
        return reader.read_next_batch()

    def update(self, rows: pa.RecordBatch) -> None:
        blob = serialize_record_batch(rows)
        self.tx._rpc.api.update_rows(self.bucket.name, self.schema.name, self.name, record_batch=blob, txid=self.tx.txid)

    def delete(self, rows: Union[pa.RecordBatch, pa.Table]) -> None:
        delete_rows_rb = pa.record_batch(schema=pa.schema([(INTERNAL_ROW_ID, pa.uint64())]),
                                         data=[rows.to_pydict()[INTERNAL_ROW_ID]])

        blob = serialize_record_batch(delete_rows_rb)
        self.tx._rpc.api.delete_rows(self.bucket.name, self.schema.name, self.name, record_batch=blob, txid=self.tx.txid)

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

    def __getitem__(self, col_name):
        return self._ibis_table[col_name]


def _parse_table_info(table_info, schema: "Schema"):
    stats = TableStats(num_rows=table_info.num_rows, size=table_info.size_in_bytes)
    return Table(name=table_info.name, schema=schema, handle=int(table_info.handle), stats=stats)


def _parse_bucket_and_object_names(path: str) -> (str, str):
    if not path.startswith('/'):
        raise InvalidArgumentError(f"Path {path} must start with a '/'")
    components = path.split(os.path.sep)
    bucket_name = components[1]
    object_path = os.path.sep.join(components[2:])
    return bucket_name, object_path


def _serialize_record_batch(record_batch: pa.RecordBatch) -> pa.lib.Buffer:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, record_batch.schema) as writer:
        writer.write(record_batch)
    return sink.getvalue()


def _parse_endpoint(endpoint):
    if ":" in endpoint:
        endpoint, port = endpoint.split(":")
        port = int(port)
    else:
        port = 80
    log.debug("endpoint: %s, port: %d", endpoint, port)
    return endpoint, port
